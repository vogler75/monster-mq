package at.rocworks.extensions.redfish

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.Version
import at.rocworks.auth.UserManager
import at.rocworks.extensions.graphql.JwtService
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.handlers.SessionHandler
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.IMessageStore
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.handler.CorsHandler
import java.time.Instant
import java.util.Base64
import java.util.logging.Logger

class RedfishServer(
    private val host: String = "0.0.0.0",
    private val port: Int = 0,
    private val mountPath: String = "/redfish/v1",
    private val defaultChassisId: String = "EdgeNode",
    private val defaultSystemId: String = "edge-node",
    private val defaultManagerId: String = "monstermq",
    private val anonymousEnabled: Boolean = true,
    private val archiveHandler: ArchiveHandler?,
    private val sessionHandler: SessionHandler?,
    private val deviceConfigStore: IDeviceConfigStore?,
    private val userManager: UserManager?
) : AbstractVerticle() {

    private val logger: Logger = Utils.getLogger(this::class.java)
    private var ingestion: RedfishIngestion? = null

    companion object {
        private const val CSDL_METADATA = """<?xml version="1.0" encoding="UTF-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">
  <edmx:DataServices>
    <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="ServiceRoot">
      <EntityType Name="ServiceRoot"/>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>"""
    }

    override fun start(startPromise: Promise<Void>) {
        logger.fine("Initializing Redfish subsystem...")
        initIngestion()

        if (port > 0) {
            val router = Router.router(vertx)
            registerRoutes(router, mountPath)
            registerRoutes(router, "") // Also serve directly at root of standalone listener

            val options = HttpServerOptions().setPort(port).setHost(host)
            vertx.createHttpServer(options)
                .requestHandler(router)
                .listen()
                .onSuccess { server ->
                    logger.info("Redfish Standalone REST Server started on port ${server.actualPort()} (mount: $mountPath)")
                    startPromise.complete()
                }
                .onFailure { err ->
                    logger.severe("Failed to start Redfish REST Server on port $port: ${err.message}")
                    startPromise.fail(err)
                }
        } else {
            startPromise.complete()
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        ingestion?.stop()
        stopPromise.complete()
    }

    fun initIngestion() {
        if (ingestion == null && sessionHandler != null) {
            ingestion = RedfishIngestion(vertx, sessionHandler, archiveHandler, deviceConfigStore)
            ingestion?.start()
        }
    }

    /**
     * Registers all Redfish endpoints onto the given router under the specified base path.
     */
    fun registerRoutes(router: Router, base: String = mountPath) {
        val prefix = base.trimEnd('/')
        val pattern = if (prefix.isEmpty()) "/*" else "$prefix/*"

        logger.info("Registering Redfish routes under '$prefix'")

        // CORS and Redfish Headers
        router.route(pattern).handler(
            CorsHandler.create()
                .addOrigin("*")
                .allowedMethod(io.vertx.core.http.HttpMethod.GET)
                .allowedMethod(io.vertx.core.http.HttpMethod.HEAD)
                .allowedMethod(io.vertx.core.http.HttpMethod.OPTIONS)
                .allowedHeader("Content-Type")
                .allowedHeader("Authorization")
                .allowedHeader("X-Auth-Token")
                .allowedHeader("OData-Version")
        )

        router.route(pattern).handler(BodyHandler.create())

        // Header & Auth Middleware
        router.route(pattern).handler { ctx ->
            ctx.response().putHeader("OData-Version", "4.0")
            if (ctx.request().method() == io.vertx.core.http.HttpMethod.OPTIONS) {
                ctx.response().setStatusCode(204).end()
                return@handler
            }
            authenticateRequest(ctx) {
                ctx.next()
            }
        }

        // ServiceRoot & Metadata
        val rootPath = if (prefix.isEmpty()) "/" else prefix
        router.get(rootPath).handler { ctx -> handleServiceRoot(ctx) }
        router.get("$prefix/").handler { ctx -> handleServiceRoot(ctx) }
        router.get("$prefix/odata").handler { ctx -> handleOData(ctx) }
        router.get("$prefix/\$metadata").handler { ctx -> handleMetadata(ctx) }

        // Chassis & Sensors
        router.get("$prefix/Chassis").handler { ctx -> handleChassisCollection(ctx) }
        router.get("$prefix/Chassis/:chassisId").handler { ctx -> handleChassis(ctx) }
        router.get("$prefix/Chassis/:chassisId/Sensors").handler { ctx -> handleSensorCollection(ctx) }
        router.get("$prefix/Chassis/:chassisId/Sensors/:sensorId").handler { ctx -> handleSensor(ctx) }
        router.get("$prefix/Chassis/:chassisId/Thermal").handler { ctx -> handleThermal(ctx) }
        router.get("$prefix/Chassis/:chassisId/Power").handler { ctx -> handlePower(ctx) }

        // Systems
        router.get("$prefix/Systems").handler { ctx -> handleSystemsCollection(ctx) }
        router.get("$prefix/Systems/:systemId").handler { ctx -> handleSystem(ctx) }

        // Managers
        router.get("$prefix/Managers").handler { ctx -> handleManagersCollection(ctx) }
        router.get("$prefix/Managers/:managerId").handler { ctx -> handleManager(ctx) }

        // Telemetry
        router.get("$prefix/TelemetryService").handler { ctx -> handleTelemetryService(ctx) }
        router.get("$prefix/TelemetryService/MetricReports").handler { ctx -> handleMetricReportsCollection(ctx) }
        router.get("$prefix/TelemetryService/MetricReports/:reportId").handler { ctx -> handleMetricReport(ctx) }

        // EventService
        router.get("$prefix/EventService").handler { ctx -> handleEventService(ctx) }
        router.get("$prefix/EventService/Subscriptions").handler { ctx -> handleEventSubscriptions(ctx) }

        // JSON Schemas
        router.get("$prefix/JsonSchemas").handler { ctx -> handleJsonSchemas(ctx) }
        router.get("$prefix/JsonSchemas/:schemaId").handler { ctx -> handleJsonSchema(ctx) }

        logger.info("Redfish REST routes registered under '$prefix'")
    }

    // ========== Route Handlers ==========

    private fun handleServiceRoot(ctx: RoutingContext) {
        val nodeId = Monster.getClusterNodeId(vertx)
        val root = RedfishServiceRoot(
            uuid = "monstermq-$nodeId"
        )
        writeJson(ctx, 200, root)
    }

    private fun handleOData(ctx: RoutingContext) {
        val doc = mapOf(
            "@odata.context" to "/redfish/v1/\$metadata",
            "value" to listOf(
                mapOf("name" to "ServiceRoot", "kind" to "Singleton", "url" to "/redfish/v1"),
                mapOf("name" to "Chassis", "kind" to "EntitySet", "url" to "/redfish/v1/Chassis"),
                mapOf("name" to "Systems", "kind" to "EntitySet", "url" to "/redfish/v1/Systems"),
                mapOf("name" to "Managers", "kind" to "EntitySet", "url" to "/redfish/v1/Managers"),
                mapOf("name" to "TelemetryService", "kind" to "Singleton", "url" to "/redfish/v1/TelemetryService"),
                mapOf("name" to "EventService", "kind" to "Singleton", "url" to "/redfish/v1/EventService"),
                mapOf("name" to "JsonSchemas", "kind" to "EntitySet", "url" to "/redfish/v1/JsonSchemas")
            )
        )
        writeJson(ctx, 200, doc)
    }

    private fun handleMetadata(ctx: RoutingContext) {
        ctx.response()
            .setStatusCode(200)
            .putHeader("Content-Type", "application/xml;charset=utf-8")
            .end(CSDL_METADATA)
    }

    private fun handleChassisCollection(ctx: RoutingContext) {
        val chassisSet = mutableSetOf(defaultChassisId)
        val records = getAllSensorRecords()
        for (rec in records) {
            if (rec.chassisId.isNotBlank()) {
                chassisSet.add(rec.chassisId)
            }
        }

        val members = chassisSet.sorted().map { ODataLink("/redfish/v1/Chassis/$it") }
        val col = RedfishCollection(
            odataContext = "/redfish/v1/\$metadata#ChassisCollection.ChassisCollection",
            odataId = "/redfish/v1/Chassis",
            odataType = "#ChassisCollection.ChassisCollection",
            name = "Chassis Collection",
            membersCount = members.size,
            members = members
        )
        writeJson(ctx, 200, col)
    }

    private fun handleChassis(ctx: RoutingContext) {
        val chassisId = ctx.pathParam("chassisId")
        if (chassisId.isNullOrBlank()) {
            writeError(ctx, 404, "ResourceMissingAtURI", "Chassis not found")
            return
        }

        val sensors = getSensorsForChassis(chassisId)
        var health = "OK"
        for (rec in sensors) {
            if (rec.health == "Critical") {
                health = "Critical"
                break
            } else if (rec.health == "Warning") {
                health = "Warning"
            }
        }

        val chassis = RedfishChassis(
            odataId = "/redfish/v1/Chassis/$chassisId",
            id = chassisId,
            name = "Chassis $chassisId",
            chassisType = "Zone",
            manufacturer = "MonsterMQ",
            model = "Broker Node",
            status = RedfishStatus(state = "Enabled", health = health),
            sensors = ODataLink("/redfish/v1/Chassis/$chassisId/Sensors"),
            thermal = ODataLink("/redfish/v1/Chassis/$chassisId/Thermal"),
            power = ODataLink("/redfish/v1/Chassis/$chassisId/Power"),
            links = ChassisLinks(
                computerSystems = listOf(ODataLink("/redfish/v1/Systems/$defaultSystemId")),
                managedBy = listOf(ODataLink("/redfish/v1/Managers/$defaultManagerId"))
            )
        )
        writeJson(ctx, 200, chassis)
    }

    private fun handleSensorCollection(ctx: RoutingContext) {
        val chassisId = ctx.pathParam("chassisId") ?: defaultChassisId
        val sensors = getSensorsForChassis(chassisId)
        val members = sensors.map { ODataLink("/redfish/v1/Chassis/$chassisId/Sensors/${it.sensorId}") }
            .sortedBy { it.odataId }

        val col = RedfishCollection(
            odataContext = "/redfish/v1/\$metadata#SensorCollection.SensorCollection",
            odataId = "/redfish/v1/Chassis/$chassisId/Sensors",
            odataType = "#SensorCollection.SensorCollection",
            name = "Sensors for Chassis $chassisId",
            membersCount = members.size,
            members = members
        )
        writeJson(ctx, 200, col)
    }

    private fun handleSensor(ctx: RoutingContext) {
        val chassisId = ctx.pathParam("chassisId") ?: defaultChassisId
        val sensorId = ctx.pathParam("sensorId") ?: ""

        val rec = findSensorRecord(chassisId, sensorId)
        if (rec == null) {
            writeError(ctx, 404, "ResourceMissingAtURI", "Sensor $sensorId not found in Chassis $chassisId")
            return
        }

        val sensorThresholds = rec.thresholds?.let { th ->
            SensorThresholds(
                upperCaution = th.upperCaution?.let { ThresholdReading(it) },
                upperCritical = th.upperCritical?.let { ThresholdReading(it) },
                lowerCaution = th.lowerCaution?.let { ThresholdReading(it) },
                lowerCritical = th.lowerCritical?.let { ThresholdReading(it) }
            )
        }

        val sensor = RedfishSensor(
            odataId = "/redfish/v1/Chassis/$chassisId/Sensors/$sensorId",
            id = sensorId,
            name = rec.name,
            reading = rec.reading,
            readingType = rec.readingType,
            readingUnits = rec.readingUnits,
            readingRangeMin = rec.rangeMin,
            readingRangeMax = rec.rangeMax,
            status = RedfishStatus(state = rec.state, health = rec.health),
            thresholds = sensorThresholds,
            oem = mapOf(
                "MonsterMQ" to mapOf(
                    "Topic" to rec.sourceTopic,
                    "NormalizedTopic" to "${rec.topicPrefix}/$chassisId/sensors/$sensorId",
                    "Timestamp" to rec.timestamp,
                    "Gateway" to rec.gatewayName
                )
            )
        )
        writeJson(ctx, 200, sensor)
    }

    private fun handleThermal(ctx: RoutingContext) {
        val chassisId = ctx.pathParam("chassisId") ?: defaultChassisId
        val sensors = getSensorsForChassis(chassisId)

        val temps = mutableListOf<TemperatureMember>()
        val fans = mutableListOf<FanMember>()
        var overallHealth = "OK"

        var idx = 0
        for (rec in sensors) {
            if (rec.readingType.equals("Temperature", ignoreCase = true)) {
                val temp = TemperatureMember(
                    odataId = "/redfish/v1/Chassis/$chassisId/Thermal#/Temperatures/$idx",
                    memberId = idx.toString(),
                    name = rec.name,
                    sensorNumber = idx + 1,
                    readingCelsius = rec.reading,
                    upperThresholdNonCritical = rec.thresholds?.upperCaution,
                    upperThresholdCritical = rec.thresholds?.upperCritical,
                    lowerThresholdNonCritical = rec.thresholds?.lowerCaution,
                    lowerThresholdCritical = rec.thresholds?.lowerCritical,
                    status = RedfishStatus(state = rec.state, health = rec.health)
                )
                temps.add(temp)
                if (rec.health == "Critical") overallHealth = "Critical"
                else if (rec.health == "Warning" && overallHealth != "Critical") overallHealth = "Warning"
                idx++
            } else if (rec.readingType.equals("AirFlow", ignoreCase = true) || rec.readingUnits.equals("RPM", ignoreCase = true)) {
                fans.add(
                    FanMember(
                        odataId = "/redfish/v1/Chassis/$chassisId/Thermal#/Fans/${fans.size}",
                        memberId = fans.size.toString(),
                        name = rec.name,
                        reading = rec.reading,
                        readingUnits = rec.readingUnits,
                        status = RedfishStatus(state = rec.state, health = rec.health)
                    )
                )
            }
        }

        val thermal = RedfishThermal(
            odataId = "/redfish/v1/Chassis/$chassisId/Thermal",
            name = "Thermal info for $chassisId",
            temperatures = temps,
            fans = fans,
            status = RedfishStatus(state = "Enabled", health = overallHealth)
        )
        writeJson(ctx, 200, thermal)
    }

    private fun handlePower(ctx: RoutingContext) {
        val chassisId = ctx.pathParam("chassisId") ?: defaultChassisId
        val sensors = getSensorsForChassis(chassisId)

        val powerControls = mutableListOf<PowerControlMember>()
        val voltages = mutableListOf<VoltageMember>()
        var overallHealth = "OK"

        for (rec in sensors) {
            if (rec.readingType.equals("Voltage", ignoreCase = true)) {
                voltages.add(
                    VoltageMember(
                        odataId = "/redfish/v1/Chassis/$chassisId/Power#/Voltages/${voltages.size}",
                        memberId = voltages.size.toString(),
                        name = rec.name,
                        readingVolts = rec.reading,
                        status = RedfishStatus(state = rec.state, health = rec.health)
                    )
                )
                if (rec.health == "Critical") overallHealth = "Critical"
                else if (rec.health == "Warning" && overallHealth != "Critical") overallHealth = "Warning"
            } else if (rec.readingType.equals("Power", ignoreCase = true)) {
                powerControls.add(
                    PowerControlMember(
                        odataId = "/redfish/v1/Chassis/$chassisId/Power#/PowerControl/${powerControls.size}",
                        memberId = powerControls.size.toString(),
                        name = rec.name,
                        powerConsumedWatts = rec.reading,
                        status = RedfishStatus(state = rec.state, health = rec.health)
                    )
                )
                if (rec.health == "Critical") overallHealth = "Critical"
                else if (rec.health == "Warning" && overallHealth != "Critical") overallHealth = "Warning"
            }
        }

        val power = RedfishPower(
            odataId = "/redfish/v1/Chassis/$chassisId/Power",
            name = "Power info for $chassisId",
            powerControl = powerControls,
            voltages = voltages,
            status = RedfishStatus(state = "Enabled", health = overallHealth)
        )
        writeJson(ctx, 200, power)
    }

    private fun handleSystemsCollection(ctx: RoutingContext) {
        val col = RedfishCollection(
            odataContext = "/redfish/v1/\$metadata#ComputerSystemCollection.ComputerSystemCollection",
            odataId = "/redfish/v1/Systems",
            odataType = "#ComputerSystemCollection.ComputerSystemCollection",
            name = "Computer Systems Collection",
            membersCount = 1,
            members = listOf(ODataLink("/redfish/v1/Systems/$defaultSystemId"))
        )
        writeJson(ctx, 200, col)
    }

    private fun handleSystem(ctx: RoutingContext) {
        val systemId = ctx.pathParam("systemId") ?: defaultSystemId
        val nodeId = Monster.getClusterNodeId(vertx)
        val system = RedfishComputerSystem(
            odataId = "/redfish/v1/Systems/$systemId",
            id = systemId,
            name = "MonsterMQ System ($nodeId)",
            systemType = "OS",
            manufacturer = "MonsterMQ",
            model = "Broker Host",
            status = RedfishStatus(state = "Enabled", health = "OK"),
            links = SystemLinks(
                chassis = listOf(ODataLink("/redfish/v1/Chassis/$defaultChassisId")),
                managedBy = listOf(ODataLink("/redfish/v1/Managers/$defaultManagerId"))
            )
        )
        writeJson(ctx, 200, system)
    }

    private fun handleManagersCollection(ctx: RoutingContext) {
        val col = RedfishCollection(
            odataContext = "/redfish/v1/\$metadata#ManagerCollection.ManagerCollection",
            odataId = "/redfish/v1/Managers",
            odataType = "#ManagerCollection.ManagerCollection",
            name = "Managers Collection",
            membersCount = 1,
            members = listOf(ODataLink("/redfish/v1/Managers/$defaultManagerId"))
        )
        writeJson(ctx, 200, col)
    }

    private fun handleManager(ctx: RoutingContext) {
        val managerId = ctx.pathParam("managerId") ?: defaultManagerId
        val mgr = RedfishManager(
            odataId = "/redfish/v1/Managers/$managerId",
            id = managerId,
            name = "MonsterMQ Management Service",
            managerType = "Service",
            firmwareVersion = Version.getVersion(),
            status = RedfishStatus(state = "Enabled", health = "OK")
        )
        writeJson(ctx, 200, mgr)
    }

    private fun handleTelemetryService(ctx: RoutingContext) {
        val ts = RedfishTelemetryService()
        writeJson(ctx, 200, ts)
    }

    private fun handleMetricReportsCollection(ctx: RoutingContext) {
        val col = RedfishCollection(
            odataContext = "/redfish/v1/\$metadata#MetricReportCollection.MetricReportCollection",
            odataId = "/redfish/v1/TelemetryService/MetricReports",
            odataType = "#MetricReportCollection.MetricReportCollection",
            name = "Metric Reports Collection",
            membersCount = 1,
            members = listOf(ODataLink("/redfish/v1/TelemetryService/MetricReports/SensorsSummary"))
        )
        writeJson(ctx, 200, col)
    }

    private fun handleMetricReport(ctx: RoutingContext) {
        val reportId = ctx.pathParam("reportId") ?: "SensorsSummary"
        val sensors = getAllSensorRecords()
        val values = sensors.map { rec ->
            MetricValue(
                metricId = rec.sensorId,
                metricValue = rec.reading.toString(),
                timestamp = rec.timestamp,
                metricProperty = "/redfish/v1/Chassis/${rec.chassisId}/Sensors/${rec.sensorId}"
            )
        }

        val report = RedfishMetricReport(
            odataId = "/redfish/v1/TelemetryService/MetricReports/$reportId",
            id = reportId,
            name = "Metric Report $reportId",
            timestamp = RedfishUtils.formatTimeRFC3339(),
            metricValues = values
        )
        writeJson(ctx, 200, report)
    }

    private fun handleEventService(ctx: RoutingContext) {
        val es = mapOf(
            "@odata.context" to "/redfish/v1/\$metadata#EventService.EventService",
            "@odata.id" to "/redfish/v1/EventService",
            "@odata.type" to "#EventService.v1_10_0.EventService",
            "Id" to "EventService",
            "Name" to "Event Service",
            "Status" to RedfishStatus(state = "Enabled", health = "OK"),
            "Subscriptions" to ODataLink("/redfish/v1/EventService/Subscriptions"),
            "ServerSentEventUri" to "/redfish/v1/EventService/SSE"
        )
        writeJson(ctx, 200, es)
    }

    private fun handleEventSubscriptions(ctx: RoutingContext) {
        val col = RedfishCollection(
            odataContext = "/redfish/v1/\$metadata#EventDestinationCollection.EventDestinationCollection",
            odataId = "/redfish/v1/EventService/Subscriptions",
            odataType = "#EventDestinationCollection.EventDestinationCollection",
            name = "Event Subscriptions Collection",
            membersCount = 0,
            members = emptyList()
        )
        writeJson(ctx, 200, col)
    }

    private fun handleJsonSchemas(ctx: RoutingContext) {
        val schemas = listOf("ServiceRoot", "Chassis", "Sensor", "Thermal", "Power", "ComputerSystem", "Manager", "MetricReport")
        val members = schemas.map { ODataLink("/redfish/v1/JsonSchemas/$it") }
        val col = RedfishCollection(
            odataContext = "/redfish/v1/\$metadata#JsonSchemaFileCollection.JsonSchemaFileCollection",
            odataId = "/redfish/v1/JsonSchemas",
            odataType = "#JsonSchemaFileCollection.JsonSchemaFileCollection",
            name = "Json Schema File Collection",
            membersCount = members.size,
            members = members
        )
        writeJson(ctx, 200, col)
    }

    private fun handleJsonSchema(ctx: RoutingContext) {
        val schemaId = ctx.pathParam("schemaId") ?: "Sensor"
        val schema = mapOf(
            "@odata.context" to "/redfish/v1/\$metadata#JsonSchemaFile.JsonSchemaFile",
            "@odata.id" to "/redfish/v1/JsonSchemas/$schemaId",
            "@odata.type" to "#JsonSchemaFile.v1_1_4.JsonSchemaFile",
            "Id" to schemaId,
            "Name" to "$schemaId Schema File",
            "Schema" to "#$schemaId.$schemaId"
        )
        writeJson(ctx, 200, schema)
    }

    // ========== Storage Query Helpers ==========

    fun getAllSensorRecords(): List<NormalizedSensorRecord> {
        val lastVal = getLastValStore() ?: return emptyList()
        val prefixes = getKnownTopicPrefixes()
        val records = mutableListOf<NormalizedSensorRecord>()
        val seen = mutableSetOf<String>()

        for (prefix in prefixes) {
            val pattern = "$prefix/+/sensors/#"
            lastVal.findMatchingMessages(pattern) { bm ->
                try {
                    val json = JsonObject(String(bm.payload, Charsets.UTF_8))
                    val rec = NormalizedSensorRecord.fromJsonObject(json)
                    val key = "${rec.chassisId}/${rec.sensorId}"
                    if (!seen.contains(key)) {
                        seen.add(key)
                        records.add(rec)
                    }
                } catch (e: Exception) {
                    // Ignore non-json or non-sensor messages
                }
                true
            }
        }
        return records
    }

    fun getSensorsForChassis(chassisId: String): List<NormalizedSensorRecord> {
        val lastVal = getLastValStore() ?: return emptyList()
        val prefixes = getKnownTopicPrefixes()
        val records = mutableListOf<NormalizedSensorRecord>()
        val seen = mutableSetOf<String>()

        for (prefix in prefixes) {
            val pattern = "$prefix/$chassisId/sensors/#"
            lastVal.findMatchingMessages(pattern) { bm ->
                try {
                    val json = JsonObject(String(bm.payload, Charsets.UTF_8))
                    val rec = NormalizedSensorRecord.fromJsonObject(json)
                    if (!seen.contains(rec.sensorId)) {
                        seen.add(rec.sensorId)
                        records.add(rec)
                    }
                } catch (e: Exception) {
                    // Ignore
                }
                true
            }
        }
        return records
    }

    fun findSensorRecord(chassisId: String, sensorId: String): NormalizedSensorRecord? {
        val lastVal = getLastValStore() ?: return null
        val prefixes = getKnownTopicPrefixes()

        for (prefix in prefixes) {
            val topic = "$prefix/$chassisId/sensors/$sensorId"
            val bm = lastVal.get(topic)
            if (bm != null && bm.payload.isNotEmpty()) {
                try {
                    val json = JsonObject(String(bm.payload, Charsets.UTF_8))
                    return NormalizedSensorRecord.fromJsonObject(json)
                } catch (e: Exception) {
                    // Continue search
                }
            }
        }

        // Fallback search in case chassis was wildcarded
        var found: NormalizedSensorRecord? = null
        val pattern = "+/$chassisId/sensors/$sensorId"
        lastVal.findMatchingMessages(pattern) { bm ->
            try {
                val json = JsonObject(String(bm.payload, Charsets.UTF_8))
                found = NormalizedSensorRecord.fromJsonObject(json)
                false // stop iteration
            } catch (e: Exception) {
                true
            }
        }
        return found
    }

    /**
     * Used by GraphQL query resolver redfishLiveSensors
     */
    fun getLiveSensors(chassisId: String?): List<Map<String, Any?>> {
        val records = if (!chassisId.isNullOrBlank()) {
            getSensorsForChassis(chassisId)
        } else {
            getAllSensorRecords()
        }
        return records.map { it.toMap() }
    }

    private fun getKnownTopicPrefixes(): List<String> {
        val set = mutableSetOf("redfish")
        ingestion?.getGateways()?.values?.forEach { gw ->
            if (gw.topicPrefix.isNotBlank()) set.add(gw.topicPrefix)
        }
        return set.toList()
    }

    private fun getLastValStore(): IMessageStore? {
        return ingestion?.getLastValStore()
            ?: archiveHandler?.getDeployedArchiveGroups()?.get("Default")?.lastValStore
            ?: archiveHandler?.getDeployedArchiveGroups()?.values?.firstOrNull { it.lastValStore != null }?.lastValStore
    }

    // ========== Utilities & Auth ==========

    private fun writeJson(ctx: RoutingContext, status: Int, data: Any) {
        val jsonStr = try {
            Json.encode(data)
        } catch (e: Exception) {
            JsonObject.mapFrom(data).encode()
        }

        ctx.response()
            .setStatusCode(status)
            .putHeader("Content-Type", "application/json;charset=utf-8")
            .end(jsonStr)
    }

    private fun writeError(ctx: RoutingContext, status: Int, code: String, message: String) {
        val err = mapOf(
            "error" to mapOf(
                "code" to "Base.1.0.$code",
                "message" to message
            )
        )
        writeJson(ctx, status, err)
    }

    private fun authenticateRequest(ctx: RoutingContext, onSuccess: () -> Unit) {
        if (anonymousEnabled) {
            onSuccess()
            return
        }
        if (userManager == null || !userManager.isUserManagementEnabled()) {
            onSuccess()
            return
        }

        val authHeader = ctx.request().getHeader("Authorization")
        if (authHeader == null) {
            ctx.response().setStatusCode(401)
                .putHeader("Content-Type", "application/json;charset=utf-8")
                .putHeader("WWW-Authenticate", "Basic realm=\"MonsterMQ Redfish\"")
                .end(JsonObject().put("error", mapOf("code" to "Base.1.0.GeneralError", "message" to "Authentication required")).encode())
            return
        }

        if (authHeader.startsWith("Basic ", ignoreCase = true)) {
            val base64Credentials = authHeader.substring(6).trim()
            val credentials = try {
                String(Base64.getDecoder().decode(base64Credentials), Charsets.UTF_8).split(":", limit = 2)
            } catch (e: Exception) {
                null
            }
            if (credentials != null && credentials.size == 2) {
                val (user, pass) = credentials
                userManager.authenticate(user, pass).onComplete { ar ->
                    if (ar.succeeded() && ar.result()?.enabled == true) {
                        onSuccess()
                    } else {
                        ctx.response().setStatusCode(401)
                            .putHeader("Content-Type", "application/json;charset=utf-8")
                            .putHeader("WWW-Authenticate", "Basic realm=\"MonsterMQ Redfish\"")
                            .end(JsonObject().put("error", mapOf("code" to "Base.1.0.GeneralError", "message" to "Invalid credentials")).encode())
                    }
                }
                return
            }
        } else if (authHeader.startsWith("Bearer ", ignoreCase = true)) {
            val token = authHeader.substring(7).trim()
            val claims = JwtService.validateToken(token)
            if (claims != null) {
                onSuccess()
                return
            }
        }

        ctx.response().setStatusCode(403)
            .putHeader("Content-Type", "application/json;charset=utf-8")
            .end(JsonObject().put("error", mapOf("code" to "Base.1.0.AccessDenied", "message" to "Access denied")).encode())
    }
}
