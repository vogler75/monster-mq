package at.rocworks.extensions.redfish

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ODataLink(
    @JsonProperty("@odata.id") val odataId: String
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class RedfishStatus(
    @JsonProperty("State") val state: String = "Enabled",
    @JsonProperty("Health") val health: String = "OK",
    @JsonProperty("HealthRollup") val healthRollup: String? = null
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class RedfishServiceRoot(
    @JsonProperty("@odata.context") val odataContext: String = "/redfish/v1/\$metadata#ServiceRoot.ServiceRoot",
    @JsonProperty("@odata.id") val odataId: String = "/redfish/v1",
    @JsonProperty("@odata.type") val odataType: String = "#ServiceRoot.v1_15_0.ServiceRoot",
    @JsonProperty("Id") val id: String = "RootService",
    @JsonProperty("Name") val name: String = "MonsterMQ Redfish Service",
    @JsonProperty("RedfishVersion") val redfishVersion: String = "1.18.0",
    @JsonProperty("UUID") val uuid: String,
    @JsonProperty("Chassis") val chassis: ODataLink = ODataLink("/redfish/v1/Chassis"),
    @JsonProperty("Systems") val systems: ODataLink = ODataLink("/redfish/v1/Systems"),
    @JsonProperty("Managers") val managers: ODataLink = ODataLink("/redfish/v1/Managers"),
    @JsonProperty("TelemetryService") val telemetryService: ODataLink = ODataLink("/redfish/v1/TelemetryService"),
    @JsonProperty("EventService") val eventService: ODataLink = ODataLink("/redfish/v1/EventService"),
    @JsonProperty("JsonSchemas") val jsonSchemas: ODataLink = ODataLink("/redfish/v1/JsonSchemas")
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class RedfishCollection(
    @JsonProperty("@odata.context") val odataContext: String? = null,
    @JsonProperty("@odata.id") val odataId: String,
    @JsonProperty("@odata.type") val odataType: String,
    @JsonProperty("Name") val name: String,
    @JsonProperty("Description") val description: String? = null,
    @JsonProperty("Members@odata.count") val membersCount: Int,
    @JsonProperty("Members") val members: List<ODataLink>
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ChassisLinks(
    @JsonProperty("ComputerSystems") val computerSystems: List<ODataLink>? = null,
    @JsonProperty("ManagedBy") val managedBy: List<ODataLink>? = null
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class RedfishChassis(
    @JsonProperty("@odata.context") val odataContext: String = "/redfish/v1/\$metadata#Chassis.Chassis",
    @JsonProperty("@odata.id") val odataId: String,
    @JsonProperty("@odata.type") val odataType: String = "#Chassis.v1_22_0.Chassis",
    @JsonProperty("Id") val id: String,
    @JsonProperty("Name") val name: String,
    @JsonProperty("ChassisType") val chassisType: String = "Zone",
    @JsonProperty("Manufacturer") val manufacturer: String = "MonsterMQ",
    @JsonProperty("Model") val model: String = "Broker",
    @JsonProperty("Status") val status: RedfishStatus = RedfishStatus(),
    @JsonProperty("Sensors") val sensors: ODataLink,
    @JsonProperty("Thermal") val thermal: ODataLink,
    @JsonProperty("Power") val power: ODataLink,
    @JsonProperty("Links") val links: ChassisLinks? = null
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ThresholdReading(
    @JsonProperty("Reading") val reading: Double
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class SensorThresholds(
    @JsonProperty("UpperCaution") val upperCaution: ThresholdReading? = null,
    @JsonProperty("UpperCritical") val upperCritical: ThresholdReading? = null,
    @JsonProperty("LowerCaution") val lowerCaution: ThresholdReading? = null,
    @JsonProperty("LowerCritical") val lowerCritical: ThresholdReading? = null
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class RedfishSensor(
    @JsonProperty("@odata.context") val odataContext: String = "/redfish/v1/\$metadata#Sensor.Sensor",
    @JsonProperty("@odata.id") val odataId: String,
    @JsonProperty("@odata.type") val odataType: String = "#Sensor.v1_7_0.Sensor",
    @JsonProperty("Id") val id: String,
    @JsonProperty("Name") val name: String,
    @JsonProperty("Reading") val reading: Double?,
    @JsonProperty("ReadingType") val readingType: String?,
    @JsonProperty("ReadingUnits") val readingUnits: String?,
    @JsonProperty("ReadingRangeMin") val readingRangeMin: Double? = null,
    @JsonProperty("ReadingRangeMax") val readingRangeMax: Double? = null,
    @JsonProperty("Accuracy") val accuracy: Double? = null,
    @JsonProperty("Precision") val precision: Int? = null,
    @JsonProperty("Status") val status: RedfishStatus = RedfishStatus(),
    @JsonProperty("Thresholds") val thresholds: SensorThresholds? = null,
    @JsonProperty("Oem") val oem: Map<String, Any?>? = null
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class TemperatureMember(
    @JsonProperty("@odata.id") val odataId: String,
    @JsonProperty("MemberId") val memberId: String,
    @JsonProperty("Name") val name: String,
    @JsonProperty("SensorNumber") val sensorNumber: Int,
    @JsonProperty("ReadingCelsius") val readingCelsius: Double?,
    @JsonProperty("UpperThresholdNonCritical") val upperThresholdNonCritical: Double? = null,
    @JsonProperty("UpperThresholdCritical") val upperThresholdCritical: Double? = null,
    @JsonProperty("LowerThresholdNonCritical") val lowerThresholdNonCritical: Double? = null,
    @JsonProperty("LowerThresholdCritical") val lowerThresholdCritical: Double? = null,
    @JsonProperty("MinReadingRangeTemp") val minReadingRangeTemp: Double? = null,
    @JsonProperty("MaxReadingRangeTemp") val maxReadingRangeTemp: Double? = null,
    @JsonProperty("Status") val status: RedfishStatus = RedfishStatus()
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class FanMember(
    @JsonProperty("@odata.id") val odataId: String,
    @JsonProperty("MemberId") val memberId: String,
    @JsonProperty("Name") val name: String,
    @JsonProperty("Reading") val reading: Double?,
    @JsonProperty("ReadingUnits") val readingUnits: String?,
    @JsonProperty("Status") val status: RedfishStatus = RedfishStatus()
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class RedfishThermal(
    @JsonProperty("@odata.context") val odataContext: String = "/redfish/v1/\$metadata#Thermal.Thermal",
    @JsonProperty("@odata.id") val odataId: String,
    @JsonProperty("@odata.type") val odataType: String = "#Thermal.v1_7_0.Thermal",
    @JsonProperty("Id") val id: String = "Thermal",
    @JsonProperty("Name") val name: String,
    @JsonProperty("Temperatures") val temperatures: List<TemperatureMember> = emptyList(),
    @JsonProperty("Fans") val fans: List<FanMember> = emptyList(),
    @JsonProperty("Status") val status: RedfishStatus = RedfishStatus()
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class PowerControlMember(
    @JsonProperty("@odata.id") val odataId: String,
    @JsonProperty("MemberId") val memberId: String,
    @JsonProperty("Name") val name: String,
    @JsonProperty("PowerConsumedWatts") val powerConsumedWatts: Double?,
    @JsonProperty("Status") val status: RedfishStatus = RedfishStatus()
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class VoltageMember(
    @JsonProperty("@odata.id") val odataId: String,
    @JsonProperty("MemberId") val memberId: String,
    @JsonProperty("Name") val name: String,
    @JsonProperty("ReadingVolts") val readingVolts: Double?,
    @JsonProperty("Status") val status: RedfishStatus = RedfishStatus()
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class RedfishPower(
    @JsonProperty("@odata.context") val odataContext: String = "/redfish/v1/\$metadata#Power.Power",
    @JsonProperty("@odata.id") val odataId: String,
    @JsonProperty("@odata.type") val odataType: String = "#Power.v1_7_0.Power",
    @JsonProperty("Id") val id: String = "Power",
    @JsonProperty("Name") val name: String,
    @JsonProperty("PowerControl") val powerControl: List<PowerControlMember> = emptyList(),
    @JsonProperty("Voltages") val voltages: List<VoltageMember> = emptyList(),
    @JsonProperty("Status") val status: RedfishStatus = RedfishStatus()
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class SystemLinks(
    @JsonProperty("Chassis") val chassis: List<ODataLink>? = null,
    @JsonProperty("ManagedBy") val managedBy: List<ODataLink>? = null
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class RedfishComputerSystem(
    @JsonProperty("@odata.context") val odataContext: String = "/redfish/v1/\$metadata#ComputerSystem.ComputerSystem",
    @JsonProperty("@odata.id") val odataId: String,
    @JsonProperty("@odata.type") val odataType: String = "#ComputerSystem.v1_20_0.ComputerSystem",
    @JsonProperty("Id") val id: String,
    @JsonProperty("Name") val name: String,
    @JsonProperty("SystemType") val systemType: String = "OS",
    @JsonProperty("Manufacturer") val manufacturer: String = "MonsterMQ",
    @JsonProperty("Model") val model: String = "Broker System",
    @JsonProperty("Status") val status: RedfishStatus = RedfishStatus(),
    @JsonProperty("Links") val links: SystemLinks? = null
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class RedfishManager(
    @JsonProperty("@odata.context") val odataContext: String = "/redfish/v1/\$metadata#Manager.Manager",
    @JsonProperty("@odata.id") val odataId: String,
    @JsonProperty("@odata.type") val odataType: String = "#Manager.v1_19_0.Manager",
    @JsonProperty("Id") val id: String,
    @JsonProperty("Name") val name: String = "MonsterMQ Management Service",
    @JsonProperty("ManagerType") val managerType: String = "Service",
    @JsonProperty("FirmwareVersion") val firmwareVersion: String? = null,
    @JsonProperty("Status") val status: RedfishStatus = RedfishStatus()
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class MetricValue(
    @JsonProperty("MetricId") val metricId: String,
    @JsonProperty("MetricValue") val metricValue: String,
    @JsonProperty("Timestamp") val timestamp: String,
    @JsonProperty("MetricProperty") val metricProperty: String? = null
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class RedfishMetricReport(
    @JsonProperty("@odata.context") val odataContext: String = "/redfish/v1/\$metadata#MetricReport.MetricReport",
    @JsonProperty("@odata.id") val odataId: String,
    @JsonProperty("@odata.type") val odataType: String = "#MetricReport.v1_5_0.MetricReport",
    @JsonProperty("Id") val id: String,
    @JsonProperty("Name") val name: String,
    @JsonProperty("Timestamp") val timestamp: String,
    @JsonProperty("MetricValues") val metricValues: List<MetricValue>
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class RedfishTelemetryService(
    @JsonProperty("@odata.context") val odataContext: String = "/redfish/v1/\$metadata#TelemetryService.TelemetryService",
    @JsonProperty("@odata.id") val odataId: String = "/redfish/v1/TelemetryService",
    @JsonProperty("@odata.type") val odataType: String = "#TelemetryService.v1_3_0.TelemetryService",
    @JsonProperty("Id") val id: String = "TelemetryService",
    @JsonProperty("Name") val name: String = "Telemetry Service",
    @JsonProperty("Status") val status: RedfishStatus = RedfishStatus(),
    @JsonProperty("MetricReports") val metricReports: ODataLink = ODataLink("/redfish/v1/TelemetryService/MetricReports")
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ThresholdsConfig(
    val upperCaution: Double? = null,
    val upperCritical: Double? = null,
    val lowerCaution: Double? = null,
    val lowerCritical: Double? = null
) {
    companion object {
        fun fromJsonObject(json: JsonObject?): ThresholdsConfig? {
            if (json == null) return null
            return ThresholdsConfig(
                upperCaution = json.getDouble("upperCaution") ?: json.getDouble("UpperCaution"),
                upperCritical = json.getDouble("upperCritical") ?: json.getDouble("UpperCritical"),
                lowerCaution = json.getDouble("lowerCaution") ?: json.getDouble("LowerCaution"),
                lowerCritical = json.getDouble("lowerCritical") ?: json.getDouble("LowerCritical")
            )
        }
    }
}

@JsonInclude(JsonInclude.Include.NON_NULL)
data class NormalizedSensorRecord(
    val chassisId: String,
    val sensorId: String,
    val name: String,
    val reading: Double,
    val readingType: String,
    val readingUnits: String,
    val rangeMin: Double? = null,
    val rangeMax: Double? = null,
    val state: String = "Enabled",
    val health: String = "OK",
    val thresholds: ThresholdsConfig? = null,
    val sourceTopic: String,
    val timestamp: String,
    var gatewayName: String = "",
    val topicPrefix: String = "redfish"
) {
    fun toMap(): Map<String, Any?> = mapOf(
        "id" to sensorId,
        "sensorId" to sensorId,
        "name" to name,
        "chassisId" to chassisId,
        "topic" to "$topicPrefix/$chassisId/sensors/$sensorId",
        "reading" to reading,
        "readingType" to readingType,
        "readingUnits" to readingUnits,
        "rangeMin" to rangeMin,
        "rangeMax" to rangeMax,
        "state" to state,
        "health" to health,
        "thresholds" to thresholds?.let {
            mapOf(
                "upperCaution" to it.upperCaution,
                "upperCritical" to it.upperCritical,
                "lowerCaution" to it.lowerCaution,
                "lowerCritical" to it.lowerCritical
            )
        },
        "sourceTopic" to sourceTopic,
        "timestamp" to timestamp,
        "lastUpdated" to timestamp,
        "gatewayName" to gatewayName,
        "topicPrefix" to topicPrefix
    )

    fun toJsonObject(): JsonObject = JsonObject(toMap())

    companion object {
        fun fromJsonObject(json: JsonObject): NormalizedSensorRecord {
            return NormalizedSensorRecord(
                chassisId = json.getString("chassisId", "EdgeNode"),
                sensorId = json.getString("sensorId", "sensor"),
                name = json.getString("name", json.getString("sensorId", "sensor")),
                reading = json.getDouble("reading", 0.0),
                readingType = json.getString("readingType", "Temperature"),
                readingUnits = json.getString("readingUnits", "Cel"),
                rangeMin = json.getDouble("rangeMin"),
                rangeMax = json.getDouble("rangeMax"),
                state = json.getString("state", "Enabled"),
                health = json.getString("health", "OK"),
                thresholds = ThresholdsConfig.fromJsonObject(json.getJsonObject("thresholds")),
                sourceTopic = json.getString("sourceTopic", ""),
                timestamp = json.getString("timestamp", Instant.now().toString()),
                gatewayName = json.getString("gatewayName", ""),
                topicPrefix = json.getString("topicPrefix", "redfish")
            )
        }
    }
}

data class GatewayConfig(
    val topicPrefix: String = "redfish",
    val topicFilters: List<String> = listOf("sensors/#"),
    val chassisId: String = "EdgeNode",
    val defaultReadingType: String = "Temperature",
    val defaultReadingUnits: String = "Cel",
    val thresholds: ThresholdsConfig? = null,
    val jsonSchema: Map<String, Any?> = emptyMap()
) {
    companion object {
        fun fromJsonObject(json: JsonObject): GatewayConfig {
            val filters = json.getJsonArray("topicFilters")?.list?.filterIsInstance<String>()
                ?: listOf("sensors/#")
            return GatewayConfig(
                topicPrefix = json.getString("topicPrefix", "redfish").ifBlank { "redfish" },
                topicFilters = filters,
                chassisId = json.getString("chassisId", "EdgeNode").ifBlank { "EdgeNode" },
                defaultReadingType = json.getString("defaultReadingType", "Temperature").ifBlank { "Temperature" },
                defaultReadingUnits = json.getString("defaultReadingUnits", "Cel").ifBlank { "Cel" },
                thresholds = ThresholdsConfig.fromJsonObject(json.getJsonObject("thresholds")),
                jsonSchema = json.getJsonObject("jsonSchema", JsonObject()).map
            )
        }
    }
}

object RedfishUtils {
    fun calculateHealth(reading: Double, thresholds: ThresholdsConfig?): String {
        if (thresholds == null) return "OK"
        if ((thresholds.upperCritical != null && reading >= thresholds.upperCritical) ||
            (thresholds.lowerCritical != null && reading <= thresholds.lowerCritical)) {
            return "Critical"
        }
        if ((thresholds.upperCaution != null && reading >= thresholds.upperCaution) ||
            (thresholds.lowerCaution != null && reading <= thresholds.lowerCaution)) {
            return "Warning"
        }
        return "OK"
    }

    fun formatTimeRFC3339(instant: Instant = Instant.now()): String {
        return DateTimeFormatter.ISO_INSTANT.format(instant.atOffset(ZoneOffset.UTC))
    }
}
