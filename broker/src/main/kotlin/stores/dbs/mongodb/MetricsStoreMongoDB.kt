package at.rocworks.stores.mongodb

import at.rocworks.Utils
import at.rocworks.extensions.graphql.BrokerMetrics
import at.rocworks.extensions.graphql.SessionMetrics
import at.rocworks.extensions.graphql.MqttClientMetrics
import at.rocworks.extensions.graphql.OpcUaDeviceMetrics
import at.rocworks.stores.IMetricsStoreAsync
import at.rocworks.stores.MetricsStoreType
import at.rocworks.stores.MetricKind
import io.vertx.core.json.JsonObject
import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.WriteConcern
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.*
import org.bson.conversions.Bson
import io.vertx.core.Vertx
import io.vertx.core.Future
import io.vertx.core.Promise
import org.bson.Document
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit

class MetricsStoreMongoDB(
    private val name: String,
    private val connectionString: String,
    private val databaseName: String
) : IMetricsStoreAsync {

    private val logger = Utils.getLogger(this::class.java, name)
    private val collectionName = "metrics"
    private lateinit var vertx: Vertx

    private lateinit var mongoClient: MongoClient
    private lateinit var database: MongoDatabase
    private lateinit var collection: MongoCollection<Document>

    fun start(vertx: Vertx, startPromise: Promise<Void>) {
        try {
            val clientSettings = MongoClientSettings.builder()
                .applyConnectionString(ConnectionString(connectionString))
                .writeConcern(WriteConcern.MAJORITY.withWTimeout(5000, TimeUnit.MILLISECONDS))
                .build()

            mongoClient = MongoClients.create(clientSettings)
            database = mongoClient.getDatabase(databaseName)
            collection = database.getCollection(collectionName)

            vertx.executeBlocking<Void>(Callable {
                try {
                    val typeIdentifierTimestampIndex = Indexes.compoundIndex(
                        Indexes.ascending("metric_type"),
                        Indexes.ascending("identifier"),
                        Indexes.descending("timestamp")
                    )
                    val timestampIndex = Indexes.ascending("timestamp")
                    collection.createIndexes(listOf(
                        IndexModel(typeIdentifierTimestampIndex),
                        IndexModel(timestampIndex)
                    ))
                    logger.info("MongoDB metrics store initialized successfully")
                } catch (e: Exception) {
                    logger.warning("Failed to create indexes: ${e.message}")
                    throw e
                }
                null
            }).onComplete { result ->
                if (result.succeeded()) {
                    startPromise.complete()
                } else {
                    startPromise.fail(result.cause())
                }
            }
        } catch (e: Exception) {
            logger.severe("Failed to initialize MongoDB metrics store: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun startStore(vertx: Vertx): Future<Void> {
        this.vertx = vertx
        val promise = Promise.promise<Void>()
        start(vertx, promise)
        return promise.future()
    }

    override fun stopStore(): Future<Void> {
        val promise = Promise.promise<Void>()
        try {
            if (::mongoClient.isInitialized) {
                mongoClient.close()
            }
            promise.complete()
        } catch (e: Exception) {
            promise.fail(e)
        }
        return promise.future()
    }

    override fun getName(): String = name

    override fun getType(): MetricsStoreType = MetricsStoreType.MONGODB

    private fun kindToString(kind: MetricKind) = kind.toDbString()

    // Generic store method
    override fun storeMetrics(kind: MetricKind, timestamp: Instant, identifier: String, metricsJson: JsonObject): Future<Void> {
        return vertx.executeBlocking<Void>(Callable {
            try {
                val metricsDoc = Document()
                metricsJson.forEach { (k,v) -> metricsDoc.append(k, v) }
                val document = Document()
                    .append("timestamp", Date.from(timestamp))
                    .append("metric_type", kindToString(kind))
                    .append("identifier", identifier)
                    .append("metrics", metricsDoc)
                val filter = Filters.and(
                    Filters.eq("timestamp", Date.from(timestamp)),
                    Filters.eq("metric_type", kindToString(kind)),
                    Filters.eq("identifier", identifier)
                )
                collection.replaceOne(filter, document, ReplaceOptions().upsert(true))
            } catch (e: Exception) {
                logger.warning("Error storing $kind metrics for $identifier: ${e.message}")
                throw e
            }
            null
        })
    }

    override fun storeBrokerMetrics(timestamp: Instant, nodeId: String, metrics: BrokerMetrics): Future<Void> =
        storeMetrics(MetricKind.BROKER, timestamp, nodeId, brokerMetricsToJson(metrics))

    override fun storeSessionMetrics(timestamp: Instant, clientId: String, metrics: SessionMetrics): Future<Void> =
        storeMetrics(MetricKind.SESSION, timestamp, clientId, sessionMetricsToJson(metrics))

    override fun storeMqttClientMetrics(timestamp: Instant, clientName: String, metrics: MqttClientMetrics): Future<Void> =
        storeMetrics(MetricKind.MQTTCLIENT, timestamp, clientName, mqttClientMetricsToJson(metrics))

    override fun storeWinCCOaClientMetrics(timestamp: Instant, clientName: String, metrics: at.rocworks.extensions.graphql.WinCCOaClientMetrics): Future<Void> =
        storeMetrics(MetricKind.WINCCOACLIENT, timestamp, clientName, winCCOaClientMetricsToJson(metrics))

    override fun storeWinCCUaClientMetrics(timestamp: Instant, clientName: String, metrics: at.rocworks.extensions.graphql.WinCCUaClientMetrics): Future<Void> =
        storeMetrics(MetricKind.WINCCUACLIENT, timestamp, clientName, winCCUaClientMetricsToJson(metrics))

    override fun storeKafkaClientMetrics(timestamp: Instant, clientName: String, metrics: at.rocworks.extensions.graphql.KafkaClientMetrics): Future<Void> =
        storeMetrics(MetricKind.KAFKACLIENT, timestamp, clientName, kafkaClientMetricsToJson(metrics))

    override fun getLatestMetrics(kind: MetricKind, identifier: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<JsonObject> {
        return vertx.executeBlocking<JsonObject>(Callable {
            val (fromTime, toTime) = calculateTimeRange(from, to, lastMinutes)
            if (fromTime == null) throw IllegalArgumentException("Historical query requires time range")
            val filterList = mutableListOf<Bson>()
            filterList.addAll(listOf(
                Filters.eq("metric_type", kindToString(kind)),
                Filters.eq("identifier", identifier),
                Filters.gte("timestamp", Date.from(fromTime))
            ))
            if (toTime != null) filterList.add(Filters.lte("timestamp", Date.from(toTime)))
            val filter = Filters.and(filterList)
            val document = collection.find(filter).sort(Sorts.descending("timestamp")).limit(1).firstOrNull()
            if (document != null) {
                val metricsDoc = document.get("metrics", Document::class.java)
                JsonObject(metricsDoc.entries.associate { it.key to it.value })
            } else JsonObject()
        })
    }

    override fun getBrokerMetrics(nodeId: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<BrokerMetrics> =
        getLatestMetrics(MetricKind.BROKER, nodeId, from, to, lastMinutes).map { jsonToBrokerMetrics(it) }

    override fun getOpcUaDeviceMetrics(deviceName: String, from: Instant?, to: Instant?, lastMinutes: Int?) =
        getLatestMetrics(MetricKind.OPCUADEVICE, deviceName, from, to, lastMinutes).map { jsonToOpcUaDeviceMetrics(it) }

    override fun getSessionMetrics(clientId: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<SessionMetrics> =
        getLatestMetrics(MetricKind.SESSION, clientId, from, to, lastMinutes).map { jsonToSessionMetrics(it) }

    override fun getMqttClientMetrics(clientName: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<MqttClientMetrics> =
        getLatestMetrics(MetricKind.MQTTCLIENT, clientName, from, to, lastMinutes).map { jsonToMqttClientMetrics(it) }

    override fun getKafkaClientMetrics(clientName: String, from: Instant?, to: Instant?, lastMinutes: Int?) =
        getLatestMetrics(MetricKind.KAFKACLIENT, clientName, from, to, lastMinutes).map { jsonToKafkaClientMetrics(it) }

    override fun getMetricsHistory(kind: MetricKind, identifier: String, from: Instant?, to: Instant?, lastMinutes: Int?, limit: Int): Future<List<Pair<Instant, JsonObject>>> {
        return vertx.executeBlocking<List<Pair<Instant, JsonObject>>>(Callable {
            val (fromTime, toTime) = calculateTimeRange(from, to, lastMinutes)
            if (fromTime == null) throw IllegalArgumentException("Historical query requires time range")
            val filterList = mutableListOf<Bson>()
            filterList.addAll(listOf(
                Filters.eq("metric_type", kindToString(kind)),
                Filters.eq("identifier", identifier),
                Filters.gte("timestamp", Date.from(fromTime))
            ))
            if (toTime != null) filterList.add(Filters.lte("timestamp", Date.from(toTime)))
            val filter = Filters.and(filterList)
            val documents = collection.find(filter).sort(Sorts.descending("timestamp")).limit(limit).toList()
            documents.map { document ->
                val ts = document.getDate("timestamp").toInstant()
                val metricsDoc = document.get("metrics", Document::class.java)
                val json = JsonObject(metricsDoc.entries.associate { it.key to it.value })
                ts to json
            }
        })
    }

    override fun getBrokerMetricsHistory(
        nodeId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int
    ): Future<List<Pair<Instant, BrokerMetrics>>> =
        getMetricsHistory(MetricKind.BROKER, nodeId, from, to, lastMinutes, limit).map { list -> list.map { it.first to jsonToBrokerMetrics(it.second) } }

    override fun getOpcUaDeviceMetricsHistory(
        deviceName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int
    ) = getMetricsHistory(MetricKind.OPCUADEVICE, deviceName, from, to, lastMinutes, limit).map { list -> list.map { it.first to jsonToOpcUaDeviceMetrics(it.second) } }

    override fun getSessionMetricsHistory(
        clientId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int
    ): Future<List<Pair<Instant, SessionMetrics>>> =
        getMetricsHistory(MetricKind.SESSION, clientId, from, to, lastMinutes, limit).map { list -> list.map { it.first to jsonToSessionMetrics(it.second) } }

    override fun getMqttClientMetricsHistory(
        clientName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int
    ): Future<List<Pair<Instant, MqttClientMetrics>>> =
        getMetricsHistory(MetricKind.MQTTCLIENT, clientName, from, to, lastMinutes, limit).map { list -> list.map { it.first to jsonToMqttClientMetrics(it.second) } }

    override fun getKafkaClientMetricsHistory(
        clientName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int
    ) = getMetricsHistory(MetricKind.KAFKACLIENT, clientName, from, to, lastMinutes, limit).map { list -> list.map { it.first to jsonToKafkaClientMetrics(it.second) } }

    override fun getBrokerMetricsList(
        nodeId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<List<BrokerMetrics>> =
        getBrokerMetricsHistory(nodeId, from, to, lastMinutes, Int.MAX_VALUE).map { list -> list.map { it.second } }

    override fun getOpcUaDeviceMetricsList(
        deviceName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ) = getOpcUaDeviceMetricsHistory(deviceName, from, to, lastMinutes, Int.MAX_VALUE).map { list -> list.map { it.second } }

    override fun getSessionMetricsList(
        clientId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<List<SessionMetrics>> =
        getSessionMetricsHistory(clientId, from, to, lastMinutes, Int.MAX_VALUE).map { list -> list.map { it.second } }

    override fun getMqttClientMetricsList(
        clientName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<List<MqttClientMetrics>> =
        getMqttClientMetricsHistory(clientName, from, to, lastMinutes, Int.MAX_VALUE).map { list -> list.map { it.second } }

    override fun getKafkaClientMetricsList(
        clientName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ) = getKafkaClientMetricsHistory(clientName, from, to, lastMinutes, Int.MAX_VALUE).map { list -> list.map { it.second } }

    override fun getWinCCOaClientMetricsList(
        clientName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<List<at.rocworks.extensions.graphql.WinCCOaClientMetrics>> =
        getMetricsHistory(MetricKind.WINCCOACLIENT, clientName, from, to, lastMinutes, Int.MAX_VALUE).map { list -> list.map { jsonToWinCCOaClientMetrics(it.second) } }

    override fun getWinCCUaClientMetricsList(
        clientName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<List<at.rocworks.extensions.graphql.WinCCUaClientMetrics>> =
        getMetricsHistory(MetricKind.WINCCUACLIENT, clientName, from, to, lastMinutes, Int.MAX_VALUE).map { list -> list.map { jsonToWinCCUaClientMetrics(it.second) } }

    override fun purgeOldMetrics(olderThan: Instant): Future<Long> {
        return vertx.executeBlocking<Long>(Callable {
            try {
                val filter = Filters.lt("timestamp", Date.from(olderThan))
                val result = collection.deleteMany(filter)
                result.deletedCount
            } catch (e: Exception) {
                logger.warning("Error purging old metrics: ${e.message}")
                throw e
            }
        })
    }

    private fun brokerMetricsToJson(m: BrokerMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("messagesOut", m.messagesOut)
        .put("nodeSessionCount", m.nodeSessionCount)
        .put("clusterSessionCount", m.clusterSessionCount)
        .put("queuedMessagesCount", m.queuedMessagesCount)
        .put("topicIndexSize", m.topicIndexSize)
        .put("clientNodeMappingSize", m.clientNodeMappingSize)
        .put("topicNodeMappingSize", m.topicNodeMappingSize)
        .put("messageBusIn", m.messageBusIn)
        .put("messageBusOut", m.messageBusOut)
        .put("mqttClientIn", m.mqttClientIn)
        .put("mqttClientOut", m.mqttClientOut)
        .put("opcUaClientIn", m.opcUaClientIn)
        .put("opcUaClientOut", m.opcUaClientOut)
        .put("kafkaClientIn", m.kafkaClientIn)
        .put("kafkaClientOut", m.kafkaClientOut)
        .put("winCCOaClientIn", m.winCCOaClientIn)
        .put("winCCUaClientIn", m.winCCUaClientIn)
        .put("timestamp", m.timestamp)

    private fun sessionMetricsToJson(m: SessionMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("messagesOut", m.messagesOut)
        .put("timestamp", m.timestamp)

    private fun mqttClientMetricsToJson(m: MqttClientMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("messagesOut", m.messagesOut)
        .put("timestamp", m.timestamp)

    private fun kafkaClientMetricsToJson(m: at.rocworks.extensions.graphql.KafkaClientMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("messagesOut", m.messagesOut)
        .put("timestamp", m.timestamp)

    private fun winCCOaClientMetricsToJson(m: at.rocworks.extensions.graphql.WinCCOaClientMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("timestamp", m.timestamp)

    private fun winCCUaClientMetricsToJson(m: at.rocworks.extensions.graphql.WinCCUaClientMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("connected", m.connected)
        .put("timestamp", m.timestamp)

    private fun opcUaDeviceMetricsToJson(m: OpcUaDeviceMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("messagesOut", m.messagesOut)
        .put("timestamp", m.timestamp)

    private fun jsonToBrokerMetrics(j: JsonObject) = if (j.isEmpty) BrokerMetrics(
        messagesIn = 0.0,
        messagesOut = 0.0,
        nodeSessionCount = 0,
        clusterSessionCount = 0,
        queuedMessagesCount = 0,
        topicIndexSize = 0,
        clientNodeMappingSize = 0,
        topicNodeMappingSize = 0,
        messageBusIn = 0.0,
        messageBusOut = 0.0,
        mqttClientIn = 0.0,
        mqttClientOut = 0.0,
        opcUaClientIn = 0.0,
        opcUaClientOut = 0.0,
        kafkaClientIn = 0.0,
        kafkaClientOut = 0.0,
        winCCOaClientIn = 0.0,
        winCCUaClientIn = 0.0,
        timestamp = at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    ) else BrokerMetrics(
        messagesIn = j.getDouble("messagesIn",0.0),
        messagesOut = j.getDouble("messagesOut",0.0),
        nodeSessionCount = j.getInteger("nodeSessionCount",0),
        clusterSessionCount = j.getInteger("clusterSessionCount",0),
        queuedMessagesCount = j.getLong("queuedMessagesCount",0),
        topicIndexSize = j.getInteger("topicIndexSize",0),
        clientNodeMappingSize = j.getInteger("clientNodeMappingSize",0),
        topicNodeMappingSize = j.getInteger("topicNodeMappingSize",0),
        messageBusIn = j.getDouble("messageBusIn",0.0),
        messageBusOut = j.getDouble("messageBusOut",0.0),
        mqttClientIn = j.getDouble("mqttClientIn",0.0),
        mqttClientOut = j.getDouble("mqttClientOut",0.0),
        opcUaClientIn = j.getDouble("opcUaClientIn",0.0),
        opcUaClientOut = j.getDouble("opcUaClientOut",0.0),
        kafkaClientIn = j.getDouble("kafkaClientIn",0.0),
        kafkaClientOut = j.getDouble("kafkaClientOut",0.0),
        winCCOaClientIn = j.getDouble("winCCOaClientIn",0.0),
        winCCUaClientIn = j.getDouble("winCCUaClientIn",0.0),
        timestamp = j.getString("timestamp")?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    )

    private fun jsonToSessionMetrics(j: JsonObject) = if (j.isEmpty) SessionMetrics(0.0,0.0, at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()) else SessionMetrics(
        messagesIn = j.getDouble("messagesIn",0.0),
        messagesOut = j.getDouble("messagesOut",0.0),
        timestamp = j.getString("timestamp")?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    )

    private fun jsonToMqttClientMetrics(j: JsonObject) = if (j.isEmpty) MqttClientMetrics(0.0,0.0, at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()) else MqttClientMetrics(
        messagesIn = j.getDouble("messagesIn",0.0),
        messagesOut = j.getDouble("messagesOut",0.0),
        timestamp = j.getString("timestamp")?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    )

    private fun jsonToKafkaClientMetrics(j: JsonObject) = if (j.isEmpty) at.rocworks.extensions.graphql.KafkaClientMetrics(0.0,0.0, at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()) else at.rocworks.extensions.graphql.KafkaClientMetrics(
        messagesIn = j.getDouble("messagesIn",0.0),
        messagesOut = j.getDouble("messagesOut",0.0),
        timestamp = j.getString("timestamp")?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    )

    private fun jsonToWinCCOaClientMetrics(j: JsonObject) = if (j.isEmpty) at.rocworks.extensions.graphql.WinCCOaClientMetrics(0.0, at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()) else at.rocworks.extensions.graphql.WinCCOaClientMetrics(
        messagesIn = j.getDouble("messagesIn",0.0),
        timestamp = j.getString("timestamp")?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    )

    private fun jsonToWinCCUaClientMetrics(j: JsonObject) = if (j.isEmpty) at.rocworks.extensions.graphql.WinCCUaClientMetrics(0.0, false, at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()) else at.rocworks.extensions.graphql.WinCCUaClientMetrics(
        messagesIn = j.getDouble("messagesIn",0.0),
        connected = j.getBoolean("connected", false),
        timestamp = j.getString("timestamp")?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    )

    private fun jsonToOpcUaDeviceMetrics(j: JsonObject) = if (j.isEmpty) OpcUaDeviceMetrics(0.0,0.0, at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()) else OpcUaDeviceMetrics(
        messagesIn = j.getDouble("messagesIn",0.0),
        messagesOut = j.getDouble("messagesOut",0.0),
        timestamp = j.getString("timestamp")?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    )

    private fun calculateTimeRange(from: Instant?, to: Instant?, lastMinutes: Int?): Pair<Instant?, Instant?> = when {
        lastMinutes != null -> {
            val toTime = Instant.now()
            val fromTime = toTime.minus(lastMinutes.toLong(), ChronoUnit.MINUTES)
            Pair(fromTime, toTime)
        }
        from != null -> Pair(from, to)
        else -> Pair(null, null)
    }
}
