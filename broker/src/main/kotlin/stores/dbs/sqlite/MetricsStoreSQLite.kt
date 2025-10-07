package at.rocworks.stores.sqlite

import at.rocworks.Utils
import at.rocworks.extensions.graphql.BrokerMetrics
import at.rocworks.extensions.graphql.SessionMetrics
import at.rocworks.extensions.graphql.MqttClientMetrics
import at.rocworks.extensions.graphql.OpcUaDeviceMetrics
import at.rocworks.extensions.graphql.KafkaClientMetrics
import at.rocworks.stores.IMetricsStoreAsync
import at.rocworks.stores.MetricsStoreType
import at.rocworks.stores.MetricKind
import io.vertx.core.Vertx
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.Callable

class MetricsStoreSQLite(
    private val name: String,
    private val dbPath: String
) : IMetricsStoreAsync {

    private val logger = Utils.getLogger(this::class.java, name)
    private lateinit var vertx: Vertx
    private lateinit var sqlClient: SQLiteClient

    override fun startStore(vertx: Vertx): Future<Void> {
        this.vertx = vertx
        val promise = Promise.promise<Void>()
        try {
            sqlClient = SQLiteClient(vertx, dbPath)
            val createTableSQL = JsonArray()
                .add(
                    """
                CREATE TABLE IF NOT EXISTS metrics (
                    timestamp TEXT NOT NULL,
                    metric_type TEXT NOT NULL,
                    identifier TEXT NOT NULL,
                    metrics TEXT NOT NULL,
                    PRIMARY KEY (timestamp, metric_type, identifier)
                )
                """.trimIndent()
                )
                .add(
                    """
                CREATE INDEX IF NOT EXISTS metrics_timestamp_idx
                ON metrics (timestamp)
                """.trimIndent()
                )
                .add(
                    """
                CREATE INDEX IF NOT EXISTS metrics_type_identifier_idx
                ON metrics (metric_type, identifier, timestamp)
                """.trimIndent()
                )
            sqlClient.initDatabase(createTableSQL).onComplete { result ->
                if (result.succeeded()) {
                    logger.info("SQLite MetricsStore [$name] initialized successfully")
                    promise.complete()
                } else {
                    logger.severe("Failed to initialize SQLite MetricsStore [$name]: ${result.cause()?.message}")
                    promise.fail(result.cause())
                }
            }
        } catch (e: Exception) {
            logger.severe("Error starting SQLite MetricsStore [$name]: ${e.message}")
            promise.fail(e)
        }
        return promise.future()
    }

    override fun stopStore(): Future<Void> {
        val promise = Promise.promise<Void>()
        try {
            logger.info("SQLite MetricsStore [$name] stopped")
            promise.complete()
        } catch (e: Exception) {
            logger.warning("Error stopping SQLite MetricsStore [$name]: ${e.message}")
            promise.fail(e)
        }
        return promise.future()
    }

    override fun getName(): String = name
    override fun getType(): MetricsStoreType = MetricsStoreType.SQLITE

    override fun storeMetrics(kind: MetricKind, timestamp: Instant, identifier: String, metricsJson: JsonObject): Future<Void> {
        return vertx.executeBlocking<Void>(Callable {
            try {
                val insertSQL = """
                    INSERT OR REPLACE INTO metrics (timestamp, metric_type, identifier, metrics)
                    VALUES (?, ?, ?, ?)
                """.trimIndent()
                val params = JsonArray()
                    .add(timestamp.toString())
                    .add(kind.toDbString())
                    .add(identifier)
                    .add(metricsJson.encode())
                sqlClient.executeUpdate(insertSQL, params)
                    .toCompletionStage().toCompletableFuture().get(5000, java.util.concurrent.TimeUnit.MILLISECONDS)
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

    override fun storeKafkaClientMetrics(timestamp: Instant, clientName: String, metrics: KafkaClientMetrics): Future<Void> =
        storeMetrics(MetricKind.KAFKACLIENT, timestamp, clientName, kafkaClientMetricsToJson(metrics))

    override fun getLatestMetrics(kind: MetricKind, identifier: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<JsonObject> {
        return vertx.executeBlocking<JsonObject>(Callable {
            val (fromTimestamp, toTimestamp) = calculateTimeRange(from, to, lastMinutes)
            if (fromTimestamp == null) throw IllegalArgumentException("Historical query requires time range")
            val sql = if (toTimestamp != null) {
                """
                    SELECT metrics
                    FROM metrics
                    WHERE metric_type = ?
                    AND identifier = ?
                    AND timestamp BETWEEN ? AND ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                """.trimIndent()
            } else {
                """
                    SELECT metrics
                    FROM metrics
                    WHERE metric_type = ?
                    AND identifier = ?
                    AND timestamp >= ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                """.trimIndent()
            }
            val params = JsonArray()
                .add(kind.toDbString())
                .add(identifier)
                .add(fromTimestamp.toString())
            if (toTimestamp != null) params.add(toTimestamp.toString())
            val results = sqlClient.executeQuerySync(sql, params)
            if (results.size() > 0) {
                val row = results.getJsonObject(0)
                JsonObject(row.getString("metrics"))
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
            val (fromTimestamp, toTimestamp) = calculateTimeRange(from, to, lastMinutes)
            if (fromTimestamp == null) throw IllegalArgumentException("Historical query requires time range")
            val sql = if (toTimestamp != null) {
                """
                    SELECT timestamp, metrics
                    FROM metrics
                    WHERE metric_type = ?
                    AND identifier = ?
                    AND timestamp BETWEEN ? AND ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                """.trimIndent()
            } else {
                """
                    SELECT timestamp, metrics
                    FROM metrics
                    WHERE metric_type = ?
                    AND identifier = ?
                    AND timestamp >= ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                """.trimIndent()
            }
            val params = JsonArray()
                .add(kind.toDbString())
                .add(identifier)
                .add(fromTimestamp.toString())
            if (toTimestamp != null) params.add(toTimestamp.toString())
            params.add(limit)
            val results = sqlClient.executeQuerySync(sql, params)
            val list = mutableListOf<Pair<Instant, JsonObject>>()
            for (i in 0 until results.size()) {
                val row = results.getJsonObject(i)
                val ts = Instant.parse(row.getString("timestamp"))
                val json = JsonObject(row.getString("metrics"))
                list.add(ts to json)
            }
            list
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

    override fun purgeOldMetrics(olderThan: Instant): Future<Long> {
        return vertx.executeBlocking<Long>(Callable {
            try {
                val deleteSQL = """
                    DELETE FROM metrics
                    WHERE timestamp < ?
                """.trimIndent()
                val params = JsonArray().add(olderThan.toString())
                val deletedCount = sqlClient.executeUpdate(deleteSQL, params)
                    .toCompletionStage().toCompletableFuture().get(5000, java.util.concurrent.TimeUnit.MILLISECONDS).toLong()
                logger.fine("Purged $deletedCount old metrics records older than $olderThan")
                deletedCount
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
        .put("opcUaIn", m.opcUaIn)
        .put("opcUaOut", m.opcUaOut)
        .put("kafkaClientIn", m.kafkaClientIn)
        .put("kafkaClientOut", m.kafkaClientOut)
        .put("timestamp", m.timestamp)

    private fun sessionMetricsToJson(m: SessionMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("messagesOut", m.messagesOut)
        .put("timestamp", m.timestamp)

    private fun mqttClientMetricsToJson(m: MqttClientMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("messagesOut", m.messagesOut)
        .put("timestamp", m.timestamp)

    private fun kafkaClientMetricsToJson(m: KafkaClientMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("messagesOut", m.messagesOut)
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
        opcUaIn = 0.0,
        opcUaOut = 0.0,
        kafkaClientIn = 0.0,
        kafkaClientOut = 0.0,
        timestamp = at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    ) else BrokerMetrics(
        messagesIn = j.getDouble("messagesIn", 0.0),
        messagesOut = j.getDouble("messagesOut", 0.0),
        nodeSessionCount = j.getInteger("nodeSessionCount", 0),
        clusterSessionCount = j.getInteger("clusterSessionCount", 0),
        queuedMessagesCount = j.getLong("queuedMessagesCount", 0),
        topicIndexSize = j.getInteger("topicIndexSize", 0),
        clientNodeMappingSize = j.getInteger("clientNodeMappingSize", 0),
        topicNodeMappingSize = j.getInteger("topicNodeMappingSize", 0),
        messageBusIn = j.getDouble("messageBusIn", 0.0),
        messageBusOut = j.getDouble("messageBusOut", 0.0),
        mqttClientIn = j.getDouble("mqttClientIn", 0.0),
        mqttClientOut = j.getDouble("mqttClientOut", 0.0),
        opcUaIn = j.getDouble("opcUaIn", 0.0),
        opcUaOut = j.getDouble("opcUaOut", 0.0),
        kafkaClientIn = j.getDouble("kafkaClientIn", 0.0),
        kafkaClientOut = j.getDouble("kafkaClientOut", 0.0),
        timestamp = j.getString("timestamp") ?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    )

    private fun jsonToSessionMetrics(j: JsonObject) = if (j.isEmpty) SessionMetrics(
        0.0,
        0.0,
        at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    ) else SessionMetrics(
        messagesIn = j.getDouble("messagesIn", 0.0),
        messagesOut = j.getDouble("messagesOut", 0.0),
        timestamp = j.getString("timestamp") ?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    )

    private fun jsonToMqttClientMetrics(j: JsonObject) = if (j.isEmpty) MqttClientMetrics(
        0.0,
        0.0,
        at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    ) else MqttClientMetrics(
        messagesIn = j.getDouble("messagesIn", 0.0),
        messagesOut = j.getDouble("messagesOut", 0.0),
        timestamp = j.getString("timestamp") ?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    )

    private fun jsonToKafkaClientMetrics(j: JsonObject) = if (j.isEmpty) KafkaClientMetrics(
        0.0,
        0.0,
        at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    ) else KafkaClientMetrics(
        messagesIn = j.getDouble("messagesIn", 0.0),
        messagesOut = j.getDouble("messagesOut", 0.0),
        timestamp = j.getString("timestamp") ?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    )

    private fun jsonToOpcUaDeviceMetrics(j: JsonObject) = if (j.isEmpty) OpcUaDeviceMetrics(
        0.0,
        0.0,
        at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    ) else OpcUaDeviceMetrics(
        messagesIn = j.getDouble("messagesIn", 0.0),
        messagesOut = j.getDouble("messagesOut", 0.0),
        timestamp = j.getString("timestamp") ?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
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
