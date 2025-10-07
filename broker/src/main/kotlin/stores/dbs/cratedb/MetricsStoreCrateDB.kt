package at.rocworks.stores.cratedb

import at.rocworks.Utils
import at.rocworks.extensions.graphql.BrokerMetrics
import at.rocworks.extensions.graphql.MqttClientMetrics
import at.rocworks.extensions.graphql.OpcUaDeviceMetrics
import at.rocworks.extensions.graphql.SessionMetrics
import at.rocworks.stores.DatabaseConnection
import at.rocworks.stores.IMetricsStoreAsync
import at.rocworks.stores.MetricKind
import at.rocworks.stores.MetricsStoreType
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.sql.Connection
import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.Callable

/**
 * CrateDB implementation of the metrics store.
 * Uses a single table with: timestamp, metric_type, identifier, metrics(JSON)
 */
class MetricsStoreCrateDB(
    private val name: String,
    private val url: String,
    private val username: String,
    private val password: String
) : IMetricsStoreAsync {

    private val logger = Utils.getLogger(this::class.java, name)
    private lateinit var vertx: Vertx

    private val tableName = "metrics"

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun init(connection: Connection): Future<Void> {
            val promise = Promise.promise<Void>()
            try {
                connection.autoCommit = false
                createTableIfNotExists(connection)
                connection.commit()
                promise.complete()
            } catch (e: Exception) {
                logger.severe("Failed to initialize metrics table (CrateDB): ${e.message}")
                try { connection.rollback() } catch (_: Exception) {}
                promise.fail(e)
            }
            return promise.future()
        }
    }

    private fun createTableIfNotExists(connection: Connection) {
        // CrateDB: OBJECT(DYNAMIC) for JSON-like structure
        val createTableSql = """
            CREATE TABLE IF NOT EXISTS $tableName (
                "timestamp" TIMESTAMP WITH TIME ZONE NOT NULL,
                metric_type STRING NOT NULL,
                identifier STRING NOT NULL,
                metrics OBJECT(DYNAMIC) NOT NULL,
                PRIMARY KEY ("timestamp", metric_type, identifier)
            )
        """.trimIndent()

        connection.createStatement().use { st ->
            st.execute(createTableSql)
        }
        // Primary key provides implicit index; additional composite index usually not needed for Crate queries.
    }

    override fun startStore(vertx: Vertx): Future<Void> {
        this.vertx = vertx
        val promise = Promise.promise<Void>()
        db.start(vertx, promise)
        return promise.future()
    }

    override fun stopStore(): Future<Void> {
        val promise = Promise.promise<Void>()
        try {
            db.connection?.close()
            promise.complete()
        } catch (e: Exception) {
            promise.fail(e)
        }
        return promise.future()
    }

    override fun getName(): String = name
    override fun getType(): MetricsStoreType = MetricsStoreType.CRATEDB

    // Typed store convenience wrappers
    override fun storeBrokerMetrics(timestamp: Instant, nodeId: String, metrics: BrokerMetrics) =
        storeMetrics(MetricKind.BROKER, timestamp, nodeId, brokerMetricsToJson(metrics))

    override fun storeSessionMetrics(timestamp: Instant, clientId: String, metrics: SessionMetrics) =
        storeMetrics(MetricKind.SESSION, timestamp, clientId, sessionMetricsToJson(metrics))

    override fun storeMqttClientMetrics(timestamp: Instant, clientName: String, metrics: MqttClientMetrics) =
        storeMetrics(MetricKind.MQTTCLIENT, timestamp, clientName, mqttClientMetricsToJson(metrics))

    override fun storeKafkaClientMetrics(timestamp: Instant, clientName: String, metrics: at.rocworks.extensions.graphql.KafkaClientMetrics) =
        storeMetrics(MetricKind.KAFKACLIENT, timestamp, clientName, kafkaClientMetricsToJson(metrics))

    // Typed retrieval wrappers (latest)
    override fun getBrokerMetrics(nodeId: String, from: Instant?, to: Instant?, lastMinutes: Int?) =
        getLatestMetrics(MetricKind.BROKER, nodeId, from, to, lastMinutes).map { jsonToBrokerMetrics(it) }

    override fun getSessionMetrics(clientId: String, from: Instant?, to: Instant?, lastMinutes: Int?) =
        getLatestMetrics(MetricKind.SESSION, clientId, from, to, lastMinutes).map { jsonToSessionMetrics(it) }

    override fun getMqttClientMetrics(clientName: String, from: Instant?, to: Instant?, lastMinutes: Int?) =
        getLatestMetrics(MetricKind.MQTTCLIENT, clientName, from, to, lastMinutes).map { jsonToMqttClientMetrics(it) }

    override fun getKafkaClientMetrics(clientName: String, from: Instant?, to: Instant?, lastMinutes: Int?) =
        getLatestMetrics(MetricKind.KAFKACLIENT, clientName, from, to, lastMinutes).map { jsonToKafkaClientMetrics(it) }

    override fun getOpcUaDeviceMetrics(deviceName: String, from: Instant?, to: Instant?, lastMinutes: Int?) =
        getLatestMetrics(MetricKind.OPCUADEVICE, deviceName, from, to, lastMinutes).map { jsonToOpcUaDeviceMetrics(it) }

    // History wrappers
    override fun getBrokerMetricsHistory(nodeId: String, from: Instant?, to: Instant?, lastMinutes: Int?, limit: Int) =
        getMetricsHistory(MetricKind.BROKER, nodeId, from, to, lastMinutes, limit).map { list -> list.map { it.first to jsonToBrokerMetrics(it.second) } }

    override fun getSessionMetricsHistory(clientId: String, from: Instant?, to: Instant?, lastMinutes: Int?, limit: Int) =
        getMetricsHistory(MetricKind.SESSION, clientId, from, to, lastMinutes, limit).map { list -> list.map { it.first to jsonToSessionMetrics(it.second) } }

    override fun getMqttClientMetricsHistory(clientName: String, from: Instant?, to: Instant?, lastMinutes: Int?, limit: Int) =
        getMetricsHistory(MetricKind.MQTTCLIENT, clientName, from, to, lastMinutes, limit).map { list -> list.map { it.first to jsonToMqttClientMetrics(it.second) } }

    override fun getKafkaClientMetricsHistory(clientName: String, from: Instant?, to: Instant?, lastMinutes: Int?, limit: Int) =
        getMetricsHistory(MetricKind.KAFKACLIENT, clientName, from, to, lastMinutes, limit).map { list -> list.map { it.first to jsonToKafkaClientMetrics(it.second) } }

    override fun getOpcUaDeviceMetricsHistory(deviceName: String, from: Instant?, to: Instant?, lastMinutes: Int?, limit: Int) =
        getMetricsHistory(MetricKind.OPCUADEVICE, deviceName, from, to, lastMinutes, limit).map { list -> list.map { it.first to jsonToOpcUaDeviceMetrics(it.second) } }

    // List wrappers
    override fun getBrokerMetricsList(nodeId: String, from: Instant?, to: Instant?, lastMinutes: Int?) =
        getBrokerMetricsHistory(nodeId, from, to, lastMinutes, Int.MAX_VALUE).map { it.map { pair -> pair.second } }

    override fun getSessionMetricsList(clientId: String, from: Instant?, to: Instant?, lastMinutes: Int?) =
        getSessionMetricsHistory(clientId, from, to, lastMinutes, Int.MAX_VALUE).map { it.map { pair -> pair.second } }

    override fun getMqttClientMetricsList(clientName: String, from: Instant?, to: Instant?, lastMinutes: Int?) =
        getMqttClientMetricsHistory(clientName, from, to, lastMinutes, Int.MAX_VALUE).map { it.map { pair -> pair.second } }

    override fun getKafkaClientMetricsList(clientName: String, from: Instant?, to: Instant?, lastMinutes: Int?) =
        getKafkaClientMetricsHistory(clientName, from, to, lastMinutes, Int.MAX_VALUE).map { it.map { pair -> pair.second } }

    override fun getOpcUaDeviceMetricsList(deviceName: String, from: Instant?, to: Instant?, lastMinutes: Int?) =
        getOpcUaDeviceMetricsHistory(deviceName, from, to, lastMinutes, Int.MAX_VALUE).map { it.map { pair -> pair.second } }

    override fun getWinCCOaClientMetricsList(
        clientName: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<List<at.rocworks.extensions.graphql.WinCCOaClientMetrics>> = Future.succeededFuture(emptyList())

    override fun purgeOldMetrics(olderThan: Instant): Future<Long> {
        return vertx.executeBlocking<Long>(Callable {
            try {
                val connection = db.connection ?: throw IllegalStateException("Database connection not available")
                val deleteSql = "DELETE FROM $tableName WHERE \"timestamp\" < ?"
                connection.prepareStatement(deleteSql).use { st ->
                    st.setTimestamp(1, Timestamp.from(olderThan))
                    val deleted = st.executeUpdate().toLong()
                    deleted
                }
            } catch (e: Exception) {
                logger.warning("Error purging old metrics: ${e.message}")
                throw e
            }
        })
    }

    // Generic API implementation ------------------------------------------------------------
    override fun storeMetrics(kind: MetricKind, timestamp: Instant, identifier: String, metricsJson: JsonObject): Future<Void> {
        return vertx.executeBlocking<Void>(Callable {
            val connection = db.connection ?: throw IllegalStateException("Database connection not available")
            // Upsert (two phase: update first, if none updated then insert)
            val updateSql = "UPDATE $tableName SET metrics = ? WHERE \"timestamp\" = ? AND metric_type = ? AND identifier = ?"
            val kindStr = kind.toDbString()
            var updatedRows = 0
            connection.prepareStatement(updateSql).use { st ->
                st.setObject(1, metricsJson.map) // OBJECT(DYNAMIC) accepts map
                st.setTimestamp(2, Timestamp.from(timestamp))
                st.setString(3, kindStr)
                st.setString(4, identifier)
                updatedRows = st.executeUpdate()
            }
            if (updatedRows == 0) {
                val insertSql = "INSERT INTO $tableName (\"timestamp\", metric_type, identifier, metrics) VALUES (?, ?, ?, ?)"
                connection.prepareStatement(insertSql).use { st ->
                    st.setTimestamp(1, Timestamp.from(timestamp))
                    st.setString(2, kindStr)
                    st.setString(3, identifier)
                    st.setObject(4, metricsJson.map)
                    st.executeUpdate()
                }
            }
            null
        })
    }

    override fun getLatestMetrics(kind: MetricKind, identifier: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<JsonObject> {
        return vertx.executeBlocking<JsonObject>(Callable {
            val (fromTs, toTs) = calculateTimeRange(from, to, lastMinutes)
            if (fromTs == null) throw IllegalArgumentException("Historical query requires time range")
            val connection = db.connection ?: throw IllegalStateException("Database connection not available")
            val kindStr = kind.toDbString()
            val sql = if (toTs != null) {
                """
                    SELECT metrics
                    FROM $tableName
                    WHERE metric_type = ? AND identifier = ? AND "timestamp" BETWEEN ? AND ?
                    ORDER BY "timestamp" DESC
                    LIMIT 1
                """.trimIndent()
            } else {
                """
                    SELECT metrics
                    FROM $tableName
                    WHERE metric_type = ? AND identifier = ? AND "timestamp" >= ?
                    ORDER BY "timestamp" DESC
                    LIMIT 1
                """.trimIndent()
            }
            connection.prepareStatement(sql).use { st ->
                st.setString(1, kindStr)
                st.setString(2, identifier)
                st.setTimestamp(3, Timestamp.from(fromTs))
                if (toTs != null) st.setTimestamp(4, Timestamp.from(toTs))
                val rs = st.executeQuery()
                return@use if (rs.next()) JsonObject(rs.getObject("metrics") as Map<String, Any?>) else JsonObject()
            }
        })
    }

    override fun getMetricsHistory(kind: MetricKind, identifier: String, from: Instant?, to: Instant?, lastMinutes: Int?, limit: Int): Future<List<Pair<Instant, JsonObject>>> {
        return vertx.executeBlocking<List<Pair<Instant, JsonObject>>>(Callable {
            val (fromTs, toTs) = calculateTimeRange(from, to, lastMinutes)
            if (fromTs == null) throw IllegalArgumentException("Historical query requires time range")
            val connection = db.connection ?: throw IllegalStateException("Database connection not available")
            val kindStr = kind.toDbString()
            val sql = if (toTs != null) {
                """
                    SELECT "timestamp", metrics
                    FROM $tableName
                    WHERE metric_type = ? AND identifier = ? AND "timestamp" BETWEEN ? AND ?
                    ORDER BY "timestamp" DESC
                    LIMIT ?
                """.trimIndent()
            } else {
                """
                    SELECT "timestamp", metrics
                    FROM $tableName
                    WHERE metric_type = ? AND identifier = ? AND "timestamp" >= ?
                    ORDER BY "timestamp" DESC
                    LIMIT ?
                """.trimIndent()
            }
            connection.prepareStatement(sql).use { st ->
                st.setString(1, kindStr)
                st.setString(2, identifier)
                st.setTimestamp(3, Timestamp.from(fromTs))
                if (toTs != null) {
                    st.setTimestamp(4, Timestamp.from(toTs))
                    st.setInt(5, limit)
                } else {
                    st.setInt(4, limit)
                }
                val rs = st.executeQuery()
                val list = mutableListOf<Pair<Instant, JsonObject>>()
                while (rs.next()) {
                    val ts = rs.getTimestamp("timestamp").toInstant()
                    @Suppress("UNCHECKED_CAST")
                    val jsonMap = rs.getObject("metrics") as Map<String, Any?>
                    list.add(ts to JsonObject(jsonMap))
                }
                list
            }
        })
    }

    // JSON helpers ---------------------------------------------------------------------------------
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

    private fun jsonToOpcUaDeviceMetrics(j: JsonObject) = if (j.isEmpty) OpcUaDeviceMetrics(0.0,0.0, at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()) else OpcUaDeviceMetrics(
        messagesIn = j.getDouble("messagesIn",0.0),
        messagesOut = j.getDouble("messagesOut",0.0),
        timestamp = j.getString("timestamp")?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    )

    private fun calculateTimeRange(from: Instant?, to: Instant?, lastMinutes: Int?): Pair<Instant?, Instant?> {
        return when {
            lastMinutes != null -> {
                val toTime = Instant.now()
                val fromTime = toTime.minus(lastMinutes.toLong(), ChronoUnit.MINUTES)
                Pair(fromTime, toTime)
            }
            from != null -> Pair(from, to)
            else -> Pair(null, null)
        }
    }
}
