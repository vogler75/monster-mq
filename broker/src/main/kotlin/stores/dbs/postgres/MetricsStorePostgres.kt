package at.rocworks.stores.postgres

import at.rocworks.Utils
import at.rocworks.extensions.graphql.BrokerMetrics
import at.rocworks.extensions.graphql.SessionMetrics
import at.rocworks.extensions.graphql.MqttClientMetrics
import at.rocworks.extensions.graphql.OpcUaDeviceMetrics
import at.rocworks.stores.DatabaseConnection
import at.rocworks.stores.IMetricsStoreAsync
import at.rocworks.stores.MetricsStoreType
import at.rocworks.stores.MetricKind
import io.vertx.core.Vertx
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.sql.Connection
import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.Callable

class MetricsStorePostgres(
    private val name: String,
    private val url: String,
    private val username: String,
    private val password: String,
    private val schema: String? = null
) : IMetricsStoreAsync {

    private val logger = Utils.getLogger(this::class.java, name)
    private lateinit var vertx: Vertx

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun init(connection: Connection): Future<Void> {
            val promise = Promise.promise<Void>()
            try {
                connection.autoCommit = false

                // Set PostgreSQL schema if specified
                if (!schema.isNullOrBlank()) {
                    connection.createStatement().use { stmt ->
                        stmt.execute("SET search_path TO \"$schema\", public")
                    }
                }

                // Create metrics table if it doesn't exist
                connection.createStatement().use { statement ->
                    val createTableSQL = """
                        CREATE TABLE IF NOT EXISTS public.metrics (
                            "timestamp" timestamptz NOT NULL,
                            metric_type varchar(30) NOT NULL,
                            identifier varchar(255) NOT NULL,
                            metrics jsonb NOT NULL,
                            CONSTRAINT metrics_pkey PRIMARY KEY ("timestamp", metric_type, identifier)
                        )
                    """.trimIndent()

                    statement.execute(createTableSQL)

                    // Create indexes if they don't exist
                    val createTimestampIndex = """
                        CREATE INDEX IF NOT EXISTS metrics_timestamp_idx
                        ON public.metrics USING btree ("timestamp")
                    """.trimIndent()

                    val createTypeIdentifierIndex = """
                        CREATE INDEX IF NOT EXISTS metrics_type_identifier_idx
                        ON public.metrics USING btree (metric_type, identifier, "timestamp")
                    """.trimIndent()

                    statement.execute(createTimestampIndex)
                    statement.execute(createTypeIdentifierIndex)

                    connection.commit()
                }
                promise.complete()
            } catch (e: Exception) {
                logger.severe("Failed to initialize metrics table: ${e.message}")
                e.printStackTrace()
                promise.fail(e)
            }
            return promise.future()
        }
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

    override fun getType(): MetricsStoreType = MetricsStoreType.POSTGRES

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

    override fun getBrokerMetricsHistory(nodeId: String, from: Instant?, to: Instant?, lastMinutes: Int?, limit: Int): Future<List<Pair<Instant, BrokerMetrics>>> =
        getMetricsHistory(MetricKind.BROKER, nodeId, from, to, lastMinutes, limit).map { list -> list.map { it.first to jsonToBrokerMetrics(it.second) } }

    override fun getOpcUaDeviceMetricsHistory(deviceName: String, from: Instant?, to: Instant?, lastMinutes: Int?, limit: Int) =
        getMetricsHistory(MetricKind.OPCUADEVICE, deviceName, from, to, lastMinutes, limit).map { list -> list.map { it.first to jsonToOpcUaDeviceMetrics(it.second) } }


    override fun getSessionMetricsHistory(clientId: String, from: Instant?, to: Instant?, lastMinutes: Int?, limit: Int): Future<List<Pair<Instant, SessionMetrics>>> =
        getMetricsHistory(MetricKind.SESSION, clientId, from, to, lastMinutes, limit).map { list -> list.map { it.first to jsonToSessionMetrics(it.second) } }

    override fun getMqttClientMetricsHistory(clientName: String, from: Instant?, to: Instant?, lastMinutes: Int?, limit: Int): Future<List<Pair<Instant, MqttClientMetrics>>> =
        getMetricsHistory(MetricKind.MQTTCLIENT, clientName, from, to, lastMinutes, limit).map { list -> list.map { it.first to jsonToMqttClientMetrics(it.second) } }

    override fun getKafkaClientMetricsHistory(clientName: String, from: Instant?, to: Instant?, lastMinutes: Int?, limit: Int) =
        getMetricsHistory(MetricKind.KAFKACLIENT, clientName, from, to, lastMinutes, limit).map { list -> list.map { it.first to jsonToKafkaClientMetrics(it.second) } }


    override fun getBrokerMetricsList(nodeId: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<List<BrokerMetrics>> =
        getBrokerMetricsHistory(nodeId, from, to, lastMinutes, Int.MAX_VALUE).map { history -> history.map { it.second } }

    override fun getOpcUaDeviceMetricsList(deviceName: String, from: Instant?, to: Instant?, lastMinutes: Int?) =
        getOpcUaDeviceMetricsHistory(deviceName, from, to, lastMinutes, Int.MAX_VALUE).map { list -> list.map { it.second } }


    override fun getSessionMetricsList(clientId: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<List<SessionMetrics>> =
        getSessionMetricsHistory(clientId, from, to, lastMinutes, Int.MAX_VALUE).map { history -> history.map { it.second } }

    override fun getMqttClientMetricsList(clientName: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<List<MqttClientMetrics>> =
        getMqttClientMetricsHistory(clientName, from, to, lastMinutes, Int.MAX_VALUE).map { history -> history.map { it.second } }

    override fun getKafkaClientMetricsList(clientName: String, from: Instant?, to: Instant?, lastMinutes: Int?) =
        getKafkaClientMetricsHistory(clientName, from, to, lastMinutes, Int.MAX_VALUE).map { list -> list.map { it.second } }

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
                val connection = db.connection ?: throw IllegalStateException("Database connection not available")

                val deleteSQL = """
                    DELETE FROM public.metrics
                    WHERE "timestamp" < ?
                """.trimIndent()

                connection.prepareStatement(deleteSQL).use { statement ->
                    statement.setTimestamp(1, Timestamp.from(olderThan))
                    val deletedCount = statement.executeUpdate().toLong()
                    connection.commit()
                    deletedCount
                }
            } catch (e: Exception) {
                logger.warning("Error purging old metrics: ${e.message}")
                try {
                    db.connection?.rollback()
                } catch (rollbackE: Exception) {
                    logger.warning("Error rolling back purge transaction: ${rollbackE.message}")
                }
                throw e
            }
        })
    }

    // Generic store method
    override fun storeMetrics(kind: MetricKind, timestamp: Instant, identifier: String, metricsJson: JsonObject): Future<Void> {
        return vertx.executeBlocking<Void>(Callable {
            try {
                val connection = db.connection ?: throw IllegalStateException("Database connection not available")
                val insertSQL = """
                    INSERT INTO public.metrics ("timestamp", metric_type, identifier, metrics)
                    VALUES (?, ?, ?, ?::jsonb)
                    ON CONFLICT ("timestamp", metric_type, identifier) DO UPDATE SET metrics = EXCLUDED.metrics
                """.trimIndent()
                connection.prepareStatement(insertSQL).use { statement ->
                    statement.setTimestamp(1, Timestamp.from(timestamp))
                    statement.setString(2, kind.toDbString())
                    statement.setString(3, identifier)
                    statement.setString(4, metricsJson.encode())
                    statement.executeUpdate()
                }
                connection.commit()
            } catch (e: Exception) {
                logger.warning("Error storing $kind metrics for $identifier: ${e.message}")
                try { db.connection?.rollback() } catch (rollbackE: Exception) { logger.warning("Rollback failure: ${rollbackE.message}") }
                throw e
            }
            null
        })
    }

    override fun getLatestMetrics(kind: MetricKind, identifier: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<JsonObject> {
        return vertx.executeBlocking<JsonObject>(Callable {
            val (fromTs, toTs) = calculateTimeRange(from, to, lastMinutes)
            if (fromTs == null) throw IllegalArgumentException("Historical query requires time range")
            val connection = db.connection ?: throw IllegalStateException("Database connection not available")
            val sql = if (toTs != null) {
                """
                    SELECT metrics
                    FROM public.metrics
                    WHERE metric_type = ? AND identifier = ? AND "timestamp" BETWEEN ? AND ?
                    ORDER BY "timestamp" DESC
                    LIMIT 1
                """.trimIndent()
            } else {
                """
                    SELECT metrics
                    FROM public.metrics
                    WHERE metric_type = ? AND identifier = ? AND "timestamp" >= ?
                    ORDER BY "timestamp" DESC
                    LIMIT 1
                """.trimIndent()
            }
            connection.prepareStatement(sql).use { st ->
                st.setString(1, kind.toDbString())
                st.setString(2, identifier)
                st.setTimestamp(3, Timestamp.from(fromTs))
                if (toTs != null) st.setTimestamp(4, Timestamp.from(toTs))
                val rs = st.executeQuery()
                if (rs.next()) JsonObject(rs.getString("metrics")) else JsonObject()
            }
        })
    }

    override fun getMetricsHistory(kind: MetricKind, identifier: String, from: Instant?, to: Instant?, lastMinutes: Int?, limit: Int): Future<List<Pair<Instant, JsonObject>>> {
        return vertx.executeBlocking<List<Pair<Instant, JsonObject>>>(Callable {
            val (fromTs, toTs) = calculateTimeRange(from, to, lastMinutes)
            if (fromTs == null) throw IllegalArgumentException("Historical query requires time range")
            val connection = db.connection ?: throw IllegalStateException("Database connection not available")
            val sql = if (toTs != null) {
                """
                    SELECT "timestamp", metrics
                    FROM public.metrics
                    WHERE metric_type = ? AND identifier = ? AND "timestamp" BETWEEN ? AND ?
                    ORDER BY "timestamp" DESC
                    LIMIT ?
                """.trimIndent()
            } else {
                """
                    SELECT "timestamp", metrics
                    FROM public.metrics
                    WHERE metric_type = ? AND identifier = ? AND "timestamp" >= ?
                    ORDER BY "timestamp" DESC
                    LIMIT ?
                """.trimIndent()
            }
            connection.prepareStatement(sql).use { st ->
                st.setString(1, kind.toDbString())
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
                    val json = JsonObject(rs.getString("metrics"))
                    list.add(ts to json)
                }
                list
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

    private fun mqttClientMetricsToJson(m: MqttClientMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("messagesOut", m.messagesOut)
        .put("timestamp", m.timestamp)

    private fun jsonToMqttClientMetrics(j: JsonObject) = if (j.isEmpty) MqttClientMetrics(0.0,0.0, at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()) else MqttClientMetrics(
        messagesIn = j.getDouble("messagesIn",0.0),
        messagesOut = j.getDouble("messagesOut",0.0),
        timestamp = j.getString("timestamp")?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    )

    private fun kafkaClientMetricsToJson(m: at.rocworks.extensions.graphql.KafkaClientMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("messagesOut", m.messagesOut)
        .put("timestamp", m.timestamp)

    private fun jsonToKafkaClientMetrics(j: JsonObject) = if (j.isEmpty) at.rocworks.extensions.graphql.KafkaClientMetrics(0.0,0.0, at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()) else at.rocworks.extensions.graphql.KafkaClientMetrics(
        messagesIn = j.getDouble("messagesIn",0.0),
        messagesOut = j.getDouble("messagesOut",0.0),
        timestamp = j.getString("timestamp")?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    )

    private fun winCCOaClientMetricsToJson(m: at.rocworks.extensions.graphql.WinCCOaClientMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("connected", m.connected)
        .put("timestamp", m.timestamp)

    private fun jsonToWinCCOaClientMetrics(j: JsonObject) = if (j.isEmpty) at.rocworks.extensions.graphql.WinCCOaClientMetrics(0.0, false, at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()) else at.rocworks.extensions.graphql.WinCCOaClientMetrics(
        messagesIn = j.getDouble("messagesIn",0.0),
        connected = j.getBoolean("connected", false),
        timestamp = j.getString("timestamp")?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    )

    private fun winCCUaClientMetricsToJson(m: at.rocworks.extensions.graphql.WinCCUaClientMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("connected", m.connected)
        .put("timestamp", m.timestamp)

    private fun jsonToWinCCUaClientMetrics(j: JsonObject) = if (j.isEmpty) at.rocworks.extensions.graphql.WinCCUaClientMetrics(0.0, false, at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()) else at.rocworks.extensions.graphql.WinCCUaClientMetrics(
        messagesIn = j.getDouble("messagesIn",0.0),
        connected = j.getBoolean("connected", false),
        timestamp = j.getString("timestamp")?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    )

    private fun opcUaDeviceMetricsToJson(m: OpcUaDeviceMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("messagesOut", m.messagesOut)
        .put("timestamp", m.timestamp)

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
            from != null -> {
                Pair(from, to)
            }
            else -> Pair(null, null)
        }
    }
}
