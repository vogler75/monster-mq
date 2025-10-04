package at.rocworks.stores.postgres

import at.rocworks.Utils
import at.rocworks.extensions.graphql.BrokerMetrics
import at.rocworks.extensions.graphql.SessionMetrics
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
    private val password: String
) : IMetricsStoreAsync {

    private val logger = Utils.getLogger(this::class.java, name)
    private lateinit var vertx: Vertx

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun init(connection: Connection): Future<Void> {
            val promise = Promise.promise<Void>()
            try {
                connection.autoCommit = false

                // Create metrics table if it doesn't exist
                connection.createStatement().use { statement ->
                    val createTableSQL = """
                        CREATE TABLE IF NOT EXISTS public.metrics (
                            "timestamp" timestamptz NOT NULL,
                            metric_type varchar(10) NOT NULL,
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

    override fun getBrokerMetrics(nodeId: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<BrokerMetrics> =
        getLatestMetrics(MetricKind.BROKER, nodeId, from, to, lastMinutes).map { jsonToBrokerMetrics(it) }

    override fun getSessionMetrics(clientId: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<SessionMetrics> =
        getLatestMetrics(MetricKind.SESSION, clientId, from, to, lastMinutes).map { jsonToSessionMetrics(it) }

    override fun getBrokerMetricsHistory(nodeId: String, from: Instant?, to: Instant?, lastMinutes: Int?, limit: Int): Future<List<Pair<Instant, BrokerMetrics>>> =
        getMetricsHistory(MetricKind.BROKER, nodeId, from, to, lastMinutes, limit).map { list -> list.map { it.first to jsonToBrokerMetrics(it.second) } }


    override fun getSessionMetricsHistory(clientId: String, from: Instant?, to: Instant?, lastMinutes: Int?, limit: Int): Future<List<Pair<Instant, SessionMetrics>>> =
        getMetricsHistory(MetricKind.SESSION, clientId, from, to, lastMinutes, limit).map { list -> list.map { it.first to jsonToSessionMetrics(it.second) } }


    override fun getBrokerMetricsList(nodeId: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<List<BrokerMetrics>> =
        getBrokerMetricsHistory(nodeId, from, to, lastMinutes, Int.MAX_VALUE).map { history -> history.map { it.second } }


    override fun getSessionMetricsList(clientId: String, from: Instant?, to: Instant?, lastMinutes: Int?): Future<List<SessionMetrics>> =
        getSessionMetricsHistory(clientId, from, to, lastMinutes, Int.MAX_VALUE).map { history -> history.map { it.second } }


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
                    statement.setString(2, when(kind){ MetricKind.BROKER->"broker"; MetricKind.SESSION->"session" })
                    statement.setString(3, identifier)
                    statement.setString(4, metricsJson.encode())
                    statement.executeUpdate()
                }
                connection.commit()
            } catch (e: Exception) {
                logger.warning("Error storing ${'$'}kind metrics for ${'$'}identifier: ${'$'}{e.message}")
                try { db.connection?.rollback() } catch (rollbackE: Exception) { logger.warning("Rollback failure: ${'$'}{rollbackE.message}") }
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
                st.setString(1, when(kind){ MetricKind.BROKER->"broker"; MetricKind.SESSION->"session" })
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
                st.setString(1, when(kind){ MetricKind.BROKER->"broker"; MetricKind.SESSION->"session" })
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
        .put("timestamp", m.timestamp)

    private fun sessionMetricsToJson(m: SessionMetrics) = JsonObject()
        .put("messagesIn", m.messagesIn)
        .put("messagesOut", m.messagesOut)
        .put("timestamp", m.timestamp)

    private fun jsonToBrokerMetrics(j: JsonObject) = if (j.isEmpty) BrokerMetrics(0.0,0.0,0,0,0,0,0,0,0.0,0.0, at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()) else BrokerMetrics(
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
        timestamp = j.getString("timestamp")?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
    )

    private fun jsonToSessionMetrics(j: JsonObject) = if (j.isEmpty) SessionMetrics(0.0,0.0, at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()) else SessionMetrics(
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