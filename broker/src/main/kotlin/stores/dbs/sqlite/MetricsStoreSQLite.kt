package at.rocworks.stores.sqlite

import at.rocworks.Utils
import at.rocworks.extensions.graphql.BrokerMetrics
import at.rocworks.extensions.graphql.SessionMetrics
import at.rocworks.stores.IMetricsStoreAsync
import at.rocworks.stores.MetricsStoreType
import io.vertx.core.Vertx
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.Callable

/**
 * SQLite implementation of IMetricsStoreAsync for broker and session metrics storage
 * Uses SQLiteVerticle for database operations via event bus communication
 */
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
            // Initialize SQLiteClient - assumes SQLiteVerticle is already deployed
            sqlClient = SQLiteClient(vertx, dbPath)

            // Create metrics table
            val createTableSQL = JsonArray()
                .add("""
                CREATE TABLE IF NOT EXISTS metrics (
                    timestamp TEXT NOT NULL,
                    metric_type TEXT NOT NULL,
                    identifier TEXT NOT NULL,
                    metrics TEXT NOT NULL,
                    PRIMARY KEY (timestamp, metric_type, identifier)
                )
                """.trimIndent())
                .add("""
                CREATE INDEX IF NOT EXISTS metrics_timestamp_idx
                ON metrics (timestamp)
                """.trimIndent())
                .add("""
                CREATE INDEX IF NOT EXISTS metrics_type_identifier_idx
                ON metrics (metric_type, identifier, timestamp)
                """.trimIndent())

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

    override fun storeBrokerMetrics(timestamp: Instant, nodeId: String, metrics: BrokerMetrics): Future<Void> {
        return vertx.executeBlocking<Void>(Callable {
            try {
                val metricsJson = JsonObject()
                    .put("messagesIn", metrics.messagesIn)
                    .put("messagesOut", metrics.messagesOut)
                    .put("nodeSessionCount", metrics.nodeSessionCount)
                    .put("clusterSessionCount", metrics.clusterSessionCount)
                    .put("queuedMessagesCount", metrics.queuedMessagesCount)
                    .put("topicIndexSize", metrics.topicIndexSize)
                    .put("clientNodeMappingSize", metrics.clientNodeMappingSize)
                    .put("topicNodeMappingSize", metrics.topicNodeMappingSize)
                    .put("messageBusIn", metrics.messageBusIn)
                    .put("messageBusOut", metrics.messageBusOut)
                    .put("timestamp", metrics.timestamp)

                val insertSQL = """
                    INSERT OR REPLACE INTO metrics (timestamp, metric_type, identifier, metrics)
                    VALUES (?, ?, ?, ?)
                """.trimIndent()

                val params = JsonArray()
                    .add(timestamp.toString())
                    .add("broker")
                    .add(nodeId)
                    .add(metricsJson.encode())

                val updateResult = sqlClient.executeUpdate(insertSQL, params)
                    .toCompletionStage().toCompletableFuture().get(5000, java.util.concurrent.TimeUnit.MILLISECONDS)

            } catch (e: Exception) {
                logger.warning("Error storing broker metrics: ${e.message}")
                throw e
            }
            null
        })
    }

    override fun storeSessionMetrics(timestamp: Instant, clientId: String, metrics: SessionMetrics): Future<Void> {
        return vertx.executeBlocking<Void>(Callable {
            try {
                val metricsJson = JsonObject()
                    .put("messagesIn", metrics.messagesIn)
                    .put("messagesOut", metrics.messagesOut)
                    .put("timestamp", metrics.timestamp)

                val insertSQL = """
                    INSERT OR REPLACE INTO metrics (timestamp, metric_type, identifier, metrics)
                    VALUES (?, ?, ?, ?)
                """.trimIndent()

                val params = JsonArray()
                    .add(timestamp.toString())
                    .add("session")
                    .add(clientId)
                    .add(metricsJson.encode())

                val updateResult = sqlClient.executeUpdate(insertSQL, params)
                    .toCompletionStage().toCompletableFuture().get(5000, java.util.concurrent.TimeUnit.MILLISECONDS)

            } catch (e: Exception) {
                logger.warning("Error storing session metrics for client $clientId: ${e.message}")
                throw e
            }
            null
        })
    }

    override fun getBrokerMetrics(
        nodeId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<BrokerMetrics> {
        return vertx.executeBlocking<BrokerMetrics>(Callable {
            val (fromTimestamp, toTimestamp) = calculateTimeRange(from, to, lastMinutes)

            if (fromTimestamp == null) {
                throw IllegalArgumentException("Historical query requires time range")
            }

            val sql = if (toTimestamp != null) {
                """
                    SELECT metrics
                    FROM metrics
                    WHERE metric_type = 'broker'
                    AND identifier = ?
                    AND timestamp BETWEEN ? AND ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                """.trimIndent()
            } else {
                """
                    SELECT metrics
                    FROM metrics
                    WHERE metric_type = 'broker'
                    AND identifier = ?
                    AND timestamp >= ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                """.trimIndent()
            }

            val params = JsonArray().add(nodeId).add(fromTimestamp.toString())
            if (toTimestamp != null) {
                params.add(toTimestamp.toString())
            }

            val results = sqlClient.executeQuerySync(sql, params)
            if (results.size() > 0) {
                val row = results.getJsonObject(0)
                val metricsJson = JsonObject(row.getString("metrics"))
                BrokerMetrics(
                    messagesIn = metricsJson.getLong("messagesIn", 0L),
                    messagesOut = metricsJson.getLong("messagesOut", 0L),
                    nodeSessionCount = metricsJson.getInteger("nodeSessionCount", 0),
                    clusterSessionCount = metricsJson.getInteger("clusterSessionCount", 0),
                    queuedMessagesCount = metricsJson.getLong("queuedMessagesCount", 0L),
                    topicIndexSize = metricsJson.getInteger("topicIndexSize", 0),
                    clientNodeMappingSize = metricsJson.getInteger("clientNodeMappingSize", 0),
                    topicNodeMappingSize = metricsJson.getInteger("topicNodeMappingSize", 0),
                    messageBusIn = metricsJson.getLong("messageBusIn", 0L),
                    messageBusOut = metricsJson.getLong("messageBusOut", 0L),
                    timestamp = metricsJson.getString("timestamp") ?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
                )
            } else {
                // No historical data found, return zero metrics
                BrokerMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString())
            }
        })
    }

    override fun getSessionMetrics(
        clientId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<SessionMetrics> {
        return vertx.executeBlocking<SessionMetrics>(Callable {
            val (fromTimestamp, toTimestamp) = calculateTimeRange(from, to, lastMinutes)

            if (fromTimestamp == null) {
                throw IllegalArgumentException("Historical query requires time range")
            }

            val sql = if (toTimestamp != null) {
                """
                    SELECT metrics
                    FROM metrics
                    WHERE metric_type = 'session'
                    AND identifier = ?
                    AND timestamp BETWEEN ? AND ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                """.trimIndent()
            } else {
                """
                    SELECT metrics
                    FROM metrics
                    WHERE metric_type = 'session'
                    AND identifier = ?
                    AND timestamp >= ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                """.trimIndent()
            }

            val params = JsonArray().add(clientId).add(fromTimestamp.toString())
            if (toTimestamp != null) {
                params.add(toTimestamp.toString())
            }

            val results = sqlClient.executeQuerySync(sql, params)
            if (results.size() > 0) {
                val row = results.getJsonObject(0)
                val metricsJson = JsonObject(row.getString("metrics"))
                SessionMetrics(
                    messagesIn = metricsJson.getLong("messagesIn", 0L),
                    messagesOut = metricsJson.getLong("messagesOut", 0L),
                    timestamp = metricsJson.getString("timestamp") ?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
                )
            } else {
                // No historical data found, return zero metrics
                SessionMetrics(0, 0, at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString())
            }
        })
    }

    override fun getBrokerMetricsHistory(
        nodeId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int
    ): Future<List<Pair<Instant, BrokerMetrics>>> {
        return vertx.executeBlocking<List<Pair<Instant, BrokerMetrics>>>(Callable {
            val (fromTimestamp, toTimestamp) = calculateTimeRange(from, to, lastMinutes)

            if (fromTimestamp == null) {
                throw IllegalArgumentException("Historical query requires time range")
            }

            val sql = if (toTimestamp != null) {
                """
                    SELECT timestamp, metrics
                    FROM metrics
                    WHERE metric_type = 'broker'
                    AND identifier = ?
                    AND timestamp BETWEEN ? AND ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                """.trimIndent()
            } else {
                """
                    SELECT timestamp, metrics
                    FROM metrics
                    WHERE metric_type = 'broker'
                    AND identifier = ?
                    AND timestamp >= ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                """.trimIndent()
            }

            val params = JsonArray().add(nodeId).add(fromTimestamp.toString())
            if (toTimestamp != null) {
                params.add(toTimestamp.toString())
            }
            params.add(limit)

            val results = sqlClient.executeQuerySync(sql, params)
            val resultList = mutableListOf<Pair<Instant, BrokerMetrics>>()

            for (i in 0 until results.size()) {
                val row = results.getJsonObject(i)
                val timestamp = Instant.parse(row.getString("timestamp"))
                val metricsJson = JsonObject(row.getString("metrics"))
                val metrics = BrokerMetrics(
                    messagesIn = metricsJson.getLong("messagesIn", 0L),
                    messagesOut = metricsJson.getLong("messagesOut", 0L),
                    nodeSessionCount = metricsJson.getInteger("nodeSessionCount", 0),
                    clusterSessionCount = metricsJson.getInteger("clusterSessionCount", 0),
                    queuedMessagesCount = metricsJson.getLong("queuedMessagesCount", 0L),
                    topicIndexSize = metricsJson.getInteger("topicIndexSize", 0),
                    clientNodeMappingSize = metricsJson.getInteger("clientNodeMappingSize", 0),
                    topicNodeMappingSize = metricsJson.getInteger("topicNodeMappingSize", 0),
                    messageBusIn = metricsJson.getLong("messageBusIn", 0L),
                    messageBusOut = metricsJson.getLong("messageBusOut", 0L),
                    timestamp = at.rocworks.extensions.graphql.TimestampConverter.instantToIsoString(timestamp)
                )
                resultList.add(timestamp to metrics)
            }

            resultList
        })
    }

    override fun getSessionMetricsHistory(
        clientId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?,
        limit: Int
    ): Future<List<Pair<Instant, SessionMetrics>>> {
        return vertx.executeBlocking<List<Pair<Instant, SessionMetrics>>>(Callable {
            val (fromTimestamp, toTimestamp) = calculateTimeRange(from, to, lastMinutes)

            if (fromTimestamp == null) {
                throw IllegalArgumentException("Historical query requires time range")
            }

            val sql = if (toTimestamp != null) {
                """
                    SELECT timestamp, metrics
                    FROM metrics
                    WHERE metric_type = 'session'
                    AND identifier = ?
                    AND timestamp BETWEEN ? AND ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                """.trimIndent()
            } else {
                """
                    SELECT timestamp, metrics
                    FROM metrics
                    WHERE metric_type = 'session'
                    AND identifier = ?
                    AND timestamp >= ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                """.trimIndent()
            }

            val params = JsonArray().add(clientId).add(fromTimestamp.toString())
            if (toTimestamp != null) {
                params.add(toTimestamp.toString())
            }
            params.add(limit)

            val results = sqlClient.executeQuerySync(sql, params)
            val resultList = mutableListOf<Pair<Instant, SessionMetrics>>()

            for (i in 0 until results.size()) {
                val row = results.getJsonObject(i)
                val timestamp = Instant.parse(row.getString("timestamp"))
                val metricsJson = JsonObject(row.getString("metrics"))
                val metrics = SessionMetrics(
                    messagesIn = metricsJson.getLong("messagesIn", 0L),
                    messagesOut = metricsJson.getLong("messagesOut", 0L),
                    timestamp = at.rocworks.extensions.graphql.TimestampConverter.instantToIsoString(timestamp)
                )
                resultList.add(timestamp to metrics)
            }

            resultList
        })
    }

    override fun getBrokerMetricsList(
        nodeId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<List<BrokerMetrics>> {
        return getBrokerMetricsHistory(nodeId, from, to, lastMinutes, Int.MAX_VALUE).map { history ->
            history.map { it.second }
        }
    }

    override fun getSessionMetricsList(
        clientId: String,
        from: Instant?,
        to: Instant?,
        lastMinutes: Int?
    ): Future<List<SessionMetrics>> {
        return getSessionMetricsHistory(clientId, from, to, lastMinutes, Int.MAX_VALUE).map { history ->
            history.map { it.second }
        }
    }

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