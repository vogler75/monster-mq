package at.rocworks.stores.cratedb

import at.rocworks.Utils
import at.rocworks.extensions.graphql.BrokerMetrics
import at.rocworks.extensions.graphql.SessionMetrics
import at.rocworks.stores.DatabaseConnection
import at.rocworks.stores.IMetricsStoreAsync
import at.rocworks.stores.MetricsStoreType
import io.vertx.core.Vertx
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.sql.Connection
import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.Callable

class MetricsStoreCrateDB(
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
                        CREATE TABLE IF NOT EXISTS "metrics" (
                            "timestamp" TIMESTAMP WITH TIME ZONE NOT NULL,
                            "metric_type" STRING NOT NULL,
                            "identifier" STRING NOT NULL,
                            "metrics" OBJECT NOT NULL,
                            PRIMARY KEY ("timestamp", "metric_type", "identifier")
                        )
                    """.trimIndent()

                    statement.execute(createTableSQL)

                    // CrateDB will automatically create indexes for primary key columns
                    // Additional indexes can be created if needed
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

    override fun getType(): MetricsStoreType = MetricsStoreType.CRATEDB

    override fun storeBrokerMetrics(timestamp: Instant, nodeId: String, metrics: BrokerMetrics): Future<Void> {
        return vertx.executeBlocking<Void>(Callable {
            try {
                val connection = db.connection ?: throw IllegalStateException("Database connection not available")

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

                // CrateDB uses INSERT with ON DUPLICATE KEY UPDATE for upsert
                val insertSQL = """
                    INSERT INTO "metrics" ("timestamp", "metric_type", "identifier", "metrics")
                    VALUES (?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE "metrics" = VALUES("metrics")
                """.trimIndent()

                connection.prepareStatement(insertSQL).use { statement ->
                    statement.setTimestamp(1, Timestamp.from(timestamp))
                    statement.setString(2, "broker")
                    statement.setString(3, nodeId)
                    statement.setObject(4, metricsJson.map)
                    statement.executeUpdate()
                }

                connection.commit()
            } catch (e: Exception) {
                logger.warning("Error storing broker metrics: ${e.message}")
                try {
                    db.connection?.rollback()
                } catch (rollbackE: Exception) {
                    logger.warning("Error rolling back broker metrics transaction: ${rollbackE.message}")
                }
                throw e
            }
            null
        })
    }

    override fun storeSessionMetrics(timestamp: Instant, clientId: String, metrics: SessionMetrics): Future<Void> {
        return vertx.executeBlocking<Void>(Callable {
            try {
                val connection = db.connection ?: throw IllegalStateException("Database connection not available")

                val metricsJson = JsonObject()
                    .put("messagesIn", metrics.messagesIn)
                    .put("messagesOut", metrics.messagesOut)
                    .put("timestamp", metrics.timestamp)

                val insertSQL = """
                    INSERT INTO "metrics" ("timestamp", "metric_type", "identifier", "metrics")
                    VALUES (?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE "metrics" = VALUES("metrics")
                """.trimIndent()

                connection.prepareStatement(insertSQL).use { statement ->
                    statement.setTimestamp(1, Timestamp.from(timestamp))
                    statement.setString(2, "session")
                    statement.setString(3, clientId)
                    statement.setObject(4, metricsJson.map)
                    statement.executeUpdate()
                }

                connection.commit()
            } catch (e: Exception) {
                logger.warning("Error storing session metrics for client $clientId: ${e.message}")
                try {
                    db.connection?.rollback()
                } catch (rollbackE: Exception) {
                    logger.warning("Error rolling back session metrics transaction: ${rollbackE.message}")
                }
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

            val connection = db.connection ?: throw IllegalStateException("Database connection not available")

            val sql = if (toTimestamp != null) {
                """
                    SELECT "metrics"
                    FROM "metrics"
                    WHERE "metric_type" = 'broker'
                    AND "identifier" = ?
                    AND "timestamp" BETWEEN ? AND ?
                    ORDER BY "timestamp" DESC
                    LIMIT 1
                """.trimIndent()
            } else {
                """
                    SELECT "metrics"
                    FROM "metrics"
                    WHERE "metric_type" = 'broker'
                    AND "identifier" = ?
                    AND "timestamp" >= ?
                    ORDER BY "timestamp" DESC
                    LIMIT 1
                """.trimIndent()
            }

            connection.prepareStatement(sql).use { statement ->
                statement.setString(1, nodeId)
                statement.setTimestamp(2, Timestamp.from(fromTimestamp))
                if (toTimestamp != null) {
                    statement.setTimestamp(3, Timestamp.from(toTimestamp))
                }

                val resultSet = statement.executeQuery()
                if (resultSet.next()) {
                    @Suppress("UNCHECKED_CAST")
                    val metricsMap = resultSet.getObject("metrics") as? Map<String, Any> ?: emptyMap()
                    BrokerMetrics(
                        messagesIn = (metricsMap["messagesIn"] as? Number)?.toDouble() ?: 0.0,
                        messagesOut = (metricsMap["messagesOut"] as? Number)?.toDouble() ?: 0.0,
                        nodeSessionCount = (metricsMap["nodeSessionCount"] as? Number)?.toInt() ?: 0,
                        clusterSessionCount = (metricsMap["clusterSessionCount"] as? Number)?.toInt() ?: 0,
                        queuedMessagesCount = (metricsMap["queuedMessagesCount"] as? Number)?.toLong() ?: 0L,
                        topicIndexSize = (metricsMap["topicIndexSize"] as? Number)?.toInt() ?: 0,
                        clientNodeMappingSize = (metricsMap["clientNodeMappingSize"] as? Number)?.toInt() ?: 0,
                        topicNodeMappingSize = (metricsMap["topicNodeMappingSize"] as? Number)?.toInt() ?: 0,
                        messageBusIn = (metricsMap["messageBusIn"] as? Number)?.toDouble() ?: 0.0,
                        messageBusOut = (metricsMap["messageBusOut"] as? Number)?.toDouble() ?: 0.0,
                        timestamp = (metricsMap["timestamp"] as? String) ?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
                    )
                } else {
                    // No historical data found, return zero metrics
                    BrokerMetrics(0.0, 0.0, 0, 0, 0, 0, 0, 0, 0.0, 0.0, at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString())
                }
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

            val connection = db.connection ?: throw IllegalStateException("Database connection not available")

            val sql = if (toTimestamp != null) {
                """
                    SELECT "metrics"
                    FROM "metrics"
                    WHERE "metric_type" = 'session'
                    AND "identifier" = ?
                    AND "timestamp" BETWEEN ? AND ?
                    ORDER BY "timestamp" DESC
                    LIMIT 1
                """.trimIndent()
            } else {
                """
                    SELECT "metrics"
                    FROM "metrics"
                    WHERE "metric_type" = 'session'
                    AND "identifier" = ?
                    AND "timestamp" >= ?
                    ORDER BY "timestamp" DESC
                    LIMIT 1
                """.trimIndent()
            }

            connection.prepareStatement(sql).use { statement ->
                statement.setString(1, clientId)
                statement.setTimestamp(2, Timestamp.from(fromTimestamp))
                if (toTimestamp != null) {
                    statement.setTimestamp(3, Timestamp.from(toTimestamp))
                }

                val resultSet = statement.executeQuery()
                if (resultSet.next()) {
                    @Suppress("UNCHECKED_CAST")
                    val metricsMap = resultSet.getObject("metrics") as? Map<String, Any> ?: emptyMap()
                    SessionMetrics(
                        messagesIn = (metricsMap["messagesIn"] as? Number)?.toDouble() ?: 0.0,
                        messagesOut = (metricsMap["messagesOut"] as? Number)?.toDouble() ?: 0.0,
                        timestamp = (metricsMap["timestamp"] as? String) ?: at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString()
                    )
                } else {
                    // No historical data found, return zero metrics
                    SessionMetrics(0.0, 0.0, at.rocworks.extensions.graphql.TimestampConverter.currentTimeIsoString())
                }
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

            val connection = db.connection ?: throw IllegalStateException("Database connection not available")

            val sql = if (toTimestamp != null) {
                """
                    SELECT "timestamp", "metrics"
                    FROM "metrics"
                    WHERE "metric_type" = 'broker'
                    AND "identifier" = ?
                    AND "timestamp" BETWEEN ? AND ?
                    ORDER BY "timestamp" DESC
                    LIMIT ?
                """.trimIndent()
            } else {
                """
                    SELECT "timestamp", "metrics"
                    FROM "metrics"
                    WHERE "metric_type" = 'broker'
                    AND "identifier" = ?
                    AND "timestamp" >= ?
                    ORDER BY "timestamp" DESC
                    LIMIT ?
                """.trimIndent()
            }

            connection.prepareStatement(sql).use { statement ->
                statement.setString(1, nodeId)
                statement.setTimestamp(2, Timestamp.from(fromTimestamp))
                if (toTimestamp != null) {
                    statement.setTimestamp(3, Timestamp.from(toTimestamp))
                    statement.setInt(4, limit)
                } else {
                    statement.setInt(3, limit)
                }

                val resultSet = statement.executeQuery()
                val results = mutableListOf<Pair<Instant, BrokerMetrics>>()

                while (resultSet.next()) {
                    val timestamp = resultSet.getTimestamp("timestamp").toInstant()
                    @Suppress("UNCHECKED_CAST")
                    val metricsMap = resultSet.getObject("metrics") as? Map<String, Any> ?: emptyMap()
                    val metrics = BrokerMetrics(
                        messagesIn = (metricsMap["messagesIn"] as? Number)?.toDouble() ?: 0.0,
                        messagesOut = (metricsMap["messagesOut"] as? Number)?.toDouble() ?: 0.0,
                        nodeSessionCount = (metricsMap["nodeSessionCount"] as? Number)?.toInt() ?: 0,
                        clusterSessionCount = (metricsMap["clusterSessionCount"] as? Number)?.toInt() ?: 0,
                        queuedMessagesCount = (metricsMap["queuedMessagesCount"] as? Number)?.toLong() ?: 0L,
                        topicIndexSize = (metricsMap["topicIndexSize"] as? Number)?.toInt() ?: 0,
                        clientNodeMappingSize = (metricsMap["clientNodeMappingSize"] as? Number)?.toInt() ?: 0,
                        topicNodeMappingSize = (metricsMap["topicNodeMappingSize"] as? Number)?.toInt() ?: 0,
                        messageBusIn = (metricsMap["messageBusIn"] as? Number)?.toDouble() ?: 0.0,
                        messageBusOut = (metricsMap["messageBusOut"] as? Number)?.toDouble() ?: 0.0,
                        timestamp = at.rocworks.extensions.graphql.TimestampConverter.instantToIsoString(resultSet.getTimestamp("timestamp").toInstant())
                    )
                    results.add(timestamp to metrics)
                }

                results
            }
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

            val connection = db.connection ?: throw IllegalStateException("Database connection not available")

            val sql = if (toTimestamp != null) {
                """
                    SELECT "timestamp", "metrics"
                    FROM "metrics"
                    WHERE "metric_type" = 'session'
                    AND "identifier" = ?
                    AND "timestamp" BETWEEN ? AND ?
                    ORDER BY "timestamp" DESC
                    LIMIT ?
                """.trimIndent()
            } else {
                """
                    SELECT "timestamp", "metrics"
                    FROM "metrics"
                    WHERE "metric_type" = 'session'
                    AND "identifier" = ?
                    AND "timestamp" >= ?
                    ORDER BY "timestamp" DESC
                    LIMIT ?
                """.trimIndent()
            }

            connection.prepareStatement(sql).use { statement ->
                statement.setString(1, clientId)
                statement.setTimestamp(2, Timestamp.from(fromTimestamp))
                if (toTimestamp != null) {
                    statement.setTimestamp(3, Timestamp.from(toTimestamp))
                    statement.setInt(4, limit)
                } else {
                    statement.setInt(3, limit)
                }

                val resultSet = statement.executeQuery()
                val results = mutableListOf<Pair<Instant, SessionMetrics>>()

                while (resultSet.next()) {
                    val timestamp = resultSet.getTimestamp("timestamp").toInstant()
                    @Suppress("UNCHECKED_CAST")
                    val metricsMap = resultSet.getObject("metrics") as? Map<String, Any> ?: emptyMap()
                    val metrics = SessionMetrics(
                        messagesIn = (metricsMap["messagesIn"] as? Number)?.toDouble() ?: 0.0,
                        messagesOut = (metricsMap["messagesOut"] as? Number)?.toDouble() ?: 0.0,
                        timestamp = at.rocworks.extensions.graphql.TimestampConverter.instantToIsoString(resultSet.getTimestamp("timestamp").toInstant())
                    )
                    results.add(timestamp to metrics)
                }

                results
            }
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
                val connection = db.connection ?: throw IllegalStateException("Database connection not available")

                val deleteSQL = """
                    DELETE FROM "metrics"
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