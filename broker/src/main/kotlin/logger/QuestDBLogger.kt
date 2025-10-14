package at.rocworks.logger

import io.vertx.core.Future
import io.vertx.core.Promise
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

/**
 * QuestDB implementation of JDBC Logger
 * Optimized for time-series data with fast ingestion
 */
class QuestDBLogger : JDBCLoggerBase() {

    private var connection: Connection? = null

    override fun connect(): Future<Void> {
        val promise = Promise.promise<Void>()

        vertx.executeBlocking<Void>({
            try {
                logger.info("Connecting to QuestDB: ${cfg.jdbcUrl}")

                // Load QuestDB JDBC driver
                Class.forName("org.questdb.jdbc.Driver")

                // Create connection
                val properties = java.util.Properties()
                properties.setProperty("user", cfg.username)
                properties.setProperty("password", cfg.password)

                connection = DriverManager.getConnection(cfg.jdbcUrl, properties)

                logger.info("Connected to QuestDB successfully")
                null
            } catch (e: Exception) {
                logger.severe("Failed to connect to QuestDB: ${e.message}")
                throw e
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete()
            } else {
                promise.fail(result.cause())
            }
        }

        return promise.future()
    }

    override fun disconnect(): Future<Void> {
        val promise = Promise.promise<Void>()

        try {
            connection?.close()
            connection = null
            logger.info("Disconnected from QuestDB")
            promise.complete()
        } catch (e: Exception) {
            logger.warning("Error disconnecting from QuestDB: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    override fun writeBulk(tableName: String, rows: List<BufferedRow>) {
        val conn = connection ?: throw SQLException("Not connected to QuestDB")

        if (rows.isEmpty()) {
            return
        }

        try {
            // Build INSERT statement based on fields in first row
            val fields = rows.first().fields.keys.toList()
            val placeholders = fields.joinToString(", ") { "?" }
            val fieldNames = fields.joinToString(", ")

            val sql = "INSERT INTO $tableName ($fieldNames) VALUES ($placeholders)"

            logger.fine { "Writing bulk of ${rows.size} rows to table $tableName" }

            conn.prepareStatement(sql).use { ps ->
                rows.forEach { row ->
                    // Set parameter values
                    fields.forEachIndexed { index, fieldName ->
                        val value = row.fields[fieldName]
                        ps.setObject(index + 1, value)
                    }
                    ps.addBatch()
                }

                // Execute batch
                val results = ps.executeBatch()

                logger.fine { "Successfully wrote ${results.size} rows to table $tableName" }
            }

        } catch (e: SQLException) {
            logger.warning("SQL error writing to table $tableName: ${e.message}")
            throw e
        }
    }

    override fun isConnectionError(e: Exception): Boolean {
        return when (e) {
            is SQLException -> {
                // Check for connection-related error messages
                val message = e.message?.lowercase() ?: ""
                message.contains("connection") ||
                        message.contains("broken") ||
                        message.contains("closed") ||
                        message.contains("timeout") ||
                        message.contains("unreachable") ||
                        message.contains("refused")
            }
            else -> false
        }
    }
}
