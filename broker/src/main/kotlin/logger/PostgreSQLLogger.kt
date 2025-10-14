package at.rocworks.logger

import io.vertx.core.Future
import io.vertx.core.Promise
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

/**
 * PostgreSQL implementation of JDBC Logger
 * Supports PostgreSQL and compatible databases (TimescaleDB, QuestDB via PostgreSQL wire protocol)
 */
class PostgreSQLLogger : JDBCLoggerBase() {

    private var connection: Connection? = null

    override fun connect(): Future<Void> {
        val promise = Promise.promise<Void>()

        vertx.executeBlocking<Void>({
            try {
                logger.info("Connecting to PostgreSQL: ${cfg.jdbcUrl}")
                logger.info("Using driver: org.postgresql.Driver")
                logger.info("Username: ${cfg.username}")

                // Load PostgreSQL JDBC driver
                Class.forName("org.postgresql.Driver")

                // Create connection
                val properties = java.util.Properties()
                properties.setProperty("user", cfg.username)
                properties.setProperty("password", cfg.password)

                connection = DriverManager.getConnection(cfg.jdbcUrl, properties)

                logger.info("Connected to PostgreSQL successfully")
                null
            } catch (e: Exception) {
                logger.severe("Failed to connect to PostgreSQL: ${e.javaClass.name}: ${e.message}")
                logger.severe("Connection details - URL: ${cfg.jdbcUrl}, Username: ${cfg.username}")
                e.printStackTrace() // Print full stack trace
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
            logger.info("Disconnected from PostgreSQL")
            promise.complete()
        } catch (e: Exception) {
            logger.warning("Error disconnecting from PostgreSQL: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    override fun writeBulk(tableName: String, rows: List<BufferedRow>) {
        val conn = connection ?: throw SQLException("Not connected to PostgreSQL")

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
            logger.fine { "SQL: $sql" }

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
            logger.warning("SQL error writing to table $tableName: ${e.javaClass.name}: ${e.message}")
            logger.warning("SQL State: ${e.sqlState}, Error Code: ${e.errorCode}")

            // Check for table not found errors
            if (e.message?.contains("table", ignoreCase = true) == true ||
                e.message?.contains("relation", ignoreCase = true) == true ||
                e.sqlState == "42P01") {  // PostgreSQL error code for undefined_table
                logger.severe("=".repeat(80))
                logger.severe("ERROR: Table '$tableName' does not exist!")
                logger.severe("You need to create the table first based on your JSON schema.")
                logger.severe("Example SQL for your schema:")
                logger.severe("  CREATE TABLE $tableName (")
                rows.first().fields.forEach { (name, value) ->
                    val type = when (value) {
                        is String -> "TEXT"
                        is Int -> "INTEGER"
                        is Long -> "BIGINT"
                        is Double -> "DOUBLE PRECISION"
                        is Float -> "REAL"
                        is Boolean -> "BOOLEAN"
                        else -> "TEXT"
                    }
                    logger.severe("    $name $type,")
                }
                logger.severe("    PRIMARY KEY (...)")
                logger.severe("  );")
                logger.severe("=".repeat(80))
            }

            e.printStackTrace()
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
