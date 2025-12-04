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
open class PostgreSQLLogger : JDBCLoggerBase() {

    protected var connection: Connection? = null

    override fun connect(): Future<Void> {
        val promise = Promise.promise<Void>()

        vertx.executeBlocking<Void>({
            try {
                // Determine the correct driver based on JDBC URL to avoid conflicts with other drivers on classpath
                val driverClassName = cfg.getDriverClassName()
                logger.info("Connecting to PostgreSQL: ${cfg.jdbcUrl}, Using driver: $driverClassName, Username: ${cfg.username}")

                // Load the specific JDBC driver - this ensures the correct driver is used
                // even when multiple JDBC drivers (e.g., Neo4j, MySQL) are on the classpath
                Class.forName(driverClassName)

                // Create connection
                val properties = java.util.Properties()
                properties.setProperty("user", cfg.username)
                properties.setProperty("password", cfg.password)

                connection = DriverManager.getConnection(cfg.jdbcUrl, properties)

                logger.info("Connected to PostgreSQL successfully")

                // Auto-create table if enabled and table name is fixed (not dynamic)
                if (cfg.autoCreateTable && cfg.tableName != null) {
                    createTableIfNotExists(cfg.tableName!!)
                }

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

        val fields = rows.first().fields.keys.toList()
        val allFields = if (cfg.topicNameColumn != null) {
            fields + cfg.topicNameColumn!!
        } else {
            fields
        }
        val placeholders = allFields.joinToString(", ") { "?" }
        val fieldNames = allFields.joinToString(", ")
        val sql = "INSERT INTO $tableName ($fieldNames) VALUES ($placeholders)"

        // Try batch insert first (fast path)
        try {
            logger.fine { "Writing bulk of ${rows.size} rows to table $tableName (batch mode)" }

            conn.prepareStatement(sql).use { ps ->
                rows.forEach { row ->
                    var paramIndex = 1

                    fields.forEach { fieldName ->
                        val value = row.fields[fieldName]
                        setParameterValue(ps, paramIndex, value)
                        paramIndex++
                    }

                    if (cfg.topicNameColumn != null) {
                        ps.setString(paramIndex, row.topic)
                    }

                    ps.addBatch()
                }

                val results = ps.executeBatch()
                messagesWrittenCounter.addAndGet(rows.size.toLong())
                logger.fine { "Batch insert succeeded: ${rows.size} rows" }
            }
        } catch (batchError: SQLException) {
            // Batch failed - fall back to individual inserts to salvage as many rows as possible
            logger.warning("Batch insert failed, falling back to individual inserts: ${batchError.message}")
            writeRowsIndividually(tableName, rows, fields, sql)
        }
    }

    /**
     * Write rows individually, allowing partial success when batch fails.
     * Each row is attempted separately so that successful rows are inserted even if others fail.
     */
    private fun writeRowsIndividually(tableName: String, rows: List<BufferedRow>, fields: List<String>, sql: String) {
        val conn = connection ?: throw SQLException("Not connected to PostgreSQL")

        var successCount = 0
        var failureCount = 0

        conn.prepareStatement(sql).use { ps ->
            rows.forEach { row ->
                try {
                    var paramIndex = 1

                    fields.forEach { fieldName ->
                        val value = row.fields[fieldName]
                        setParameterValue(ps, paramIndex, value)
                        paramIndex++
                    }

                    if (cfg.topicNameColumn != null) {
                        ps.setString(paramIndex, row.topic)
                    }

                    ps.executeUpdate()
                    successCount++

                } catch (rowError: SQLException) {
                    failureCount++
                    logger.fine { "Row insert failed: ${rowError.sqlState} ${rowError.message}" }
                }
            }
        }

        messagesWrittenCounter.addAndGet(successCount.toLong())
        logger.info("Individual insert completed for table '$tableName': $successCount/${rows.size} rows inserted, $failureCount failed")

        // If all rows failed, throw an exception to trigger retry logic in base class
        if (successCount == 0) {
            throw SQLException("All ${rows.size} rows failed to insert (see logs for details)")
        }
    }

    protected open fun createTableIfNotExists(tableName: String) {
        val conn = connection ?: throw SQLException("Not connected")

        try {
            logger.info("Checking if PostgreSQL table '$tableName' exists...")

            // Extract field definitions from JSON schema
            val schemaProperties = cfg.jsonSchema.getJsonObject("properties")
            if (schemaProperties == null || schemaProperties.isEmpty) {
                logger.warning("No properties defined in JSON schema, skipping table creation")
                return
            }

            // Find timestamp field (first one with format=timestamp or format=timestampms)
            var timestampField: String? = null
            val columns = mutableListOf<String>()

            schemaProperties.fieldNames().forEach { fieldName ->
                val fieldSchema = schemaProperties.getJsonObject(fieldName)
                val fieldType = fieldSchema?.getString("type") ?: "string"
                val format = fieldSchema?.getString("format")

                // Determine PostgreSQL type
                val sqlType = when {
                    format == "timestamp" || format == "timestampms" -> {
                        if (timestampField == null) {
                            timestampField = fieldName  // Remember first timestamp field
                        }
                        "TIMESTAMP"
                    }
                    fieldType == "string" -> "TEXT"
                    fieldType == "number" -> "DOUBLE PRECISION"
                    fieldType == "integer" -> "BIGINT"
                    fieldType == "boolean" -> "BOOLEAN"
                    else -> "TEXT"
                }

                columns.add("    $fieldName $sqlType")
            }

            // Add topic name column if configured
            if (cfg.topicNameColumn != null) {
                columns.add("    ${cfg.topicNameColumn} TEXT")
            }

            // Build CREATE TABLE statement with SERIAL primary key
            val columnsSQL = columns.joinToString(",\n")
            val createTableSQL = """
                CREATE TABLE IF NOT EXISTS "$tableName" (
                    id SERIAL PRIMARY KEY,
                $columnsSQL
                )
                """.trimIndent()

            logger.finer("Executing CREATE TABLE:\n$createTableSQL")

            conn.createStatement().use { stmt ->
                stmt.execute(createTableSQL)
            }

            // Create index on timestamp field if it exists
            if (timestampField != null) {
                val createIndexSQL = """
                    CREATE INDEX IF NOT EXISTS "${tableName}_${timestampField}_idx"
                    ON "$tableName" ($timestampField DESC)
                    """.trimIndent()

                logger.info("Creating index on timestamp field '$timestampField'")
                conn.createStatement().use { stmt ->
                    stmt.execute(createIndexSQL)
                }
            }

            logger.info("PostgreSQL table '$tableName' is ready")

        } catch (e: SQLException) {
            logger.warning("Error creating table '$tableName': ${e.message}")
            // Don't throw - table might already exist, which is fine
        }
    }

    override fun isConnectionError(e: Exception): Boolean {
        return when (e) {
            is SQLException -> {
                // Check this exception and all chained exceptions
                var current: SQLException? = e
                while (current != null) {
                    // Check SQL state codes for connection errors
                    // 08xxx = Connection Exception
                    // 57P01 = admin_shutdown
                    // 57P02 = crash_shutdown
                    // 57P03 = cannot_connect_now
                    when (current.sqlState) {
                        "08000", // connection_exception
                        "08003", // connection_does_not_exist
                        "08006", // connection_failure
                        "08001", // sqlclient_unable_to_establish_sqlconnection
                        "08004", // sqlserver_rejected_establishment_of_sqlconnection
                        "08007", // transaction_resolution_unknown
                        "57P01", // admin_shutdown
                        "57P02", // crash_shutdown
                        "57P03"  // cannot_connect_now
                        -> return true
                    }

                    // Check for connection-related error messages
                    val message = current.message?.lowercase() ?: ""
                    if (message.contains("connection") ||
                        message.contains("broken") ||
                        message.contains("closed") ||
                        message.contains("timeout") ||
                        message.contains("unreachable") ||
                        message.contains("refused") ||
                        message.contains("i/o error") ||
                        message.contains("socket") ||
                        message.contains("broken pipe") ||
                        message.contains("connection reset") ||
                        message.contains("not connected")
                    ) {
                        return true
                    }

                    // Move to next exception in chain
                    current = current.nextException
                }
                false
            }
            else -> false
        }
    }
}
