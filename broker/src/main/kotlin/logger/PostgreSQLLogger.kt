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

        try {
            // Build INSERT statement based on fields in first row + optional topic column
            val fields = rows.first().fields.keys.toList()
            val allFields = if (cfg.topicNameColumn != null) {
                fields + cfg.topicNameColumn!!
            } else {
                fields
            }
            val placeholders = allFields.joinToString(", ") { "?" }
            val fieldNames = allFields.joinToString(", ")

            // Use regular INSERT - duplicate key errors will be caught and counted
            val sql = "INSERT INTO $tableName ($fieldNames) VALUES ($placeholders)"

            logger.fine { "Writing bulk of ${rows.size} rows to table $tableName SQL: $sql" }

            conn.prepareStatement(sql).use { ps ->
                rows.forEach { row ->
                    var paramIndex = 1

                    // Set parameter values from fields with type-specific setters
                    fields.forEach { fieldName ->
                        val value = row.fields[fieldName]
                        logger.fine { "Setting parameter $paramIndex to value '$value' (${value?.javaClass?.name ?: "null"})" }
                        when (value) {
                            null -> ps.setNull(paramIndex, java.sql.Types.NULL)
                            is String -> ps.setString(paramIndex, value)
                            is Int -> ps.setInt(paramIndex, value)
                            is Long -> ps.setLong(paramIndex, value)
                            is Double -> ps.setDouble(paramIndex, value)
                            is Float -> ps.setFloat(paramIndex, value)
                            is Boolean -> ps.setBoolean(paramIndex, value)
                            is java.sql.Timestamp -> ps.setTimestamp(paramIndex, value)
                            is java.sql.Date -> ps.setDate(paramIndex, value)
                            is java.sql.Time -> ps.setTime(paramIndex, value)
                            is java.math.BigDecimal -> ps.setBigDecimal(paramIndex, value)
                            is ByteArray -> ps.setBytes(paramIndex, value)
                            else -> ps.setObject(paramIndex, value)  // Fallback for other types
                        }
                        paramIndex++
                    }

                    // Add topic column if configured
                    if (cfg.topicNameColumn != null) {
                        ps.setString(paramIndex, row.topic)
                        logger.fine { "Setting parameter $paramIndex (topic) to value '${row.topic}'" }
                    }

                    ps.addBatch()
                }

                // Execute batch
                val results = ps.executeBatch()

                logger.fine { "Successfully wrote ${results.size} rows to table $tableName" }
            }

        } catch (e: SQLException) {
            // Check for duplicate key errors FIRST (PostgreSQL SQL State: 23505)
            if (e.sqlState == "23505") {
                // Count duplicates and silently ignore - only log at FINE level for debugging
                duplicatesIgnoredCounter.incrementAndGet()
                logger.fine { "Duplicate key violation detected - ignoring: ${e.message}" }
                return  // Don't rethrow the exception
            }

            // Log all other errors as severe
            logger.severe("SQL error writing to table $tableName: ${e.javaClass.name}: ${e.message}")
            logger.severe("SQL State: ${e.sqlState}, Error Code: ${e.errorCode}")

            // Log the full exception with stack trace
            val sw = java.io.StringWriter()
            e.printStackTrace(java.io.PrintWriter(sw))
            logger.severe("Stack trace:\n$sw")

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

            throw e
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
