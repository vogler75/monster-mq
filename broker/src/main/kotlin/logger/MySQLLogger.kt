package at.rocworks.logger

import java.sql.SQLException

/**
 * MySQL implementation of JDBC Logger
 * Extends PostgreSQLLogger and overrides MySQL-specific table creation
 */
class MySQLLogger : PostgreSQLLogger() {

    override fun connect(): io.vertx.core.Future<Void> {
        val promise = io.vertx.core.Promise.promise<Void>()

        vertx.executeBlocking<Void>({
            try {
                // Determine the correct driver based on JDBC URL to avoid conflicts with other drivers on classpath
                val driverClassName = cfg.getDriverClassName()
                logger.info("Connecting to MySQL: ${cfg.jdbcUrl}, Using driver: $driverClassName, Username: ${cfg.username}")

                // Load the specific JDBC driver - this ensures the correct driver is used
                // even when multiple JDBC drivers (e.g., Neo4j, PostgreSQL) are on the classpath
                Class.forName(driverClassName)

                // Create connection
                val properties = java.util.Properties()
                properties.setProperty("user", cfg.username)
                properties.setProperty("password", cfg.password)

                connection = java.sql.DriverManager.getConnection(cfg.jdbcUrl, properties)

                logger.info("Connected to MySQL successfully")

                // Auto-create table if enabled and table name is fixed (not dynamic)
                if (cfg.autoCreateTable && cfg.tableName != null) {
                    createTableIfNotExists(cfg.tableName!!)
                }

                null
            } catch (e: Exception) {
                logger.severe("Failed to connect to MySQL: ${e.javaClass.name}: ${e.message}")
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

    override fun disconnect(): io.vertx.core.Future<Void> {
        val promise = io.vertx.core.Promise.promise<Void>()

        try {
            connection?.close()
            connection = null
            logger.info("Disconnected from MySQL")
            promise.complete()
        } catch (e: Exception) {
            logger.warning("Error disconnecting from MySQL: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    override fun writeBulk(tableName: String, rows: List<BufferedRow>) {
        val conn = connection ?: throw SQLException("Not connected to MySQL")

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
            val fieldNames = allFields.joinToString(", ") { "`$it`" }  // MySQL uses backticks

            // Use INSERT IGNORE to silently skip duplicates
            val sql = "INSERT IGNORE INTO `$tableName` ($fieldNames) VALUES ($placeholders)"

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
            // Check for duplicate key errors FIRST (MySQL Error Code: 1062)
            if (e.errorCode == 1062) {
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
                e.message?.contains("doesn't exist", ignoreCase = true) == true ||
                e.errorCode == 1146) {  // MySQL error code for table doesn't exist
                logger.severe("=".repeat(80))
                logger.severe("ERROR: Table '$tableName' does not exist!")
                logger.severe("You need to create the table first based on your JSON schema.")
                logger.severe("Example SQL for your schema:")
                logger.severe("  CREATE TABLE `$tableName` (")
                rows.first().fields.forEach { (name, value) ->
                    val type = when (value) {
                        is String -> "VARCHAR(255)"
                        is Int -> "INT"
                        is Long -> "BIGINT"
                        is Double -> "DOUBLE"
                        is Float -> "FLOAT"
                        is Boolean -> "BOOLEAN"
                        else -> "TEXT"
                    }
                    logger.severe("    `$name` $type,")
                }
                logger.severe("    PRIMARY KEY (...)")
                logger.severe("  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
                logger.severe("=".repeat(80))
            }

            throw e
        }
    }

    override fun createTableIfNotExists(tableName: String) {
        val conn = connection ?: throw SQLException("Not connected")

        try {
            logger.info("Checking if MySQL table '$tableName' exists...")

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

                // Determine MySQL type
                val sqlType = when {
                    format == "timestamp" || format == "timestampms" -> {
                        if (timestampField == null) {
                            timestampField = fieldName  // Remember first timestamp field
                        }
                        "TIMESTAMP"
                    }
                    fieldType == "string" -> "TEXT"
                    fieldType == "number" -> "DOUBLE"
                    fieldType == "integer" -> "BIGINT"
                    fieldType == "boolean" -> "BOOLEAN"
                    else -> "TEXT"
                }

                columns.add("    `$fieldName` $sqlType")
            }

            // Add topic name column if configured
            if (cfg.topicNameColumn != null) {
                columns.add("    `${cfg.topicNameColumn}` TEXT")
            }

            // Build CREATE TABLE statement with AUTO_INCREMENT primary key
            val columnsSQL = columns.joinToString(",\n")
            val createTableSQL = """
                CREATE TABLE IF NOT EXISTS `$tableName` (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                $columnsSQL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """.trimIndent()

            logger.finer("Executing CREATE TABLE:\n$createTableSQL")

            conn.createStatement().use { stmt ->
                stmt.execute(createTableSQL)
            }

            // Create index on timestamp field if it exists
            if (timestampField != null) {
                val createIndexSQL = """
                    CREATE INDEX `${tableName}_${timestampField}_idx`
                    ON `$tableName` (`$timestampField` DESC)
                    """.trimIndent()

                logger.info("Creating index on timestamp field '$timestampField'")
                try {
                    conn.createStatement().use { stmt ->
                        stmt.execute(createIndexSQL)
                    }
                } catch (e: SQLException) {
                    // Index might already exist, which is fine
                    if (e.errorCode != 1061) {  // 1061 = Duplicate key name
                        logger.warning("Error creating index: ${e.message}")
                    }
                }
            }

            logger.info("MySQL table '$tableName' is ready")

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
                    // Check MySQL-specific error codes for connection errors
                    when (current.errorCode) {
                        2002, // Can't connect to MySQL server
                        2003, // Can't connect to MySQL server on '%s' (%d)
                        2006, // MySQL server has gone away
                        2013, // Lost connection to MySQL server during query
                        2055  // Lost connection to MySQL server at '%s', system error: %d
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
                        message.contains("not connected") ||
                        message.contains("gone away")
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
