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

        val fields = rows.first().fields.keys.toList()
        val allFields = if (cfg.topicNameColumn != null) {
            fields + cfg.topicNameColumn!!
        } else {
            fields
        }
        val placeholders = allFields.joinToString(", ") { "?" }
        val fieldNames = allFields.joinToString(", ") { "`$it`" }  // MySQL uses backticks

        // Use INSERT IGNORE to silently skip duplicates in batch mode
        val sql = "INSERT IGNORE INTO `$tableName` ($fieldNames) VALUES ($placeholders)"

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
        val conn = connection ?: throw SQLException("Not connected to MySQL")

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
                    logger.fine { "Row insert failed: ${rowError.errorCode} ${rowError.message}" }
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
