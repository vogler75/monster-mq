package at.rocworks.logger

import io.vertx.core.Future
import io.vertx.core.Promise
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Timestamp
import java.util.*

/**
 * Snowflake implementation of JDBC Logger using Snowflake JDBC Thin Driver
 *
 * Note: Despite the name "JDBC Logger", Snowflake uses a different authentication mechanism
 * (private key) compared to standard username/password JDBC authentication.
 *
 * Required dbSpecificConfig fields:
 * - account: Snowflake account identifier
 * - privateKeyFile: Path to RSA private key file (.p8)
 * - warehouse: Snowflake warehouse name
 * - database: Database name
 * - schema: Schema name
 *
 * Optional dbSpecificConfig fields:
 * - role: Snowflake role to use (optional, default: accountadmin)
 *
 * Note: JDBC URL is used from the main configuration (cfg.jdbcUrl)
 */
class SnowflakeLogger : JDBCLoggerBase() {

    private var connection: Connection? = null

    // Snowflake-specific configuration
    private lateinit var account: String
    private lateinit var privateKeyFile: String
    private lateinit var role: String
    private lateinit var database: String
    private lateinit var schema: String
    private lateinit var warehouse: String

    override fun connect(): Future<Void> {
        val promise = Promise.promise<Void>()

        vertx.executeBlocking<Void>({
            try {
                // Load Snowflake-specific configuration from dbSpecificConfig
                account = cfg.getDbSpecificString("account")
                privateKeyFile = cfg.getDbSpecificString("privateKeyFile")
                role = cfg.getDbSpecificString("role", "accountadmin")
                database = cfg.getDbSpecificString("database")
                schema = cfg.getDbSpecificString("schema")
                warehouse = cfg.getDbSpecificString("warehouse")

                logger.info("Connecting to Snowflake via JDBC: Account=$account, Database=$database, Schema=$schema, User=${cfg.username}")

                // Read and parse private key
                val privateKeyPath = Paths.get(privateKeyFile)
                if (!Files.exists(privateKeyPath)) {
                    throw IllegalArgumentException("Private key file not found: $privateKeyFile")
                }

                val privateKeyContent = String(Files.readAllBytes(privateKeyPath), Charsets.UTF_8)
                val privateKeyPEM = privateKeyContent
                    .replace("-----BEGIN PRIVATE KEY-----", "")
                    .replace("-----END PRIVATE KEY-----", "")
                    .replace("-----BEGIN RSA PRIVATE KEY-----", "")
                    .replace("-----END RSA PRIVATE KEY-----", "")
                    .replace("\\s".toRegex(), "")

                // Decode the base64 encoded private key
                val privateKeyBytes = java.util.Base64.getDecoder().decode(privateKeyPEM)

                // Create PrivateKey object from bytes
                val keySpec = java.security.spec.PKCS8EncodedKeySpec(privateKeyBytes)
                val keyFactory = java.security.KeyFactory.getInstance("RSA")
                val privateKey = keyFactory.generatePrivate(keySpec)

                // Use JDBC URL from configuration
                val jdbcUrl = cfg.jdbcUrl

                logger.info("Snowflake JDBC URL: $jdbcUrl")

                // Build connection properties
                val props = Properties()
                props.setProperty("user", cfg.username)
                props.setProperty("account", account)
                props.setProperty("role", role)
                props.setProperty("db", database)
                props.setProperty("schema", schema)
                props.setProperty("warehouse", warehouse)

                // Private key authentication - use put() for PrivateKey object, not setProperty() which only handles strings
                props.setProperty("authenticator", "snowflake_jwt")
                props.put("privateKey", privateKey)  // PrivateKey object, not string

                // Optional: SSL settings
                props.setProperty("ssl", "on")

                // Load Snowflake JDBC driver explicitly to avoid conflicts with other JDBC drivers (e.g., Neo4j)
                val snowflakeDriver = Class.forName("net.snowflake.client.jdbc.SnowflakeDriver").getDeclaredConstructor().newInstance() as java.sql.Driver

                // Create connection using the driver directly (safer than DriverManager which can pick wrong driver)
                connection = snowflakeDriver.connect(jdbcUrl, props)

                logger.info("Connected to Snowflake successfully via JDBC")

                // Auto-create table if enabled and table name is fixed (not dynamic)
                if (cfg.autoCreateTable && cfg.tableName != null) {
                    createTableIfNotExists(cfg.tableName!!)
                }

                null
            } catch (e: Exception) {
                logger.severe("Failed to connect to Snowflake: ${e.javaClass.name}: ${e.message}")
                logger.severe("Connection details - Account: $account, Database: $database, Schema: $schema, User: ${cfg.username}")
                e.printStackTrace()
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
            logger.info("Disconnected from Snowflake")
            promise.complete()
        } catch (e: Exception) {
            logger.warning("Error disconnecting from Snowflake: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    override fun writeBulk(tableName: String, rows: List<BufferedRow>) {
        val conn = connection ?: throw SQLException("Not connected to Snowflake")

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
        val fieldNames = allFields.joinToString(", ") { "\"${it.uppercase()}\"" }

        val sql = "INSERT INTO \"$database\".\"$schema\".\"${tableName.uppercase()}\" ($fieldNames) VALUES ($placeholders)"

        // Try batch insert first (fast path)
        try {
            logger.fine { "Writing bulk of ${rows.size} rows to Snowflake table $tableName (batch mode)" }

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
        val conn = connection ?: throw SQLException("Not connected to Snowflake")

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

    override fun isConnectionError(e: Exception): Boolean {
        return when (e) {
            is SQLException -> {
                // Check this exception and all chained exceptions
                var current: SQLException? = e
                while (current != null) {
                    val message = current.message?.lowercase() ?: ""

                    // Snowflake-specific connection error patterns
                    if (message.contains("connection") ||
                        message.contains("network") ||
                        message.contains("timeout") ||
                        message.contains("authentication") ||
                        message.contains("jwt") ||
                        message.contains("session") ||
                        message.contains("expired") ||
                        message.contains("invalid") && message.contains("token") ||
                        message.contains("closed") ||
                        message.contains("broken") ||
                        message.contains("unreachable") ||
                        message.contains("refused") ||
                        message.contains("reset")
                    ) {
                        return true
                    }

                    current = current.nextException
                }
                false
            }
            else -> {
                val message = e.message?.lowercase() ?: ""
                message.contains("connection") ||
                message.contains("network") ||
                message.contains("timeout") ||
                message.contains("authentication")
            }
        }
    }

    private fun createTableIfNotExists(tableName: String) {
        val conn = connection ?: throw SQLException("Not connected to Snowflake")

        try {
            logger.info("Checking if Snowflake table '$tableName' exists in $database.$schema...")

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

                // Determine Snowflake type
                val sqlType = when {
                    format == "timestamp" || format == "timestampms" -> {
                        if (timestampField == null) {
                            timestampField = fieldName  // Remember first timestamp field
                        }
                        "TIMESTAMP_NTZ"  // Snowflake timestamp without timezone
                    }
                    fieldType == "string" -> "VARCHAR"
                    fieldType == "number" -> "DOUBLE"
                    fieldType == "integer" -> "BIGINT"
                    fieldType == "boolean" -> "BOOLEAN"
                    else -> "VARIANT"  // Snowflake's flexible type for JSON
                }

                // Snowflake uses uppercase identifiers by default, quote them to preserve case
                columns.add("    \"${fieldName.uppercase()}\" $sqlType")
            }

            // Add topic name column if configured
            if (cfg.topicNameColumn != null) {
                columns.add("    \"${cfg.topicNameColumn!!.uppercase()}\" VARCHAR")
            }

            // Build CREATE TABLE statement
            val columnsSQL = columns.joinToString(",\n")
            val fullTableName = "\"$database\".\"$schema\".\"${tableName.uppercase()}\""
            val createTableSQL = """
                CREATE TABLE IF NOT EXISTS $fullTableName (
                $columnsSQL
                )
                """.trimIndent()

            logger.finer("Executing CREATE TABLE:\n$createTableSQL")

            conn.createStatement().use { stmt ->
                stmt.execute(createTableSQL)
            }

            // Create clustering key on timestamp field if it exists (improves query performance)
            if (timestampField != null) {
                val clusterSQL = """
                    ALTER TABLE $fullTableName
                    CLUSTER BY (\"${timestampField.uppercase()}\")
                    """.trimIndent()

                logger.info("Creating clustering key on timestamp field '$timestampField'")
                try {
                    conn.createStatement().use { stmt ->
                        stmt.execute(clusterSQL)
                    }
                } catch (e: SQLException) {
                    // Clustering key might already exist or table might already be clustered
                    logger.finer("Could not add clustering key (may already exist): ${e.message}")
                }
            }

            logger.info("Snowflake table '$tableName' is ready in $database.$schema")

        } catch (e: SQLException) {
            logger.severe("Failed to create Snowflake table '$tableName': ${e.message}")
            e.printStackTrace()
            throw e
        }
    }
}
