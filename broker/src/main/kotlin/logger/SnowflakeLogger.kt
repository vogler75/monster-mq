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

        try {
            logger.fine { "Writing bulk of ${rows.size} rows to Snowflake table $tableName" }

            // Build INSERT statement based on fields in first row
            val fields = rows.first().fields.keys.toList()
            val placeholders = fields.joinToString(", ") { "?" }

            // Snowflake uses uppercase identifiers by default, but we'll quote them to preserve case
            val fieldNames = fields.joinToString(", ") { "\"${it.uppercase()}\"" }

            val sql = "INSERT INTO \"$database\".\"$schema\".\"${tableName.uppercase()}\" ($fieldNames) VALUES ($placeholders)"

            logger.fine { "Snowflake SQL: $sql" }

            conn.prepareStatement(sql).use { ps ->
                rows.forEach { row ->
                    // Set parameter values with type-specific setters
                    fields.forEachIndexed { index, fieldName ->
                        val value = row.fields[fieldName]
                        val paramIndex = index + 1
                        logger.fine { "Setting parameter $paramIndex to value '$value' (${value?.javaClass?.name ?: "null"})" }

                        when (value) {
                            null -> ps.setNull(paramIndex, java.sql.Types.NULL)
                            is String -> ps.setString(paramIndex, value)
                            is Int -> ps.setInt(paramIndex, value)
                            is Long -> ps.setLong(paramIndex, value)
                            is Double -> ps.setDouble(paramIndex, value)
                            is Float -> ps.setFloat(paramIndex, value)
                            is Boolean -> ps.setBoolean(paramIndex, value)
                            is Timestamp -> ps.setTimestamp(paramIndex, value)
                            is java.sql.Date -> ps.setDate(paramIndex, value)
                            is java.sql.Time -> ps.setTime(paramIndex, value)
                            is java.math.BigDecimal -> ps.setBigDecimal(paramIndex, value)
                            is ByteArray -> ps.setBytes(paramIndex, value)
                            else -> ps.setObject(paramIndex, value)
                        }
                    }
                    ps.addBatch()
                }

                // Execute batch
                val results = ps.executeBatch()

                logger.fine { "Successfully wrote ${results.size} rows to Snowflake table $tableName" }
            }

        } catch (e: SQLException) {
            logger.severe("SQL error writing to Snowflake table $tableName: ${e.javaClass.name}: ${e.message}")
            logger.severe("SQL State: ${e.sqlState}, Error Code: ${e.errorCode}")

            // Log full stack trace
            val sw = java.io.StringWriter()
            e.printStackTrace(java.io.PrintWriter(sw))
            logger.severe("Stack trace:\n$sw")

            // Check for table not found errors
            if (e.message?.contains("does not exist", ignoreCase = true) == true ||
                e.message?.contains("table", ignoreCase = true) == true) {
                logger.severe("=".repeat(80))
                logger.severe("ERROR: Table '$tableName' does not exist in Snowflake!")
                logger.severe("You need to create the table first in database: $database, schema: $schema")
                logger.severe("Example SQL for your schema:")
                logger.severe("  CREATE TABLE \"$database\".\"$schema\".\"${tableName.uppercase()}\" (")
                rows.first().fields.forEach { (name, value) ->
                    val type = when (value) {
                        is String -> "VARCHAR"
                        is Int -> "INTEGER"
                        is Long -> "BIGINT"
                        is Double -> "DOUBLE"
                        is Float -> "FLOAT"
                        is Boolean -> "BOOLEAN"
                        is Timestamp -> "TIMESTAMP_NTZ"
                        else -> "VARIANT"
                    }
                    logger.severe("    \"${name.uppercase()}\" $type,")
                }
                logger.severe("  );")
                logger.severe("=".repeat(80))
            }

            throw e
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
}
