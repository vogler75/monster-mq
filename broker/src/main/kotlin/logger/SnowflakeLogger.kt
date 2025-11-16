package at.rocworks.logger

import io.vertx.core.Future
import io.vertx.core.Promise
import net.snowflake.ingest.streaming.OpenChannelRequest
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.SQLException
import java.sql.Timestamp
import java.util.*

/**
 * Snowflake implementation of JDBC Logger using Snowflake Streaming Ingest SDK
 *
 * Note: Despite the name "JDBC Logger", this uses Snowflake's native streaming API
 * rather than JDBC, as it provides better performance for high-throughput data ingestion.
 *
 * Required dbSpecificConfig fields:
 * - privateKeyFile: Path to RSA private key file (.p8)
 * - account: Snowflake account identifier
 * - url: Snowflake connection URL
 * - role: Snowflake role to use
 * - scheme: Connection scheme (https)
 * - port: Connection port (443)
 * - database: Database name
 * - schema: Schema name
 */
class SnowflakeLogger : JDBCLoggerBase() {

    private var client: SnowflakeStreamingIngestClient? = null
    private var channel: SnowflakeStreamingIngestChannel? = null

    // Snowflake-specific configuration
    private lateinit var privateKeyFile: String
    private lateinit var account: String
    private lateinit var url: String
    private lateinit var role: String
    private lateinit var scheme: String
    private var port: Int = 443
    private lateinit var database: String
    private lateinit var schema: String

    override fun connect(): Future<Void> {
        val promise = Promise.promise<Void>()

        vertx.executeBlocking<Void>({
            try {
                // Load Snowflake-specific configuration from dbSpecificConfig
                privateKeyFile = cfg.getDbSpecificString("privateKeyFile")
                account = cfg.getDbSpecificString("account")
                url = cfg.getDbSpecificString("url")
                role = cfg.getDbSpecificString("role", "accountadmin")
                scheme = cfg.getDbSpecificString("scheme", "https")
                port = cfg.getDbSpecificInteger("port", 443)
                database = cfg.getDbSpecificString("database")
                schema = cfg.getDbSpecificString("schema")

                logger.info("Connecting to Snowflake: $url, Database: $database, Schema: $schema, User: ${cfg.username}")

                // Read and prepare private key
                val privateKeyPath = Paths.get(privateKeyFile)
                if (!Files.exists(privateKeyPath)) {
                    throw IllegalArgumentException("Private key file not found: $privateKeyFile")
                }

                val privateKeyContent = Files.readAllBytes(privateKeyPath).toString(Charsets.UTF_8)
                val privateKey = privateKeyContent
                    .replace("-----BEGIN PRIVATE KEY-----", "")
                    .replace("-----END PRIVATE KEY-----", "")
                    .replace("\\s".toRegex(), "")

                // Build Snowflake client properties
                val props = Properties()
                props.put("account", account)
                props.put("url", url)
                props.put("user", cfg.username)
                props.put("private_key", privateKey)
                props.put("role", role)
                props.put("scheme", scheme)
                props.put("port", port.toString())

                // Create Snowflake streaming ingest client
                logger.info("Creating Snowflake client [${device.name}]...")
                client = SnowflakeStreamingIngestClientFactory.builder(device.name).setProperties(props).build()

                // Open streaming channel
                logger.info("Opening Snowflake channel [${device.name}]...")
                val tableName = cfg.tableName ?: throw IllegalArgumentException("Snowflake logger requires fixed tableName")

                val request = OpenChannelRequest.builder(device.name)
                    .setDBName(database)
                    .setSchemaName(schema)
                    .setTableName(tableName)
                    .setOnErrorOption(OpenChannelRequest.OnErrorOption.ABORT)
                    .build()

                channel = client!!.openChannel(request)

                logger.info("Connected to Snowflake successfully")
                null
            } catch (e: Exception) {
                logger.severe("Failed to connect to Snowflake: ${e.javaClass.name}: ${e.message}")
                logger.severe("Connection details - URL: $url, Database: $database, Schema: $schema, User: ${cfg.username}")
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
            channel?.close()
            channel = null
            client?.close()
            client = null
            logger.info("Disconnected from Snowflake")
            promise.complete()
        } catch (e: Exception) {
            logger.warning("Error disconnecting from Snowflake: ${e.message}")
            promise.fail(e)
        }

        return promise.future()
    }

    override fun writeBulk(tableName: String, rows: List<BufferedRow>) {
        val ch = channel ?: throw SQLException("Not connected to Snowflake")

        if (rows.isEmpty()) {
            return
        }

        try {
            logger.fine { "Writing bulk of ${rows.size} rows to Snowflake table $tableName" }

            // Convert BufferedRow objects to Snowflake-compatible maps
            rows.forEachIndexed { index, bufferedRow ->
                val record = HashMap<String, Any?>()

                // Convert field values to Snowflake-compatible types
                bufferedRow.fields.forEach { (fieldName, value) ->
                    val snowflakeValue = when (value) {
                        is Timestamp -> value.toString() // Convert to string for Snowflake TIMESTAMP
                        is java.sql.Date -> value.toString()
                        is java.sql.Time -> value.toString()
                        else -> value
                    }
                    // Snowflake expects uppercase column names by default
                    record[fieldName.uppercase()] = snowflakeValue
                }

                // Insert row into Snowflake channel
                val response = ch.insertRow(record, index.toString())

                if (response.hasErrors()) {
                    val errorMsg = response.insertErrors.firstOrNull()?.message ?: "Unknown error"
                    logger.warning("Error inserting row $index: $errorMsg")
                    throw SQLException("Snowflake insert error: $errorMsg")
                }
            }

            logger.fine { "Successfully wrote ${rows.size} rows to Snowflake table $tableName" }

        } catch (e: Exception) {
            logger.severe("Error writing to Snowflake table $tableName: ${e.javaClass.name}: ${e.message}")

            // Log full stack trace
            val sw = java.io.StringWriter()
            e.printStackTrace(java.io.PrintWriter(sw))
            logger.severe("Stack trace:\n$sw")

            // Check for table not found errors
            if (e.message?.contains("table", ignoreCase = true) == true ||
                e.message?.contains("does not exist", ignoreCase = true) == true) {
                logger.severe("=".repeat(80))
                logger.severe("ERROR: Table '$tableName' does not exist in Snowflake!")
                logger.severe("You need to create the table first in database: $database, schema: $schema")
                logger.severe("Example SQL for your schema:")
                logger.severe("  CREATE TABLE $database.$schema.$tableName (")
                rows.first().fields.forEach { (name, value) ->
                    val type = when (value) {
                        is String -> "VARCHAR"
                        is Int -> "INTEGER"
                        is Long -> "BIGINT"
                        is Double -> "DOUBLE"
                        is Float -> "FLOAT"
                        is Boolean -> "BOOLEAN"
                        is Timestamp -> "TIMESTAMP"
                        else -> "VARIANT"
                    }
                    logger.severe("    ${name.uppercase()} $type,")
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
                val message = e.message?.lowercase() ?: ""
                message.contains("connection") ||
                message.contains("broken") ||
                message.contains("closed") ||
                message.contains("timeout") ||
                message.contains("unreachable") ||
                message.contains("refused") ||
                message.contains("network") ||
                message.contains("socket") ||
                message.contains("not connected") ||
                message.contains("authentication failed") ||
                message.contains("session") ||
                message.contains("channel")
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
