package at.rocworks.stores.postgres

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.stores.ArchiveGroup
import at.rocworks.stores.ArchiveGroupConfig
import at.rocworks.stores.DatabaseConnection
import at.rocworks.stores.IConfigStore
import at.rocworks.stores.MessageStoreType
import at.rocworks.stores.MessageArchiveType
import at.rocworks.utils.DurationParser
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import io.vertx.core.json.JsonArray
import java.sql.*

class ConfigStorePostgres(
    private val url: String,
    private val username: String,
    private val password: String
): AbstractVerticle(), IConfigStore {
    private val logger = Utils.getLogger(this::class.java)

    private val configTableName = "archiveconfigs"

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun getType(): String = "POSTGRES"

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun init(connection: Connection): Future<Void> {
            val promise = Promise.promise<Void>()
            try {
                connection.autoCommit = false

                val createTableSQL = """
                CREATE TABLE IF NOT EXISTS $configTableName (
                    name VARCHAR(255) PRIMARY KEY,
                    enabled BOOLEAN NOT NULL,
                    topic_filter JSONB NOT NULL,
                    retained_only BOOLEAN NOT NULL,
                    last_val_type VARCHAR(50) NOT NULL,
                    archive_type VARCHAR(50) NOT NULL,
                    last_val_retention VARCHAR(50),
                    archive_retention VARCHAR(50),
                    purge_interval VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """.trimIndent()

                connection.createStatement().use { statement ->
                    statement.executeUpdate(createTableSQL)
                }
                connection.commit()
                logger.info("Archive config table is ready [${Utils.getCurrentFunctionName()}]")
                promise.complete()
            } catch (e: Exception) {
                logger.severe("Error in getting archive config table ready: ${e.message} [${Utils.getCurrentFunctionName()}]")
                promise.fail(e)
            }
            return promise.future()
        }
    }

    override fun start(startPromise: Promise<Void>) {
        db.start(vertx, startPromise)
    }

    override fun getAllArchiveGroups(): List<ArchiveGroupConfig> {
        val archiveGroups = mutableListOf<ArchiveGroupConfig>()
        val sql = "SELECT name, enabled, topic_filter, retained_only, last_val_type, archive_type, last_val_retention, archive_retention, purge_interval FROM $configTableName ORDER BY name"

        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    val resultSet = preparedStatement.executeQuery()
                    while (resultSet.next()) {
                        val name = resultSet.getString("name")
                        val enabled = resultSet.getBoolean("enabled")
                        val topicFilterJson = resultSet.getString("topic_filter")
                        val retainedOnly = resultSet.getBoolean("retained_only")
                        val lastValType = resultSet.getString("last_val_type")
                        val archiveType = resultSet.getString("archive_type")
                        val lastValRetention = resultSet.getString("last_val_retention")
                        val archiveRetention = resultSet.getString("archive_retention")
                        val purgeInterval = resultSet.getString("purge_interval")

                        val topicFilter = try {
                            // Try to parse as direct array first (new format)
                            JsonArray(topicFilterJson).list.map { it.toString() }
                        } catch (e: Exception) {
                            // Fall back to old format with "filters" wrapper
                            JsonObject(topicFilterJson).getJsonArray("filters")?.list?.map { it.toString() } ?: emptyList()
                        }

                        val archiveGroup = ArchiveGroup(
                            name = name,
                            topicFilter = topicFilter,
                            retainedOnly = retainedOnly,
                            lastValType = MessageStoreType.valueOf(lastValType),
                            archiveType = MessageArchiveType.valueOf(archiveType),
                            lastValRetentionMs = DurationParser.parse(lastValRetention),
                            archiveRetentionMs = DurationParser.parse(archiveRetention),
                            purgeIntervalMs = DurationParser.parse(purgeInterval),
                            lastValRetentionStr = lastValRetention,
                            archiveRetentionStr = archiveRetention,
                            purgeIntervalStr = purgeInterval,
                            databaseConfig = JsonObject()
                        )
                        archiveGroups.add(ArchiveGroupConfig(archiveGroup, enabled))
                    }
                }
            } ?: run {
                logger.severe("Getting archive groups not possible without database connection! [${Utils.getCurrentFunctionName()}]")
            }
        } catch (e: SQLException) {
            logger.warning("Error fetching archive groups: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }

        return archiveGroups
    }

    override fun getArchiveGroup(name: String): ArchiveGroupConfig? {
        val sql = "SELECT name, enabled, topic_filter, retained_only, last_val_type, archive_type, last_val_retention, archive_retention, purge_interval FROM $configTableName WHERE name = ?"

        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, name)
                    val resultSet = preparedStatement.executeQuery()
                    if (resultSet.next()) {
                        val enabled = resultSet.getBoolean("enabled")
                        val topicFilterJson = resultSet.getString("topic_filter")
                        val retainedOnly = resultSet.getBoolean("retained_only")
                        val lastValType = resultSet.getString("last_val_type")
                        val archiveType = resultSet.getString("archive_type")
                        val lastValRetention = resultSet.getString("last_val_retention")
                        val archiveRetention = resultSet.getString("archive_retention")
                        val purgeInterval = resultSet.getString("purge_interval")

                        val topicFilter = try {
                            // Try to parse as direct array first (new format)
                            JsonArray(topicFilterJson).list.map { it.toString() }
                        } catch (e: Exception) {
                            // Fall back to old format with "filters" wrapper
                            JsonObject(topicFilterJson).getJsonArray("filters")?.list?.map { it.toString() } ?: emptyList()
                        }

                        val archiveGroup = ArchiveGroup(
                            name = name,
                            topicFilter = topicFilter,
                            retainedOnly = retainedOnly,
                            lastValType = MessageStoreType.valueOf(lastValType),
                            archiveType = MessageArchiveType.valueOf(archiveType),
                            lastValRetentionMs = DurationParser.parse(lastValRetention),
                            archiveRetentionMs = DurationParser.parse(archiveRetention),
                            purgeIntervalMs = DurationParser.parse(purgeInterval),
                            lastValRetentionStr = lastValRetention,
                            archiveRetentionStr = archiveRetention,
                            purgeIntervalStr = purgeInterval,
                            databaseConfig = JsonObject()
                        )
                        return ArchiveGroupConfig(archiveGroup, enabled)
                    }
                }
            } ?: run {
                logger.severe("Getting archive group not possible without database connection! [${Utils.getCurrentFunctionName()}]")
            }
        } catch (e: SQLException) {
            logger.warning("Error fetching archive group $name: ${e.message} [${Utils.getCurrentFunctionName()}]")
        }

        return null
    }

    override fun saveArchiveGroup(archiveGroup: ArchiveGroup, enabled: Boolean): Boolean {
        val sql = """
            INSERT INTO $configTableName
            (name, enabled, topic_filter, retained_only, last_val_type, archive_type, last_val_retention, archive_retention, purge_interval, updated_at)
            VALUES (?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (name) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                topic_filter = EXCLUDED.topic_filter,
                retained_only = EXCLUDED.retained_only,
                last_val_type = EXCLUDED.last_val_type,
                archive_type = EXCLUDED.archive_type,
                last_val_retention = EXCLUDED.last_val_retention,
                archive_retention = EXCLUDED.archive_retention,
                purge_interval = EXCLUDED.purge_interval,
                updated_at = CURRENT_TIMESTAMP
        """.trimIndent()

        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    val topicFilterJson = JsonArray(archiveGroup.topicFilter).encode()

                    // Use the string retention values directly to preserve original format
                    val lastValRetention = archiveGroup.getLastValRetention()
                    val archiveRetention = archiveGroup.getArchiveRetention()
                    val purgeInterval = archiveGroup.getPurgeInterval()

                    preparedStatement.setString(1, archiveGroup.name)
                    preparedStatement.setBoolean(2, enabled)
                    preparedStatement.setString(3, topicFilterJson)
                    preparedStatement.setBoolean(4, archiveGroup.retainedOnly)
                    preparedStatement.setString(5, archiveGroup.getLastValType().name)
                    preparedStatement.setString(6, archiveGroup.getArchiveType().name)
                    preparedStatement.setString(7, lastValRetention)
                    preparedStatement.setString(8, archiveRetention)
                    preparedStatement.setString(9, purgeInterval)

                    val rowsAffected = preparedStatement.executeUpdate()
                    connection.commit()
                    return rowsAffected > 0
                }
            } ?: run {
                logger.severe("Saving archive group not possible without database connection! [${Utils.getCurrentFunctionName()}]")
                return false
            }
        } catch (e: SQLException) {
            logger.warning("Error saving archive group ${archiveGroup.name}: ${e.message} [${Utils.getCurrentFunctionName()}]")
            return false
        }
    }

    override fun updateArchiveGroup(archiveGroup: ArchiveGroup, enabled: Boolean): Boolean {
        return saveArchiveGroup(archiveGroup, enabled) // PostgreSQL UPSERT handles both insert and update
    }

    override fun deleteArchiveGroup(name: String): Boolean {
        val sql = "DELETE FROM $configTableName WHERE name = ?"

        try {
            db.connection?.let { connection ->
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, name)
                    val rowsAffected = preparedStatement.executeUpdate()
                    connection.commit()
                    return rowsAffected > 0
                }
            } ?: run {
                logger.severe("Deleting archive group not possible without database connection! [${Utils.getCurrentFunctionName()}]")
                return false
            }
        } catch (e: SQLException) {
            logger.warning("Error deleting archive group $name: ${e.message} [${Utils.getCurrentFunctionName()}]")
            return false
        }
    }
}