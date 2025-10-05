package at.rocworks.stores.sqlite

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.stores.*
import at.rocworks.utils.DurationParser
import at.rocworks.handlers.ArchiveGroup
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.sql.*
import java.util.concurrent.Callable
import java.util.logging.Logger

/**
 * SQLite implementation of IConfigStore for archive group configuration management
 */
class ArchiveConfigStoreSQLite(
    private val dbPath: String
) : AbstractVerticle(), IArchiveConfigStore {

    private val logger: Logger = Utils.getLogger(this::class.java)
    private val configTableName = "archiveconfigs"

    private val db = object : DatabaseConnection(logger, "jdbc:sqlite:$dbPath", "", "") {
        override fun init(connection: Connection): io.vertx.core.Future<Void> {
            val promise = Promise.promise<Void>()
            try {
                createConfigTable(connection)
                promise.complete()
            } catch (e: Exception) {
                logger.severe("Failed to initialize ConfigStoreSQLite: ${e.message}")
                promise.fail(e)
            }
            return promise.future()
        }
    }

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun start(startPromise: Promise<Void>) {
        db.start(vertx, startPromise)
    }

    override fun stop(stopPromise: Promise<Void>) {
        try {
            db.connection?.close()
            logger.info("SQLite ConfigStore stopped")
            stopPromise.complete()
        } catch (e: Exception) {
            logger.warning("Error stopping SQLite ConfigStore: ${e.message}")
            stopPromise.complete() // Complete anyway
        }
    }

    private fun createConfigTable(connection: Connection) {
        val sql = """
            CREATE TABLE IF NOT EXISTS $configTableName (
                name TEXT PRIMARY KEY,
                enabled INTEGER NOT NULL DEFAULT 0,
                topic_filter TEXT NOT NULL,
                retained_only INTEGER NOT NULL DEFAULT 0,
                last_val_type TEXT NOT NULL,
                archive_type TEXT NOT NULL,
                last_val_retention TEXT,
                archive_retention TEXT,
                purge_interval TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                payload_format TEXT DEFAULT 'DEFAULT'
            )
        """.trimIndent()

        connection.createStatement().use { statement ->
            statement.execute(sql)

            // Insert default archive config if it doesn't exist
            val insertDefaultSQL = """
                INSERT OR IGNORE INTO $configTableName (
                    name, enabled, topic_filter, retained_only,
                    last_val_type, archive_type, last_val_retention,
                    archive_retention, purge_interval, payload_format
                ) VALUES (
                    'Default', 1, '["#"]', 0,
                    'MEMORY', 'NONE', '1h', '1h', '1h', 'DEFAULT'
                )
            """.trimIndent()

            statement.execute(insertDefaultSQL)
        }
        logger.info("Archive config table created/verified with default entry in SQLite")
    }

    override fun getAllArchiveGroups(): Future<List<ArchiveGroupConfig>> {
        val promise = Promise.promise<List<ArchiveGroupConfig>>()

        vertx.executeBlocking(Callable {
            val sql = "SELECT * FROM $configTableName ORDER BY name"

            try {
                db.connection?.let { connection ->
                    connection.prepareStatement(sql).use { preparedStatement ->
                        val resultSet = preparedStatement.executeQuery()
                        val results = mutableListOf<ArchiveGroupConfig>()

                        while (resultSet.next()) {
                            try {
                                val archiveGroup = resultSetToArchiveGroup(resultSet)
                                val enabled = resultSet.getInt("enabled") == 1
                                results.add(ArchiveGroupConfig(archiveGroup, enabled))
                            } catch (e: Exception) {
                                logger.warning("Error parsing archive group from SQLite: ${e.message}")
                            }
                        }

                        logger.fine("Retrieved ${results.size} archive groups from SQLite")
                        results
                    }
                } ?: run {
                    logger.severe("Getting archive groups not possible without database connection! [getAllArchiveGroups]")
                    emptyList()
                }
            } catch (e: SQLException) {
                logger.severe("Error retrieving archive groups from SQLite: ${e.message}")
                emptyList()
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in getAllArchiveGroups: ${result.cause()?.message}")
                promise.complete(emptyList())
            }
        }

        return promise.future()
    }

    override fun getArchiveGroup(name: String): Future<ArchiveGroupConfig?> {
        val promise = Promise.promise<ArchiveGroupConfig?>()

        vertx.executeBlocking(Callable {
            val sql = "SELECT * FROM $configTableName WHERE name = ?"

            try {
                db.connection?.let { connection ->
                    connection.prepareStatement(sql).use { preparedStatement ->
                        preparedStatement.setString(1, name)
                        val resultSet = preparedStatement.executeQuery()

                        if (resultSet.next()) {
                            val archiveGroup = resultSetToArchiveGroup(resultSet)
                            val enabled = resultSet.getInt("enabled") == 1
                            ArchiveGroupConfig(archiveGroup, enabled)
                        } else {
                            null
                        }
                    }
                } ?: run {
                    logger.severe("Getting archive group not possible without database connection! [getArchiveGroup]")
                    null
                }
            } catch (e: SQLException) {
                logger.severe("Error retrieving archive group '$name' from SQLite: ${e.message}")
                null
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in getArchiveGroup: ${result.cause()?.message}")
                promise.complete(null)
            }
        }

        return promise.future()
    }

    override fun saveArchiveGroup(archiveGroup: ArchiveGroup, enabled: Boolean): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        vertx.executeBlocking(Callable {
            val sql = """
                INSERT OR REPLACE INTO $configTableName
                (name, enabled, topic_filter, retained_only, last_val_type, archive_type, last_val_retention, archive_retention, purge_interval, payload_format, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """.trimIndent()

            try {
                db.connection?.let { connection ->
                    connection.prepareStatement(sql).use { preparedStatement ->
                        val lastValRetention = archiveGroup.getLastValRetentionMs()?.let { DurationParser.formatDuration(it) }
                        val archiveRetention = archiveGroup.getArchiveRetentionMs()?.let { DurationParser.formatDuration(it) }
                        val purgeInterval = archiveGroup.getPurgeIntervalMs()?.let { DurationParser.formatDuration(it) }

                        preparedStatement.setString(1, archiveGroup.name)
                        preparedStatement.setInt(2, if (enabled) 1 else 0)

                        // Convert topic filter list to JSON string for SQLite storage
                        val topicFilterJson = JsonArray(archiveGroup.topicFilter).encode()
                        preparedStatement.setString(3, topicFilterJson)

                        preparedStatement.setInt(4, if (archiveGroup.retainedOnly) 1 else 0)
                        preparedStatement.setString(5, archiveGroup.getLastValType().name)
                        preparedStatement.setString(6, archiveGroup.getArchiveType().name)
                        preparedStatement.setString(7, lastValRetention)
                        preparedStatement.setString(8, archiveRetention)
                        preparedStatement.setString(9, purgeInterval)
                        preparedStatement.setString(10, archiveGroup.payloadFormat.name)

                        val rowsAffected = preparedStatement.executeUpdate()
                        val success = rowsAffected > 0

                        if (success) {
                            logger.info("Archive group '${archiveGroup.name}' saved successfully to SQLite")
                        }
                        success
                    }
                } ?: run {
                    logger.severe("Saving archive group not possible without database connection! [saveArchiveGroup]")
                    false
                }
            } catch (e: SQLException) {
                logger.severe("Error saving archive group '${archiveGroup.name}' to SQLite: ${e.message}")
                false
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in saveArchiveGroup: ${result.cause()?.message}")
                promise.complete(false)
            }
        }

        return promise.future()
    }

    override fun updateArchiveGroup(archiveGroup: ArchiveGroup, enabled: Boolean): Future<Boolean> {
        return saveArchiveGroup(archiveGroup, enabled) // SQLite UPSERT handles both insert and update
    }

    override fun deleteArchiveGroup(name: String): Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        vertx.executeBlocking(Callable {
            val sql = "DELETE FROM $configTableName WHERE name = ?"

            try {
                db.connection?.let { connection ->
                    connection.prepareStatement(sql).use { preparedStatement ->
                        preparedStatement.setString(1, name)
                        val rowsAffected = preparedStatement.executeUpdate()
                        val success = rowsAffected > 0

                        if (success) {
                            logger.info("Archive group '$name' deleted successfully from SQLite")
                        } else {
                            logger.warning("Archive group '$name' not found in SQLite for deletion")
                        }
                        success
                    }
                } ?: run {
                    logger.severe("Deleting archive group not possible without database connection! [deleteArchiveGroup]")
                    false
                }
            } catch (e: SQLException) {
                logger.severe("Error deleting archive group '$name' from SQLite: ${e.message}")
                false
            }
        }).onComplete { result ->
            if (result.succeeded()) {
                promise.complete(result.result())
            } else {
                logger.severe("Error in deleteArchiveGroup: ${result.cause()?.message}")
                promise.complete(false)
            }
        }

        return promise.future()
    }

    private fun resultSetToArchiveGroup(resultSet: ResultSet): ArchiveGroup {
        val name = resultSet.getString("name")

        // Parse topic filter from JSON string
        val topicFilterJson = resultSet.getString("topic_filter")
        val topicFilter = if (topicFilterJson != null) {
            try {
                JsonArray(topicFilterJson).map { it.toString() }
            } catch (e: Exception) {
                logger.warning("Error parsing topic filter JSON: ${e.message}")
                emptyList()
            }
        } else {
            emptyList()
        }

        val retainedOnly = resultSet.getInt("retained_only") == 1
        val lastValType = MessageStoreType.valueOf(resultSet.getString("last_val_type"))
        val archiveType = MessageArchiveType.valueOf(resultSet.getString("archive_type"))

        val lastValRetention = resultSet.getString("last_val_retention")
        val archiveRetention = resultSet.getString("archive_retention")
        val purgeInterval = resultSet.getString("purge_interval")

        val payloadFormatStr = try { resultSet.getString("payload_format") } catch (e: Exception) { null }
        val payloadFormat = PayloadFormat.parse(payloadFormatStr)

        return ArchiveGroup(
            name = name,
            topicFilter = topicFilter,
            retainedOnly = retainedOnly,
            lastValType = lastValType,
            archiveType = archiveType,
            payloadFormat = payloadFormat,
            lastValRetentionMs = lastValRetention?.let { DurationParser.parse(it) },
            archiveRetentionMs = archiveRetention?.let { DurationParser.parse(it) },
            purgeIntervalMs = purgeInterval?.let { DurationParser.parse(it) },
            databaseConfig = JsonObject() // Will be populated from config
        )
    }

    override fun getType(): String = "ConfigStoreSQLite"
}