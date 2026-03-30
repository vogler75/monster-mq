package at.rocworks.stores.sqlite

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.stores.*
import at.rocworks.handlers.ArchiveGroup
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.util.logging.Logger

/**
 * SQLite implementation of IConfigStore for archive group configuration management.
 * Routes all operations through SQLiteClient → SQLiteVerticle.
 */
class ArchiveConfigStoreSQLite(
    private val dbPath: String
) : AbstractVerticle(), IArchiveConfigStore {

    private val logger: Logger = Utils.getLogger(this::class.java)
    private val configTableName = "archiveconfigs"
    private lateinit var sqliteClient: SQLiteClient

    init {
        logger.level = Const.DEBUG_LEVEL
    }

    override fun start(startPromise: Promise<Void>) {
        sqliteClient = SQLiteClient(vertx, dbPath)
        val initSql = JsonArray()
            .add("""
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
            """.trimIndent())

        sqliteClient.initDatabase(initSql).onComplete { result ->
            if (result.succeeded()) {
                logger.info("Archive config table created/verified in SQLite")
                startPromise.complete()
            } else {
                logger.severe("Failed to initialize ConfigStoreSQLite: ${result.cause()?.message}")
                startPromise.fail(result.cause())
            }
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        // Connection is managed by SQLiteVerticle — nothing to close here
        logger.info("SQLite ConfigStore stopped")
        stopPromise.complete()
    }

    private fun mapRowToArchiveGroupConfig(row: JsonObject): ArchiveGroupConfig? {
        return try {
            val name = row.getString("name")
            val enabled = row.getInteger("enabled", 0) == 1

            val topicFilterJson = row.getString("topic_filter")
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

            val retainedOnly = row.getInteger("retained_only", 0) == 1
            val lastValType = MessageStoreType.valueOf(row.getString("last_val_type"))
            val archiveType = MessageArchiveType.valueOf(row.getString("archive_type"))

            val lastValRetention = row.getString("last_val_retention")
            val archiveRetention = row.getString("archive_retention")
            val purgeInterval = row.getString("purge_interval")

            val payloadFormatStr = row.getString("payload_format")
            val payloadFormat = PayloadFormat.parse(payloadFormatStr)

            val archiveGroup = ArchiveGroup(
                name = name,
                topicFilter = topicFilter,
                retainedOnly = retainedOnly,
                lastValType = lastValType,
                archiveType = archiveType,
                payloadFormat = payloadFormat,
                lastValRetentionMs = lastValRetention?.let { Utils.parseDuration(it) },
                archiveRetentionMs = archiveRetention?.let { Utils.parseDuration(it) },
                purgeIntervalMs = purgeInterval?.let { Utils.parseDuration(it) },
                databaseConfig = JsonObject()
            )

            ArchiveGroupConfig(archiveGroup, enabled)
        } catch (e: Exception) {
            logger.warning("Error parsing archive group from SQLite: ${e.message}")
            null
        }
    }

    override fun getAllArchiveGroups(): Future<List<ArchiveGroupConfig>> {
        val sql = "SELECT * FROM $configTableName ORDER BY name"
        return sqliteClient.executeQuery(sql).map { results ->
            results.mapNotNull { mapRowToArchiveGroupConfig(it as JsonObject) }
        }.otherwise { e ->
            logger.severe("Error retrieving archive groups from SQLite: ${e.message}")
            emptyList()
        }
    }

    override fun getArchiveGroup(name: String): Future<ArchiveGroupConfig?> {
        val sql = "SELECT * FROM $configTableName WHERE name = ?"
        val params = JsonArray().add(name)
        return sqliteClient.executeQuery(sql, params).map { results ->
            if (results.size() > 0) {
                mapRowToArchiveGroupConfig(results.getJsonObject(0))
            } else null
        }.otherwise { e ->
            logger.severe("Error retrieving archive group '$name' from SQLite: ${e.message}")
            null
        }
    }

    override fun saveArchiveGroup(archiveGroup: ArchiveGroup, enabled: Boolean): Future<Boolean> {
        val sql = """
            INSERT OR REPLACE INTO $configTableName
            (name, enabled, topic_filter, retained_only, last_val_type, archive_type, last_val_retention, archive_retention, purge_interval, payload_format, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """.trimIndent()

        val topicFilterJson = JsonArray(archiveGroup.topicFilter).encode()
        val params = JsonArray()
            .add(archiveGroup.name)
            .add(if (enabled) 1 else 0)
            .add(topicFilterJson)
            .add(if (archiveGroup.retainedOnly) 1 else 0)
            .add(archiveGroup.getLastValType().name)
            .add(archiveGroup.getArchiveType().name)
            .add(archiveGroup.getLastValRetention())
            .add(archiveGroup.getArchiveRetention())
            .add(archiveGroup.getPurgeInterval())
            .add(archiveGroup.payloadFormat.name)

        return sqliteClient.executeUpdate(sql, params).map { rowsAffected ->
            val success = rowsAffected > 0
            if (success) {
                logger.info("Archive group '${archiveGroup.name}' saved successfully to SQLite")
            }
            success
        }.otherwise { e ->
            logger.severe("Error saving archive group '${archiveGroup.name}' to SQLite: ${e.message}")
            false
        }
    }

    override fun updateArchiveGroup(archiveGroup: ArchiveGroup, enabled: Boolean): Future<Boolean> {
        return saveArchiveGroup(archiveGroup, enabled)
    }

    override fun deleteArchiveGroup(name: String): Future<Boolean> {
        val sql = "DELETE FROM $configTableName WHERE name = ?"
        val params = JsonArray().add(name)

        return sqliteClient.executeUpdate(sql, params).map { rowsAffected ->
            val success = rowsAffected > 0
            if (success) {
                logger.info("Archive group '$name' deleted successfully from SQLite")
            } else {
                logger.warning("Archive group '$name' not found in SQLite for deletion")
            }
            success
        }.otherwise { e ->
            logger.severe("Error deleting archive group '$name' from SQLite: ${e.message}")
            false
        }
    }

    override fun getType(): String = "ConfigStoreSQLite"

    override suspend fun tableExists(): Boolean {
        return try {
            val sql = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?"
            val params = JsonArray().add(configTableName)
            val results = sqliteClient.executeQuerySync(sql, params)
            results.size() > 0
        } catch (e: Exception) {
            logger.warning("Error checking if table [$configTableName] exists: ${e.message}")
            false
        }
    }
}