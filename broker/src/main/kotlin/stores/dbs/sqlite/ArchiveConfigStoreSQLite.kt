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
 * Routes all operations through SQLiteVerticle via SQLiteClient to avoid
 * holding a separate JDBC connection that can cause SQLITE_BUSY_SNAPSHOT.
 */
class ArchiveConfigStoreSQLite(
    private val dbPath: String
) : AbstractVerticle(), IArchiveConfigStore {

    private val logger: Logger = Utils.getLogger(this::class.java)
    private val configTableName = "archiveconfigs"
    private val connectionTableName = "databaseconnections"

    private lateinit var sqliteClient: SQLiteClient


    override fun start(startPromise: Promise<Void>) {
        sqliteClient = SQLiteClient(vertx, dbPath)

        val initSql = JsonArray().add("""
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
                payload_format TEXT DEFAULT 'DEFAULT',
                database_connection_name TEXT
            )
        """.trimIndent())
            .add("""
                CREATE TABLE IF NOT EXISTS $connectionTableName (
                    name TEXT PRIMARY KEY,
                    type TEXT NOT NULL,
                    url TEXT NOT NULL,
                    username TEXT,
                    password TEXT,
                    database_name TEXT,
                    schema_name TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """.trimIndent())

        sqliteClient.initDatabase(initSql).onComplete { result ->
            if (result.succeeded()) {
                sqliteClient.executeUpdate(
                    "ALTER TABLE $configTableName ADD COLUMN database_connection_name TEXT",
                    useTransaction = false
                ).onFailure { /* Column already exists on upgraded databases. */ }
                logger.info("Archive config table created/verified in SQLite")
                startPromise.complete()
            } else {
                logger.severe("Failed to initialize ConfigStoreSQLite: ${result.cause()?.message}")
                startPromise.fail(result.cause())
            }
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        // Connection is owned by SQLiteVerticle — nothing to close here
        stopPromise.complete()
    }

    private fun toBool(value: Any?): Boolean {
        return when (value) {
            is Boolean -> value
            is Number -> value.toInt() != 0
            else -> false
        }
    }

    override fun getAllArchiveGroups(): Future<List<ArchiveGroupConfig>> {
        val sql = "SELECT * FROM $configTableName ORDER BY name"

        return sqliteClient.executeQuery(sql).map { results ->
            (0 until results.size()).mapNotNull { i ->
                try {
                    val row = results.getJsonObject(i)
                    val archiveGroup = rowToArchiveGroup(row)
                    val enabled = toBool(row.getValue("enabled"))
                    ArchiveGroupConfig(archiveGroup, enabled)
                } catch (e: Exception) {
                    logger.warning("Error parsing archive group from SQLite: ${e.message}")
                    null
                }
            }
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
                val row = results.getJsonObject(0)
                val archiveGroup = rowToArchiveGroup(row)
                val enabled = toBool(row.getValue("enabled"))
                ArchiveGroupConfig(archiveGroup, enabled)
            } else {
                null
            }
        }.otherwise { e ->
            logger.severe("Error retrieving archive group '$name' from SQLite: ${e.message}")
            null
        }
    }

    override fun saveArchiveGroup(archiveGroup: ArchiveGroup, enabled: Boolean): Future<Boolean> {
        val sql = """
            INSERT OR REPLACE INTO $configTableName
            (name, enabled, topic_filter, retained_only, last_val_type, archive_type, last_val_retention, archive_retention, purge_interval, payload_format, database_connection_name, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
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
            .add(archiveGroup.getDatabaseConnectionName())

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
        return saveArchiveGroup(archiveGroup, enabled) // SQLite UPSERT handles both insert and update
    }

    override fun getAllDatabaseConnections(): Future<List<DatabaseConnectionConfig>> {
        return sqliteClient.executeQuery("SELECT * FROM $connectionTableName ORDER BY name").map { results ->
            (0 until results.size()).map { i -> rowToDatabaseConnection(results.getJsonObject(i)) }
        }.otherwise { e ->
            logger.severe("Error retrieving database connections from SQLite: ${e.message}")
            emptyList()
        }
    }

    override fun getDatabaseConnection(name: String): Future<DatabaseConnectionConfig?> {
        return sqliteClient.executeQuery(
            "SELECT * FROM $connectionTableName WHERE name = ?",
            JsonArray().add(name)
        ).map { results ->
            if (results.size() > 0) rowToDatabaseConnection(results.getJsonObject(0)) else null
        }.otherwise { e ->
            logger.severe("Error retrieving database connection '$name' from SQLite: ${e.message}")
            null
        }
    }

    override fun saveDatabaseConnection(connection: DatabaseConnectionConfig): Future<Boolean> {
        val sql = """
            INSERT OR REPLACE INTO $connectionTableName
            (name, type, url, username, password, database_name, schema_name, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """.trimIndent()
        val params = JsonArray()
            .add(connection.name)
            .add(connection.type.name)
            .add(connection.url)
            .add(connection.username)
            .add(connection.password)
            .add(connection.database)
            .add(connection.schema)
        return sqliteClient.executeUpdate(sql, params).map { it > 0 }.otherwise { e ->
            logger.severe("Error saving database connection '${connection.name}' to SQLite: ${e.message}")
            false
        }
    }

    override fun deleteDatabaseConnection(name: String): Future<Boolean> {
        return sqliteClient.executeUpdate(
            "DELETE FROM $connectionTableName WHERE name = ?",
            JsonArray().add(name)
        ).map { it > 0 }.otherwise { e ->
            logger.severe("Error deleting database connection '$name' from SQLite: ${e.message}")
            false
        }
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

    private fun rowToArchiveGroup(row: JsonObject): ArchiveGroup {
        val name = row.getString("name")

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

        val retainedOnly = toBool(row.getValue("retained_only"))
        val lastValType = MessageStoreType.valueOf(row.getString("last_val_type"))
        val archiveType = MessageArchiveType.valueOf(row.getString("archive_type"))

        val lastValRetention = row.getString("last_val_retention")
        val archiveRetention = row.getString("archive_retention")
        val purgeInterval = row.getString("purge_interval")

        val payloadFormatStr = try { row.getString("payload_format") } catch (e: Exception) { null }
        val payloadFormat = PayloadFormat.parse(payloadFormatStr)

        return ArchiveGroup(
            name = name,
            topicFilter = topicFilter,
            retainedOnly = retainedOnly,
            lastValType = lastValType,
            archiveType = archiveType,
            payloadFormat = payloadFormat,
            lastValRetentionMs = lastValRetention?.let { Utils.parseDuration(it) },
            archiveRetentionMs = archiveRetention?.let { Utils.parseDuration(it) },
            purgeIntervalMs = purgeInterval?.let { Utils.parseDuration(it) },
            lastValRetentionStr = lastValRetention,
            archiveRetentionStr = archiveRetention,
            purgeIntervalStr = purgeInterval,
            databaseConnectionName = row.getString("database_connection_name"),
            databaseConfig = JsonObject() // Will be populated from config
        )
    }

    private fun rowToDatabaseConnection(row: JsonObject): DatabaseConnectionConfig {
        return DatabaseConnectionConfig(
            name = row.getString("name"),
            type = DatabaseConnectionType.valueOf(row.getString("type")),
            url = row.getString("url"),
            username = row.getString("username"),
            password = row.getString("password"),
            database = row.getString("database_name"),
            schema = row.getString("schema_name"),
            createdAt = row.getString("created_at"),
            updatedAt = row.getString("updated_at")
        )
    }

    override fun getType(): String = "ConfigStoreSQLite"

    override suspend fun tableExists(): Boolean {
        return try {
            val results = sqliteClient.executeQuerySync(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                JsonArray().add(configTableName)
            )
            results.size() > 0
        } catch (e: Exception) {
            logger.warning("Error checking if table [$configTableName] exists: ${e.message}")
            false
        }
    }
}
