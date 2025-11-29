package at.rocworks.stores.cratedb

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.stores.*
import at.rocworks.handlers.ArchiveGroup
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.sql.*
import java.util.logging.Logger
import java.util.concurrent.Callable

/**
 * CrateDB implementation of IConfigStore for archive group configuration management
 */
class ArchiveConfigStoreCrateDB(
    private val url: String,
    private val username: String,
    private val password: String
) : AbstractVerticle(), IArchiveConfigStore {

    private val logger: Logger = Utils.getLogger(this::class.java)
    private val configTableName = "archiveconfigs"

    private val db = object : DatabaseConnection(logger, url, username, password) {
        override fun init(connection: Connection): io.vertx.core.Future<Void> {
            val promise = Promise.promise<Void>()
            try {
                createConfigTable(connection)
                promise.complete()
            } catch (e: Exception) {
                logger.severe("Failed to initialize ConfigStoreCrateDB: ${e.message}")
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
            logger.info("CrateDB ConfigStore stopped")
            stopPromise.complete()
        } catch (e: Exception) {
            logger.warning("Error stopping CrateDB ConfigStore: ${e.message}")
            stopPromise.complete() // Complete anyway
        }
    }

    private fun createConfigTable(connection: Connection) {
        try {
            // First, try to drop the old table if it exists with wrong schema
            // CrateDB requires explicit DROP TABLE with CASCADE for schema migration
            connection.createStatement().use { statement ->
                try {
                    // Check if table exists
                    val checkSql = "SELECT 1 FROM information_schema.tables WHERE table_name = ?"
                    connection.prepareStatement(checkSql).use { stmt ->
                        stmt.setString(1, configTableName)
                        val rs = stmt.executeQuery()
                        if (rs.next()) {
                            // Table exists, drop it to recreate with new schema
                            logger.info("Dropping existing archive config table to migrate schema")
                            statement.execute("DROP TABLE IF EXISTS $configTableName")
                        }
                    }
                } catch (e: Exception) {
                    // If check fails, continue anyway
                    logger.finer("Could not check for existing table: ${e.message}")
                }
            }
        } catch (e: Exception) {
            logger.finer("Could not drop existing table: ${e.message}")
        }

        val sql = """
            CREATE TABLE IF NOT EXISTS $configTableName (
                name STRING PRIMARY KEY,
                enabled BOOLEAN NOT NULL DEFAULT FALSE,
                topic_filter TEXT NOT NULL,
                retained_only BOOLEAN NOT NULL DEFAULT FALSE,
                last_val_type STRING NOT NULL,
                archive_type STRING NOT NULL,
                last_val_retention STRING,
                archive_retention STRING,
                purge_interval STRING,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                payload_format STRING DEFAULT 'DEFAULT'
            )
        """.trimIndent()

        connection.createStatement().use { statement ->
            statement.execute(sql)
        }
        logger.info("Archive config table created/verified in CrateDB")
    }

    override fun getAllArchiveGroups(): io.vertx.core.Future<List<ArchiveGroupConfig>> {
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
                                val enabled = resultSet.getBoolean("enabled")
                                results.add(ArchiveGroupConfig(archiveGroup, enabled))
                            } catch (e: Exception) {
                                logger.warning("Error parsing archive group from CrateDB: ${e.message}")
                            }
                        }

                        results
                    }
                } ?: run {
                    logger.severe("Getting archive groups not possible without database connection! [getAllArchiveGroups]")
                    emptyList()
                }
            } catch (e: SQLException) {
                logger.severe("Error retrieving archive groups from CrateDB: ${e.message}")
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

    override fun getArchiveGroup(name: String): io.vertx.core.Future<ArchiveGroupConfig?> {
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
                            val enabled = resultSet.getBoolean("enabled")
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
                logger.severe("Error retrieving archive group '$name' from CrateDB: ${e.message}")
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

    override fun saveArchiveGroup(archiveGroup: ArchiveGroup, enabled: Boolean): io.vertx.core.Future<Boolean> {
        val promise = Promise.promise<Boolean>()

        vertx.executeBlocking(Callable {
            val sql = """
                INSERT INTO $configTableName
                (name, enabled, topic_filter, retained_only, last_val_type, archive_type, last_val_retention, archive_retention, purge_interval, payload_format, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (name) DO UPDATE SET
                    enabled = EXCLUDED.enabled,
                    topic_filter = EXCLUDED.topic_filter,
                    retained_only = EXCLUDED.retained_only,
                    last_val_type = EXCLUDED.last_val_type,
                    archive_type = EXCLUDED.archive_type,
                    last_val_retention = EXCLUDED.last_val_retention,
                    archive_retention = EXCLUDED.archive_retention,
                    purge_interval = EXCLUDED.purge_interval,
                    payload_format = EXCLUDED.payload_format,
                    updated_at = CURRENT_TIMESTAMP
            """.trimIndent()

            try {
                db.connection?.let { connection ->
                    connection.prepareStatement(sql).use { preparedStatement ->
                        val lastValRetention = archiveGroup.getLastValRetentionMs()?.let { Utils.formatDuration(it) }
                        val archiveRetention = archiveGroup.getArchiveRetentionMs()?.let { Utils.formatDuration(it) }
                        val purgeInterval = archiveGroup.getPurgeIntervalMs()?.let { Utils.formatDuration(it) }

                        preparedStatement.setString(1, archiveGroup.name)
                        preparedStatement.setBoolean(2, enabled)

                        // Convert topic filter list to JSON string
                        val topicFilterJson = io.vertx.core.json.JsonArray(archiveGroup.topicFilter).encode()
                        preparedStatement.setString(3, topicFilterJson)

                        preparedStatement.setBoolean(4, archiveGroup.retainedOnly)
                        preparedStatement.setString(5, archiveGroup.getLastValType().name)
                        preparedStatement.setString(6, archiveGroup.getArchiveType().name)
                        preparedStatement.setString(7, lastValRetention)
                        preparedStatement.setString(8, archiveRetention)
                        preparedStatement.setString(9, purgeInterval)
                        preparedStatement.setString(10, archiveGroup.payloadFormat.name)

                        val rowsAffected = preparedStatement.executeUpdate()
                        val success = rowsAffected > 0

                        if (success) {
                            logger.info("Archive group '${archiveGroup.name}' saved successfully to CrateDB")
                        }
                        success
                    }
                } ?: run {
                    logger.severe("Saving archive group not possible without database connection! [saveArchiveGroup]")
                    false
                }
            } catch (e: SQLException) {
                logger.severe("Error saving archive group '${archiveGroup.name}' to CrateDB: ${e.message}")
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

    override fun updateArchiveGroup(archiveGroup: ArchiveGroup, enabled: Boolean): io.vertx.core.Future<Boolean> {
        return saveArchiveGroup(archiveGroup, enabled) // CrateDB UPSERT handles both insert and update
    }

    override fun deleteArchiveGroup(name: String): io.vertx.core.Future<Boolean> {
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
                            logger.info("Archive group '$name' deleted successfully from CrateDB")
                        } else {
                            logger.warning("Archive group '$name' not found in CrateDB for deletion")
                        }
                        success
                    }
                } ?: run {
                    logger.severe("Deleting archive group not possible without database connection! [deleteArchiveGroup]")
                    false
                }
            } catch (e: SQLException) {
                logger.severe("Error deleting archive group '$name' from CrateDB: ${e.message}")
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
                io.vertx.core.json.JsonArray(topicFilterJson).list.map { it.toString() }
            } catch (e: Exception) {
                logger.warning("Failed to parse topic_filter JSON: ${e.message}")
                emptyList()
            }
        } else {
            emptyList()
        }

        val retainedOnly = resultSet.getBoolean("retained_only")
        val lastValType = MessageStoreType.valueOf(resultSet.getString("last_val_type"))
        val archiveType = MessageArchiveType.valueOf(resultSet.getString("archive_type"))

        val lastValRetention = resultSet.getString("last_val_retention")
        val archiveRetention = resultSet.getString("archive_retention")
        val purgeInterval = resultSet.getString("purge_interval")

        val payloadFormatStr = try { resultSet.getString("payload_format") } catch (e: Exception) { null }
        val payloadFormat = at.rocworks.stores.PayloadFormat.parse(payloadFormatStr)

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
            databaseConfig = JsonObject() // Will be populated from config
        )
    }

    override fun getType(): String = "ConfigStoreCrateDB"

    override suspend fun tableExists(): Boolean {
        return try {
            db.connection?.let { connection ->
                val sql = "SELECT 1 FROM information_schema.tables WHERE table_name = ?"
                connection.prepareStatement(sql).use { preparedStatement ->
                    preparedStatement.setString(1, configTableName)
                    val resultSet = preparedStatement.executeQuery()
                    resultSet.next()
                }
            } ?: false
        } catch (e: SQLException) {
            logger.warning("Error checking if table [$configTableName] exists: ${e.message}")
            false
        }
    }
}