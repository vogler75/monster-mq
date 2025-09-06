package at.rocworks.stores.sqlite

import at.rocworks.Utils
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Types
import java.util.concurrent.ConcurrentHashMap

/**
 * SQLite Verticle that manages database connections and handles all database operations
 * through the Vert.x event bus. This ensures all operations are properly serialized
 * and avoids concurrency issues with SQLite.
 */
class SQLiteVerticle : AbstractVerticle() {
    private val logger = Utils.getLogger(this::class.java)
    private val connections = ConcurrentHashMap<String, Connection>()
    
    companion object {
        // Event bus addresses for different operations
        const val EB_ADDRESS_PREFIX = "sqlite."
        const val EB_EXECUTE_UPDATE = "${EB_ADDRESS_PREFIX}execute.update"
        const val EB_EXECUTE_QUERY = "${EB_ADDRESS_PREFIX}execute.query"
        const val EB_EXECUTE_BATCH = "${EB_ADDRESS_PREFIX}execute.batch"
        const val EB_INIT_DB = "${EB_ADDRESS_PREFIX}init.db"
    }
    
    override fun start(startPromise: Promise<Void>) {
        try {
            // Register event bus consumers
            vertx.eventBus().consumer<JsonObject>(EB_INIT_DB, this::handleInitDb)
            vertx.eventBus().consumer<JsonObject>(EB_EXECUTE_UPDATE, this::handleExecuteUpdate)
            vertx.eventBus().consumer<JsonObject>(EB_EXECUTE_QUERY, this::handleExecuteQuery)
            vertx.eventBus().consumer<JsonObject>(EB_EXECUTE_BATCH, this::handleExecuteBatch)
            
            logger.info("SQLiteVerticle started and listening on event bus")
            startPromise.complete()
        } catch (e: Exception) {
            logger.severe("Failed to start SQLiteVerticle: ${e.message}")
            startPromise.fail(e)
        }
    }
    
    override fun stop(stopPromise: Promise<Void>) {
        try {
            // Close all connections
            connections.forEach { (dbPath, connection) ->
                try {
                    connection.close()
                    logger.info("Closed SQLite connection for [$dbPath]")
                } catch (e: Exception) {
                    logger.warning("Error closing connection for [$dbPath]: ${e.message}")
                }
            }
            connections.clear()
            stopPromise.complete()
        } catch (e: Exception) {
            stopPromise.fail(e)
        }
    }
    
    private fun getOrCreateConnection(dbPath: String): Connection {
        return connections.computeIfAbsent(dbPath) { path ->
            val connection = DriverManager.getConnection("jdbc:sqlite:$path", "", "")
            
            // Configure SQLite for optimal concurrent access
            connection.createStatement().use { statement ->
                statement.executeUpdate("PRAGMA journal_mode = WAL")
                statement.executeUpdate("PRAGMA synchronous = NORMAL")
                statement.executeUpdate("PRAGMA cache_size = -64000")
                statement.executeUpdate("PRAGMA temp_store = MEMORY")
                statement.executeUpdate("PRAGMA wal_autocheckpoint = 1000")
                statement.executeUpdate("PRAGMA busy_timeout = 60000")
                statement.executeUpdate("PRAGMA foreign_keys = ON")
            }
            
            logger.info("Created SQLite connection for [$path] with WAL mode")
            connection
        }
    }
    
    private fun handleInitDb(message: Message<JsonObject>) {
        val request = message.body()
        val dbPath = request.getString("dbPath")
        val initSql = request.getJsonArray("initSql", JsonArray())
        
        try {
            val connection = getOrCreateConnection(dbPath)
            connection.createStatement().use { statement ->
                initSql.forEach { sql ->
                    statement.executeUpdate(sql.toString())
                }
            }
            message.reply(JsonObject().put("success", true))
        } catch (e: Exception) {
            logger.severe("Error initializing database [$dbPath]: ${e.message}")
            message.fail(500, e.message)
        }
    }
    
    private fun handleExecuteUpdate(message: Message<JsonObject>) {
        val request = message.body()
        val dbPath = request.getString("dbPath")
        val sql = request.getString("sql")
        val params = request.getJsonArray("params", JsonArray())
        val useTransaction = request.getBoolean("transaction", true)
        
        logger.info("SQL UPDATE: $sql with params: $params")
        
        try {
            val connection = getOrCreateConnection(dbPath)
            
            if (useTransaction) {
                connection.autoCommit = false
            }
            
            val rowsAffected = connection.prepareStatement(sql).use { preparedStatement ->
                // Set parameters
                params.forEachIndexed { index, value ->
                    when (value) {
                        null -> preparedStatement.setNull(index + 1, Types.NULL)
                        is String -> preparedStatement.setString(index + 1, value)
                        is Number -> {
                            when (value) {
                                is Int -> preparedStatement.setInt(index + 1, value)
                                is Long -> preparedStatement.setLong(index + 1, value)
                                is Double -> preparedStatement.setDouble(index + 1, value)
                                is Float -> preparedStatement.setFloat(index + 1, value)
                                else -> preparedStatement.setObject(index + 1, value)
                            }
                        }
                        is Boolean -> preparedStatement.setBoolean(index + 1, value)
                        is ByteArray -> preparedStatement.setBytes(index + 1, value)
                        is JsonArray -> preparedStatement.setString(index + 1, value.encode())
                        is JsonObject -> preparedStatement.setString(index + 1, value.encode())
                        else -> preparedStatement.setObject(index + 1, value)
                    }
                }
                preparedStatement.executeUpdate()
            }
            
            if (useTransaction) {
                connection.commit()
                connection.autoCommit = true
            }
            
            message.reply(JsonObject().put("success", true).put("rowsAffected", rowsAffected))
        } catch (e: Exception) {
            logger.severe("Error executing update: ${e.message}")
            try {
                val connection = connections[dbPath]
                connection?.rollback()
                connection?.autoCommit = true
            } catch (re: Exception) {
                logger.warning("Error rolling back transaction: ${re.message}")
            }
            message.fail(500, e.message)
        }
    }
    
    private fun handleExecuteQuery(message: Message<JsonObject>) {
        val request = message.body()
        val dbPath = request.getString("dbPath")
        val sql = request.getString("sql")
        val params = request.getJsonArray("params", JsonArray())
        
        logger.info("SQL QUERY: $sql with params: $params")
        
        try {
            val connection = getOrCreateConnection(dbPath)
            val results = JsonArray()
            
            connection.prepareStatement(sql).use { preparedStatement ->
                // Set parameters
                params.forEachIndexed { index, value ->
                    when (value) {
                        null -> preparedStatement.setNull(index + 1, Types.NULL)
                        is String -> preparedStatement.setString(index + 1, value)
                        is Number -> {
                            when (value) {
                                is Int -> preparedStatement.setInt(index + 1, value)
                                is Long -> preparedStatement.setLong(index + 1, value)
                                is Double -> preparedStatement.setDouble(index + 1, value)
                                is Float -> preparedStatement.setFloat(index + 1, value)
                                else -> preparedStatement.setObject(index + 1, value)
                            }
                        }
                        is Boolean -> preparedStatement.setBoolean(index + 1, value)
                        else -> preparedStatement.setObject(index + 1, value)
                    }
                }
                
                val resultSet = preparedStatement.executeQuery()
                val metaData = resultSet.metaData
                val columnCount = metaData.columnCount
                
                while (resultSet.next()) {
                    val row = JsonObject()
                    for (i in 1..columnCount) {
                        val columnName = metaData.getColumnName(i)
                        val value = when (metaData.getColumnType(i)) {
                            Types.VARCHAR, Types.CHAR, Types.LONGVARCHAR -> resultSet.getString(i)
                            Types.INTEGER, Types.SMALLINT, Types.TINYINT -> resultSet.getInt(i)
                            Types.BIGINT -> resultSet.getLong(i)
                            Types.FLOAT, Types.REAL -> resultSet.getFloat(i)
                            Types.DOUBLE -> resultSet.getDouble(i)
                            Types.BOOLEAN, Types.BIT -> resultSet.getBoolean(i)
                            Types.BLOB, Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> resultSet.getBytes(i)
                            else -> resultSet.getObject(i)
                        }
                        if (value != null) {
                            row.put(columnName, value)
                        }
                    }
                    results.add(row)
                }
            }
            
            message.reply(JsonObject().put("success", true).put("results", results))
        } catch (e: Exception) {
            logger.severe("Error executing query: ${e.message}")
            message.fail(500, e.message)
        }
    }
    
    private fun handleExecuteBatch(message: Message<JsonObject>) {
        val request = message.body()
        val dbPath = request.getString("dbPath")
        val sql = request.getString("sql")
        val batchParams = request.getJsonArray("batchParams", JsonArray())
        
        try {
            val connection = getOrCreateConnection(dbPath)
            connection.autoCommit = false
            
            connection.prepareStatement(sql).use { preparedStatement ->
                batchParams.forEach { paramsObj ->
                    val params = paramsObj as JsonArray
                    params.forEachIndexed { index, value ->
                        when (value) {
                            null -> preparedStatement.setNull(index + 1, Types.NULL)
                            is String -> preparedStatement.setString(index + 1, value)
                            is Number -> {
                                when (value) {
                                    is Int -> preparedStatement.setInt(index + 1, value)
                                    is Long -> preparedStatement.setLong(index + 1, value)
                                    is Double -> preparedStatement.setDouble(index + 1, value)
                                    is Float -> preparedStatement.setFloat(index + 1, value)
                                    else -> preparedStatement.setObject(index + 1, value)
                                }
                            }
                            is Boolean -> preparedStatement.setBoolean(index + 1, value)
                            is ByteArray -> preparedStatement.setBytes(index + 1, value)
                            is JsonArray -> preparedStatement.setString(index + 1, value.encode())
                            is JsonObject -> preparedStatement.setString(index + 1, value.encode())
                            else -> preparedStatement.setObject(index + 1, value)
                        }
                    }
                    preparedStatement.addBatch()
                }
                
                val results = preparedStatement.executeBatch()
                connection.commit()
                connection.autoCommit = true
                
                message.reply(JsonObject().put("success", true).put("results", JsonArray(results.toList())))
            }
        } catch (e: Exception) {
            logger.severe("Error executing batch: ${e.message}")
            try {
                val connection = connections[dbPath]
                connection?.rollback()
                connection?.autoCommit = true
            } catch (re: Exception) {
                logger.warning("Error rolling back transaction: ${re.message}")
            }
            message.fail(500, e.message)
        }
    }
}