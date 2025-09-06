package at.rocworks.stores.sqlite

import at.rocworks.Utils
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

/**
 * Singleton SQLite connection manager to prevent multiple connections to the same database file.
 * This solves the SQLITE_BUSY_SNAPSHOT errors caused by multiple verticles accessing the same SQLite file.
 */
object SharedSQLiteConnection {
    private val logger = Utils.getLogger(this::class.java)
    private val connections = ConcurrentHashMap<String, Connection>()
    private lateinit var vertx: Vertx

    fun initialize(vertx: Vertx) {
        this.vertx = vertx
        logger.info("SharedSQLiteConnection initialized")
    }

    fun getConnection(dbPath: String): Connection? {
        return connections.computeIfAbsent(dbPath) { path ->
            try {
                val connection = DriverManager.getConnection("jdbc:sqlite:$path", "", "")
                
                // Configure SQLite for optimal concurrent access - CRITICAL ORDER!
                connection.createStatement().use { statement ->
                    // Enable WAL mode FIRST (must be in autocommit mode)
                    statement.executeUpdate("PRAGMA journal_mode = WAL")
                    statement.executeUpdate("PRAGMA synchronous = NORMAL")
                    statement.executeUpdate("PRAGMA cache_size = -64000")
                    statement.executeUpdate("PRAGMA temp_store = MEMORY")
                    statement.executeUpdate("PRAGMA wal_autocheckpoint = 1000")
                    statement.executeUpdate("PRAGMA busy_timeout = 60000") // 60 seconds
                    statement.executeUpdate("PRAGMA foreign_keys = ON")
                }
                
                logger.info("Shared SQLite connection created for [$path] with WAL mode")
                connection
                
            } catch (e: Exception) {
                logger.severe("Failed to create shared SQLite connection for [$path]: ${e.message}")
                throw RuntimeException("Failed to create connection for $path", e)
            }
        }
    }

    fun <T> executeBlocking(dbPath: String, operation: (Connection) -> T): Future<T> {
        val promise = Promise.promise<T>()
        
        val connection = getConnection(dbPath)
        if (connection == null) {
            promise.fail(RuntimeException("Failed to get SQLite connection for $dbPath"))
            return promise.future()
        }

        // Log warning - this method is being called which can block!
        logger.warning("BLOCKING OPERATION DETECTED on event loop for [$dbPath]")
        
        vertx.executeBlocking(Callable {
            try {
                logger.fine("Executing blocking operation for [$dbPath]")
                val result = operation(connection)
                result
            } catch (e: Exception) {
                logger.warning("SQLite operation failed for [$dbPath]: ${e.message}")
                throw e
            }
        }).onComplete(promise)
        
        return promise.future()
    }

    fun executeBlockingVoid(dbPath: String, operation: (Connection) -> Unit): Future<Void> {
        return executeBlocking(dbPath) {
            operation(it)
            null
        }.map { null as Void? }
    }

    fun closeConnection(dbPath: String) {
        connections.remove(dbPath)?.let { connection ->
            try {
                connection.close()
                logger.info("Shared SQLite connection closed for [$dbPath]")
            } catch (e: Exception) {
                logger.warning("Error closing shared SQLite connection for [$dbPath]: ${e.message}")
            }
        }
    }

    fun closeAll() {
        connections.forEach { (dbPath, connection) ->
            try {
                connection.close()
                logger.info("Shared SQLite connection closed for [$dbPath]")
            } catch (e: Exception) {
                logger.warning("Error closing shared SQLite connection for [$dbPath]: ${e.message}")
            }
        }
        connections.clear()
    }
}