package at.rocworks.stores.sqlite

import at.rocworks.Utils
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Types
import java.util.concurrent.TimeUnit

/**
 * Client helper for interacting with SQLiteVerticle via event bus.
 * Provides convenient methods for common database operations.
 *
 * Also provides a direct read-only JDBC connection for synchronous queries
 * that would otherwise deadlock when called from within vertx.executeBlocking
 * (the event bus reply cannot be dispatched while the caller blocks).
 * SQLite WAL mode supports concurrent readers safely.
 */
class SQLiteClient(private val vertx: Vertx, private val dbPath: String) {
    private val logger = Utils.getLogger(this::class.java)

    /** Absolute path to the DB file, resolved at construction time when the working directory is correct. */
    private val absoluteDbPath: String = File(dbPath).absolutePath

    /** Direct read-only JDBC connection for synchronous queries (thread-safe for reads in WAL mode) */
    private val readConnection: Connection by lazy {
        val conn = DriverManager.getConnection("jdbc:sqlite:$absoluteDbPath", "", "")
        conn.createStatement().use { stmt ->
            stmt.executeUpdate("PRAGMA journal_mode = WAL")
            stmt.executeUpdate("PRAGMA synchronous = NORMAL")
            stmt.executeUpdate("PRAGMA cache_size = -16000")
            stmt.executeUpdate("PRAGMA query_only = ON")
            stmt.executeUpdate("PRAGMA busy_timeout = 5000")
        }
        logger.info { "Created direct read-only SQLite connection for [$absoluteDbPath]" }
        conn
    }
    
    /**
     * Execute an UPDATE/INSERT/DELETE statement asynchronously
     */
    fun executeUpdate(sql: String, params: JsonArray = JsonArray(), useTransaction: Boolean = true): Future<Int> {
        val request = JsonObject()
            .put("dbPath", dbPath)
            .put("sql", sql)
            .put("params", params)
            .put("transaction", useTransaction)
            
        return vertx.eventBus()
            .request<JsonObject>(SQLiteVerticle.EB_EXECUTE_UPDATE, request)
            .map { message ->
                val response = message.body()
                if (response.getBoolean("success", false)) {
                    response.getInteger("rowsAffected", 0)
                } else {
                    throw RuntimeException("SQL update failed: $sql")
                }
            }
    }
    
    /**
     * Execute an UPDATE/INSERT/DELETE statement and ignore the result (fire-and-forget)
     */
    fun executeUpdateAsync(sql: String, params: JsonArray = JsonArray(), useTransaction: Boolean = true) {
        executeUpdate(sql, params, useTransaction).onComplete { result ->
            if (result.failed()) {
                logger.warning("Async SQL update failed [$sql]: ${result.cause()?.message}")
            }
        }
    }
    
    /**
     * Execute a SELECT query and return results
     */
    fun executeQuery(sql: String, params: JsonArray = JsonArray()): Future<JsonArray> {
        val request = JsonObject()
            .put("dbPath", dbPath)
            .put("sql", sql)
            .put("params", params)
            
        return vertx.eventBus()
            .request<JsonObject>(SQLiteVerticle.EB_EXECUTE_QUERY, request)
            .map { message ->
                val response = message.body()
                if (response.getBoolean("success", false)) {
                    response.getJsonArray("results", JsonArray())
                } else {
                    throw RuntimeException("SQL query failed: $sql")
                }
            }
    }
    
    /**
     * Execute a SELECT query synchronously (for interface compatibility)
     * WARNING: This blocks the current thread - use sparingly!
     */
    fun executeQuerySync(sql: String, params: JsonArray = JsonArray(), timeoutMs: Long = 1000): JsonArray {
        return try {
            executeQuery(sql, params)
                .toCompletionStage()
                .toCompletableFuture()
                .get(timeoutMs, TimeUnit.MILLISECONDS)
        } catch (e: Exception) {
            logger.warning("Sync query failed [$sql]: ${e.message}")
            JsonArray()
        }
    }

    fun executeUpdateSync(sql: String, params: JsonArray = JsonArray(), timeoutMs: Long = 1000): Int {
        return try {
            executeUpdate(sql, params)
                .toCompletionStage()
                .toCompletableFuture()
                .get(timeoutMs, TimeUnit.MILLISECONDS)
        } catch (e: Exception) {
            logger.warning("Sync update failed [$sql]: ${e.message}")
            0
        }
    }

    /**
     * Execute batch updates (multiple statements with different parameters)
     */
    fun executeBatch(sql: String, batchParams: JsonArray): Future<JsonArray> {
        val request = JsonObject()
            .put("dbPath", dbPath)
            .put("sql", sql)
            .put("batchParams", batchParams)

        return vertx.eventBus()
            .request<JsonObject>(SQLiteVerticle.EB_EXECUTE_BATCH, request)
            .map { message ->
                val response = message.body()
                if (response.getBoolean("success", false)) {
                    response.getJsonArray("results", JsonArray())
                } else {
                    throw RuntimeException("SQL batch failed: $sql")
                }
            }
    }

    /**
     * Initialize database with multiple SQL statements
     */
    fun initDatabase(initSql: JsonArray): Future<Void> {
        val request = JsonObject()
            .put("dbPath", dbPath)
            .put("initSql", initSql)
            
        return vertx.eventBus()
            .request<JsonObject>(SQLiteVerticle.EB_INIT_DB, request)
            .mapEmpty()
    }

    /**
     * Execute a SELECT query using a direct JDBC connection, bypassing the event bus.
     * Use this for synchronous queries called from within vertx.executeBlocking or worker threads,
     * where executeQuerySync would deadlock waiting for an event bus reply.
     * Safe for reads in WAL mode — all writes remain serialized through the SQLiteVerticle.
     */
    @Synchronized
    fun executeQueryDirect(sql: String, params: JsonArray = JsonArray()): JsonArray {
        val results = JsonArray()
        try {
            readConnection.prepareStatement(sql).use { stmt ->
                params.forEachIndexed { index, value ->
                    when (value) {
                        null -> stmt.setNull(index + 1, Types.NULL)
                        is String -> stmt.setString(index + 1, value)
                        is Number -> {
                            when (value) {
                                is Int -> stmt.setInt(index + 1, value)
                                is Long -> stmt.setLong(index + 1, value)
                                is Double -> stmt.setDouble(index + 1, value)
                                is Float -> stmt.setFloat(index + 1, value)
                                else -> stmt.setObject(index + 1, value)
                            }
                        }
                        is Boolean -> stmt.setBoolean(index + 1, value)
                        else -> stmt.setObject(index + 1, value)
                    }
                }
                val rs = stmt.executeQuery()
                val meta = rs.metaData
                val columnCount = meta.columnCount
                while (rs.next()) {
                    val row = JsonObject()
                    for (i in 1..columnCount) {
                        val columnName = meta.getColumnName(i)
                        // SQLite JDBC driver reports unreliable column types (e.g. BLOB as VARCHAR).
                        // Use getObject() and dispatch on the actual Java runtime type instead.
                        val rawValue = rs.getObject(i)
                        if (rawValue != null) {
                            when (rawValue) {
                                is ByteArray -> row.put(columnName, rawValue)
                                is Int -> row.put(columnName, rawValue)
                                is Long -> row.put(columnName, rawValue)
                                is Float -> row.put(columnName, rawValue)
                                is Double -> row.put(columnName, rawValue)
                                is Boolean -> row.put(columnName, rawValue)
                                is String -> row.put(columnName, rawValue)
                                else -> row.put(columnName, rawValue.toString())
                            }
                        }
                    }
                    results.add(row)
                }
            }
        } catch (e: Exception) {
            logger.warning("Direct query failed [$sql]: ${e.message}")
        }
        return results
    }
}