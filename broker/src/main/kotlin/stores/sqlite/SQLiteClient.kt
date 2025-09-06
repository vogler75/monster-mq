package at.rocworks.stores.sqlite

import at.rocworks.Utils
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.util.concurrent.TimeUnit

/**
 * Client helper for interacting with SQLiteVerticle via event bus.
 * Provides convenient methods for common database operations.
 */
class SQLiteClient(private val vertx: Vertx, private val dbPath: String) {
    private val logger = Utils.getLogger(this::class.java)
    
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
}