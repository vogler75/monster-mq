package at.rocworks.flowengine

import at.rocworks.Utils
import at.rocworks.stores.devices.DatabaseConnectionConfig
import com.fasterxml.jackson.databind.ObjectMapper
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.sql.*
import java.util.logging.Logger

/**
 * JDBC connection manager using standard Java JDBC DriverManager
 */
class JdbcManager {
    private val logger: Logger = Utils.getLogger(JdbcManager::class.java)
    private val objectMapper = ObjectMapper()

    /**
     * Get a direct JDBC connection
     */
    fun getConnection(config: DatabaseConnectionConfig): Connection {
        Class.forName(config.getDriverClassName())
        return DriverManager.getConnection(
            config.jdbcUrl,
            config.username,
            config.password
        )
    }

    /**
     * Test a database connection
     */
    fun testConnection(config: DatabaseConnectionConfig): Result<String> {
        return try {
            Class.forName(config.getDriverClassName())
            val conn = DriverManager.getConnection(
                config.jdbcUrl,
                config.username,
                config.password
            )
            conn.use { connection ->
                val metaData = connection.metaData
                Result.success("Connected to ${metaData.databaseProductName} ${metaData.databaseProductVersion}")
            }
        } catch (e: Exception) {
            logger.severe("Connection test failed for '${config.name}': ${e.message}")
            Result.failure(e)
        }
    }

    /**
     * Convert objects to JSON using Jackson ObjectMapper
     * Handles complex objects like Neo4j nodes, relationships, etc.
     */
    private fun convertToJsonSerializable(obj: Any?): Any? {
        return when {
            obj == null -> null
            obj is String || obj is Number || obj is Boolean -> obj
            obj is Map<*, *> -> {
                val map = mutableMapOf<String, Any?>()
                for (entry in obj) {
                    @Suppress("UNCHECKED_CAST") val castEntry = entry as Map.Entry<Any?, Any?>
                    map[castEntry.key.toString()] = convertToJsonSerializable(castEntry.value)
                }
                map
            }
            obj is List<*> -> obj.map { convertToJsonSerializable(it) }
            else -> {
                // Handle Neo4j objects by extracting their properties
                val className = obj.javaClass.name
                if (className.contains("neo4j") || className.contains("Neo4j")) {
                    extractNeo4jObjectProperties(obj)
                } else {
                    // Use Jackson to convert other complex objects
                    try {
                        val jsonStr = objectMapper.writeValueAsString(obj)
                        objectMapper.readValue(jsonStr, Map::class.java)
                    } catch (e: Exception) {
                        // Fallback to toString if Jackson serialization fails
                        logger.fine("Could not serialize ${obj.javaClass.name} with Jackson: ${e.message}, using toString()")
                        obj.toString()
                    }
                }
            }
        }
    }

    /**
     * Extract properties from Neo4j objects (Node, Relationship, Record, etc.)
     */
    private fun extractNeo4jObjectProperties(obj: Any): Any? {
        return try {
            val result = mutableMapOf<String, Any?>()
            val javaClass = obj.javaClass

            // Get all methods and extract property values
            javaClass.methods.forEach { method ->
                try {
                    // Skip certain methods
                    if (method.name.startsWith("get") && method.parameterCount == 0 &&
                        !method.name.endsWith("Class") && method.returnType != Void.TYPE) {
                        val value = method.invoke(obj)
                        val propertyName = method.name.removePrefix("get").replaceFirstChar { it.lowercase() }
                        result[propertyName] = convertToJsonSerializable(value)
                    }
                } catch (e: Exception) {
                    // Skip properties that can't be accessed
                }
            }

            // If we extracted properties, return the map; otherwise try Jackson
            if (result.isNotEmpty()) {
                result
            } else {
                val jsonStr = objectMapper.writeValueAsString(obj)
                objectMapper.readValue(jsonStr, Map::class.java)
            }
        } catch (e: Exception) {
            logger.fine("Could not extract Neo4j object properties: ${e.message}")
            obj.toString()
        }
    }

    /**
     * Execute a SELECT query and return results as JSON
     * Output format: [[col1, col2, ...], [type1, type2, ...], [val1, val2, ...], ...]
     */
    fun executeQuery(config: DatabaseConnectionConfig, sql: String, arguments: List<Any?>? = null): Result<JsonArray> {
        return try {
            val driverClass = config.getDriverClassName()
            logger.fine("JdbcManager: Loading driver: $driverClass for URL: ${config.jdbcUrl}")
            Class.forName(driverClass)
            logger.fine("JdbcManager: Connecting to URL: ${config.jdbcUrl} with user: ${config.username}")
            val conn = DriverManager.getConnection(
                config.jdbcUrl,
                config.username,
                config.password
            )
            conn.use { connection ->
                val preparedStmt = connection.prepareStatement(sql)

                // Bind parameters if provided
                arguments?.forEachIndexed { index, arg ->
                    preparedStmt.setObject(index + 1, arg)
                }

                val resultSet = preparedStmt.executeQuery()
                val result = JsonArray()

                // Get metadata
                val metaData = resultSet.metaData
                val columnCount = metaData.columnCount

                // Row 0: Column names
                val columnNames = mutableListOf<Any>()
                val columnTypes = mutableListOf<Any>()

                for (i in 1..columnCount) {
                    columnNames.add(metaData.getColumnName(i))
                    columnTypes.add(metaData.getColumnTypeName(i))
                }

                result.add(JsonArray(columnNames))
                // Row 1: Column types
                result.add(JsonArray(columnTypes))

                // Rows 2+: Data (convert to JSON-serializable types)
                while (resultSet.next()) {
                    val row = mutableListOf<Any?>()
                    for (i in 1..columnCount) {
                        row.add(convertToJsonSerializable(resultSet.getObject(i)))
                    }
                    result.add(JsonArray(row))
                }

                resultSet.close()
                preparedStmt.close()

                Result.success(result)
            }
        } catch (e: Exception) {
            logger.severe("Query execution failed for '${config.name}': $sql - ${e.message}")
            Result.failure(e)
        }
    }

    /**
     * Execute a DML statement (INSERT, UPDATE, DELETE)
     * Returns the number of affected rows
     */
    fun executeDml(config: DatabaseConnectionConfig, sql: String, arguments: List<Any?>? = null): Result<Int> {
        return try {
            val driverClass = config.getDriverClassName()
            logger.fine("JdbcManager: Loading driver: $driverClass for URL: ${config.jdbcUrl}")
            Class.forName(driverClass)
            logger.fine("JdbcManager: Connecting to URL: ${config.jdbcUrl} with user: ${config.username}")
            val conn = DriverManager.getConnection(
                config.jdbcUrl,
                config.username,
                config.password
            )
            conn.use { connection ->
                val preparedStmt = connection.prepareStatement(sql)

                // Bind parameters if provided
                arguments?.forEachIndexed { index, arg ->
                    preparedStmt.setObject(index + 1, arg)
                }

                val affectedRows = preparedStmt.executeUpdate()
                preparedStmt.close()

                Result.success(affectedRows)
            }
        } catch (e: Exception) {
            logger.severe("DML execution failed for '${config.name}': $sql - ${e.message}")
            Result.failure(e)
        }
    }

    /**
     * Check if a connection is valid
     */
    fun isConnectionValid(config: DatabaseConnectionConfig): Boolean {
        return try {
            Class.forName(config.getDriverClassName())
            val conn = DriverManager.getConnection(
                config.jdbcUrl,
                config.username,
                config.password
            )
            conn.use { connection ->
                connection.isValid(5)  // 5 second timeout
            }
        } catch (e: Exception) {
            logger.warning("Connection validity check failed for '${config.name}': ${e.message}")
            false
        }
    }
}
