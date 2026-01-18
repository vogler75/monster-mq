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
                    val castEntry = entry
                    map[castEntry.key.toString()] = convertToJsonSerializable(castEntry.value)
                }
                map
            }
            obj is List<*> -> obj.map { convertToJsonSerializable(it) }
            else -> {
                // Try generic entity extraction first (works with Neo4j, graph databases, etc.)
                val extracted = extractEntityProperties(obj)
                // If entity extraction returned a map with properties, use it
                if (extracted is Map<*, *> && extracted.isNotEmpty()) {
                    extracted
                } else {
                    // Use Jackson to convert other complex objects
                    try {
                        val jsonStr = objectMapper.writeValueAsString(obj)
                        objectMapper.readValue(jsonStr, Map::class.java)
                    } catch (e: Exception) {
                        // Fallback to toString if Jackson serialization fails
                        logger.finer("Could not serialize ${obj.javaClass.name} with Jackson: ${e.message}, using toString()")
                        obj.toString()
                    }
                }
            }
        }
    }

    /**
     * Extract properties from entity-like objects (works with Neo4j, graph databases, etc.)
     * Uses reflection to call common entity methods: id(), identity(), labels(), properties(), asMap(), etc.
     * This is database-agnostic and works with any object that exposes entity-like methods.
     */
    private fun extractEntityProperties(obj: Any): Any? {
        return try {
            val result = mutableMapOf<String, Any?>()
            val javaClass = obj.javaClass

            // Try to extract common entity properties using various method names
            // This works generically with any object that exposes entity-like methods
            val methodNamesToTry = listOf(
                "id" to "identity",
                "identity" to "identity",
                "getId" to "identity",
                "getIdentity" to "identity",
                "elementId" to "elementId",
                "getElementId" to "elementId"
            )

            for ((methodName, resultKey) in methodNamesToTry) {
                try {
                    val method = javaClass.getMethod(methodName)
                    method.isAccessible = true
                    val value = method.invoke(obj)
                    if (value != null) {
                        result[resultKey] = value
                    }
                } catch (e: NoSuchMethodException) {
                    // Method doesn't exist, continue to next
                } catch (e: Exception) {
                    // Skip on other errors
                }
            }

            // Try to extract labels/types
            val labelMethodNames = listOf("labels", "getLabels", "types", "getTypes")
            for (methodName in labelMethodNames) {
                try {
                    val method = javaClass.getMethod(methodName)
                    method.isAccessible = true
                    val value = method.invoke(obj)
                    if (value is Collection<*>) {
                        result["labels"] = value.toList()
                        break
                    }
                } catch (e: NoSuchMethodException) {
                    // Method doesn't exist, continue
                } catch (e: Exception) {
                    // Skip on other errors
                }
            }

            // Try to extract properties as Map
            val propertyMethodNames = listOf("properties", "getProperties", "asMap", "toMap")
            for (methodName in propertyMethodNames) {
                try {
                    val method = javaClass.getMethod(methodName)
                    method.isAccessible = true
                    val propsValue = method.invoke(obj)
                    if (propsValue is Map<*, *>) {
                        @Suppress("UNCHECKED_CAST")
                        val propsMap = propsValue as Map<String, Any?>
                        result["properties"] = convertToJsonSerializable(propsMap)
                        break
                    }
                } catch (e: NoSuchMethodException) {
                    // Method doesn't exist, continue
                } catch (e: Exception) {
                    // Skip on other errors
                }
            }

            // If we extracted some entity properties, return the map
            if (result.isNotEmpty()) {
                result
            } else {
                // Fallback to generic getter extraction for plain objects
                javaClass.methods.forEach { method ->
                    try {
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

                if (result.isNotEmpty()) {
                    result
                } else {
                    obj.toString()
                }
            }
        } catch (e: Exception) {
            logger.finer("Could not extract entity properties: ${e.message}")
            emptyMap<String, Any?>()
        }
    }

    /**
     * Execute a SELECT query and return results as JSON
     * Output format: [[col1, col2, ...], [type1, type2, ...], [val1, val2, ...], ...]
     */
    fun executeQuery(config: DatabaseConnectionConfig, sql: String, arguments: List<Any?>? = null): Result<JsonArray> {
        return try {
            val driverClass = config.getDriverClassName()
            logger.finer("JdbcManager: Loading driver: $driverClass for URL: ${config.jdbcUrl}")
            Class.forName(driverClass)
            logger.finer("JdbcManager: Connecting to URL: ${config.jdbcUrl} with user: ${config.username}")
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
                        val obj = resultSet.getObject(i)
                        logger.finer("Column $i (${metaData.getColumnName(i)}): type=${obj?.javaClass?.name}, value=$obj")
                        row.add(convertToJsonSerializable(obj))
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
            logger.finer("JdbcManager: Loading driver: $driverClass for URL: ${config.jdbcUrl}")
            Class.forName(driverClass)
            logger.finer("JdbcManager: Connecting to URL: ${config.jdbcUrl} with user: ${config.username}")
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
