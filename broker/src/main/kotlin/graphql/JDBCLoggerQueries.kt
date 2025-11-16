package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.JDBCLoggerConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL queries for JDBC Logger configuration management
 * Provides query resolvers for retrieving JDBC Logger configurations across the cluster
 */
class JDBCLoggerQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(JDBCLoggerQueries::class.java)

    fun jdbcLoggers(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any?>>>()
            try {
                val name = env.getArgument<String?>("name")
                val nodeId = env.getArgument<String?>("node")

                when {
                    // Filter by both name and node
                    name != null && nodeId != null -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_JDBC_LOGGER && it.name == name }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching JDBC Loggers: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    // Filter by name only
                    name != null -> {
                        deviceStore.getDevice(name).onComplete { result ->
                            if (result.succeeded()) {
                                val device = result.result()
                                if (device != null && device.type == DeviceConfig.DEVICE_TYPE_JDBC_LOGGER) {
                                    future.complete(listOf(deviceToMap(device)))
                                } else {
                                    future.complete(emptyList())
                                }
                            } else {
                                logger.severe("Error fetching JDBC Logger: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    // Filter by node only
                    nodeId != null -> {
                        deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_JDBC_LOGGER }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching JDBC Loggers by node: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                    // No filters - return all
                    else -> {
                        deviceStore.getAllDevices().onComplete { result ->
                            if (result.succeeded()) {
                                val deviceMaps = result.result()
                                    .filter { it.type == DeviceConfig.DEVICE_TYPE_JDBC_LOGGER }
                                    .map { deviceToMap(it) }
                                future.complete(deviceMaps)
                            } else {
                                logger.severe("Error fetching JDBC Loggers: ${result.cause()?.message}")
                                future.complete(emptyList())
                            }
                        }
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching JDBC Loggers: ${e.message}")
                future.complete(emptyList())
            }
            future
        }
    }

    private fun deviceToMap(device: DeviceConfig): Map<String, Any?> {
        val currentNodeId = Monster.getClusterNodeId(vertx) ?: "local"
        val config = try { JDBCLoggerConfig.fromJson(device.config) } catch (e: Exception) {
            logger.severe("Failed to parse JDBCLoggerConfig for ${device.name}: ${e.message}")
            // Provide minimal fallback so the UI can still show something
            JDBCLoggerConfig(
                databaseType = "QuestDB",
                jdbcUrl = "jdbc:questdb:http://localhost:9000",
                username = "admin",
                password = "quest",
                topicFilters = emptyList(),
                jsonSchema = JsonObject()
            )
        }

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "config" to mapOf(
                "databaseType" to config.databaseType.uppercase(),
                "jdbcUrl" to config.jdbcUrl,
                "username" to config.username,
                "password" to null, // Don't expose password in queries for security
                "topicFilters" to config.topicFilters,
                "tableName" to config.tableName,
                "tableNameJsonPath" to config.tableNameJsonPath,
                "payloadFormat" to config.payloadFormat.uppercase(),
                "jsonSchema" to config.jsonSchema.map,
                "queueType" to config.queueType.uppercase(),
                "queueSize" to config.queueSize,
                "diskPath" to config.diskPath,
                "bulkSize" to config.bulkSize,
                "bulkTimeoutMs" to config.bulkTimeoutMs,
                "reconnectDelayMs" to config.reconnectDelayMs,
                "autoCreateTable" to config.autoCreateTable,
                "dbSpecificConfig" to config.dbSpecificConfig.map
            ),
            "enabled" to device.enabled,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString(),
            "isOnCurrentNode" to device.isAssignedToNode(currentNodeId),
            // Empty metrics list placeholders; actual resolvers in MetricsResolver
            "metrics" to emptyList<Map<String, Any?>>(),
            "metricsHistory" to emptyList<Map<String, Any?>>()
        )
    }
}
