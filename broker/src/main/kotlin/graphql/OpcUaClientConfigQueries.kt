package at.rocworks.graphql

import at.rocworks.Utils
import at.rocworks.Monster
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.OpcUaConnectionConfig
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL queries for OPC UA client configuration management
 */
class OpcUaClientConfigQueries(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(OpcUaClientConfigQueries::class.java)

    fun opcUaDevices(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { _ ->
            val future = CompletableFuture<List<Map<String, Any>>>()

            try {
                deviceStore.getAllDevices().onComplete { result ->
                    if (result.succeeded()) {
                        // Filter to only return OPCUA-Client type devices
                        val deviceMaps = result.result()
                            .filter { device -> device.type == DeviceConfig.DEVICE_TYPE_OPCUA_CLIENT }
                            .map { device -> deviceToMap(device) }
                        future.complete(deviceMaps)
                    } else {
                        logger.severe("Error fetching OPC UA devices: ${result.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching OPC UA devices: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    fun opcUaDevice(): DataFetcher<CompletableFuture<Map<String, Any>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>?>()

            try {
                val name = env.getArgument<String>("name")
                if (name == null) {
                    future.complete(null)
                    return@DataFetcher future
                }

                deviceStore.getDevice(name).onComplete { result ->
                    if (result.succeeded()) {
                        val device = result.result()
                        // Only return device if it's OPCUA-Client type
                        if (device != null && device.type == DeviceConfig.DEVICE_TYPE_OPCUA_CLIENT) {
                            future.complete(deviceToMap(device))
                        } else {
                            future.complete(null)
                        }
                    } else {
                        logger.severe("Error fetching OPC UA device: ${result.cause()?.message}")
                        future.complete(null)
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching OPC UA device: ${e.message}")
                future.complete(null)
            }

            future
        }
    }

    fun opcUaDevicesByNode(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any>>>()

            try {
                val nodeId = env.getArgument<String>("nodeId")
                if (nodeId == null) {
                    future.complete(emptyList())
                    return@DataFetcher future
                }

                deviceStore.getDevicesByNode(nodeId).onComplete { result ->
                    if (result.succeeded()) {
                        // Filter to only return OPCUA-Client type devices
                        val deviceMaps = result.result()
                            .filter { device -> device.type == DeviceConfig.DEVICE_TYPE_OPCUA_CLIENT }
                            .map { device -> deviceToMap(device) }
                        future.complete(deviceMaps)
                    } else {
                        logger.severe("Error fetching OPC UA devices by node: ${result.cause()?.message}")
                        future.complete(emptyList())
                    }
                }
            } catch (e: Exception) {
                logger.severe("Error fetching OPC UA devices by node: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    fun clusterNodes(): DataFetcher<CompletableFuture<List<Map<String, Any>>>> {
        return DataFetcher { _ ->
            val future = CompletableFuture<List<Map<String, Any>>>()

            try {
                val nodes = Monster.Companion.getClusterNodeIds(vertx)
                val currentNodeId = Monster.Companion.getClusterNodeId(vertx) ?: "local"

                val nodeMaps = nodes.map { nodeId ->
                    mapOf(
                        "nodeId" to nodeId,
                        "isCurrent" to (nodeId == currentNodeId)
                    )
                }

                future.complete(nodeMaps)
            } catch (e: Exception) {
                logger.severe("Error fetching cluster nodes: ${e.message}")
                future.complete(emptyList())
            }

            future
        }
    }

    private fun deviceToMap(device: DeviceConfig): Map<String, Any> {
        val currentNodeId = Monster.Companion.getClusterNodeId(vertx) ?: "local"

        // Parse config from JsonObject for OPC UA Client devices
        val config = OpcUaConnectionConfig.fromJsonObject(device.config)

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "config" to mapOf(
                "endpointUrl" to config.endpointUrl,
                "updateEndpointUrl" to config.updateEndpointUrl,
                "securityPolicy" to config.securityPolicy,
                "username" to (config.username ?: ""),
                "password" to (config.password ?: ""),
                "subscriptionSamplingInterval" to config.subscriptionSamplingInterval,
                "keepAliveFailuresAllowed" to config.keepAliveFailuresAllowed,
                "reconnectDelay" to config.reconnectDelay,
                "connectionTimeout" to config.connectionTimeout,
                "requestTimeout" to config.requestTimeout,
                "monitoringParameters" to mapOf(
                    "bufferSize" to config.monitoringParameters.bufferSize,
                    "samplingInterval" to config.monitoringParameters.samplingInterval,
                    "discardOldest" to config.monitoringParameters.discardOldest
                ),
                "addresses" to config.addresses.map { address ->
                    mapOf(
                        "address" to address.address,
                        "topic" to address.topic,
                        "publishMode" to address.publishMode,
                        "removePath" to address.removePath
                    )
                },
                "certificateConfig" to mapOf(
                    "securityDir" to config.certificateConfig.securityDir,
                    "applicationName" to config.certificateConfig.applicationName,
                    "applicationUri" to config.certificateConfig.applicationUri,
                    "organization" to config.certificateConfig.organization,
                    "organizationalUnit" to config.certificateConfig.organizationalUnit,
                    "localityName" to config.certificateConfig.localityName,
                    "countryCode" to config.certificateConfig.countryCode,
                    "createSelfSigned" to config.certificateConfig.createSelfSigned,
                    "keystorePassword" to config.certificateConfig.keystorePassword,
                    "validateServerCertificate" to config.certificateConfig.validateServerCertificate,
                    "autoAcceptServerCertificates" to config.certificateConfig.autoAcceptServerCertificates
                )
            ),
            "enabled" to device.enabled,
            // type is not exposed in GraphQL - always OPCUA-Client for this API
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString(),
            "isOnCurrentNode" to device.isAssignedToNode(currentNodeId)
        )
    }
}