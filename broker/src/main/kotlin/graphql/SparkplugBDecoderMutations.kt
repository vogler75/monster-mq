package at.rocworks.graphql

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.devices.sparkplugb.SparkplugBDecoderExtension
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.DeviceConfigRequest
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.devices.SparkplugBDecoderConfig
import at.rocworks.stores.devices.SparkplugBDecoderRule
import graphql.schema.DataFetcher
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL mutations for SparkplugB Decoder device management
 */
class SparkplugBDecoderMutations(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    private val logger: Logger = Utils.getLogger(SparkplugBDecoderMutations::class.java)

    fun createSparkplugBDecoder(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val input = env.getArgument<Map<String, Any>>("input")
                    ?: return@DataFetcher future.apply {
                        complete(mapOf("success" to false, "errors" to listOf("Input is required")))
                    }

                // Parse input
                val request = parseDeviceConfigRequest(input)
                val validationErrors = request.validate()

                if (validationErrors.isNotEmpty()) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to validationErrors
                        )
                    )
                    return@DataFetcher future
                }

                // Check if name already exists
                deviceStore.getDevice(request.name).onComplete { existingResult ->
                    if (existingResult.failed()) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Database error: ${existingResult.cause()?.message}")
                            )
                        )
                        return@onComplete
                    }

                    if (existingResult.result() != null) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Device with name '${request.name}' already exists")
                            )
                        )
                        return@onComplete
                    }

                    // Save device
                    val device = request.toDeviceConfig()
                    deviceStore.saveDevice(device).onComplete { saveResult ->
                        if (saveResult.succeeded()) {
                            val savedDevice = saveResult.result()

                            // Notify extension about the change
                            notifyDeviceConfigChange("add", savedDevice)

                            future.complete(
                                mapOf(
                                    "success" to true,
                                    "decoder" to deviceToMap(savedDevice),
                                    "errors" to emptyList<String>()
                                )
                            )
                        } else {
                            future.complete(
                                mapOf(
                                    "success" to false,
                                    "errors" to listOf("Failed to save device: ${saveResult.cause()?.message}")
                                )
                            )
                        }
                    }
                }

            } catch (e: Exception) {
                logger.severe("Error creating SparkplugB Decoder: ${e.message}")
                future.complete(
                    mapOf(
                        "success" to false,
                        "errors" to listOf("Failed to create decoder: ${e.message}")
                    )
                )
            }

            future
        }
    }

    fun updateSparkplugBDecoder(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val name = env.getArgument<String>("name")
                val input = env.getArgument<Map<String, Any>>("input")

                if (name == null || input == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("Name and input are required")
                        )
                    )
                    return@DataFetcher future
                }

                // Check if device exists
                deviceStore.getDevice(name).onComplete { existingResult ->
                    if (existingResult.failed()) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Database error: ${existingResult.cause()?.message}")
                            )
                        )
                        return@onComplete
                    }

                    val existingDevice = existingResult.result()
                    if (existingDevice == null) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Device '$name' not found")
                            )
                        )
                        return@onComplete
                    }

                    // Parse and validate update
                    val request = parseDeviceConfigRequest(input)
                    val validationErrors = request.validate()

                    if (validationErrors.isNotEmpty()) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to validationErrors
                            )
                        )
                        return@onComplete
                    }

                    // Update device (preserve timestamps)
                    val updatedDevice = DeviceConfig(
                        name = name, // Keep original name
                        namespace = request.namespace,
                        nodeId = request.nodeId,
                        config = request.config,
                        enabled = request.enabled,
                        type = DeviceConfig.DEVICE_TYPE_SPARKPLUGB_DECODER,
                        createdAt = existingDevice.createdAt,
                        updatedAt = Instant.now()
                    )

                    deviceStore.saveDevice(updatedDevice).onComplete { updateResult ->
                        if (updateResult.succeeded()) {
                            val savedDevice = updateResult.result()

                            // Notify extension about the change
                            notifyDeviceConfigChange("update", savedDevice)

                            future.complete(
                                mapOf(
                                    "success" to true,
                                    "decoder" to deviceToMap(savedDevice),
                                    "errors" to emptyList<String>()
                                )
                            )
                        } else {
                            future.complete(
                                mapOf(
                                    "success" to false,
                                    "errors" to listOf("Failed to update device: ${updateResult.cause()?.message}")
                                )
                            )
                        }
                    }
                }

            } catch (e: Exception) {
                logger.severe("Error updating SparkplugB Decoder: ${e.message}")
                future.complete(
                    mapOf(
                        "success" to false,
                        "errors" to listOf("Failed to update decoder: ${e.message}")
                    )
                )
            }

            future
        }
    }

    fun deleteSparkplugBDecoder(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val name = env.getArgument<String>("name")

                if (name == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("Name is required")
                        )
                    )
                    return@DataFetcher future
                }

                deviceStore.deleteDevice(name).onComplete { deleteResult ->
                    if (deleteResult.succeeded()) {
                        // Notify extension about the deletion
                        val notification = JsonObject()
                            .put("operation", "delete")
                            .put("deviceName", name)

                        vertx.eventBus().publish(
                            SparkplugBDecoderExtension.ADDRESS_DEVICE_CONFIG_CHANGED,
                            notification
                        )

                        future.complete(
                            mapOf<String, Any>(
                                "success" to true,
                                "errors" to emptyList<String>()
                            )
                        )
                    } else {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Failed to delete device: ${deleteResult.cause()?.message}")
                            )
                        )
                    }
                }

            } catch (e: Exception) {
                logger.severe("Error deleting SparkplugB Decoder: ${e.message}")
                future.complete(
                    mapOf(
                        "success" to false,
                        "errors" to listOf("Failed to delete decoder: ${e.message}")
                    )
                )
            }

            future
        }
    }

    fun toggleSparkplugBDecoder(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val name = env.getArgument<String>("name")
                val enabled = env.getArgument<Boolean>("enabled")

                if (name == null || enabled == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("Name and enabled are required")
                        )
                    )
                    return@DataFetcher future
                }

                deviceStore.getDevice(name).onComplete { existingResult ->
                    if (existingResult.failed() || existingResult.result() == null) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Device '$name' not found")
                            )
                        )
                        return@onComplete
                    }

                    val device = existingResult.result()!!
                    val updatedDevice = device.copy(enabled = enabled, updatedAt = Instant.now())

                    deviceStore.saveDevice(updatedDevice).onComplete { updateResult ->
                        if (updateResult.succeeded()) {
                            // Notify extension
                            val notification = JsonObject()
                                .put("operation", "toggle")
                                .put("deviceName", name)
                                .put("enabled", enabled)

                            vertx.eventBus().publish(
                                SparkplugBDecoderExtension.ADDRESS_DEVICE_CONFIG_CHANGED,
                                notification
                            )

                            future.complete(
                                mapOf(
                                    "success" to true,
                                    "decoder" to deviceToMap(updatedDevice),
                                    "errors" to emptyList<String>()
                                )
                            )
                        } else {
                            future.complete(
                                mapOf(
                                    "success" to false,
                                    "errors" to listOf("Failed to toggle device: ${updateResult.cause()?.message}")
                                )
                            )
                        }
                    }
                }

            } catch (e: Exception) {
                logger.severe("Error toggling SparkplugB Decoder: ${e.message}")
                future.complete(
                    mapOf(
                        "success" to false,
                        "errors" to listOf("Failed to toggle decoder: ${e.message}")
                    )
                )
            }

            future
        }
    }

    fun reassignSparkplugBDecoder(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val name = env.getArgument<String>("name")
                val nodeId = env.getArgument<String>("nodeId")

                if (name == null || nodeId == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("Name and nodeId are required")
                        )
                    )
                    return@DataFetcher future
                }

                deviceStore.getDevice(name).onComplete { existingResult ->
                    if (existingResult.failed() || existingResult.result() == null) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Device '$name' not found")
                            )
                        )
                        return@onComplete
                    }

                    val device = existingResult.result()!!
                    val updatedDevice = device.copy(nodeId = nodeId, updatedAt = Instant.now())

                    deviceStore.saveDevice(updatedDevice).onComplete { updateResult ->
                        if (updateResult.succeeded()) {
                            // Notify extension
                            val notification = JsonObject()
                                .put("operation", "reassign")
                                .put("deviceName", name)
                                .put("nodeId", nodeId)

                            vertx.eventBus().publish(
                                SparkplugBDecoderExtension.ADDRESS_DEVICE_CONFIG_CHANGED,
                                notification
                            )

                            future.complete(
                                mapOf(
                                    "success" to true,
                                    "decoder" to deviceToMap(updatedDevice),
                                    "errors" to emptyList<String>()
                                )
                            )
                        } else {
                            future.complete(
                                mapOf(
                                    "success" to false,
                                    "errors" to listOf("Failed to reassign device: ${updateResult.cause()?.message}")
                                )
                            )
                        }
                    }
                }

            } catch (e: Exception) {
                logger.severe("Error reassigning SparkplugB Decoder: ${e.message}")
                future.complete(
                    mapOf(
                        "success" to false,
                        "errors" to listOf("Failed to reassign decoder: ${e.message}")
                    )
                )
            }

            future
        }
    }

    fun addRule(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val deviceName = env.getArgument<String>("deviceName")
                val ruleInput = env.getArgument<Map<String, Any>>("rule")

                if (deviceName == null || ruleInput == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("deviceName and rule are required")
                        )
                    )
                    return@DataFetcher future
                }

                deviceStore.getDevice(deviceName).onComplete { existingResult ->
                    if (existingResult.failed() || existingResult.result() == null) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Device '$deviceName' not found")
                            )
                        )
                        return@onComplete
                    }

                    val device = existingResult.result()!!
                    val config = SparkplugBDecoderConfig.fromJsonObject(device.config)

                    // Parse new rule
                    val newRule = parseRule(ruleInput)
                    val ruleErrors = newRule.validate()

                    if (ruleErrors.isNotEmpty()) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to ruleErrors
                            )
                        )
                        return@onComplete
                    }

                    // Check for duplicate rule name
                    if (config.rules.any { it.name == newRule.name }) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Rule with name '${newRule.name}' already exists")
                            )
                        )
                        return@onComplete
                    }

                    // Add rule
                    val updatedRules = config.rules + newRule
                    val updatedConfig = config.copy(rules = updatedRules)
                    val updatedDevice = device.copy(
                        config = updatedConfig.toJsonObject(),
                        updatedAt = Instant.now()
                    )

                    deviceStore.saveDevice(updatedDevice).onComplete { updateResult ->
                        if (updateResult.succeeded()) {
                            // Notify extension
                            notifyDeviceConfigChange("addRule", updatedDevice)

                            future.complete(
                                mapOf(
                                    "success" to true,
                                    "decoder" to deviceToMap(updatedDevice),
                                    "errors" to emptyList<String>()
                                )
                            )
                        } else {
                            future.complete(
                                mapOf(
                                    "success" to false,
                                    "errors" to listOf("Failed to add rule: ${updateResult.cause()?.message}")
                                )
                            )
                        }
                    }
                }

            } catch (e: Exception) {
                logger.severe("Error adding rule: ${e.message}")
                future.complete(
                    mapOf(
                        "success" to false,
                        "errors" to listOf("Failed to add rule: ${e.message}")
                    )
                )
            }

            future
        }
    }

    fun deleteRule(): DataFetcher<CompletableFuture<Map<String, Any>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any>>()

            try {
                val deviceName = env.getArgument<String>("deviceName")
                val ruleName = env.getArgument<String>("ruleName")

                if (deviceName == null || ruleName == null) {
                    future.complete(
                        mapOf(
                            "success" to false,
                            "errors" to listOf("deviceName and ruleName are required")
                        )
                    )
                    return@DataFetcher future
                }

                deviceStore.getDevice(deviceName).onComplete { existingResult ->
                    if (existingResult.failed() || existingResult.result() == null) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Device '$deviceName' not found")
                            )
                        )
                        return@onComplete
                    }

                    val device = existingResult.result()!!
                    val config = SparkplugBDecoderConfig.fromJsonObject(device.config)

                    // Remove rule
                    val updatedRules = config.rules.filter { it.name != ruleName }

                    if (updatedRules.size == config.rules.size) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Rule '$ruleName' not found")
                            )
                        )
                        return@onComplete
                    }

                    if (updatedRules.isEmpty()) {
                        future.complete(
                            mapOf(
                                "success" to false,
                                "errors" to listOf("Cannot delete last rule - at least one rule is required")
                            )
                        )
                        return@onComplete
                    }

                    val updatedConfig = config.copy(rules = updatedRules)
                    val updatedDevice = device.copy(
                        config = updatedConfig.toJsonObject(),
                        updatedAt = Instant.now()
                    )

                    deviceStore.saveDevice(updatedDevice).onComplete { updateResult ->
                        if (updateResult.succeeded()) {
                            // Notify extension
                            notifyDeviceConfigChange("deleteRule", updatedDevice)

                            future.complete(
                                mapOf(
                                    "success" to true,
                                    "decoder" to deviceToMap(updatedDevice),
                                    "errors" to emptyList<String>()
                                )
                            )
                        } else {
                            future.complete(
                                mapOf(
                                    "success" to false,
                                    "errors" to listOf("Failed to delete rule: ${updateResult.cause()?.message}")
                                )
                            )
                        }
                    }
                }

            } catch (e: Exception) {
                logger.severe("Error deleting rule: ${e.message}")
                future.complete(
                    mapOf(
                        "success" to false,
                        "errors" to listOf("Failed to delete rule: ${e.message}")
                    )
                )
            }

            future
        }
    }

    /**
     * Parse DeviceConfigRequest from GraphQL input
     */
    private fun parseDeviceConfigRequest(input: Map<String, Any>): DeviceConfigRequest {
        val name = input["name"] as String
        val namespace = input["namespace"] as String
        val nodeId = input["nodeId"] as String
        val enabled = input["enabled"] as? Boolean ?: true

        val configInput = input["config"] as Map<*, *>
        val config = parseConfig(configInput)

        return DeviceConfigRequest(
            name = name,
            namespace = namespace,
            nodeId = nodeId,
            config = config.toJsonObject(),
            enabled = enabled,
            type = DeviceConfig.DEVICE_TYPE_SPARKPLUGB_DECODER
        )
    }

    /**
     * Parse SparkplugBDecoderConfig from GraphQL input
     */
    private fun parseConfig(configInput: Map<*, *>): SparkplugBDecoderConfig {
        val sourceNamespace = configInput["sourceNamespace"] as? String ?: "spBv1.0"

        val subscriptionsInput = configInput["subscriptions"] as? List<*> ?: emptyList<Any>()
        val subscriptions = subscriptionsInput.mapNotNull { subInput ->
            if (subInput is Map<*, *>) {
                @Suppress("UNCHECKED_CAST")
                parseSubscription(subInput as Map<String, Any>)
            } else {
                null
            }
        }

        val rulesInput = configInput["rules"] as? List<*> ?: emptyList<Any>()
        val rules = rulesInput.mapNotNull { ruleInput ->
            if (ruleInput is Map<*, *>) {
                @Suppress("UNCHECKED_CAST")
                parseRule(ruleInput as Map<String, Any>)
            } else {
                null
            }
        }

        return SparkplugBDecoderConfig(
            sourceNamespace = sourceNamespace,
            subscriptions = subscriptions,
            rules = rules
        )
    }

    /**
     * Parse SparkplugBSubscription from GraphQL input
     */
    private fun parseSubscription(subInput: Map<String, Any>): at.rocworks.stores.devices.SparkplugBSubscription {
        val nodeId = subInput["nodeId"] as String
        val deviceIds = (subInput["deviceIds"] as? List<*>)?.mapNotNull { it as? String } ?: emptyList()

        return at.rocworks.stores.devices.SparkplugBSubscription(
            nodeId = nodeId,
            deviceIds = deviceIds
        )
    }

    /**
     * Parse SparkplugBDecoderRule from GraphQL input
     */
    private fun parseRule(ruleInput: Map<String, Any>): SparkplugBDecoderRule {
        val name = ruleInput["name"] as String
        val nodeIdRegex = ruleInput["nodeIdRegex"] as String
        val deviceIdRegex = ruleInput["deviceIdRegex"] as String
        val destinationTopic = ruleInput["destinationTopic"] as String
        val transformations = (ruleInput["transformations"] as? Map<*, *>)?.mapKeys { it.key.toString() }?.mapValues { it.value.toString() } ?: emptyMap()

        return SparkplugBDecoderRule(
            name = name,
            nodeIdRegex = nodeIdRegex,
            deviceIdRegex = deviceIdRegex,
            destinationTopic = destinationTopic,
            transformations = transformations
        )
    }

    /**
     * Notify extension about device configuration change
     */
    private fun notifyDeviceConfigChange(operation: String, device: DeviceConfig) {
        val notification = JsonObject()
            .put("operation", operation)
            .put("deviceName", device.name)
            .put("device", device.toJsonObject())

        vertx.eventBus().publish(
            SparkplugBDecoderExtension.ADDRESS_DEVICE_CONFIG_CHANGED,
            notification
        )
    }

    /**
     * Convert DeviceConfig to GraphQL-compatible map
     */
    private fun deviceToMap(device: DeviceConfig): Map<String, Any> {
        val config = SparkplugBDecoderConfig.fromJsonObject(device.config)

        return mapOf(
            "name" to device.name,
            "namespace" to device.namespace,
            "nodeId" to device.nodeId,
            "config" to configToMap(config),
            "enabled" to device.enabled,
            "createdAt" to device.createdAt.toString(),
            "updatedAt" to device.updatedAt.toString()
        )
    }

    /**
     * Convert SparkplugBDecoderConfig to GraphQL-compatible map
     */
    private fun configToMap(config: SparkplugBDecoderConfig): Map<String, Any> {
        return mapOf(
            "sourceNamespace" to config.sourceNamespace,
            "subscriptions" to config.subscriptions.map { sub ->
                mapOf(
                    "nodeId" to sub.nodeId,
                    "deviceIds" to sub.deviceIds
                )
            },
            "rules" to config.rules.map { rule ->
                mapOf(
                    "name" to rule.name,
                    "nodeIdRegex" to rule.nodeIdRegex,
                    "deviceIdRegex" to rule.deviceIdRegex,
                    "destinationTopic" to rule.destinationTopic,
                    "transformations" to rule.transformations
                )
            }
        )
    }
}
