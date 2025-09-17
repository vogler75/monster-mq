package at.rocworks.devices.opcua

import at.rocworks.data.MqttMessage
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import org.eclipse.milo.opcua.sdk.client.OpcUaClient
import org.eclipse.milo.opcua.sdk.client.api.identity.AnonymousProvider
import org.eclipse.milo.opcua.sdk.client.api.identity.IdentityProvider
import org.eclipse.milo.opcua.sdk.client.api.identity.UsernameProvider
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription
import org.eclipse.milo.opcua.stack.core.Identifiers
import org.eclipse.milo.opcua.stack.core.types.builtin.*
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UByte
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UShort
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.ULong
import org.eclipse.milo.opcua.stack.core.types.enumerated.*
import org.eclipse.milo.opcua.stack.core.types.structured.*
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.OpcUaAddress
import at.rocworks.stores.OpcUaConnectionConfig
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger
import kotlin.concurrent.thread

/**
 * OPC UA Connector - Handles connection to a single OPC UA server
 *
 * Responsibilities:
 * - Maintains OPC UA client connection
 * - Handles MQTT subscription requests for OPC UA topics
 * - Browses OPC UA node trees for path-based subscriptions
 * - Publishes OPC UA value changes as MQTT messages
 * - Handles reconnection and error recovery
 */
class OpcUaConnector : AbstractVerticle() {

    private val logger: Logger = Logger.getLogger(OpcUaConnector::class.java.name)

    // Device configuration
    private lateinit var deviceConfig: DeviceConfig
    private lateinit var opcUaConfig: OpcUaConnectionConfig

    // OPC UA client
    private var client: OpcUaClient? = null
    private var subscription: UaSubscription? = null

    // Connection state
    private var isConnected = false
    private var isReconnecting = false
    private var reconnectTimerId: Long? = null

    // Static address subscriptions
    private val addressSubscriptions = ConcurrentHashMap<String, AddressSubscription>() // address -> subscription
    private val opcUaMonitoredItems = ConcurrentHashMap<String, UaMonitoredItem>() // address -> item

    data class AddressSubscription(
        val address: OpcUaAddress,
        val opcUaNodeId: NodeId
    )

    override fun start(startPromise: Promise<Void>) {
        try {
            // Load device configuration
            val deviceJson = config().getJsonObject("device")
            deviceConfig = DeviceConfig.fromJsonObject(deviceJson)
            opcUaConfig = deviceConfig.config

            logger.info("Starting OpcUaConnector for device: ${deviceConfig.name}")

            // Start connector successfully regardless of initial connection status
            logger.info("OpcUaConnector for device ${deviceConfig.name} started successfully")
            startPromise.complete()

            // Attempt initial connection in the background - if it fails, reconnection will be scheduled
            connectToOpcUaServer()
                .compose { setupStaticSubscriptions() }
                .onComplete { result ->
                    if (result.succeeded()) {
                        logger.info("Initial OPC UA connection successful for device ${deviceConfig.name}")
                    } else {
                        logger.warning("Initial OPC UA connection failed for device ${deviceConfig.name}: ${result.cause()?.message}. Will retry automatically.")
                    }
                }

        } catch (e: Exception) {
            logger.severe("Exception during OpcUaConnector startup: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Stopping OpcUaConnector for device: ${deviceConfig.name}")

        // Cancel any pending reconnection timer
        reconnectTimerId?.let { timerId ->
            vertx.cancelTimer(timerId)
            reconnectTimerId = null
            logger.info("Cancelled pending reconnection timer for device ${deviceConfig.name}")
        }

        disconnectFromOpcUaServer()
            .onComplete { result ->
                if (result.succeeded()) {
                    logger.info("OpcUaConnector for device ${deviceConfig.name} stopped successfully")
                } else {
                    logger.warning("Error during OpcUaConnector shutdown: ${result.cause()?.message}")
                }
                stopPromise.complete()
            }
    }

    private fun setupStaticSubscriptions(): Future<Void> {
        val promise = Promise.promise<Void>()

        if (subscription == null) {
            promise.fail(Exception("OPC UA subscription not available"))
            return promise.future()
        }

        val addresses = deviceConfig.config.addresses
        if (addresses.isEmpty()) {
            logger.info("No addresses configured for device ${deviceConfig.name}")
            promise.complete()
            return promise.future()
        }

        logger.info("Setting up ${addresses.size} static address subscriptions for device ${deviceConfig.name}")

        // Process each configured address
        val setupFutures = addresses.map { address ->
            setupAddressSubscription(address)
        }

        Future.all<Void>(setupFutures as List<Future<Void>>)
            .onComplete { result ->
                if (result.succeeded()) {
                    logger.info("Successfully set up ${addresses.size} address subscriptions for device ${deviceConfig.name}")
                    promise.complete()
                } else {
                    logger.severe("Failed to set up address subscriptions: ${result.cause()?.message}")
                    promise.fail(result.cause())
                }
            }

        return promise.future()
    }

    private fun connectToOpcUaServer(): Future<Void> {
        val promise = Promise.promise<Void>()

        if (isConnected || isReconnecting) {
            promise.complete()
            return promise.future()
        }

        isReconnecting = true

        thread {
            try {
                logger.info("Connecting to OPC UA server: ${opcUaConfig.endpointUrl}")

                // Set up identity provider
                val identityProvider: IdentityProvider = if (opcUaConfig.username != null) {
                    UsernameProvider(opcUaConfig.username, opcUaConfig.password ?: "")
                } else {
                    AnonymousProvider()
                }

                // Create client
                client = OpcUaClient.create(opcUaConfig.endpointUrl)

                // Connect
                val connectFuture = client!!.connect()
                connectFuture.whenComplete { _, throwable ->
                    if (throwable == null) {
                        isConnected = true
                        isReconnecting = false
                        logger.info("Connected to OPC UA server: ${opcUaConfig.endpointUrl}")

                        // Create subscription
                        createOpcUaSubscription()
                            .onComplete { subResult ->
                                if (subResult.succeeded()) {
                                    promise.complete()
                                } else {
                                    logger.warning("Failed to create OPC UA subscription: ${subResult.cause()?.message}")
                                    promise.complete() // Continue anyway
                                }
                            }
                    } else {
                        isReconnecting = false
                        logger.severe("Failed to connect to OPC UA server: ${throwable.message}")
                        scheduleReconnection()
                        promise.fail(throwable)
                    }
                }

            } catch (e: Exception) {
                isReconnecting = false
                logger.severe("Exception during OPC UA connection: ${e.message}")
                scheduleReconnection()
                promise.fail(e)
            }
        }

        return promise.future()
    }

    private fun createOpcUaSubscription(): Future<Void> {
        val promise = Promise.promise<Void>()

        if (client == null || !isConnected) {
            promise.fail(Exception("OPC UA client not connected"))
            return promise.future()
        }

        client!!.subscriptionManager
            .createSubscription(opcUaConfig.subscriptionSamplingInterval)
            .whenComplete { sub, throwable ->
                if (throwable == null) {
                    subscription = sub
                    logger.fine("Created OPC UA subscription: ${sub.subscriptionId}")
                    promise.complete()
                } else {
                    logger.severe("Failed to create OPC UA subscription: ${throwable.message}")
                    promise.fail(throwable)
                }
            }

        return promise.future()
    }

    private fun disconnectFromOpcUaServer(): Future<Void> {
        val promise = Promise.promise<Void>()

        // Cancel any pending reconnection timer
        reconnectTimerId?.let { timerId ->
            vertx.cancelTimer(timerId)
            reconnectTimerId = null
        }

        if (client != null && isConnected) {
            client!!.disconnect().whenComplete { _, _ ->
                isConnected = false
                isReconnecting = false
                client = null
                subscription = null
                addressSubscriptions.clear()
                opcUaMonitoredItems.clear()
                promise.complete()
            }
        } else {
            isConnected = false
            isReconnecting = false
            client = null
            subscription = null
            addressSubscriptions.clear()
            opcUaMonitoredItems.clear()
            promise.complete()
        }

        return promise.future()
    }

    private fun scheduleReconnection() {
        if (!isReconnecting) {
            // Cancel any existing reconnection timer
            reconnectTimerId?.let { timerId ->
                vertx.cancelTimer(timerId)
            }

            // Schedule new reconnection attempt
            reconnectTimerId = vertx.setTimer(opcUaConfig.reconnectDelay) {
                reconnectTimerId = null
                if (!isConnected) {
                    logger.info("Attempting to reconnect to OPC UA server for device ${deviceConfig.name}...")
                    connectToOpcUaServer()
                        .compose { setupStaticSubscriptions() }
                        .onComplete { result ->
                            if (result.failed()) {
                                logger.warning("Reconnection failed for device ${deviceConfig.name}: ${result.cause()?.message}")
                            }
                        }
                }
            }
            logger.info("Scheduled reconnection for device ${deviceConfig.name} in ${opcUaConfig.reconnectDelay}ms")
        }
    }

    private fun setupAddressSubscription(address: OpcUaAddress): Future<Void> {
        val promise = Promise.promise<Void>()

        try {
            // Parse the address to get NodeIds (can be multiple for wildcards)
            parseAddressToNodeIds(address)
                .compose { nodeIdPairs ->
                    if (nodeIdPairs.isEmpty()) {
                        promise.fail(Exception("No nodes found for address: ${address.address}"))
                        return@compose Future.succeededFuture<Void>()
                    }

                    // Create monitored items for all resolved nodes
                    val createFutures = nodeIdPairs.map { (nodeId, browsePath) ->
                        createMonitoredItemForAddress(address, nodeId, browsePath)
                    }

                    Future.all<Void>(createFutures as List<Future<Void>>)
                        .map { Void.TYPE.cast(null) }
                }
                .onComplete { result ->
                    if (result.succeeded()) {
                        logger.info("Successfully set up subscription for address: ${address.address} -> ${address.topic}")
                        promise.complete()
                    } else {
                        logger.warning("Failed to set up subscription for address ${address.address}: ${result.cause()?.message}")
                        promise.fail(result.cause())
                    }
                }

        } catch (e: Exception) {
            promise.fail(e)
        }

        return promise.future()
    }

    private fun parseAddressToNodeIds(address: OpcUaAddress): Future<List<Pair<NodeId, String>>> {
        val promise = Promise.promise<List<Pair<NodeId, String>>>()

        if (address.isNodeIdAddress()) {
            // Direct node ID address: node/<node-id>
            val nodeIdStr = address.getNodeId()
            if (nodeIdStr != null) {
                try {
                    val nodeId = NodeId.parseOrNull(nodeIdStr)
                    if (nodeId != null) {
                        promise.complete(listOf(Pair(nodeId, nodeIdStr)))
                    } else {
                        promise.fail(Exception("Invalid NodeId format: $nodeIdStr"))
                    }
                } catch (e: Exception) {
                    promise.fail(Exception("Failed to parse NodeId: $nodeIdStr", e))
                }
            } else {
                promise.fail(Exception("Invalid node address format: ${address.address}"))
            }
        } else if (address.isBrowsePathAddress()) {
            // Browse path address: path/<start-node-id>/level1/+/level3/#
            val browsePath = address.getBrowsePath()
            if (browsePath != null) {
                browsePathToNodeIds(browsePath)
                    .onComplete { result ->
                        if (result.succeeded()) {
                            promise.complete(result.result())
                        } else {
                            promise.fail(result.cause())
                        }
                    }
            } else {
                promise.fail(Exception("Invalid path address format: ${address.address}"))
            }
        } else {
            promise.fail(Exception("Unsupported address format: ${address.address}"))
        }

        return promise.future()
    }

    /**
     * Parse a browse path with support for escaped forward slashes
     * Example: "ns=2;s=85\/Mqtt\/home/Original/#" becomes ["ns=2;s=85/Mqtt/home", "Original", "#"]
     */
    private fun parsePathElements(browsePath: String): List<String> {
        val elements = mutableListOf<String>()
        val current = StringBuilder()
        var i = 0

        while (i < browsePath.length) {
            when {
                browsePath[i] == '\\' && i + 1 < browsePath.length && browsePath[i + 1] == '/' -> {
                    // Escaped forward slash - add literal '/' to current element
                    current.append('/')
                    i += 2
                }
                browsePath[i] == '/' -> {
                    // Path separator - finish current element and start new one
                    if (current.isNotEmpty()) {
                        elements.add(current.toString())
                        current.clear()
                    }
                    i++
                }
                else -> {
                    // Regular character
                    current.append(browsePath[i])
                    i++
                }
            }
        }

        // Add final element if any
        if (current.isNotEmpty()) {
            elements.add(current.toString())
        }

        return elements.filter { it.isNotEmpty() }
    }

    private fun browsePathToNodeIds(browsePath: String): Future<List<Pair<NodeId, String>>> {
        val promise = Promise.promise<List<Pair<NodeId, String>>>()

        if (client == null || !isConnected) {
            promise.fail(Exception("OPC UA client not connected"))
            return promise.future()
        }

        thread {
            var promiseCompleted = false
            try {
                val resolvedNodeIds = mutableListOf<Pair<NodeId, String>>()
                val pathElements = parsePathElements(browsePath)

                logger.fine("Browse address [$browsePath] [${pathElements.joinToString("|")}]")

                fun find(nodeStr: String, itemIdx: Int, path: String): Int {
                    val item = pathElements[itemIdx]
                    logger.finest("Find $nodeStr | $item ($itemIdx) | $path")

                    val nodeId = when (nodeStr) {
                        "Root" -> Identifiers.RootFolder
                        "Objects" -> Identifiers.ObjectsFolder
                        "Types" -> Identifiers.TypesFolder
                        "Views" -> Identifiers.ViewsFolder
                        else -> NodeId.parseOrNull(nodeStr)
                    }

                    if (nodeId != null) {
                        val browseResult = client!!.browse(
                            BrowseDescription(
                                nodeId,
                                BrowseDirection.Forward,
                                Identifiers.References,
                                true,
                                Unsigned.uint(NodeClass.Variable.value or NodeClass.Object.value),
                                Unsigned.uint(BrowseResultMask.All.value)
                            )
                        ).get()

                        if (browseResult.references != null) {
                            val filteredRefs = browseResult.references.filter {
                                it.referenceTypeId == Identifiers.Organizes ||
                                it.referenceTypeId == Identifiers.HasComponent ||
                                it.referenceTypeId == Identifiers.HasProperty
                            }

                            logger.finest("Find $nodeStr | $item ($itemIdx) | $path >>> Nodes: ${filteredRefs.joinToString { it.browseName.name ?: "unknown" }}")

                            val matchingRefs = filteredRefs.filter {
                                item == "#" || item == "+" || item == it.browseName.name
                            }

                            logger.finest("Find $nodeStr | $item ($itemIdx) | $path >>> Found: ${matchingRefs.joinToString { it.browseName.name ?: "unknown" }}")

                            val nextIdx = if (item != "#" && itemIdx + 1 < pathElements.size) itemIdx + 1 else itemIdx

                            return matchingRefs.sumOf { ref ->
                                val childNodeId = ref.nodeId.toNodeId(client!!.namespaceTable).orElse(null)
                                val browsePath = path + "/" + (ref.browseName.name ?: "unknown")

                                if (childNodeId != null) {
                                    when (ref.nodeClass) {
                                        NodeClass.Variable -> {
                                            val count = find(childNodeId.toParseableString(), nextIdx, browsePath)
                                            if (count > 0) {
                                                count
                                            } else {
                                                resolvedNodeIds.add(Pair(childNodeId, browsePath))
                                                1
                                            }
                                        }
                                        NodeClass.Object -> {
                                            find(childNodeId.toParseableString(), nextIdx, browsePath)
                                        }
                                        else -> 0
                                    }
                                } else 0
                            }
                        }
                    }
                    return 0
                }

                val start = pathElements.first()
                find(start, 1, start)

                logger.info("Browse path [$browsePath] resolved to ${resolvedNodeIds.size} nodes")
                if (!promiseCompleted) {
                    promiseCompleted = true
                    promise.complete(resolvedNodeIds)
                }

            } catch (e: Exception) {
                logger.severe("Error browsing path $browsePath: ${e.message}")
                if (!promiseCompleted) {
                    promiseCompleted = true
                    promise.fail(e)
                }
            }
        }

        return promise.future()
    }

    private fun createMonitoredItemForAddress(address: OpcUaAddress, nodeId: NodeId, browsePath: String = nodeId.toParseableString()): Future<Void> {
        val promise = Promise.promise<Void>()

        if (subscription == null) {
            promise.fail(Exception("OPC UA subscription not available"))
            return promise.future()
        }

        try {
            val clientHandle = subscription!!.nextClientHandle()

            val request = MonitoredItemCreateRequest(
                ReadValueId(nodeId, Unsigned.uint(13), null, QualifiedName.NULL_VALUE),
                MonitoringMode.Reporting,
                MonitoringParameters(
                    clientHandle,
                    opcUaConfig.monitoringParameters.samplingInterval,
                    null, // No data change filter for now
                    Unsigned.uint(opcUaConfig.monitoringParameters.bufferSize),
                    opcUaConfig.monitoringParameters.discardOldest
                )
            )

            subscription!!.createMonitoredItems(
                TimestampsToReturn.Both,
                listOf(request)
            ) { item, _ ->
                // Set value consumer
                item.setValueConsumer { dataValue ->
                    handleOpcUaValueChangeForAddress(address, nodeId, browsePath, dataValue)
                }
            }.whenComplete { items, throwable ->
                if (throwable == null && items.isNotEmpty()) {
                    val item = items[0]
                    if (item.statusCode.isGood) {
                        // Track the subscription
                        addressSubscriptions[address.address] = AddressSubscription(address, nodeId)
                        opcUaMonitoredItems[address.address] = item

                        logger.fine("Created monitored item for address ${address.address}, nodeId $nodeId")
                        promise.complete()
                    } else {
                        logger.warning("Failed to create monitored item for address ${address.address}: ${item.statusCode}")
                        promise.fail(Exception("Monitored item creation failed: ${item.statusCode}"))
                    }
                } else {
                    logger.severe("Failed to create monitored item for address ${address.address}: ${throwable?.message}")
                    promise.fail(throwable ?: Exception("Unknown error creating monitored item"))
                }
            }

        } catch (e: Exception) {
            promise.fail(e)
        }

        return promise.future()
    }

    private fun handleOpcUaValueChangeForAddress(address: OpcUaAddress, nodeId: NodeId, browsePath: String, dataValue: DataValue) {
        try {
            // Convert OPC UA value to MQTT message
            val value = dataValue.value?.value
            val timestamp = dataValue.sourceTime?.javaInstant ?: Instant.now()
            val statusCode = dataValue.statusCode?.value ?: 0

            // Create MQTT message payload with proper data types
            val payload = JsonObject()

            // Put value with correct JSON data type
            when (value) {
                null -> payload.putNull("value")
                is Boolean -> payload.put("value", value)
                is Byte -> payload.put("value", value.toInt())
                is Short -> payload.put("value", value.toInt())
                is Int -> payload.put("value", value)
                is Long -> payload.put("value", value)
                is Float -> payload.put("value", value.toDouble())
                is Double -> payload.put("value", value)
                is String -> payload.put("value", value)
                is UByte -> payload.put("value", value.toInt())
                is UShort -> payload.put("value", value.toInt())
                is UInteger -> payload.put("value", value.toLong())
                is ULong -> payload.put("value", value.toLong())
                else -> payload.put("value", value.toString())
            }

            payload.put("timestamp", timestamp.toString())
                .put("status", statusCode)

            // Generate MQTT topic based on publish mode
            val mqttTopic = address.generateMqttTopic(
                namespace = deviceConfig.namespace,
                nodeId = nodeId.toParseableString(),
                browsePath = browsePath
            )

            // Publish to the generated MQTT topic
            val mqttMessage = MqttMessage(
                messageId = 0,
                topicName = mqttTopic,
                payload = payload.encode().toByteArray(),
                qosLevel = 0,
                isRetain = false,
                isDup = false,
                isQueued = false,
                clientId = "opcua-connector-${deviceConfig.name}"
            )

            // Send to OPC UA extension for proper message bus publishing
            vertx.eventBus().publish(OpcUaExtension.Companion.ADDRESS_OPCUA_VALUE_PUBLISH, mqttMessage)

            logger.fine("Published OPC UA value change: $mqttTopic = $value (from ${address.address})")

        } catch (e: Exception) {
            logger.severe("Error handling OPC UA value change for address ${address.address}: ${e.message}")
        }
    }
}