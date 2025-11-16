package at.rocworks.devices.sparkplugb

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import at.rocworks.bus.EventBusAddresses
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.devices.SparkplugBDecoderConfig
import at.rocworks.stores.devices.SparkplugBDecoderRule
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import org.eclipse.tahu.message.SparkplugBPayloadDecoder
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger

/**
 * SparkplugB Decoder Connector - Handles SparkplugB message decoding with regex-based routing
 *
 * Responsibilities:
 * - Subscribe to SparkplugB messages (e.g., spBv1.0/#)
 * - Decode SparkplugB protobuf payloads
 * - Match nodeId/deviceId against configured rules
 * - Apply regex transformations to topic variables
 * - Publish decoded messages to destination MQTT topics
 */
class SparkplugBDecoderConnector : AbstractVerticle() {

    // Metrics counters
    private val messagesInCounter = AtomicLong(0)
    private val messagesOutCounter = AtomicLong(0)
    private val messagesSkippedCounter = AtomicLong(0)
    private var lastMetricsReset = System.currentTimeMillis()

    private val logger: Logger = Utils.getLogger(this::class.java)

    // Device configuration
    private lateinit var deviceConfig: DeviceConfig
    private lateinit var decoderConfig: SparkplugBDecoderConfig

    // SparkplugB decoder
    private val sparkplugDecoder = SparkplugBPayloadDecoder()

    // Internal MQTT client ID
    private var internalClientId: String? = null

    companion object {
        // SparkplugB topic structure: spBv1.0/namespace/group_id/message_type/edge_node_id/[device_id]
        // Message types with protobuf data
        private val PROTOBUF_MESSAGE_TYPES = listOf(
            "NBIRTH", // Birth certificate for Sparkplug Edge Nodes
            "NDEATH", // Death certificate for Sparkplug Edge Nodes
            "DBIRTH", // Birth certificate for Devices
            "DDEATH", // Death certificate for Devices
            "NDATA",  // Edge Node data message
            "DDATA",  // Device data message
            "NCMD",   // Edge Node command message
            "DCMD"    // Device command message
            // NOTE: "STATE" is not a protobuf message
        )
    }

    override fun start(startPromise: Promise<Void>) {
        try {
            // Load device configuration
            val deviceJson = config().getJsonObject("device")
            deviceConfig = DeviceConfig.fromJsonObject(deviceJson)
            decoderConfig = SparkplugBDecoderConfig.fromJsonObject(deviceConfig.config)

            logger.info("Starting SparkplugBDecoderConnector for device: ${deviceConfig.name}")

            // Validate configuration
            val validationErrors = decoderConfig.validate()
            if (validationErrors.isNotEmpty()) {
                val errorMsg = "Configuration validation failed: ${validationErrors.joinToString(", ")}"
                logger.severe(errorMsg)
                startPromise.fail(errorMsg)
                return
            }

            // Register metrics endpoint
            setupMetricsEndpoint()

            // Subscribe to SparkplugB messages
            subscribeToSparkplugMessages()
                .onComplete { result ->
                    if (result.succeeded()) {
                        logger.info("Successfully subscribed to SparkplugB messages for device ${deviceConfig.name}")
                        startPromise.complete()
                    } else {
                        logger.severe("Failed to subscribe to SparkplugB messages: ${result.cause()?.message}")
                        startPromise.fail(result.cause())
                    }
                }

        } catch (e: Exception) {
            logger.severe("Exception during SparkplugBDecoderConnector startup: ${e.message}")
            e.printStackTrace()
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Stopping SparkplugBDecoderConnector for device: ${deviceConfig.name}")

        // Unsubscribe from MQTT messages
        internalClientId?.let { clientId ->
            val sessionHandler = Monster.getSessionHandler()
            if (sessionHandler != null) {
                val subscriptionTopic = decoderConfig.getSubscriptionTopic()
                sessionHandler.unsubscribeInternalClient(clientId, subscriptionTopic)
                sessionHandler.unregisterInternalClient(clientId)
                logger.info("Unsubscribed internal client '$clientId' from topic '$subscriptionTopic'")
            }
        }

        stopPromise.complete()
    }

    private fun setupMetricsEndpoint() {
        val addr = EventBusAddresses.SparkplugBDecoder.connectorMetrics(deviceConfig.name)
        vertx.eventBus().consumer<JsonObject>(addr) { msg ->
            try {
                val now = System.currentTimeMillis()
                val elapsedMs = now - lastMetricsReset
                val elapsedSec = if (elapsedMs > 0) elapsedMs / 1000.0 else 1.0
                val inCount = messagesInCounter.getAndSet(0)
                val outCount = messagesOutCounter.getAndSet(0)
                val skippedCount = messagesSkippedCounter.getAndSet(0)
                lastMetricsReset = now

                val json = JsonObject()
                    .put("device", deviceConfig.name)
                    .put("messagesInRate", inCount / elapsedSec)
                    .put("messagesOutRate", outCount / elapsedSec)
                    .put("messagesSkippedRate", skippedCount / elapsedSec)
                    .put("elapsedMs", elapsedMs)
                msg.reply(json)
            } catch (e: Exception) {
                msg.fail(500, e.message)
            }
        }
        logger.info("Registered SparkplugB decoder metrics endpoint for device ${deviceConfig.name} at address $addr")
    }

    /**
     * Subscribe to SparkplugB MQTT messages using SessionHandler internal client mechanism
     */
    private fun subscribeToSparkplugMessages(): Future<Void> {
        val promise = Promise.promise<Void>()

        val subscriptionTopic = decoderConfig.getSubscriptionTopic()
        logger.info("Subscribing to SparkplugB messages: $subscriptionTopic for device ${deviceConfig.name}")

        // Get SessionHandler
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler == null) {
            val errorMsg = "SessionHandler not available for MQTT subscriptions"
            logger.severe(errorMsg)
            promise.fail(errorMsg)
            return promise.future()
        }

        // Use internal client ID
        internalClientId = "sparkplugb-decoder-${deviceConfig.name}"

        // Register eventBus consumer to receive MQTT messages (handles both individual and bulk messages)
        vertx.eventBus().consumer<Any>(EventBusAddresses.Client.messages(internalClientId!!)) { busMessage ->
            try {
                val messages = when (val body = busMessage.body()) {
                    is BrokerMessage -> listOf(body)
                    is at.rocworks.data.BulkClientMessage -> body.messages
                    else -> {
                        logger.warning("Unknown message type: ${body?.javaClass?.simpleName}")
                        emptyList()
                    }
                }
                messages.forEach { message ->
                    handleSparkplugMessage(message)
                }
            } catch (e: Exception) {
                logger.warning("Error processing MQTT message: ${e.message}")
            }
        }

        // Subscribe via SessionHandler - this treats us as a regular MQTT client internally
        logger.info("Internal subscription for SparkplugB decoder '$internalClientId' to MQTT topic '$subscriptionTopic' with QoS 0")
        sessionHandler.subscribeInternalClient(internalClientId!!, subscriptionTopic, 0)

        logger.info("Successfully created internal MQTT subscription for device ${deviceConfig.name}")
        promise.complete()

        return promise.future()
    }

    /**
     * Handle incoming SparkplugB message
     */
    private fun handleSparkplugMessage(message: BrokerMessage) {
        try {
            messagesInCounter.incrementAndGet()

            val topic = message.topicName
            logger.fine { "Received SparkplugB message: $topic" }

            // Parse SparkplugB topic: spBv1.0/namespace/group_id/message_type/edge_node_id/[device_id]
            val topicLevels = topic.split("/")
            if (topicLevels.size < 5) {
                logger.warning("Invalid SparkplugB topic structure: $topic (expected at least 5 levels)")
                messagesSkippedCounter.incrementAndGet()
                return
            }

            val namespace = topicLevels[0]
            val groupId = topicLevels[1]
            val messageType = topicLevels[3]
            val edgeNodeId = topicLevels[4]
            val deviceId = if (topicLevels.size > 5) topicLevels[5] else ""

            logger.fine { "Parsed SparkplugB topic - namespace: $namespace, groupId: $groupId, messageType: $messageType, nodeId: $edgeNodeId, deviceId: $deviceId" }

            // Skip if namespace doesn't match configured source namespace
            if (namespace != decoderConfig.sourceNamespace) {
                logger.fine { "Skipping message - namespace mismatch: $namespace != ${decoderConfig.sourceNamespace}" }
                messagesSkippedCounter.incrementAndGet()
                return
            }

            // Find matching rule
            val rule = decoderConfig.findMatchingRule(edgeNodeId, deviceId)
            if (rule == null) {
                logger.fine { "No matching rule found for nodeId=$edgeNodeId, deviceId=$deviceId" }
                messagesSkippedCounter.incrementAndGet()
                return
            }

            logger.fine { "Matched rule: ${rule.name} for nodeId=$edgeNodeId, deviceId=$deviceId" }

            // Decode and publish based on message type
            if (messageType == "STATE") {
                // STATE is not a protobuf message - pass through as-is
                publishDecodedMessage(rule, edgeNodeId, deviceId, "STATE", message.payload)
            } else if (messageType in PROTOBUF_MESSAGE_TYPES) {
                // Decode protobuf message and publish each metric
                decodeAndPublishMetrics(rule, edgeNodeId, deviceId, messageType, message.payload)
            } else {
                logger.warning("Unknown SparkplugB message type: $messageType")
                messagesSkippedCounter.incrementAndGet()
            }

        } catch (e: Exception) {
            logger.severe("Error handling SparkplugB message [${message.topicName}]: ${e.message}")
            e.printStackTrace()
            messagesSkippedCounter.incrementAndGet()
        }
    }

    /**
     * Decode SparkplugB protobuf payload and publish each metric as a separate MQTT message
     */
    private fun decodeAndPublishMetrics(
        rule: SparkplugBDecoderRule,
        nodeId: String,
        deviceId: String,
        messageType: String,
        payload: ByteArray
    ) {
        try {
            val spbPayload = sparkplugDecoder.buildFromByteArray(payload, null)
            logger.finest { "Decoded SparkplugB payload with ${spbPayload.metrics.size} metrics" }

            spbPayload.metrics.forEach { metric ->
                val metricName = if (metric.hasName()) metric.name else "alias/${metric.alias}"

                // Build metric payload as JSON
                val metricPayload = JsonObject()
                    .put("Timestamp", metric.timestamp)
                    .put("DataType", metric.dataType.toString())

                try {
                    metricPayload.put("Value", metric.value)
                } catch (e: Exception) {
                    logger.severe { "Failed to decode metric value [$metricName]: ${e.message}" }
                    metricPayload.put("Value", null)
                }

                // Build destination topic with metric name appended
                val destinationTopic = rule.buildDestinationTopic(nodeId, deviceId) + "/$metricName"

                publishToMqtt(destinationTopic, metricPayload.encode().toByteArray())
                messagesOutCounter.incrementAndGet()
            }

        } catch (e: Exception) {
            logger.severe("Failed to decode SparkplugB protobuf payload: ${e.message}")
            e.printStackTrace()
            messagesSkippedCounter.incrementAndGet()
        }
    }

    /**
     * Publish decoded message (for STATE messages or other non-protobuf types)
     */
    private fun publishDecodedMessage(
        rule: SparkplugBDecoderRule,
        nodeId: String,
        deviceId: String,
        suffix: String,
        payload: ByteArray
    ) {
        val destinationTopic = rule.buildDestinationTopic(nodeId, deviceId) + "/$suffix"
        publishToMqtt(destinationTopic, payload)
        messagesOutCounter.incrementAndGet()
    }

    /**
     * Publish message to MQTT broker via SessionHandler
     */
    private fun publishToMqtt(topic: String, payload: ByteArray) {
        val mqttMessage = BrokerMessage(
            messageId = 0,
            topicName = topic,
            payload = payload,
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = "sparkplugb-decoder-${deviceConfig.name}"
        )

        logger.fine { "Publishing decoded message to topic: $topic" }
        vertx.eventBus().publish(SparkplugBDecoderExtension.ADDRESS_SPARKPLUGB_DECODER_PUBLISH, mqttMessage)
    }
}
