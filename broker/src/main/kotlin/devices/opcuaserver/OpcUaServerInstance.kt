package at.rocworks.devices.opcuaserver

import at.rocworks.data.MqttMessage
import at.rocworks.Utils
import at.rocworks.auth.UserManager
import at.rocworks.handlers.SessionHandler
import io.vertx.core.Vertx
import org.eclipse.milo.opcua.sdk.server.OpcUaServer
import org.eclipse.milo.opcua.sdk.server.api.config.OpcUaServerConfigBuilder
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy
import org.eclipse.milo.opcua.stack.core.transport.TransportProfile
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode
import org.eclipse.milo.opcua.stack.server.EndpointConfiguration
import java.net.InetAddress
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger
import kotlin.concurrent.thread

/**
 * OPC UA Server instance that exposes MQTT topics as OPC UA nodes
 */
class OpcUaServerInstance(
    private val config: OpcUaServerConfig,
    private val vertx: Vertx,
    private val sessionHandler: SessionHandler,
    private val userManager: UserManager?
) {
    companion object {
        private val logger: Logger = Utils.getLogger(OpcUaServerInstance::class.java)
    }

    private var server: OpcUaServer? = null
    private var nodeManager: OpcUaServerNodeManager? = null
    private val subscribedTopics = ConcurrentHashMap<String, String>() // topic -> clientId
    private val activeConnections = AtomicInteger(0)
    private val writeValueQueue = ArrayBlockingQueue<Pair<String, ByteArray>>(config.bufferSize)
    private val writeValueStop = AtomicBoolean(false)
    private var writeValueThread: Thread? = null
    private val internalClientId = "opcua-server-${config.name}"
    private var status = OpcUaServerStatus.Status.STOPPED

    /**
     * Start the OPC UA Server
     */
    fun start(): OpcUaServerStatus {
        return try {
            logger.info("Starting OPC UA Server '${config.name}' on port ${config.port}")
            status = OpcUaServerStatus.Status.STARTING

            // Create server with simplified configuration
            val serverConfig = createServerConfig()
            server = OpcUaServer(serverConfig)

            // Create and register namespace manager
            val namespaceManager = OpcUaServerNodeManager(
                server!!,
                config.namespaceUri,
                config
            ) { topic, dataValue ->
                // Handle OPC UA node writes
                handleOpcUaWrite(topic, dataValue)
            }
            nodeManager = namespaceManager
            // Note: Namespace manager will be automatically registered during server startup
            // The ManagedNamespaceWithLifecycle will handle its own registration
            logger.info("Namespace manager created for namespace: ${namespaceManager.namespaceUri}")

            // Start the server
            server!!.startup().get(10, TimeUnit.SECONDS)

            // Initialize the namespace and create root folder
            nodeManager?.initializeNodes()

            // Create nodes for configured addresses
            setupConfiguredNodes()

            // Subscribe to MQTT topics
            subscribeToMqttTopics()

            // Start write value thread
            startWriteValueThread()

            status = OpcUaServerStatus.Status.RUNNING
            logger.info("OPC UA Server '${config.name}' started successfully on port ${config.port}")

            getStatus()

        } catch (e: Exception) {
            logger.severe("Failed to start OPC UA Server '${config.name}': ${e.message}")
            status = OpcUaServerStatus.Status.ERROR
            getStatus(error = e.message)
        }
    }

    /**
     * Stop the OPC UA Server
     */
    fun stop(): OpcUaServerStatus {
        return try {
            logger.info("Stopping OPC UA Server '${config.name}'")
            status = OpcUaServerStatus.Status.STOPPING

            // Stop write value thread
            stopWriteValueThread()

            // Unsubscribe from MQTT topics
            unsubscribeFromMqttTopics()

            // Shutdown namespace manager
            nodeManager?.cleanupNodes()

            // Shutdown server
            server?.shutdown()?.get(10, TimeUnit.SECONDS)

            status = OpcUaServerStatus.Status.STOPPED
            logger.info("OPC UA Server '${config.name}' stopped successfully")

            getStatus()

        } catch (e: Exception) {
            logger.severe("Failed to stop OPC UA Server '${config.name}': ${e.message}")
            status = OpcUaServerStatus.Status.ERROR
            getStatus(error = e.message)
        }
    }

    /**
     * Create simplified OPC UA server configuration
     */
    private fun createServerConfig(): org.eclipse.milo.opcua.sdk.server.api.config.OpcUaServerConfig {
        // Simplified server configuration without certificates for now
        val endpoints = setOf(
            EndpointConfiguration.newBuilder()
                .setBindAddress("0.0.0.0")
                .setBindPort(config.port)
                .setHostname(InetAddress.getLocalHost().hostName)
                .setPath("/${config.path}")
                .setSecurityPolicy(SecurityPolicy.None)
                .setSecurityMode(MessageSecurityMode.None)
                .setTransportProfile(TransportProfile.TCP_UASC_UABINARY)
                .build()
        )

        return OpcUaServerConfigBuilder()
            .setApplicationName(LocalizedText.english("MonsterMQ OPC UA Server - ${config.name}"))
            .setApplicationUri("urn:MonsterMQ:OpcUaServer:${config.name}")
            .setProductUri("urn:MonsterMQ:OpcUaServer")
            .setEndpoints(endpoints)
            .build()
    }

    /**
     * Setup configured nodes for OPC UA addresses
     */
    private fun setupConfiguredNodes() {
        config.addresses.forEach { address ->
            try {
                nodeManager?.createOrUpdateVariableNode(
                    address.mqttTopic,
                    address
                )
                logger.fine("Created OPC UA node for topic: ${address.mqttTopic}")
            } catch (e: Exception) {
                logger.warning("Failed to create OPC UA node for topic ${address.mqttTopic}: ${e.message}")
            }
        }
    }

    /**
     * Subscribe to MQTT topics for this OPC UA server
     */
    private fun subscribeToMqttTopics() {
        config.addresses.forEach { address ->
            try {
                sessionHandler.subscribeInternal(internalClientId, address.mqttTopic, 0) { message ->
                    handleMqttMessage(message)
                }
                subscribedTopics[address.mqttTopic] = internalClientId
                logger.fine("Subscribed to MQTT topic: ${address.mqttTopic}")
            } catch (e: Exception) {
                logger.warning("Failed to subscribe to MQTT topic ${address.mqttTopic}: ${e.message}")
            }
        }
    }

    /**
     * Unsubscribe from MQTT topics
     */
    private fun unsubscribeFromMqttTopics() {
        subscribedTopics.forEach { (topic, clientId) ->
            try {
                sessionHandler.unsubscribeInternal(clientId, topic)
                logger.fine("Unsubscribed from MQTT topic: $topic")
            } catch (e: Exception) {
                logger.warning("Failed to unsubscribe from MQTT topic $topic: ${e.message}")
            }
        }
        subscribedTopics.clear()
    }

    /**
     * Handle incoming MQTT messages and update OPC UA nodes
     */
    private fun handleMqttMessage(message: MqttMessage) {
        // Skip messages from this OPC UA server to prevent loops
        if (message.sender == internalClientId) {
            logger.finest("Skipping message from self: ${message.topicName}")
            return
        }

        try {
            // Find the corresponding address configuration that matches this topic
            val matchingAddress = config.addresses.find { address ->
                // Check for exact match first
                if (address.mqttTopic == message.topicName) {
                    return@find true
                }
                // Check for wildcard match
                if (address.mqttTopic.contains("#") || address.mqttTopic.contains("+")) {
                    return@find topicMatches(address.mqttTopic, message.topicName)
                }
                false
            }

            if (matchingAddress != null) {
                logger.fine("Processing MQTT message for topic: ${message.topicName} (matched pattern: ${matchingAddress.mqttTopic})")

                // Convert MQTT message to OPC UA DataValue
                val dataValue = OpcUaDataConverter.mqttToOpcUa(
                    message.payload,
                    matchingAddress.dataType,
                    message.time
                )

                // Update or create the OPC UA node for the specific topic (not the pattern)
                val nodeExists = nodeManager?.updateNodeValue(message.topicName, dataValue) ?: false
                if (!nodeExists) {
                    // Create new node for this specific topic using the pattern's configuration
                    val specificAddress = matchingAddress.copy(mqttTopic = message.topicName)
                    nodeManager?.createOrUpdateVariableNode(message.topicName, specificAddress, dataValue)
                    logger.info("Created new OPC UA node for topic: ${message.topicName}")
                }
            } else {
                logger.finest("No matching address pattern found for topic: ${message.topicName}")
            }
        } catch (e: Exception) {
            logger.warning("Error handling MQTT message for topic ${message.topicName}: ${e.message}")
        }
    }

    /**
     * Check if an MQTT topic matches a pattern with wildcards
     */
    private fun topicMatches(pattern: String, topic: String): Boolean {
        // Convert MQTT wildcard pattern to regex
        // + matches a single level, # matches multiple levels
        val regexPattern = pattern
            .replace("+", "[^/]+")
            .replace("#", ".*")

        return topic.matches(Regex("^$regexPattern$"))
    }

    /**
     * Handle OPC UA node writes and publish to MQTT
     */
    private fun handleOpcUaWrite(topic: String, dataValue: org.eclipse.milo.opcua.stack.core.types.builtin.DataValue) {
        try {
            // Find the corresponding address configuration
            val address = config.addresses.find { it.mqttTopic == topic }
            if (address != null) {
                // Convert OPC UA DataValue to MQTT payload
                val payload = OpcUaDataConverter.opcUaToMqtt(dataValue, address.dataType)

                // Queue for publishing to MQTT
                if (!writeValueQueue.offer(Pair(topic, payload))) {
                    logger.warning("Write value queue is full, dropping message for topic: $topic")
                }
            }
        } catch (e: Exception) {
            logger.warning("Error handling OPC UA write for topic $topic: ${e.message}")
        }
    }

    /**
     * Start the write value thread for MQTT publishing
     */
    private fun startWriteValueThread() {
        writeValueStop.set(false)
        writeValueThread = thread(name = "OpcUaServer-WriteThread-${config.name}") {
            logger.info("Write value thread started for OPC UA Server '${config.name}'")

            while (!writeValueStop.get()) {
                try {
                    val (topic, payload) = writeValueQueue.poll(100, TimeUnit.MILLISECONDS)
                        ?: continue

                    // Publish to MQTT with sender identification
                    val message = MqttMessage(
                        messageId = 0,
                        topicName = topic,
                        payload = payload,
                        qosLevel = 0,
                        isRetain = false,
                        isDup = false,
                        isQueued = false,
                        clientId = internalClientId,
                        sender = internalClientId // Mark as coming from this OPC UA server
                    )

                    sessionHandler.publishInternal(internalClientId, message)
                    logger.fine("Published OPC UA write to MQTT topic: $topic")

                } catch (e: InterruptedException) {
                    // Normal shutdown
                    break
                } catch (e: Exception) {
                    logger.warning("Error in write value thread: ${e.message}")
                }
            }
            logger.info("Write value thread stopped for OPC UA Server '${config.name}'")
        }
    }

    /**
     * Stop the write value thread
     */
    private fun stopWriteValueThread() {
        writeValueStop.set(true)
        writeValueThread?.interrupt()
        writeValueThread?.join(5000)
    }

    /**
     * Get current server status
     */
    fun getStatus(error: String? = null): OpcUaServerStatus {
        return OpcUaServerStatus(
            serverName = config.name,
            nodeId = config.nodeId,
            status = status,
            port = config.port,
            error = error
        )
    }
}