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
    private var config: OpcUaServerConfig,
    private val vertx: Vertx,
    private val sessionHandler: SessionHandler,
    private val userManager: UserManager?
) {
    companion object {
        private val logger: Logger = Utils.getLogger(OpcUaServerInstance::class.java)
    }

    private var server: OpcUaServer? = null
    private var namespace: OpcUaServerNamespace? = null
    private var nodeManager: OpcUaServerNodes? = null
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

            // Create namespace following gateway pattern
            val opcUaNamespace = OpcUaServerNamespace(server!!, config.namespaceUri)
            namespace = opcUaNamespace
            opcUaNamespace.startup()

            // Create node manager following gateway pattern
            val opcUaNodes = OpcUaServerNodes(
                config,
                server!!,
                opcUaNamespace,
                opcUaNamespace.namespaceIndex
            ) { topic, dataValue ->
                // Handle OPC UA node writes
                handleOpcUaWrite(topic, dataValue)
            }
            nodeManager = opcUaNodes
            opcUaNodes.startup()

            logger.info("Namespace and node manager created for namespace: ${config.namespaceUri}")

            // Start the server
            server!!.startup().get(10, TimeUnit.SECONDS)

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

            // Shutdown namespace and node manager
            nodeManager?.shutdown()
            namespace?.shutdown()

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
        logger.info("Setting up ${config.addresses.size} configured nodes for OPC UA server '${config.name}'")
        config.addresses.forEach { address ->
            // Skip wildcard patterns - they are for subscriptions only, not for creating nodes
            if (address.mqttTopic.contains("#") || address.mqttTopic.contains("+")) {
                logger.info("Skipping node creation for wildcard pattern: ${address.mqttTopic}")
                return@forEach
            }

            try {
                nodeManager?.createOrUpdateVariableNode(
                    address.mqttTopic,
                    address
                )
                logger.info("Created OPC UA node for topic: ${address.mqttTopic} with NodeId: ns=${nodeManager?.namespaceIndex};s=${address.mqttTopic}")
            } catch (e: Exception) {
                logger.severe("Failed to create OPC UA node for topic ${address.mqttTopic}: ${e.message}")
                e.printStackTrace()
            }
        }
    }

    /**
     * Update server configuration dynamically without restart
     */
    fun updateConfiguration(newConfig: OpcUaServerConfig) {
        logger.info("Updating configuration for OPC UA server '${config.name}'")

        // Check for new topic mappings to subscribe to
        val oldTopics = config.addresses.map { it.mqttTopic }.toSet()
        val newTopics = newConfig.addresses.map { it.mqttTopic }.toSet()
        val topicsToAdd = newTopics - oldTopics
        val topicsToRemove = oldTopics - newTopics

        // Unsubscribe from removed topics
        topicsToRemove.forEach { topic ->
            try {
                sessionHandler.unsubscribeInternal(internalClientId, topic)
                subscribedTopics.remove(topic)
                logger.info("Unsubscribed OPC UA server '${config.name}' from topic: $topic")
            } catch (e: Exception) {
                logger.warning("Failed to unsubscribe from topic $topic: ${e.message}")
            }
        }

        // Subscribe to new topics
        topicsToAdd.forEach { topic ->
            try {
                sessionHandler.subscribeInternal(internalClientId, topic, 0) { message ->
                    handleMqttMessage(message, newConfig.addresses.find { it.mqttTopic == topic })
                }
                subscribedTopics[topic] = internalClientId
                logger.info("Subscribed OPC UA server '${config.name}' to new topic: $topic")
            } catch (e: Exception) {
                logger.warning("Failed to subscribe to new topic $topic: ${e.message}")
            }
        }

        // Update the configuration
        this.config = newConfig
    }

    /**
     * Subscribe to MQTT topics for this OPC UA server
     */
    private fun subscribeToMqttTopics() {
        config.addresses.forEach { address ->
            try {
                sessionHandler.subscribeInternal(internalClientId, address.mqttTopic, 0) { message ->
                    handleMqttMessage(message, address)
                }
                subscribedTopics[address.mqttTopic] = internalClientId
                logger.info("Subscribed OPC UA server '${config.name}' to MQTT topic: ${address.mqttTopic}")
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
    private fun handleMqttMessage(message: MqttMessage, addressConfig: OpcUaServerAddress? = null) {
        // Skip messages from this OPC UA server to prevent loops
        if (message.sender == internalClientId) {
            logger.fine("Skipping message from self: ${message.topicName}")
            return
        }

        logger.info("OPC UA server '${config.name}' received MQTT message on topic: ${message.topicName}")

        try {
            // Use the provided address config, or find one that matches
            val matchingAddress = addressConfig ?: config.addresses.find { address ->
                address.mqttTopic == message.topicName ||
                // Simple wildcard check for patterns ending with #
                (address.mqttTopic.endsWith("#") &&
                 message.topicName.startsWith(address.mqttTopic.dropLast(1)))
            }

            if (matchingAddress != null) {
                logger.info("Processing MQTT message for topic: ${message.topicName}")

                // Convert MQTT message to OPC UA DataValue
                val dataValue = OpcUaDataConverter.mqttToOpcUa(
                    message.payload,
                    matchingAddress.dataType,
                    message.time
                )

                // Create or update the OPC UA node dynamically like the gateway does
                createOrUpdateNode(message.topicName, matchingAddress, dataValue)

            } else {
                logger.fine("No matching address pattern found for topic: ${message.topicName}")
            }
        } catch (e: Exception) {
            logger.warning("Error handling MQTT message for topic ${message.topicName}: ${e.message}")
            e.printStackTrace()
        }
    }

    /**
     * Create or update OPC UA node for a specific topic (like the gateway implementation)
     */
    private fun createOrUpdateNode(topicName: String, addressConfig: OpcUaServerAddress, dataValue: org.eclipse.milo.opcua.stack.core.types.builtin.DataValue) {
        // Check if node already exists
        val nodeExists = nodeManager?.updateNodeValue(topicName, dataValue) ?: false

        if (!nodeExists) {
            // Create new node dynamically
            val specificAddress = addressConfig.copy(mqttTopic = topicName)
            nodeManager?.createOrUpdateVariableNode(topicName, specificAddress, dataValue)
            logger.info("Created new OPC UA node for topic: $topicName")
        } else {
            logger.info("Updated existing OPC UA node for topic: $topicName")
        }
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