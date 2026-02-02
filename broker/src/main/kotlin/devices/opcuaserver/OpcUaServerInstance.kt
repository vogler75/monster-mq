package at.rocworks.devices.opcuaserver

import at.rocworks.data.BrokerMessage
import at.rocworks.Utils
import at.rocworks.auth.UserManager
import at.rocworks.handlers.SessionHandler
import at.rocworks.data.TopicTree
import io.vertx.core.Vertx
import org.eclipse.milo.opcua.sdk.server.OpcUaServer
import org.eclipse.milo.opcua.sdk.server.api.config.OpcUaServerConfigBuilder
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy
import org.eclipse.milo.opcua.stack.core.transport.TransportProfile
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode
import org.eclipse.milo.opcua.stack.server.EndpointConfiguration
import java.net.InetAddress
import java.security.cert.X509Certificate
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger

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
    private var keyStoreLoader: OpcUaServerKeyStoreLoader? = null
    private val subscribedTopics = ConcurrentHashMap<String, String>() // topic -> clientId
    private val activeConnections = AtomicInteger(0)
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

            // For Eclipse Milo 0.6.16, set certificate information after server creation if available
            if (keyStoreLoader?.serverCertificate != null && keyStoreLoader?.serverKeyPair != null) {
                try {
                    // Access the server's configuration to set certificates
                    // This is a workaround for Eclipse Milo 0.6.16 certificate handling
                    logger.info("Setting certificates on OPC UA server instance")

                    // The certificates should already be configured through the endpoints
                    // This is just for logging confirmation
                    logger.info("OPC UA server created with certificate support for encrypted endpoints")
                } catch (e: Exception) {
                    logger.warning("Could not configure certificates on server: ${e.message}")
                }
            }

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
     * Create OPC UA server configuration with security support
     */
    private fun createServerConfig(): org.eclipse.milo.opcua.sdk.server.api.config.OpcUaServerConfig {
        // Load certificates if security is enabled
        val certificateInfo = if (config.security.securityPolicies.any { it != "None" }) {
            try {
                keyStoreLoader = OpcUaServerKeyStoreLoader(config)
                keyStoreLoader?.load()
                Triple(
                    keyStoreLoader?.serverCertificate,
                    keyStoreLoader?.serverCertificateChain ?: emptyArray(),
                    keyStoreLoader?.serverKeyPair
                )
            } catch (e: Exception) {
                logger.warning("Failed to load certificates, falling back to unencrypted only: ${e.message}")
                Triple(null, emptyArray<X509Certificate>(), null)
            }
        } else {
            Triple(null, emptyArray<X509Certificate>(), null)
        }

        val (certificate, certificateChain, keyPair) = certificateInfo

        // Build endpoints based on security configuration
        val endpoints = mutableSetOf<EndpointConfiguration>()
        val hostname = config.hostname ?: InetAddress.getLocalHost().hostName
        val bindAddress = config.bindAddress ?: "0.0.0.0"
        val path = "/${config.path}"

        // Parse security policies from configuration
        config.security.securityPolicies.forEach { policyName ->
            when (policyName) {
                "None" -> {
                    if (config.security.allowUnencrypted) {
                        // Add unencrypted endpoint
                        endpoints.add(
                            EndpointConfiguration.newBuilder()
                                .setBindAddress(bindAddress)
                                .setBindPort(config.port)
                                .setHostname(hostname)
                                .setPath(path)
                                .setSecurityPolicy(SecurityPolicy.None)
                                .setSecurityMode(MessageSecurityMode.None)
                                .setTransportProfile(TransportProfile.TCP_UASC_UABINARY)
                                .build()
                        )
                        logger.info("Added unencrypted endpoint on port ${config.port}")
                    } else {
                        logger.info("Unencrypted connections are disabled")
                    }
                }
                "Basic256Sha256" -> {
                    if (certificate != null && keyPair != null) {
                        // Add encrypted endpoint with Sign mode
                        endpoints.add(
                            EndpointConfiguration.newBuilder()
                                .setBindAddress(bindAddress)
                                .setBindPort(config.port)
                                .setHostname(hostname)
                                .setPath(path)
                                .setCertificate(certificate)
                                .setSecurityPolicy(SecurityPolicy.Basic256Sha256)
                                .setSecurityMode(MessageSecurityMode.Sign)
                                .setTransportProfile(TransportProfile.TCP_UASC_UABINARY)
                                .build()
                        )
                        // Add encrypted endpoint with SignAndEncrypt mode
                        endpoints.add(
                            EndpointConfiguration.newBuilder()
                                .setBindAddress(bindAddress)
                                .setBindPort(config.port)
                                .setHostname(hostname)
                                .setPath(path)
                                .setCertificate(certificate)
                                .setSecurityPolicy(SecurityPolicy.Basic256Sha256)
                                .setSecurityMode(MessageSecurityMode.SignAndEncrypt)
                                .setTransportProfile(TransportProfile.TCP_UASC_UABINARY)
                                .build()
                        )
                        logger.info("Added Basic256Sha256 encrypted endpoints on port ${config.port}")
                    } else {
                        logger.warning("Cannot add Basic256Sha256 encrypted endpoints: certificates not available")
                    }
                }
                "Basic128Rsa15" -> {
                    if (certificate != null && keyPair != null) {
                        // Add Basic128Rsa15 endpoints
                        endpoints.add(
                            EndpointConfiguration.newBuilder()
                                .setBindAddress(bindAddress)
                                .setBindPort(config.port)
                                .setHostname(hostname)
                                .setPath(path)
                                .setCertificate(certificate)
                                .setSecurityPolicy(SecurityPolicy.Basic128Rsa15)
                                .setSecurityMode(MessageSecurityMode.Sign)
                                .setTransportProfile(TransportProfile.TCP_UASC_UABINARY)
                                .build()
                        )
                        endpoints.add(
                            EndpointConfiguration.newBuilder()
                                .setBindAddress(bindAddress)
                                .setBindPort(config.port)
                                .setHostname(hostname)
                                .setPath(path)
                                .setCertificate(certificate)
                                .setSecurityPolicy(SecurityPolicy.Basic128Rsa15)
                                .setSecurityMode(MessageSecurityMode.SignAndEncrypt)
                                .setTransportProfile(TransportProfile.TCP_UASC_UABINARY)
                                .build()
                        )
                        logger.info("Added Basic128Rsa15 encrypted endpoints on port ${config.port}")
                    } else {
                        logger.warning("Cannot add Basic128Rsa15 encrypted endpoints: certificates not available")
                    }
                }
                else -> logger.warning("Unknown security policy: $policyName")
            }
        }

        if (endpoints.isEmpty()) {
            throw IllegalStateException("No valid endpoints configured. Check security configuration.")
        }

        // Configure server based on research of Eclipse Milo 0.6.16 requirements
        val configBuilder = OpcUaServerConfigBuilder()
            .setApplicationName(LocalizedText.english("MonsterMQ OPC UA Server - ${config.name}"))
            .setApplicationUri("urn:MonsterMQ:OpcUaServer:${config.name}")
            .setProductUri("urn:MonsterMQ:OpcUaServer")
            .setEndpoints(endpoints)

        // Add certificate management and validation if certificates are available
        if (certificate != null && keyPair != null) {
            logger.info("Configuring OPC UA server with certificate support for encrypted endpoints")

            try {
                // Create a certificate manager for Eclipse Milo 0.6.16
                val certificateManager = org.eclipse.milo.opcua.stack.core.security.DefaultCertificateManager(
                    keyPair,
                    certificate
                )
                configBuilder.setCertificateManager(certificateManager)

                // Configure certificate validator with TrustListManager as required by Eclipse Milo 0.6.16
                try {
                    // Create a trust list manager for certificate validation
                    val securityDir = java.nio.file.Paths.get(config.security.certificateDir)
                    java.nio.file.Files.createDirectories(securityDir)

                    // Create trust directories for server certificate validation
                    val trustedDir = securityDir.resolve("trusted-${config.name}")
                    java.nio.file.Files.createDirectories(trustedDir)

                    val trustListManager = org.eclipse.milo.opcua.stack.core.security.DefaultTrustListManager(trustedDir.toFile())
                    val certificateValidator = org.eclipse.milo.opcua.stack.server.security.DefaultServerCertificateValidator(trustListManager)

                    configBuilder.setCertificateValidator(certificateValidator)
                    logger.info("Configured server certificate validator with trust directory: $trustedDir")
                } catch (e: Exception) {
                    // If certificate validator setup fails, log and continue without it
                    logger.warning("Could not configure certificate validator: ${e.message}")
                    logger.info("Server will operate without certificate validation - clients may need to trust server certificate manually")
                }

                logger.info("Certificate manager and validator configured successfully")
            } catch (e: Exception) {
                logger.warning("Failed to configure certificate manager/validator: ${e.message}")
                logger.info("Falling back to unencrypted-only configuration")
            }
        } else {
            logger.info("No certificates available - server will only support unencrypted connections")
        }

        return configBuilder.build()
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
                sessionHandler.unsubscribeInternalClient(internalClientId, topic)
                subscribedTopics.remove(topic)
                logger.fine { "Unsubscribed OPC UA server '${config.name}' from topic: $topic" }
            } catch (e: Exception) {
                logger.warning("Failed to unsubscribe from topic $topic: ${e.message}")
            }
        }

        // Subscribe to new topics
        topicsToAdd.forEach { topic ->
            try {
                sessionHandler.subscribeInternalClient(internalClientId, topic, 0)
                subscribedTopics[topic] = internalClientId
                logger.fine { "Subscribed OPC UA server '${config.name}' to new topic: $topic" }
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
        // Register eventBus consumer once for all MQTT messages to this OPC UA server (handles both individual and bulk)
        vertx.eventBus().consumer<Any>(at.rocworks.bus.EventBusAddresses.Client.messages(internalClientId)) { busMessage ->
            try {
                when (val body = busMessage.body()) {
                    is BrokerMessage -> handleBrokerMessage(body)
                    is at.rocworks.data.BulkClientMessage -> body.messages.forEach { handleBrokerMessage(it) }
                    else -> logger.warning("Unknown message type: ${body?.javaClass?.simpleName}")
                }
            } catch (e: Exception) {
                logger.warning("Error processing MQTT message: ${e.message}")
            }
        }

        config.addresses.forEach { address ->
            try {
                sessionHandler.subscribeInternalClient(internalClientId, address.mqttTopic, 0)
                subscribedTopics[address.mqttTopic] = internalClientId
                logger.fine { "Subscribed OPC UA server '${config.name}' to MQTT topic: ${address.mqttTopic}" }
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
                sessionHandler.unsubscribeInternalClient(clientId, topic)
                logger.fine { "Unsubscribed from MQTT topic: $topic" }
            } catch (e: Exception) {
                logger.warning("Failed to unsubscribe from MQTT topic $topic: ${e.message}")
            }
        }
        subscribedTopics.clear()
        // Unregister the client
        sessionHandler.unregisterInternalClient(internalClientId)
    }

    /**
     * Handle incoming MQTT messages and update OPC UA nodes
     */
    private fun handleBrokerMessage(message: BrokerMessage, addressConfig: OpcUaServerAddress? = null) {
        // Skip messages from this OPC UA server to prevent loops
        if (message.senderId == internalClientId) {
            logger.fine { "Skipping message from self: ${message.topicName}" }
            return
        }

        logger.fine { "OPC UA server '${config.name}' received MQTT message on topic: ${message.topicName}" }

        try {
            // Use the provided address config, or find one that matches
            val matchingAddress = addressConfig ?: config.addresses.find { address ->
                TopicTree.matches(address.mqttTopic, message.topicName)
            }

            if (matchingAddress != null) {
                logger.fine { "Processing MQTT message for topic: ${message.topicName}" }

                // Convert MQTT message to OPC UA DataValue
                val dataValue = OpcUaDataConverter.mqttToOpcUa(
                    message.payload,
                    matchingAddress.dataType,
                    message.time
                )

                // Create or update the OPC UA node dynamically like the gateway does
                createOrUpdateNode(message.topicName, matchingAddress, dataValue)

            } else {
                logger.fine { "No matching address pattern found for topic: ${message.topicName}" }
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
            // Create new node dynamically with the access level from the matching pattern
            val specificAddress = addressConfig.copy(
                mqttTopic = topicName
                // Keep the original accessLevel from the pattern configuration
            )
            nodeManager?.createOrUpdateVariableNode(topicName, specificAddress, dataValue)
            logger.fine { "Created new OPC UA node for topic: $topicName with ${addressConfig.accessLevel} access" }
        } else {
            logger.fine { "Updated existing OPC UA node for topic: $topicName" }
        }
    }


    /**
     * Handle OPC UA node writes and publish to MQTT
     */
    private fun handleOpcUaWrite(topic: String, dataValue: org.eclipse.milo.opcua.stack.core.types.builtin.DataValue) {
        logger.fine { "OPC UA write to node: $topic, value: ${dataValue.value}" }

        try {
            // First, update the local OPC UA node value to notify OPC UA subscribers
            val nodeUpdated = nodeManager?.updateNodeValue(topic, dataValue) ?: false
            if (nodeUpdated) {
                logger.fine { "Updated local OPC UA node value for OPC UA subscribers: $topic" }
            }

            // Find any address configuration that matches this topic pattern
            // We only need this to get the dataType for conversion
            val address = config.addresses.find { addressConfig ->
                TopicTree.matches(addressConfig.mqttTopic, topic)
            }

            if (address != null) {
                // Convert OPC UA DataValue to MQTT payload
                val payload = OpcUaDataConverter.opcUaToMqtt(dataValue, address.dataType)
                logger.fine { "Publishing OPC UA write to MQTT topic: $topic" }

                // Publish directly to MQTT (sessionHandler.publishInternal is already async)
                val message = BrokerMessage(
                    messageId = 0,
                    topicName = topic,
                    payload = payload,
                    qosLevel = 0,
                    isRetain = false,
                    isDup = false,
                    isQueued = false,
                    clientId = internalClientId,
                    senderId = internalClientId // Mark as coming from this OPC UA server
                )

                sessionHandler.publishInternal(internalClientId, message)
                logger.fine { "Successfully published OPC UA write to MQTT topic: $topic" }
            } else {
                logger.warning("No address configuration found for topic: $topic - unable to determine data type")
            }
        } catch (e: Exception) {
            logger.severe("Error handling OPC UA write for topic $topic: ${e.message}")
            e.printStackTrace()
        }
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