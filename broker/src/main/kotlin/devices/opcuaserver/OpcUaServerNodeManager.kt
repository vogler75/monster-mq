package at.rocworks.devices.opcuaserver

import at.rocworks.Utils
import org.eclipse.milo.opcua.sdk.core.AccessLevel
import org.eclipse.milo.opcua.sdk.core.Reference
import org.eclipse.milo.opcua.sdk.server.OpcUaServer
import org.eclipse.milo.opcua.sdk.server.api.DataItem
import org.eclipse.milo.opcua.sdk.server.api.ManagedNamespaceWithLifecycle
import org.eclipse.milo.opcua.sdk.server.api.MonitoredItem
import org.eclipse.milo.opcua.sdk.server.nodes.UaFolderNode
import org.eclipse.milo.opcua.sdk.server.nodes.UaVariableNode
import org.eclipse.milo.opcua.stack.core.Identifiers
import org.eclipse.milo.opcua.stack.core.types.builtin.*
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UByte
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger

/**
 * Manages OPC UA nodes and their hierarchical structure for the OPC UA Server
 */
class OpcUaServerNodeManager(
    private val server: OpcUaServer,
    private val namespaceUri: String,
    private val config: at.rocworks.devices.opcuaserver.OpcUaServerConfig,
    private val onNodeWrite: (String, DataValue) -> Unit // Callback for node writes
) : ManagedNamespaceWithLifecycle(server, namespaceUri) {

    companion object {
        private const val ROOT_FOLDER_NAME = "MonsterMQ"
        private val logger: Logger = Utils.getLogger(OpcUaServerNodeManager::class.java)
    }

    private val folderNodes = ConcurrentHashMap<String, NodeId>()
    private val variableNodes = ConcurrentHashMap<String, UaVariableNode>()
    private val topicToNodeId = ConcurrentHashMap<String, NodeId>()
    private val nodeIdToTopic = ConcurrentHashMap<NodeId, String>()
    private val nodeUpdateTimes = ConcurrentHashMap<NodeId, AtomicLong>()
    @Volatile
    private var monsterMqRootNodeId: NodeId? = null



    fun initializeNodes() {
        logger.info("OPC UA Server namespace started with index: $namespaceIndex, URI: $namespaceUri")
        createMonsterMQRootFolder()
    }

    fun cleanupNodes() {
    }

    // Required abstract methods from ManagedNamespaceWithLifecycle
    override fun onDataItemsCreated(dataItems: MutableList<DataItem>) {
        // Implementation for data item creation
    }

    override fun onDataItemsModified(dataItems: MutableList<DataItem>) {
        // Implementation for data item modification
    }

    override fun onDataItemsDeleted(dataItems: MutableList<DataItem>) {
        // Implementation for data item deletion
    }

    override fun onMonitoringModeChanged(items: MutableList<MonitoredItem>) {
        // Implementation for monitoring mode changes
    }

    private fun createRootFolder(): NodeId {
        return monsterMqRootNodeId ?: createMonsterMQRootFolder()
    }

    private fun createMonsterMQRootFolder(): NodeId {
        monsterMqRootNodeId?.let { return it }

        return synchronized(this) {
            monsterMqRootNodeId ?: run {
                // Create root folder with simple NodeId, not path-based
                val rootNodeId = NodeId(namespaceIndex, ROOT_FOLDER_NAME)
                val rootFolder = UaFolderNode(
                    nodeContext,
                    rootNodeId,
                    QualifiedName(namespaceIndex, ROOT_FOLDER_NAME),
                    LocalizedText.english(ROOT_FOLDER_NAME)
                )

                // Add to namespace manager
                nodeManager.addNode(rootFolder)

                // Add only inverse reference to Objects folder (like the gateway)
                rootFolder.addReference(
                    Reference(
                        rootNodeId,
                        Identifiers.HasComponent,
                        Identifiers.ObjectsFolder.expanded(),
                        Reference.Direction.INVERSE
                    )
                )

                // Cache the root folder with simple path
                folderNodes[ROOT_FOLDER_NAME] = rootNodeId
                monsterMqRootNodeId = rootNodeId
                logger.info("MonsterMQ root folder created with NodeId: $rootNodeId")
                rootNodeId
            }
        }
    }

    private fun ensureFolder(parentNodeId: NodeId, folderName: String): NodeId {
        val path = folderPath(parentNodeId, folderName)
        return folderNodes.computeIfAbsent(path) {
            val folderNodeId = NodeId(namespaceIndex, path)
            val folderNode = UaFolderNode(
                nodeContext,
                folderNodeId,
                QualifiedName(namespaceIndex, folderName),
                LocalizedText.english(folderName)
            )

            nodeManager.addNode(folderNode)

            folderNode.addReference(
                Reference(
                    folderNode.nodeId,
                    Identifiers.HasComponent,
                    parentNodeId.expanded(),
                    Reference.Direction.INVERSE
                )
            )

            folderNodeId
        }
    }

    private fun folderPath(parentNodeId: NodeId, folderName: String): String {
        return if (parentNodeId == Identifiers.ObjectsFolder) {
            folderName
        } else {
            "${parentNodeId.identifier}/$folderName"
        }
    }


    /**
     * Create or get a hierarchical folder structure for the given topic path
     */
    fun createHierarchicalFolders(topicPath: String): NodeId {
        val parts = topicPath.split("/").filter { it.isNotEmpty() && it != "#" && it != "+" }

        if (parts.isEmpty()) {
            return createRootFolder()
        }

        var currentParent = createRootFolder()

        for (i in 0 until parts.size - 1) {
            val folderName = parts[i]
            currentParent = createFolderUnderParent(currentParent, folderName)
        }

        return currentParent
    }

    private fun createFolderUnderParent(parentNodeId: NodeId, folderName: String): NodeId {
        return ensureFolder(parentNodeId, folderName)
    }

    /**
     * Create or update a variable node for an MQTT topic
     */
    fun createOrUpdateVariableNode(
        mqttTopic: String,
        address: OpcUaServerAddress,
        initialValue: DataValue? = null
    ): NodeId {
        // Check if node already exists
        topicToNodeId[mqttTopic]?.let { existingNodeId ->
            variableNodes[mqttTopic]?.let { node ->
                // Update existing node value if provided
                initialValue?.let {
                    try {
                        node.setValue(it)
                    } catch (e: Exception) {
                        logger.warning("Error updating existing node value: ${e.message}")
                    }
                }
                return existingNodeId
            }
        }

        // Create hierarchical folders
        val parentNodeId = createHierarchicalFolders(mqttTopic)

        // Create node ID based on topic
        val nodeId = NodeId(namespaceIndex, mqttTopic)

        // Get the last part of the topic as default browse name
        val topicParts = mqttTopic.split("/")
        val defaultBrowseName = topicParts.lastOrNull() ?: "value"
        val browseName = address.browseName ?: defaultBrowseName

        // Determine access level - use UByte values
        val accessLevelValue = when (address.accessLevel) {
            OpcUaAccessLevel.READ_ONLY -> UByte.valueOf(1) // READ_ONLY = 1
            OpcUaAccessLevel.READ_WRITE -> UByte.valueOf(3) // READ_ONLY + WRITE = 1 + 2 = 3
        }

        // Create the variable node
        val variableNode = UaVariableNode(
            nodeContext,
            nodeId,
            QualifiedName(namespaceIndex, browseName),
            LocalizedText.english(address.displayName)
        )

        // Set node properties
        // Set node properties - using try-catch for API compatibility
        try {
            // Try different API methods for setting properties
            try {
                variableNode.setAccessLevel(accessLevelValue)
            } catch (e: Exception) {
                // Alternative property access
                logger.finest("setAccessLevel method not available")
            }

            try {
                variableNode.setUserAccessLevel(accessLevelValue)
            } catch (e: Exception) {
                logger.finest("setUserAccessLevel method not available")
            }

            try {
                variableNode.setDataType(OpcUaDataConverter.getOpcUaDataType(address.dataType))
            } catch (e: Exception) {
                logger.finest("setDataType method not available")
            }

            // Note: TypeDefinition may be set automatically by the UaVariableNode constructor
            logger.finest("TypeDefinition will be set automatically")
        } catch (e: Exception) {
            logger.warning("Error setting node properties: ${e.message}")
        }
        try {
            variableNode.setValue(initialValue ?: DataValue(Variant.NULL_VALUE))
            variableNode.setValueRank(-1)

            // Add description if provided
            address.description?.let {
                variableNode.setDescription(LocalizedText.english(it))
            }
        } catch (e: Exception) {
            logger.warning("Error setting node value/description: ${e.message}")
        }

        // Add node to manager
        nodeManager.addNode(variableNode)

        // Add reference to parent folder
        variableNode.addReference(
            Reference(
                variableNode.nodeId,
                Identifiers.HasComponent,
                parentNodeId.expanded(),
                Reference.Direction.INVERSE
            )
        )

        // Store mappings
        variableNodes[mqttTopic] = variableNode
        topicToNodeId[mqttTopic] = nodeId
        nodeIdToTopic[nodeId] = mqttTopic
        nodeUpdateTimes[nodeId] = AtomicLong(System.currentTimeMillis())

        logger.fine("Created variable node for topic: $mqttTopic with NodeId: $nodeId")
        return nodeId
    }

    /**
     * Update the value of a variable node
     */
    fun updateNodeValue(mqttTopic: String, dataValue: DataValue): Boolean {
        val node = variableNodes[mqttTopic] ?: return false
        val nodeId = topicToNodeId[mqttTopic] ?: return false

        // Check update interval to avoid too frequent updates
        val lastUpdate = nodeUpdateTimes[nodeId]?.get() ?: 0L
        val now = System.currentTimeMillis()

        if (now - lastUpdate >= config.updateInterval) {
            try {
                node.setValue(dataValue)
            } catch (e: Exception) {
                logger.warning("Error setting node value: ${e.message}")
            }
            nodeUpdateTimes[nodeId]?.set(now)

            logger.finest("Updated node value for topic: $mqttTopic")
            return true
        }

        return false
    }

    /**
     * Get the MQTT topic for a given NodeId
     */
    fun getTopicForNodeId(nodeId: NodeId): String? {
        return nodeIdToTopic[nodeId]
    }

    /**
     * Get all registered MQTT topics
     */
    fun getAllTopics(): Set<String> {
        return topicToNodeId.keys.toSet()
    }

    /**
     * Remove a variable node
     */
    fun removeVariableNode(mqttTopic: String): Boolean {
        val nodeId = topicToNodeId[mqttTopic] ?: return false
        val node = variableNodes[mqttTopic] ?: return false

        try {
            // Remove from node manager
            nodeManager.removeNode(nodeId)

            // Clean up mappings
            variableNodes.remove(mqttTopic)
            topicToNodeId.remove(mqttTopic)
            nodeIdToTopic.remove(nodeId)
            nodeUpdateTimes.remove(nodeId)

            logger.info("Removed variable node for topic: $mqttTopic")
            return true
        } catch (e: Exception) {
            logger.warning("Failed to remove node for topic $mqttTopic: ${e.message}")
            return false
        }
    }

    /**
     * Get statistics about the node manager
     */
    fun getStatistics(): Map<String, Any> {
        return mapOf(
            "totalNodes" to variableNodes.size,
            "totalFolders" to folderNodes.size,
            "namespaceIndex" to namespaceIndex.toInt(),
            "namespaceUri" to namespaceUri
        )
    }
}
