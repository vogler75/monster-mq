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
        private val logger: Logger = Utils.getLogger(OpcUaServerNodeManager::class.java)
    }

    private val rootFolderName: String = config.namespace // Use namespace as root folder name

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

    fun ensureRootFolderVisibility() {
        monsterMqRootNodeId?.let { rootNodeId ->
            logger.info("Ensuring $rootFolderName root folder visibility after server startup")
            logger.info("$rootFolderName root folder NodeId: $rootNodeId")
            logger.info("Namespace URI: ${namespaceUri}, Namespace Index: $namespaceIndex")

            // Add forward reference from Objects to root folder (crucial for cross-namespace browsing)
            try {
                val objectsNode = server.addressSpaceManager.getManagedNode(Identifiers.ObjectsFolder)
                if (objectsNode.isPresent) {
                    // Check if reference already exists
                    val existingRefs = objectsNode.get().references
                    val hasForwardRef = existingRefs.any { ref ->
                        ref.targetNodeId == rootNodeId.expanded() &&
                        ref.referenceTypeId == Identifiers.Organizes &&
                        ref.direction == Reference.Direction.FORWARD
                    }

                    if (!hasForwardRef) {
                        objectsNode.get().addReference(
                            Reference(
                                Identifiers.ObjectsFolder,
                                Identifiers.Organizes,
                                rootNodeId.expanded(),
                                Reference.Direction.FORWARD
                            )
                        )
                        logger.info("✓ Added forward reference from Objects to $rootFolderName")
                    } else {
                        logger.info("✓ Forward reference from Objects to $rootFolderName already exists")
                    }

                    // Verify the node exists in our namespace
                    val monsterNode = nodeManager.get(rootNodeId)
                    if (monsterNode != null) {
                        logger.info("✓ $rootFolderName node exists in namespace manager")
                    } else {
                        logger.warning("✗ $rootFolderName node NOT found in namespace manager!")
                    }

                } else {
                    logger.warning("✗ Objects folder not found in address space manager")
                }
            } catch (e: Exception) {
                logger.warning("✗ Error ensuring root folder visibility: ${e.message}")
                e.printStackTrace()
            }
        } ?: logger.warning("✗ $rootFolderName root node ID is null - root folder was not created")
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
                // Create root folder using Eclipse Milo's proper pattern
                val rootNodeId = NodeId(namespaceIndex, rootFolderName)

                logger.info("Creating $rootFolderName root folder with namespace index: $namespaceIndex")

                val rootFolder = UaFolderNode(
                    nodeContext,
                    rootNodeId,
                    QualifiedName(namespaceIndex, rootFolderName),
                    LocalizedText(rootFolderName)
                )

                // Add to our namespace manager first
                nodeManager.addNode(rootFolder)
                logger.info("Added $rootFolderName root folder to node manager")

                // Create bidirectional references between Objects folder and root folder
                // 1. Add inverse reference from root folder to Objects (child -> parent)
                rootFolder.addReference(
                    Reference(
                        rootNodeId,
                        Identifiers.Organizes,
                        Identifiers.ObjectsFolder.expanded(),
                        Reference.Direction.INVERSE
                    )
                )
                logger.info("Added inverse reference from $rootFolderName to Objects folder")

                // Note: Forward reference from Objects to root folder will be added after server startup

                // Cache the root folder
                folderNodes[rootFolderName] = rootNodeId
                monsterMqRootNodeId = rootNodeId
                logger.info("$rootFolderName root folder created successfully with NodeId: $rootNodeId")
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
                QualifiedName(parentNodeId.namespaceIndex, folderName),
                LocalizedText(folderName)
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

        // Determine access level using AccessLevel enum like the gateway
        val accessLevelValue = when (address.accessLevel) {
            OpcUaAccessLevel.READ_ONLY -> AccessLevel.toValue(AccessLevel.READ_ONLY)
            OpcUaAccessLevel.READ_WRITE -> AccessLevel.toValue(AccessLevel.READ_WRITE)
        }

        // Create the variable node using builder pattern like the gateway
        val variableNode = UaVariableNode.UaVariableNodeBuilder(nodeContext).run {
            setNodeId(nodeId)
            setAccessLevel(accessLevelValue)
            setUserAccessLevel(accessLevelValue)
            setBrowseName(QualifiedName(parentNodeId.namespaceIndex, browseName))
            setDisplayName(LocalizedText.english(address.displayName))
            setDataType(OpcUaDataConverter.getOpcUaDataType(address.dataType))
            setTypeDefinition(Identifiers.BaseDataVariableType)
            setMinimumSamplingInterval(100.0)
            setValue(initialValue ?: DataValue(Variant.NULL_VALUE))

            // Add description if provided
            address.description?.let {
                setDescription(LocalizedText.english(it))
            }

            build()
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
