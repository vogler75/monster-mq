package at.rocworks.devices.opcuaserver

import at.rocworks.Utils
import org.eclipse.milo.opcua.sdk.core.AccessLevel
import org.eclipse.milo.opcua.sdk.core.Reference
import org.eclipse.milo.opcua.sdk.server.OpcUaServer
import org.eclipse.milo.opcua.sdk.server.api.AddressSpaceComposite
import org.eclipse.milo.opcua.sdk.server.api.DataItem
import org.eclipse.milo.opcua.sdk.server.api.ManagedAddressSpaceFragmentWithLifecycle
import org.eclipse.milo.opcua.sdk.server.api.MonitoredItem
import org.eclipse.milo.opcua.sdk.server.api.SimpleAddressSpaceFilter
import org.eclipse.milo.opcua.sdk.server.nodes.AttributeContext
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn
import org.eclipse.milo.opcua.sdk.server.nodes.UaFolderNode
import org.eclipse.milo.opcua.sdk.server.nodes.UaVariableNode
import org.eclipse.milo.opcua.stack.core.Identifiers
import org.eclipse.milo.opcua.stack.core.StatusCodes
import org.eclipse.milo.opcua.sdk.server.api.services.AttributeServices
import org.eclipse.milo.opcua.stack.core.types.structured.WriteValue
import org.eclipse.milo.opcua.stack.core.types.builtin.*
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UShort
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Logger

/**
 * MonsterMQ OPC UA Node Manager following the gateway pattern
 */
class OpcUaServerNodes(
    private val config: OpcUaServerConfig,
    private val server: OpcUaServer,
    composite: AddressSpaceComposite,
    val namespaceIndex: UShort,
    private val onNodeWrite: (String, DataValue) -> Unit
) : ManagedAddressSpaceFragmentWithLifecycle(server, composite) {

    companion object {
        private const val ROOT_FOLDER_NAME = "MonsterMQ"
        private val logger: Logger = Utils.getLogger(OpcUaServerNodes::class.java)
    }

    private val filter = SimpleAddressSpaceFilter.create {
        nodeManager.containsNode(it)
    }

    private val folderNodes = ConcurrentHashMap<String, NodeId>()
    private val variableNodes = ConcurrentHashMap<String, UaVariableNode>()
    private val topicToNodeId = ConcurrentHashMap<String, NodeId>()
    private val nodeIdToTopic = ConcurrentHashMap<NodeId, String>()
    private val nodeUpdateTimes = ConcurrentHashMap<NodeId, AtomicLong>()
    // Store DataItems for proper subscription notifications (like gateway implementation)
    private val nodeDataItems = ConcurrentHashMap<NodeId, MutableList<DataItem>>()
    @Volatile
    private var monsterMqRootNodeId: NodeId? = null

    init {
        lifecycleManager.addStartupTask {
            createMonsterMQRootFolder()
        }
    }

    override fun getFilter() = filter

    override fun onDataItemsCreated(items: List<DataItem>) {
        logger.info("DataItems created: ${items.size} items")
        items.forEach { item ->
            logger.info("  Created DataItem: ${item.readValueId.nodeId} sampling interval: ${item.samplingInterval}")

            val nodeId = item.readValueId.nodeId

            // Store the DataItem for later notifications (like gateway implementation)
            nodeDataItems.computeIfAbsent(nodeId) { mutableListOf() }.add(item)

            // Send current value to new subscriber using item.setValue() (like gateway implementation)
            val topic = nodeIdToTopic[nodeId]
            if (topic != null) {
                val node = variableNodes[topic]
                if (node != null) {
                    try {
                        // Get current value using proper OPC UA read mechanism (like gateway)
                        val currentValue = node.readAttribute(
                            AttributeContext(item.session.server),
                            item.readValueId.attributeId,
                            TimestampsToReturn.Both,
                            item.readValueId.indexRange,
                            item.readValueId.dataEncoding
                        )
                        logger.info("  Sending current value to new subscriber for topic: $topic, value: ${currentValue.value}")

                        // Use item.setValue() to properly trigger subscription notifications (like gateway)
                        item.setValue(currentValue)
                    } catch (e: Exception) {
                        logger.warning("Error sending current value to new subscriber: ${e.message}")
                    }
                } else {
                    logger.warning("  No variable node found for topic: $topic")
                }
            } else {
                logger.warning("  No topic mapping found for NodeId: $nodeId")
            }
        }
    }

    override fun onDataItemsModified(items: List<DataItem>) {
        logger.info("DataItems modified: ${items.size} items")
        items.forEach { item ->
            logger.info("  Modified DataItem: ${item.readValueId.nodeId} sampling interval: ${item.samplingInterval}")
        }
    }

    override fun onDataItemsDeleted(items: List<DataItem>) {
        logger.info("DataItems deleted: ${items.size} items")
        items.forEach { item ->
            logger.info("  Deleted DataItem: ${item.readValueId.nodeId}")

            // Remove DataItem from our tracking map to prevent memory leaks
            val nodeId = item.readValueId.nodeId
            nodeDataItems[nodeId]?.remove(item)

            // Clean up empty lists
            if (nodeDataItems[nodeId]?.isEmpty() == true) {
                nodeDataItems.remove(nodeId)
            }
        }
    }

    override fun onMonitoringModeChanged(items: List<MonitoredItem>) {
        logger.info("Monitoring mode changed: ${items.size} items")
        items.forEach { item ->
            logger.info("  MonitoredItem: ${item.readValueId.nodeId}")
        }
    }

    private fun inverseReferenceTo(node: UaVariableNode, targetNodeId: NodeId, typeId: NodeId) {
        node.addReference(
            Reference(
                node.nodeId,
                typeId,
                targetNodeId.expanded(),
                Reference.Direction.INVERSE
            )
        )
    }

    private fun createMonsterMQRootFolder(): NodeId {
        monsterMqRootNodeId?.let { return it }

        return synchronized(this) {
            monsterMqRootNodeId ?: run {
                logger.info("Creating MonsterMQ root folder with namespace index: $namespaceIndex")

                // Create root folder using gateway pattern
                val rootNodeId = NodeId(namespaceIndex, "$ROOT_FOLDER_NAME:o")
                val rootFolder = UaFolderNode(
                    nodeContext,
                    rootNodeId,
                    QualifiedName(namespaceIndex, ROOT_FOLDER_NAME),
                    LocalizedText(ROOT_FOLDER_NAME)
                )

                // Add to our namespace manager
                nodeManager.addNode(rootFolder)
                logger.info("Added MonsterMQ root folder to node manager")

                // Add inverse reference from MonsterMQ to Objects folder (like gateway pattern)
                rootFolder.addReference(
                    Reference(
                        rootNodeId,
                        Identifiers.HasComponent,
                        Identifiers.ObjectsFolder.expanded(),
                        Reference.Direction.INVERSE
                    )
                )
                logger.info("Added inverse reference from MonsterMQ to Objects folder")

                // Cache the root folder
                folderNodes[ROOT_FOLDER_NAME] = rootNodeId
                monsterMqRootNodeId = rootNodeId
                logger.info("MonsterMQ root folder created successfully with NodeId: $rootNodeId")
                rootNodeId
            }
        }
    }

    private fun createFolderUnderParent(parentNodeId: NodeId, folderName: String): NodeId {
        // Extract the base path without the :o suffix
        val parentPath = parentNodeId.identifier.toString().removeSuffix(":o")
        val path = "$parentPath/$folderName"
        return folderNodes.computeIfAbsent(path) {
            val folderNodeId = NodeId(namespaceIndex, "$path:o")
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

    /**
     * Create hierarchical folder structure for topic path
     */
    fun createHierarchicalFolders(topicPath: String): NodeId {
        val parts = topicPath.split("/").filter { it.isNotEmpty() && it != "#" && it != "+" }

        if (parts.isEmpty()) {
            return createMonsterMQRootFolder()
        }

        var currentParent = createMonsterMQRootFolder()

        for (i in 0 until parts.size - 1) {
            val folderName = parts[i]
            currentParent = createFolderUnderParent(currentParent, folderName)
        }

        return currentParent
    }

    /**
     * Create or update a variable node for MQTT topic (like gateway pattern)
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

        // Create node ID based on topic with MonsterMQ prefix and :v suffix
        val nodeId = NodeId(namespaceIndex, "$ROOT_FOLDER_NAME/$mqttTopic:v")

        // Get the last part of the topic as default browse name and display name
        val topicParts = mqttTopic.split("/")
        val defaultBrowseName = topicParts.lastOrNull() ?: "value"
        val browseName = address.browseName ?: defaultBrowseName
        val displayName = address.displayName?.takeIf { it.isNotBlank() } ?: defaultBrowseName

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
            setBrowseName(QualifiedName(namespaceIndex, browseName))
            setDisplayName(LocalizedText.english(displayName))
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

        // Store write-enabled nodes for later write handling
        if (address.accessLevel == OpcUaAccessLevel.READ_WRITE) {
            // We'll handle writes in the namespace's write method override
            logger.fine("Node $mqttTopic configured for write operations")
        }

        // Add node to manager
        nodeManager.addNode(variableNode)

        // Add reference to parent folder (like gateway pattern)
        inverseReferenceTo(variableNode, parentNodeId, Identifiers.HasComponent)

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
                // Use the value property instead of setValue() method to properly trigger OPC UA notifications
                // This is how the gateway implementation does it to ensure subscription notifications work
                node.value = dataValue

                // Notify all DataItems for this node to trigger subscription notifications (like gateway pattern)
                nodeDataItems[nodeId]?.forEach { item ->
                    try {
                        logger.finest("Notifying DataItem subscriber for topic: $mqttTopic, value: ${dataValue.value}")
                        item.setValue(dataValue)
                    } catch (e: Exception) {
                        logger.warning("Error notifying DataItem subscriber: ${e.message}")
                    }
                }
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
     * Override write method to handle OPC UA node writes
     */
    override fun write(context: AttributeServices.WriteContext, writeValues: MutableList<WriteValue>) {
        // Process each write request
        writeValues.forEach { writeValue ->
            val nodeId = writeValue.nodeId
            val topic = nodeIdToTopic[nodeId]

            if (topic != null) {
                try {
                    val dataValue = writeValue.value
                    logger.info("OPC UA write to node: $topic, value: ${dataValue.value}")
                    logger.info("Calling onNodeWrite callback for topic: $topic")
                    onNodeWrite(topic, dataValue)
                    logger.info("onNodeWrite callback completed for topic: $topic")
                } catch (e: Exception) {
                    logger.warning("Error handling OPC UA write for topic $topic: ${e.message}")
                    e.printStackTrace()
                }
            }
        }

        // Call super to perform the actual write
        super.write(context, writeValues)
    }

    /**
     * Get statistics about the node manager
     */
    fun getStatistics(): Map<String, Any> {
        return mapOf(
            "totalNodes" to variableNodes.size,
            "totalFolders" to folderNodes.size,
            "namespaceIndex" to namespaceIndex.toInt(),
            "namespaceUri" to config.namespaceUri
        )
    }
}