package at.rocworks.bus

import at.rocworks.Const

/**
 * Unified EventBus addressing scheme for MonsterMQ
 *
 * Address Format: {namespace}.{category}.{operation}[.{identifier}]
 *
 * Namespaces:
 * - mq.client - Client-specific operations
 * - mq.cluster - Cluster coordination and replication
 * - mq.node - Node-specific operations
 * - mq.store - Storage operations
 * - mq.system - System operations
 */
object EventBusAddresses {

    // Base namespaces
    private const val BASE = "mq"
    private const val CLIENT_NS = "$BASE.client"
    private const val CLUSTER_NS = "$BASE.cluster"
    private const val NODE_NS = "$BASE.node"
    private const val STORE_NS = "$BASE.store"
    private const val SYSTEM_NS = "$BASE.system"

    // Client Operations (per-client addressing)
    object Client {
        fun commands(clientId: String) = "$CLIENT_NS.cmd.$clientId"
        fun messages(clientId: String) = "$CLIENT_NS.msg.$clientId"
    }

    // Cluster Coordination (global operations for distributed state)
    object Cluster {
        // Subscription management
        const val SUBSCRIPTION_ADD = "$CLUSTER_NS.subscription.add"
        const val SUBSCRIPTION_DELETE = "$CLUSTER_NS.subscription.delete"

        // Client state management
        const val CLIENT_STATUS = "$CLUSTER_NS.client.status"

        // Data replication (used by DataReplicator and SetMapReplicator)
        const val CLIENT_NODE_MAPPING = "$CLUSTER_NS.replication.client_node_mapping"
        const val TOPIC_NODE_MAPPING = "$CLUSTER_NS.replication.topic_node_mapping"

        // Legacy compatibility (can be removed once HealthHandler is updated)
        const val CLIENT_MAPPING_COMPAT = "$CLUSTER_NS.client.mapping.compat"
    }

    // Node Operations (per-node addressing)
    object Node {
        fun messages(nodeId: String) = "$NODE_NS.msg.$nodeId"
        fun commands(deploymentId: String) = "$NODE_NS.cmd.$deploymentId"
        fun metrics(nodeId: String) = "$NODE_NS.metrics.$nodeId"
        fun metricsAndReset(nodeId: String) = "$NODE_NS.metrics.reset.$nodeId"
        fun sessionMetrics(nodeId: String, clientId: String) = "$NODE_NS.session.metrics.$nodeId.$clientId"
        fun sessionDetails(nodeId: String, clientId: String) = "$NODE_NS.session.details.$nodeId.$clientId"
        fun messageBus(deploymentId: String) = "$NODE_NS.bus.$deploymentId"
    }

    // Store Operations (per-store addressing)
    object Store {
        fun add(storeName: String) = "$STORE_NS.$storeName.add"
        fun delete(storeName: String) = "$STORE_NS.$storeName.delete"

        // SQLite operations
        const val SQLITE_INIT = "$STORE_NS.sqlite.init"
        const val SQLITE_UPDATE = "$STORE_NS.sqlite.update"
        const val SQLITE_QUERY = "$STORE_NS.sqlite.query"
        const val SQLITE_BATCH = "$STORE_NS.sqlite.batch"
    }

    // MQTT Bridge (client connectors)
    object MqttBridge {
        private const val BRIDGE_NS = "$BASE.bridge.mqtt"
        const val CONNECTORS_LIST = "$BRIDGE_NS.connectors.list"
        fun connectorMetrics(deviceName: String) = "$BRIDGE_NS.metrics.$deviceName"
    }

    // OPC UA Bridge (client connectors)
    object OpcUaBridge {
        private const val BRIDGE_NS = "$BASE.bridge.opcua"
        const val CONNECTORS_LIST = "$BRIDGE_NS.connectors.list"
        fun connectorMetrics(deviceName: String) = "$BRIDGE_NS.metrics.$deviceName"
    }

    // Kafka Subscriber Bridge (inbound Kafka -> MQTT)
    object KafkaBridge {
        private const val BRIDGE_NS = "$BASE.bridge.kafka"
        const val CONNECTORS_LIST = "$BRIDGE_NS.connectors.list"
        fun connectorMetrics(deviceName: String) = "$BRIDGE_NS.metrics.$deviceName"
    }

    // WinCC OA Client (WinCC OA -> MQTT)
    object WinCCOaBridge {
        private const val BRIDGE_NS = "$BASE.bridge.winccoa"
        const val CONNECTORS_LIST = "$BRIDGE_NS.connectors.list"
        fun connectorMetrics(deviceName: String) = "$BRIDGE_NS.metrics.$deviceName"
    }

    // WinCC Unified Client (WinCC Unified -> MQTT)
    object WinCCUaBridge {
        private const val BRIDGE_NS = "$BASE.bridge.winccua"
        const val CONNECTORS_LIST = "$BRIDGE_NS.connectors.list"
        fun connectorMetrics(deviceName: String) = "$BRIDGE_NS.metrics.$deviceName"
    }

    // PLC4X Client (PLC4X -> MQTT)
    object Plc4xBridge {
        private const val BRIDGE_NS = "$BASE.bridge.plc4x"
        const val CONNECTORS_LIST = "$BRIDGE_NS.connectors.list"
        fun connectorMetrics(deviceName: String) = "$BRIDGE_NS.metrics.$deviceName"
    }

    // Neo4j Client (MQTT -> Neo4j)
    object Neo4jBridge {
        private const val BRIDGE_NS = "$BASE.bridge.neo4j"
        const val CONNECTORS_LIST = "$BRIDGE_NS.connectors.list"
        fun connectorMetrics(deviceName: String) = "$BRIDGE_NS.metrics.$deviceName"
    }

    // Archive Groups
    object Archive {
        private const val ARCHIVE_NS = "$BASE.archive"
        const val GROUPS_LIST = "$ARCHIVE_NS.groups.list"
        fun groupMetrics(groupName: String) = "$ARCHIVE_NS.metrics.$groupName"
        fun groupBufferSize(groupName: String) = "$ARCHIVE_NS.buffer.$groupName"
    }

    // System Operations
    object System {
        // Can be used for system-wide broadcasts, health checks, etc.
        const val HEALTH_CHECK = "$SYSTEM_NS.health.check"
        const val SHUTDOWN = "$SYSTEM_NS.shutdown"
    }

    // Utility functions for address validation and parsing
    object Utils {
        /**
         * Check if an address belongs to MonsterMQ namespace
         */
        fun isMonsterMQAddress(address: String): Boolean = address.startsWith(BASE)

        /**
         * Extract the namespace from an address
         */
        fun getNamespace(address: String): String? {
            val parts = address.split(".")
            return if (parts.size >= 2 && parts[0] == "mq") parts[1] else null
        }

        /**
         * Extract the category from an address
         */
        fun getCategory(address: String): String? {
            val parts = address.split(".")
            return if (parts.size >= 3 && parts[0] == "mq") parts[2] else null
        }

        /**
         * Extract the operation from an address
         */
        fun getOperation(address: String): String? {
            val parts = address.split(".")
            return if (parts.size >= 4 && parts[0] == "mq") parts[3] else null
        }

        /**
         * Extract the identifier from an address (e.g., clientId, nodeId)
         */
        fun getIdentifier(address: String): String? {
            val parts = address.split(".")
            return if (parts.size >= 5 && parts[0] == "mq") parts[4] else null
        }

        /**
         * Create a custom address following the naming convention
         */
        fun custom(namespace: String, category: String, operation: String, identifier: String? = null): String {
            return if (identifier != null) {
                "$BASE.$namespace.$category.$operation.$identifier"
            } else {
                "$BASE.$namespace.$category.$operation"
            }
        }
    }

    // Migration helpers - maps old addresses to new ones
    object Migration {
        private val ADDRESS_MAPPING = mapOf(
            // Client addresses
            "${Const.GLOBAL_CLIENT_NAMESPACE}/{clientId}/C" to "use Client.commands(clientId)",
            "${Const.GLOBAL_CLIENT_NAMESPACE}/{clientId}/M" to "use Client.messages(clientId)",

            // Cluster addresses
            "${Const.GLOBAL_SUBSCRIPTION_TABLE_NAMESPACE}/A" to Cluster.SUBSCRIPTION_ADD,
            "${Const.GLOBAL_SUBSCRIPTION_TABLE_NAMESPACE}/D" to Cluster.SUBSCRIPTION_DELETE,
            "${Const.GLOBAL_CLIENT_TABLE_NAMESPACE}/C" to Cluster.CLIENT_STATUS,
            "${Const.GLOBAL_CLIENT_TABLE_NAMESPACE}/M" to Cluster.CLIENT_NODE_MAPPING,
            "${Const.GLOBAL_SUBSCRIPTION_TABLE_NAMESPACE}/T" to Cluster.TOPIC_NODE_MAPPING,

            // Node addresses
            "${Const.GLOBAL_EVENT_NAMESPACE}/node/{nodeId}/messages" to "use Node.messages(nodeId)",
            "${Const.GLOBAL_EVENT_NAMESPACE}/{deploymentId}/C" to "use Node.commands(deploymentId)",
            "${Const.GLOBAL_EVENT_NAMESPACE}/{deploymentId}/M" to "use Node.messageBus(deploymentId)",
            "monstermq.node.metrics.{nodeId}" to "use Node.metrics(nodeId)",

            // Store addresses
            "{storeName}/A" to "use Store.add(storeName)",
            "{storeName}/D" to "use Store.delete(storeName)",
            "sqlite.init" to Store.SQLITE_INIT,
            "sqlite.update" to Store.SQLITE_UPDATE,
            "sqlite.query" to Store.SQLITE_QUERY,
            "sqlite.batch" to Store.SQLITE_BATCH
        )

        /**
         * Get new address for old address pattern (for documentation/migration purposes)
         */
        fun getNewAddress(oldAddress: String): String? = ADDRESS_MAPPING[oldAddress]

        /**
         * Get all mappings for documentation
         */
        fun getAllMappings(): Map<String, String> = ADDRESS_MAPPING
    }
}