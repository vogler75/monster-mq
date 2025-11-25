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

        // Broadcast to all nodes in the cluster
        const val BROADCAST = "$CLUSTER_NS.bc"

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
        fun connectorsList(nodeId: String) = "$BRIDGE_NS.connectors.list.$nodeId"
        fun connectorMetrics(deviceName: String) = "$BRIDGE_NS.metrics.$deviceName"
    }

    // OPC UA Bridge (client connectors)
    object OpcUaBridge {
        private const val BRIDGE_NS = "$BASE.bridge.opcua"
        const val CONNECTORS_LIST = "$BRIDGE_NS.connectors.list"
        fun connectorsList(nodeId: String) = "$BRIDGE_NS.connectors.list.$nodeId"
        fun connectorMetrics(deviceName: String) = "$BRIDGE_NS.metrics.$deviceName"
    }

    // Kafka Subscriber Bridge (inbound Kafka -> MQTT)
    object KafkaBridge {
        private const val BRIDGE_NS = "$BASE.bridge.kafka"
        const val CONNECTORS_LIST = "$BRIDGE_NS.connectors.list"
        fun connectorsList(nodeId: String) = "$BRIDGE_NS.connectors.list.$nodeId"
        fun connectorMetrics(deviceName: String) = "$BRIDGE_NS.metrics.$deviceName"
    }

    // WinCC OA Client (WinCC OA -> MQTT via GraphQL)
    object WinCCOaBridge {
        private const val BRIDGE_NS = "$BASE.bridge.winccoa"
        const val CONNECTORS_LIST = "$BRIDGE_NS.connectors.list"
        fun connectorsList(nodeId: String) = "$BRIDGE_NS.connectors.list.$nodeId"
        fun connectorMetrics(deviceName: String) = "$BRIDGE_NS.metrics.$deviceName"
    }

    // OA Datapoint Bridge (native oa4j dpConnect for !OA/ topics)
    object OaDatapointBridge {
        private const val BRIDGE_NS = "$BASE.bridge.oa"

        // Subscription add (cluster-wide publish)
        // Unsubscription is handled via Cluster.SUBSCRIPTION_DELETE
        const val SUBSCRIPTION_ADD = "$BRIDGE_NS.subscription.add"
    }

    // WinCC Unified Client (WinCC Unified -> MQTT)
    object WinCCUaBridge {
        private const val BRIDGE_NS = "$BASE.bridge.winccua"
        const val CONNECTORS_LIST = "$BRIDGE_NS.connectors.list"
        fun connectorsList(nodeId: String) = "$BRIDGE_NS.connectors.list.$nodeId"
        fun connectorMetrics(deviceName: String) = "$BRIDGE_NS.metrics.$deviceName"
    }

    // PLC4X Client (PLC4X -> MQTT)
    object Plc4xBridge {
        private const val BRIDGE_NS = "$BASE.bridge.plc4x"
        const val CONNECTORS_LIST = "$BRIDGE_NS.connectors.list"
        fun connectorsList(nodeId: String) = "$BRIDGE_NS.connectors.list.$nodeId"
        fun connectorMetrics(deviceName: String) = "$BRIDGE_NS.metrics.$deviceName"
    }

    // Neo4j Client (MQTT -> Neo4j)
    object Neo4jBridge {
        private const val BRIDGE_NS = "$BASE.bridge.neo4j"
        const val CONNECTORS_LIST = "$BRIDGE_NS.connectors.list"
        fun connectorsList(nodeId: String) = "$BRIDGE_NS.connectors.list.$nodeId"
        fun connectorMetrics(deviceName: String) = "$BRIDGE_NS.metrics.$deviceName"
    }

    // JDBC Logger (MQTT -> Database)
    object JDBCLoggerBridge {
        private const val BRIDGE_NS = "$BASE.bridge.jdbclogger"
        const val CONNECTORS_LIST = "$BRIDGE_NS.connectors.list"
        fun connectorsList(nodeId: String) = "$BRIDGE_NS.connectors.list.$nodeId"
        fun connectorMetrics(deviceName: String) = "$BRIDGE_NS.metrics.$deviceName"
    }

    // SparkplugB Decoder (SparkplugB -> MQTT with transformations)
    object SparkplugBDecoder {
        private const val DECODER_NS = "$BASE.decoder.sparkplugb"
        const val CONNECTORS_LIST = "$DECODER_NS.connectors.list"
        fun connectorsList(nodeId: String) = "$DECODER_NS.connectors.list.$nodeId"
        fun connectorMetrics(deviceName: String) = "$DECODER_NS.metrics.$deviceName"
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

    // Syslog Operations
    object Syslog {
        private const val SYSLOG_NS = "$BASE.syslog"
        const val LOGS = "$SYSLOG_NS.logs"
        fun logsForNode(nodeId: String) = "$SYSLOG_NS.logs.$nodeId"
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
}