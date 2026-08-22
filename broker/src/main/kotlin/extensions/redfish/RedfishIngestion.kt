package at.rocworks.extensions.redfish

import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.data.BulkClientMessage
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.handlers.SessionHandler
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.IMessageStore
import at.rocworks.stores.MessageStoreMemory
import io.vertx.core.Vertx
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

class RedfishIngestion(
    private val vertx: Vertx,
    private val sessionHandler: SessionHandler,
    private val archiveHandler: ArchiveHandler?,
    private val deviceConfigStore: IDeviceConfigStore?
) {
    private val logger: Logger = Utils.getLogger(this::class.java)
    val internalClientId = "redfish-ingestion-" + Utils.getUuid().take(8)

    private val gateways = ConcurrentHashMap<String, GatewayConfig>()
    private val activeSubscriptions = ConcurrentHashMap.newKeySet<String>()

    private var messageConsumer: MessageConsumer<Any>? = null
    private var configConsumer: MessageConsumer<JsonObject>? = null
    private val fallbackStore = MessageStoreMemory("redfish-memory-lastval")

    companion object {
        const val ADDRESS_DEVICE_CONFIG_CHANGED = "redfish.device.config.changed"
    }

    fun start() {
        logger.fine("Starting Redfish Ingestion engine (clientId: $internalClientId)")

        // 1. Consumer for incoming MQTT messages
        messageConsumer = vertx.eventBus().consumer<Any>(EventBusAddresses.Client.messages(internalClientId)) { busMessage ->
            val body = busMessage.body()
            when (body) {
                is BrokerMessage -> handleBrokerMessage(body)
                is BulkClientMessage -> body.messages.forEach { handleBrokerMessage(it) }
            }
        }

        // 2. Consumer for config changes
        configConsumer = vertx.eventBus().consumer<JsonObject>(ADDRESS_DEVICE_CONFIG_CHANGED) { _ ->
            reload()
        }

        // Initial load
        reload()
    }

    fun stop() {
        messageConsumer?.unregister()
        configConsumer?.unregister()

        for (filter in activeSubscriptions) {
            try {
                sessionHandler.unsubscribeInternalClient(internalClientId, filter)
            } catch (e: Exception) {
                logger.warning("Error unsubscribing filter $filter: ${e.message}")
            }
        }
        activeSubscriptions.clear()
        gateways.clear()
    }

    fun reload() {
        if (deviceConfigStore == null) {
            logger.fine("DeviceConfigStore not available for Redfish ingestion")
            return
        }

        deviceConfigStore.getAllDevices().onComplete { ar ->
            if (ar.succeeded()) {
                val activeDevs = ar.result().filter { it.type == "Redfish" && it.enabled }
                val newGateways = mutableMapOf<String, GatewayConfig>()

                for (dev in activeDevs) {
                    try {
                        val gw = GatewayConfig.fromJsonObject(dev.config)
                        newGateways[dev.name] = gw
                    } catch (e: Exception) {
                        logger.warning("Failed to parse Redfish gateway config for '${dev.name}': ${e.message}")
                    }
                }

                gateways.clear()
                gateways.putAll(newGateways)

                // Update subscriptions
                val requiredFilters = newGateways.values.flatMap { it.topicFilters }.toSet()

                // Unsubscribe from removed filters
                val toRemove = activeSubscriptions.filter { !requiredFilters.contains(it) }
                for (f in toRemove) {
                    try {
                        sessionHandler.unsubscribeInternalClient(internalClientId, f)
                        activeSubscriptions.remove(f)
                    } catch (e: Exception) {
                        logger.warning("Error unsubscribing Redfish filter $f: ${e.message}")
                    }
                }

                // Subscribe to new filters
                val toAdd = requiredFilters.filter { !activeSubscriptions.contains(it) }
                for (f in toAdd) {
                    try {
                        sessionHandler.subscribeInternalClient(internalClientId, f, 0)
                        activeSubscriptions.add(f)
                    } catch (e: Exception) {
                        logger.warning("Error subscribing Redfish filter $f: ${e.message}")
                    }
                }

                logger.fine("Redfish ingestion reloaded: ${gateways.size} active gateways, ${activeSubscriptions.size} topic subscriptions")
            } else {
                logger.warning("Failed to load Redfish devices from store: ${ar.cause()?.message}")
            }
        }
    }

    private fun handleBrokerMessage(msg: BrokerMessage) {
        // Skip messages published by this internal client to prevent loops
        if (msg.clientId == internalClientId || msg.senderId == internalClientId) {
            return
        }

        val topic = msg.topicName
        val matchedGateways = gateways.entries.filter { (_, gw) ->
            gw.topicFilters.any { matchTopicFilter(it, topic) }
        }

        if (matchedGateways.isEmpty()) return

        for ((gwName, gw) in matchedGateways) {
            val records = RedfishMapper.extractSensorRecords(msg.payload, topic, gw)
            for (record in records) {
                record.gatewayName = gwName
                val prefix = record.topicPrefix.ifBlank { "redfish" }
                val normalizedTopic = "$prefix/${record.chassisId}/sensors/${record.sensorId}"

                val payloadBytes = record.toJsonObject().encode().toByteArray(Charsets.UTF_8)
                val bm = BrokerMessage(
                    messageUuid = Utils.getUuid(),
                    messageId = 0,
                    topicName = normalizedTopic,
                    payload = payloadBytes,
                    qosLevel = 0,
                    isRetain = true,
                    isDup = false,
                    isQueued = false,
                    clientId = internalClientId,
                    senderId = internalClientId,
                    time = Instant.now()
                )

                // 1. Store in LastValue
                val lastVal = getLastValStore()
                if (lastVal != null) {
                    lastVal.addAll(listOf(bm))
                }

                // 2. Publish to broker
                try {
                    sessionHandler.publishInternal(internalClientId, bm)
                } catch (e: Exception) {
                    logger.fine("Failed to publish normalized Redfish message to $normalizedTopic: ${e.message}")
                }
            }
        }
    }

    fun getLastValStore(): IMessageStore? {
        val defaultGroupStore = archiveHandler?.getDeployedArchiveGroups()?.get("Default")?.lastValStore
        if (defaultGroupStore != null) return defaultGroupStore

        val anyGroupStore = archiveHandler?.getDeployedArchiveGroups()?.values?.firstOrNull { it.lastValStore != null }?.lastValStore
        if (anyGroupStore != null) return anyGroupStore

        return fallbackStore
    }

    fun getGateways(): Map<String, GatewayConfig> = gateways.toMap()

    private fun matchTopicFilter(pattern: String, topic: String): Boolean {
        val pp = pattern.split("/")
        val tt = topic.split("/")
        for (i in pp.indices) {
            val p = pp[i]
            if (p == "#") return true
            if (i >= tt.size) return false
            if (p == "+") continue
            if (p != tt[i]) return false
        }
        return pp.size == tt.size
    }
}
