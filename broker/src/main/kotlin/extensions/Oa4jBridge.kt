package at.rocworks.extensions

import at.rocworks.Monster
import at.rocworks.MonsterOA
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.BrokerMessage
import at.rocworks.data.MqttSubscription
import at.rocworks.oa4j.base.IHotLink
import at.rocworks.oa4j.base.JDpConnect
import at.rocworks.oa4j.base.JDpHLGroup
import at.rocworks.oa4j.base.JDpVCItem
import at.rocworks.oa4j.`var`.DynVar
import at.rocworks.oa4j.`var`.TimeVar
import at.rocworks.oa4j.`var`.Variable
import at.rocworks.oa4j.`var`.VariableType
import io.vertx.core.AbstractVerticle
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

/**
 * OA Bridge - Native WinCC OA integration for !OA/ topics
 *
 * Allows MQTT clients to interact with WinCC OA via special topics:
 *   !OA/<namespace>/<operation>/<name>
 *
 * Supported operations:
 *   - dps: dpConnect subscription - subscribe to datapoint value changes
 *   - dpt: dpTypeGet (future) - get datapoint type information
 *   - sql: SQL query (future) - execute SQL queries
 *
 * Example topic: !OA/s1/dps/ExampleDP_Arg1
 *
 * When a client subscribes to a dps topic, this verticle performs a native dpConnect
 * via oa4j and publishes value changes to the MQTT topic as JSON.
 *
 * JSON format uses attribute config names as keys:
 *   - "_online.._value" -> "value"
 *   - "_online.._stime" -> "stime"
 *   - "_online.._status" -> "status"
 *
 * Example output: {"value": <any>, "status": <number>, "stime": "<ISO8601>"}
 *
 * Features:
 * - Configurable attributes list via config.yaml
 * - Reference counting: multiple clients subscribing to the same DP share one dpConnect
 * - Automatic dpDisconnect when all subscribers are gone
 * - Cluster-aware: subscription requests are received via EventBus from any node
 * - Only runs on JManager host (MonsterOA entry point)
 */
class Oa4jBridge : AbstractVerticle() {

    private val logger: Logger = Utils.getLogger(this::class.java)

    // Configured namespace (e.g., "s1")
    private lateinit var namespace: String

    // Configurable attributes to connect (e.g., "_online.._value", "_online.._stime", "_online.._status")
    private lateinit var attributes: List<String>

    // Track active dpConnects with reference counting
    // Key: datapointName (without attributes), Value: DpConnectionState
    private val activeConnections = ConcurrentHashMap<String, DpConnectionState>()

    // Track which clients are subscribed to which datapoints
    // Key: clientId, Value: Set of datapointNames
    private val clientSubscriptions = ConcurrentHashMap<String, MutableSet<String>>()

    data class DpConnectionState(
        val datapointName: String,
        val mqttTopic: String,
        val subscriberClientIds: MutableSet<String> = ConcurrentHashMap.newKeySet(),
        var dpConnect: JDpConnect? = null,
        var isConnected: Boolean = false
    )

    companion object {
        // Topic prefix for OA subscriptions
        const val OA_TOPIC_PREFIX = "!OA/"

        // Operation types
        const val OP_DPS = "dps"  // dpConnect subscription
        const val OP_DPT = "dpt"  // dpTypeGet (future)
        const val OP_SQL = "sql"  // SQL query (future)

        /**
         * Parse an OA subscription topic
         * Format: !OA/<namespace>/<operation>/<name>
         * Operations: dps (dpConnect), dpt (dpTypeGet), sql (SQL query)
         * Returns: Triple(namespace, operation, name) or null if invalid
         */
        fun parseOaTopic(topic: String): Triple<String, String, String>? {
            if (!topic.startsWith(OA_TOPIC_PREFIX)) return null

            val parts = topic.removePrefix(OA_TOPIC_PREFIX).split("/", limit = 3)
            if (parts.size >= 3) {
                return Triple(parts[0], parts[1], parts[2])
            }
            return null
        }
    }

    override fun start(startPromise: Promise<Void>) {
        try {
            // Check if JManager is available
            if (MonsterOA.getInstance() == null) {
                logger.info("Oa4jBridge: JManager not available, not starting")
                startPromise.complete()
                return
            }

            // Load configuration
            val config = config()
            namespace = config.getString("namespace", "")

            if (namespace.isEmpty()) {
                logger.warning("Oa4jBridge: No namespace configured, not starting")
                startPromise.complete()
                return
            }

            // Load attributes from config
            val attributesArray = config.getJsonArray("attributes")
            attributes = if (attributesArray != null && attributesArray.size() > 0) {
                attributesArray.map { it.toString() }
            } else {
                listOf("_online.._value", "_online.._stime", "_online.._status")
            }

            // Register EventBus consumers
            setupEventBusHandlers()

            logger.info("Oa4jBridge started for namespace '$namespace' with attributes: $attributes")
            startPromise.complete()

        } catch (e: Exception) {
            logger.severe("Oa4jBridge: Failed to start: ${e.message}")
            startPromise.fail(e)
        }
    }

    override fun stop(stopPromise: Promise<Void>) {
        logger.info("Oa4jBridge: Stopping...")

        // Disconnect all active dpConnects
        activeConnections.values.forEach { state ->
            try {
                state.dpConnect?.disconnect()
            } catch (e: Exception) {
                logger.warning("Oa4jBridge: Error disconnecting ${state.datapointName}: ${e.message}")
            }
        }
        activeConnections.clear()
        clientSubscriptions.clear()

        logger.info("Oa4jBridge: Stopped")
        stopPromise.complete()
    }

    private fun setupEventBusHandlers() {
        // Handle subscription add requests
        vertx.eventBus().consumer<JsonObject>(EventBusAddresses.Oa4jBridge.SUBSCRIPTION_ADD) { message ->
            val request = message.body()
            handleSubscriptionAdd(
                clientId = request.getString("clientId"),
                requestedNamespace = request.getString("namespace"),
                datapointName = request.getString("datapointName"),
                mqttTopic = request.getString("mqttTopic")
            )
        }

        // Listen to SUBSCRIPTION_DELETE for all subscription removals
        // (explicit unsubscribe AND client disconnect with clean session)
        // This is published (not request/reply) so multiple consumers are safe
        vertx.eventBus().consumer<MqttSubscription>(EventBusAddresses.Cluster.SUBSCRIPTION_DELETE) { message ->
            val subscription = message.body()
            val topic = subscription.topicName

            // Check if this is an OA topic
            val oaParsed = parseOaTopic(topic)
            if (oaParsed != null) {
                val (parsedNamespace, operation, datapointName) = oaParsed
                if (parsedNamespace == namespace) {
                    when (operation) {
                        OP_DPS -> {
                            logger.fine { "Oa4jBridge: SUBSCRIPTION_DELETE received for client '${subscription.clientId}', topic '$topic'" }
                            handleSubscriptionRemove(
                                clientId = subscription.clientId,
                                requestedNamespace = parsedNamespace,
                                datapointName = datapointName
                            )
                        }
                        // Future: OP_DPT, OP_SQL handlers
                    }
                }
            }
        }
    }

    private fun handleSubscriptionAdd(
        clientId: String,
        requestedNamespace: String,
        datapointName: String,
        mqttTopic: String
    ) {
        // Check if this is for our namespace
        if (requestedNamespace != namespace) {
            logger.fine { "Oa4jBridge: Ignoring subscription for namespace '$requestedNamespace' (we are '$namespace')" }
            return
        }

        logger.info("Oa4jBridge: Adding subscription for client '$clientId' to DP '$datapointName'")

        // Track client subscription
        clientSubscriptions.computeIfAbsent(clientId) { ConcurrentHashMap.newKeySet() }.add(datapointName)

        // Add to or create connection state
        activeConnections.compute(datapointName) { _, existing ->
            if (existing != null) {
                // Add client to existing connection
                existing.subscriberClientIds.add(clientId)
                logger.fine { "Oa4jBridge: Added client '$clientId' to existing dpConnect for '$datapointName' (${existing.subscriberClientIds.size} subscribers)" }
                existing
            } else {
                // Create new connection state and do dpConnect
                val state = DpConnectionState(
                    datapointName = datapointName,
                    mqttTopic = mqttTopic
                )
                state.subscriberClientIds.add(clientId)

                // Perform dpConnect
                performDpConnect(state)
                state
            }
        }
    }

    private fun handleSubscriptionRemove(
        clientId: String,
        requestedNamespace: String,
        datapointName: String
    ) {
        // Check if this is for our namespace
        if (requestedNamespace != namespace) return

        logger.info("Oa4jBridge: Removing subscription for client '$clientId' from DP '$datapointName'")

        // Remove from client tracking
        clientSubscriptions[clientId]?.remove(datapointName)

        // Remove from connection state
        activeConnections.computeIfPresent(datapointName) { _, state ->
            state.subscriberClientIds.remove(clientId)

            if (state.subscriberClientIds.isEmpty()) {
                // No more subscribers, do dpDisconnect
                performDpDisconnect(state)
                null  // Remove from map
            } else {
                logger.fine { "Oa4jBridge: Removed client '$clientId' from dpConnect for '$datapointName' (${state.subscriberClientIds.size} subscribers remaining)" }
                state
            }
        }
    }

    private fun performDpConnect(state: DpConnectionState) {
        // Build DPE list for connection
        val dpeList = attributes.map { attr ->
            "${state.datapointName}:$attr"
        }

        logger.info("Oa4jBridge: dpConnect for '${state.datapointName}': $dpeList")

        try {
            // Create dpConnect with hotlink callback
            val dpConnect = JDpConnect()
                .addGroup(ArrayList(dpeList))
                .hotlink(object : IHotLink {
                    override fun hotlink(hotlink: JDpHLGroup) {
                        // Callback when values change - run on Vert.x context
                        vertx.runOnContext {
                            handleDpValues(state, hotlink)
                        }
                    }
                })
                .connect()

            state.dpConnect = dpConnect
            state.isConnected = true
            logger.info("Oa4jBridge: dpConnect successful for '${state.datapointName}'")

        } catch (e: Exception) {
            logger.severe("Oa4jBridge: dpConnect failed for '${state.datapointName}': ${e.message}")
            e.printStackTrace()
        }
    }

    private fun performDpDisconnect(state: DpConnectionState) {
        try {
            state.dpConnect?.disconnect()
            logger.info("Oa4jBridge: dpDisconnect for '${state.datapointName}'")
        } catch (e: Exception) {
            logger.warning("Oa4jBridge: dpDisconnect failed for '${state.datapointName}': ${e.message}")
        }
        state.dpConnect = null
        state.isConnected = false
    }

    private fun handleDpValues(state: DpConnectionState, hotlink: JDpHLGroup) {
        try {
            logger.finer("Oa4jBridge: hotlink callback for '${state.datapointName}' with ${hotlink.numberOfItems} items")

            // Build JSON from values
            val json = JsonObject()

            hotlink.items.forEach { item: JDpVCItem ->
                val dpName = item.dpName
                val config = item.dpIdentifier.config // e.g., "_online.._value"
                logger.fine { "Oa4jBridge: Processing item dpName='$dpName', config='$config', value='${item.variable}'" }

                // Extract JSON key from config (e.g., "_online.._value" -> "value")
                val jsonKey = extractJsonKey(config)

                // Handle time values specially (TimeVar needs time extraction)
                if (item.variable.isA() == VariableType.TimeVar) {
                    val timeVar = item.variable as TimeVar
                    json.put(jsonKey, formatTime(timeVar.time))
                } else {
                    json.put(jsonKey, convertValue(item))
                }
            }

            // Publish if we have any values
            if (!json.isEmpty) {
                publishToMqtt(state.mqttTopic, json)
            } else {
                logger.warning("Oa4jBridge: Empty JSON for '${state.datapointName}', not publishing")
            }

        } catch (e: Exception) {
            logger.severe("Oa4jBridge: Error handling dpValues for '${state.datapointName}': ${e.message}")
            e.printStackTrace()
        }
    }

    /**
     * Extract JSON key from config attribute name.
     * E.g., "_online.._value" -> "value", "_online.._stime" -> "stime"
     */
    private fun extractJsonKey(config: String): String {
        val lastDoubleDot = config.lastIndexOf("..")
        return if (lastDoubleDot >= 0 && lastDoubleDot + 2 < config.length) {
            config.substring(lastDoubleDot + 2)
        } else {
            config
        }
    }

    private fun convertValue(item: JDpVCItem): Any? {
        val variable = item.variable
        return when (variable.isA()) {
            VariableType.BitVar -> variable.valueObject as Boolean
            VariableType.IntegerVar -> variable.toInt()
            VariableType.UIntegerVar -> variable.toInt()
            VariableType.LongVar -> variable.toLong()
            VariableType.FloatVar -> variable.toDouble()
            VariableType.TextVar -> variable.valueObject as String
            VariableType.TimeVar -> formatTime((variable as TimeVar).time)
            VariableType.Bit32Var -> variable.toLong()
            VariableType.Bit64Var -> variable.toLong()
            VariableType.CharVar -> variable.valueObject.toString()
            VariableType.DynVar -> convertDynVar(variable as DynVar)
            VariableType.NullVar -> null
            else -> variable.formatValue()
        }
    }

    private fun convertDynVar(dynVar: DynVar): JsonArray {
        val array = JsonArray()
        dynVar.asList().forEach { item: Variable ->
            when (item.isA()) {
                VariableType.BitVar -> array.add(item.valueObject as Boolean)
                VariableType.IntegerVar -> array.add(item.toInt())
                VariableType.UIntegerVar -> array.add(item.toInt())
                VariableType.LongVar -> array.add(item.toLong())
                VariableType.FloatVar -> array.add(item.toDouble())
                VariableType.TextVar -> array.add(item.valueObject as String)
                VariableType.TimeVar -> array.add(formatTime((item as TimeVar).time))
                VariableType.NullVar -> array.addNull()
                else -> array.add(item.formatValue())
            }
        }
        return array
    }

    private fun formatTime(timeMs: Long): String {
        return Instant.ofEpochMilli(timeMs)
            .atOffset(ZoneOffset.UTC)
            .format(DateTimeFormatter.ISO_INSTANT)
    }

    private fun publishToMqtt(topic: String, payload: JsonObject) {
        val message = BrokerMessage(
            messageId = 0,
            topicName = topic,
            payload = payload.encode().toByteArray(),
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = "oa-datapoint-bridge-$namespace"
        )

        logger.finest("Oa4jBridge: Publishing to '$topic': ${payload.encode()}")

        // Use SessionHandler to publish (ensures distribution across cluster)
        val sessionHandler = Monster.getSessionHandler()
        if (sessionHandler != null) {
            sessionHandler.publishMessage(message)
            logger.finest("Oa4jBridge: Message published successfully to '$topic'")
        } else {
            logger.severe("Oa4jBridge: SessionHandler is null, cannot publish to '$topic'")
        }
    }
}
