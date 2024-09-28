package at.rocworks.extensions

import at.rocworks.Const
import at.rocworks.Utils
import at.rocworks.data.MqttMessage
import io.vertx.core.json.JsonObject
import org.eclipse.tahu.message.SparkplugBPayloadDecoder

class SparkplugExtension(config: JsonObject) {
    private val logger = Utils.getLogger(this::class.java)

    private val decoder = SparkplugBPayloadDecoder()
    private val sourceNamespace = "spBv1.0/"
    private val expandedNamespace= "spBv1.0e/"

    private val messageTypes = listOf( // message types with protobuf data
        "NBIRTH", // Birth certificate for Sparkplug Edge Nodes
        "NDEATH", // Death certificate for Sparkplug Edge Nodes
        "DBIRTH", // Birth certificate for Devices
        "DDEATH", // Death certificate for Devices
        "NDATA",  // Edge Node data message
        "DDATA",  // Device data message
        "NCMD",   // Edge Node command message
        "DCMD",   // Device command message
        "STATE"   // Sparkplug Host Application state message ==> STATE is not protobuf
    )

    init {
        logger.level = Const.DEBUG_LEVEL
        logger.info("Initialize Sparkplug Handler")
    }

    fun metricExpansion(message: MqttMessage, callback: (MqttMessage) -> Unit) {
        if (message.topicName.startsWith(sourceNamespace))
        try {
            val levels = message.topicName.split("/") // spBv1.0/namespace/group_id/message_type/edge_node_id/[device_id]
            if (levels.size>3 && levels[3] == "STATE") { // STATE is not a protobuf message
                val topic = message.topicName.replaceFirst(sourceNamespace, expandedNamespace)
                val mqttMessage = MqttMessage(
                    messageId = 0,
                    topicName = topic,
                    payload = message.payload,
                    qosLevel = 0,
                    isRetain = false,
                    isDup = false,
                    isQueued = false,
                    clientId = message.clientId
                )
                callback(mqttMessage)
            } else {
                val spb = decoder.buildFromByteArray(message.payload, null)
                logger.finest { "Received message [${spb}] [${Utils.getCurrentFunctionName()}]" }
                spb.metrics.forEach { metric ->
                    val name = if (metric.hasName()) metric.name else "alias/${metric.alias}"
                    val topic = message.topicName.replaceFirst(sourceNamespace, expandedNamespace) + "/$name"
                    val payload = JsonObject()
                        .put("Timestamp", metric.timestamp)
                        .put("DataType", metric.dataType.toString())
                    try {
                        payload.put("Value", metric.value)
                    } catch (e: Exception) {
                        logger.severe { "Failed to decode metric value [${name}]: [${e.message}] [${Utils.getCurrentFunctionName()}]" }
                        payload.put("Value", null)
                    }

                    val mqttMessage = MqttMessage(
                        messageId = 0,
                        topicName = topic,
                        payload = payload.encode().toByteArray(),
                        qosLevel = 0,
                        isRetain = false,
                        isDup = false,
                        isQueued = false,
                        clientId = message.clientId
                    )
                    callback(mqttMessage)
                }
            }
        } catch (e: Exception) {
            logger.severe { "Failed to decode message [${message.topicName}]: [${e.message}] [${Utils.getCurrentFunctionName()}]" }
        }
    }
}