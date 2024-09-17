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

    init {
        logger.level = Const.DEBUG_LEVEL
        logger.info("Initialize SparkplugbHandler")
    }

    fun metricExpansion(message: MqttMessage, callback: (MqttMessage) -> Unit) {
        if (message.topicName.startsWith(sourceNamespace))
        try {
            val spb = decoder.buildFromByteArray(message.payload, null)
            logger.finest { "Received message [${spb}] [${Utils.getCurrentFunctionName()}]" }
            spb.metrics.forEach { metric ->
                val name = if (metric.hasName()) metric.name else "alias/${metric.alias}"
                val topic = message.topicName.replaceFirst(sourceNamespace, expandedNamespace) + "/$name"
                val payload = JsonObject()
                    .put("Value", metric.value.toString())
                    .put("Timestamp", metric.timestamp)
                    .put("Quality", metric.alias)
                    .put("DataType", metric.dataType.toString())
                val mqttMessage = MqttMessage(
                    messageId = 0,
                    topicName = topic,
                    payload = payload.encode().toByteArray(),
                    qosLevel = 0,
                    isRetain = false,
                    isDup = false,
                    clientId = message.clientId
                )
                callback(mqttMessage)
            }
        } catch (e: Exception) {
            logger.severe { "Failed to decode message [${message.topicName}]: [${e.message}] [${Utils.getCurrentFunctionName()}]" }
        }
    }
}