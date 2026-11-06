package at.rocworks.bus

import at.rocworks.Utils
import at.rocworks.data.BrokerMessage
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.Base64

/** Versioned metadata carried as a Zenoh attachment while the sample payload stays MQTT-native. */
object ZenohMessageEnvelope {
    private const val VERSION = 1

    data class Decoded(val origin: String?, val message: BrokerMessage)

    fun encode(origin: String, message: BrokerMessage): ByteArray = JsonObject()
        .put("version", VERSION)
        .put("origin", origin)
        .put("messageUuid", message.messageUuid)
        .put("messageId", message.messageId)
        .put("qos", message.qosLevel)
        .put("retain", message.isRetain)
        .put("dup", message.isDup)
        .put("queued", message.isQueued)
        .put("clientId", message.clientId)
        .put("senderId", message.senderId)
        .put("time", message.time.toEpochMilli())
        .put("messageExpiryInterval", message.messageExpiryInterval)
        .put("payloadFormatIndicator", message.payloadFormatIndicator)
        .put("contentType", message.contentType)
        .put("responseTopic", message.responseTopic)
        .put("correlationData", message.correlationData?.let(Base64.getEncoder()::encodeToString))
        // Ordered list of [key, value] pairs (duplicate keys allowed)
        .put("userProperties", message.userProperties?.let { props -> io.vertx.core.json.JsonArray(props.map { io.vertx.core.json.JsonArray().add(it.first).add(it.second) }) })
        .apply {
            val peerSrc = message.peerSource ?: message.peer?.sourceNode
            if (peerSrc != null) {
                put("peerSource", peerSrc)
            }
        }
        .encode()
        .toByteArray(Charsets.UTF_8)

    fun decode(topic: String, payload: ByteArray, attachment: ByteArray?): Decoded {
        if (attachment == null || attachment.isEmpty()) return nativeMessage(topic, payload)

        return try {
            val json = JsonObject(String(attachment, Charsets.UTF_8))
            if (json.getInteger("version") != VERSION) return nativeMessage(topic, payload)
            val properties = when (val up = json.getValue("userProperties")) {
                is io.vertx.core.json.JsonArray -> up.mapNotNull { pair ->
                    (pair as? io.vertx.core.json.JsonArray)?.takeIf { it.size() == 2 }?.let { it.getValue(0).toString() to it.getValue(1)?.toString().orEmpty() }
                }
                // Envelopes written by older versions carry a JSON object
                is JsonObject -> up.map.map { (key, value) -> key to value?.toString().orEmpty() }
                else -> null
            }

            Decoded(
                origin = json.getString("origin"),
                message = BrokerMessage(
                    messageUuid = json.getString("messageUuid", Utils.getUuid()),
                    messageId = json.getInteger("messageId", 0),
                    topicName = topic,
                    payload = payload,
                    qosLevel = json.getInteger("qos", 0).coerceIn(0, 2),
                    isRetain = json.getBoolean("retain", false),
                    isDup = json.getBoolean("dup", false),
                    isQueued = json.getBoolean("queued", false),
                    clientId = json.getString("clientId", "zenoh"),
                    senderId = json.getString("senderId"),
                    time = Instant.ofEpochMilli(json.getLong("time", System.currentTimeMillis())),
                    messageExpiryInterval = json.getLong("messageExpiryInterval"),
                    payloadFormatIndicator = json.getInteger("payloadFormatIndicator"),
                    contentType = json.getString("contentType"),
                    responseTopic = json.getString("responseTopic"),
                    correlationData = json.getString("correlationData")?.let(Base64.getDecoder()::decode),
                    userProperties = properties,
                    peerSource = json.getString("peerSource")
                )
            )
        } catch (_: Exception) {
            nativeMessage(topic, payload)
        }
    }

    private fun nativeMessage(topic: String, payload: ByteArray) = Decoded(
        origin = null,
        message = BrokerMessage(
            messageUuid = Utils.getUuid(),
            messageId = 0,
            topicName = topic,
            payload = payload,
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = "zenoh",
            senderId = "zenoh"
        )
    )
}
