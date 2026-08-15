package at.rocworks.devices.i3xclient

import at.rocworks.data.BrokerMessage
import at.rocworks.stores.devices.I3xAddress
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.time.format.DateTimeParseException

/**
 * Helper for constructing MQTT topics, formatting payloads, and building BrokerMessage instances for i3X updates.
 */
object I3xPublisher {

    /**
     * Compute the destination MQTT topic from the device namespace, address configuration, and the received elementId.
     */
    fun resolveTopic(namespace: String, address: I3xAddress, elementId: String): String {
        val cleanElement = elementId.trim().trim('/')
        val cleanSubPrefix = address.elementId.trim().trim('/')
        val cleanTopic = address.topic.trim().trim('/')

        val relativePath = if (cleanElement == cleanSubPrefix) {
            ""
        } else if (cleanElement.startsWith("$cleanSubPrefix/")) {
            cleanElement.removePrefix("$cleanSubPrefix/").trim('/')
        } else {
            cleanElement
        }

        val resolvedTopic = when {
            address.removePath -> {
                if (relativePath.isNotEmpty()) "$cleanTopic/$relativePath" else cleanTopic
            }
            address.maxDepth != 1 && cleanElement.startsWith("$cleanSubPrefix/") -> {
                if (relativePath.isNotEmpty()) "$cleanTopic/$relativePath" else cleanTopic
            }
            else -> cleanTopic
        }

        val cleanNamespace = namespace.trim().trim('/')
        return if (cleanNamespace.isNotEmpty()) {
            "$cleanNamespace/$resolvedTopic"
        } else {
            resolvedTopic
        }
    }

    /**
     * Format the i3X value update into a byte array according to the requested message format.
     */
    fun formatPayload(
        value: Any?,
        quality: String?,
        timestamp: String?,
        messageFormat: String
    ): ByteArray {
        val q = quality ?: "Good"
        val ts = timestamp ?: Instant.now().toString()

        return when (messageFormat) {
            I3xAddress.FORMAT_RAW_VALUE -> {
                when (value) {
                    null -> byteArrayOf()
                    is JsonObject -> value.encode().toByteArray(Charsets.UTF_8)
                    is JsonArray -> value.encode().toByteArray(Charsets.UTF_8)
                    is ByteArray -> value
                    else -> value.toString().toByteArray(Charsets.UTF_8)
                }
            }
            I3xAddress.FORMAT_JSON_ISO -> {
                val json = JsonObject()
                    .put("value", value)
                    .put("quality", q)
                    .put("time", ts)
                json.encode().toByteArray(Charsets.UTF_8)
            }
            I3xAddress.FORMAT_JSON_MS -> {
                val epochMs = try {
                    Instant.parse(ts).toEpochMilli()
                } catch (_: DateTimeParseException) {
                    System.currentTimeMillis()
                }
                val json = JsonObject()
                    .put("value", value)
                    .put("quality", q)
                    .put("time", epochMs)
                json.encode().toByteArray(Charsets.UTF_8)
            }
            I3xAddress.FORMAT_VQT -> {
                val json = JsonObject()
                    .put("value", value)
                    .put("quality", q)
                    .put("timestamp", ts)
                json.encode().toByteArray(Charsets.UTF_8)
            }
            else -> {
                when (value) {
                    null -> byteArrayOf()
                    is JsonObject -> value.encode().toByteArray(Charsets.UTF_8)
                    is JsonArray -> value.encode().toByteArray(Charsets.UTF_8)
                    else -> value.toString().toByteArray(Charsets.UTF_8)
                }
            }
        }
    }

    /**
     * Build a BrokerMessage to be published to the broker message bus.
     */
    fun buildBrokerMessage(
        deviceName: String,
        topic: String,
        payload: ByteArray,
        retained: Boolean,
        qos: Int = 0
    ): BrokerMessage {
        return BrokerMessage(
            messageId = 0,
            topicName = topic,
            payload = payload,
            qosLevel = qos,
            isRetain = retained,
            isDup = false,
            isQueued = false,
            clientId = "i3x-$deviceName",
            time = Instant.now()
        )
    }
}
