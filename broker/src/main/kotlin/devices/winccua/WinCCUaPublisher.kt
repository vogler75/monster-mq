package at.rocworks.devices.winccua

import at.rocworks.data.BrokerMessage
import at.rocworks.stores.devices.WinCCUaAddress
import at.rocworks.stores.devices.WinCCUaConnectionConfig
import at.rocworks.stores.devices.WinCCUaTransformConfig
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.ConcurrentMap

/**
 * Stateless helpers shared by [WinCCUaConnector] (GraphQL) and [WinCCUaPipeConnector] (Open Pipe).
 *
 * Everything downstream of "tag value received" is identical across the two transports —
 * topic transformation, message formatting, and the BrokerMessage envelope shape.
 */
object WinCCUaPublisher {

    private const val PUBLISH_CLIENT_PREFIX = "winccua-connector-"

    fun resolveTopic(
        namespace: String,
        addressTopic: String,
        tagName: String,
        transformConfig: WinCCUaTransformConfig,
        topicCache: ConcurrentMap<String, String>
    ): String = topicCache.computeIfAbsent(tagName) { name ->
        val transformed = transformConfig.transformTagNameToTopic(name)
        "$namespace/$addressTopic/$transformed"
    }

    fun formatTagPayload(
        messageFormat: String,
        value: Any?,
        timestamp: String?,
        quality: JsonObject?
    ): ByteArray {
        return when (messageFormat) {
            WinCCUaConnectionConfig.FORMAT_JSON_MS -> {
                val timestampMs = try {
                    if (timestamp != null) Instant.parse(timestamp).toEpochMilli() else System.currentTimeMillis()
                } catch (e: Exception) {
                    System.currentTimeMillis()
                }
                val json = JsonObject()
                    .put("value", value)
                    .put("time", timestampMs)
                if (quality != null) json.put("quality", quality)
                json.encode().toByteArray()
            }
            WinCCUaConnectionConfig.FORMAT_RAW_VALUE -> {
                value.toString().toByteArray()
            }
            else -> {
                // FORMAT_JSON_ISO and any unknown value fall back to ISO format
                val json = JsonObject().put("value", value)
                if (timestamp != null) json.put("time", timestamp)
                if (quality != null) json.put("quality", quality)
                json.encode().toByteArray()
            }
        }
    }

    fun buildTagBrokerMessage(
        deviceName: String,
        topic: String,
        payload: ByteArray,
        retained: Boolean
    ): BrokerMessage = BrokerMessage(
        messageId = 0,
        topicName = topic,
        payload = payload,
        qosLevel = 0,
        isRetain = retained,
        isDup = false,
        isQueued = false,
        clientId = "$PUBLISH_CLIENT_PREFIX$deviceName"
    )

    fun buildAlarmTopic(namespace: String, addressTopic: String, alarmName: String): String =
        "$namespace/$addressTopic/$alarmName"

    fun buildAlarmBrokerMessage(
        deviceName: String,
        topic: String,
        alarmJson: JsonObject,
        retained: Boolean
    ): BrokerMessage = BrokerMessage(
        messageId = 0,
        topicName = topic,
        payload = alarmJson.encode().toByteArray(),
        qosLevel = 0,
        isRetain = retained,
        isDup = false,
        isQueued = false,
        clientId = "$PUBLISH_CLIENT_PREFIX$deviceName"
    )

    fun resolveAlarmName(alarmJson: JsonObject): String? =
        alarmJson.getString("name")
            ?: alarmJson.getString("Name")
            ?: alarmJson.getString("path")
}
