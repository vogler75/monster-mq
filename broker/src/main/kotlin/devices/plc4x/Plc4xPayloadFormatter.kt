package at.rocworks.devices.plc4x

import at.rocworks.stores.devices.Plc4xPublishFormat
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

object Plc4xPayloadFormatter {
    fun format(
        publishFormat: Plc4xPublishFormat,
        value: Any?,
        deviceName: String,
        addressName: String,
        timestamp: String
    ): ByteArray {
        return when (publishFormat) {
            Plc4xPublishFormat.JSON -> JsonObject()
                .put("value", value)
                .put("timestamp", timestamp)
                .put("device", deviceName)
                .put("address", addressName)
                .encode()
                .toByteArray()
            Plc4xPublishFormat.VALUE -> encodeValue(value).toByteArray()
        }
    }

    private fun encodeValue(value: Any?): String {
        return when (value) {
            null -> "null"
            is JsonObject -> value.encode()
            is JsonArray -> value.encode()
            is Map<*, *> -> JsonObject(value.mapKeys { it.key.toString() }).encode()
            is Collection<*> -> JsonArray(value.toList()).encode()
            is Array<*> -> JsonArray(value.toList()).encode()
            else -> value.toString()
        }
    }
}
