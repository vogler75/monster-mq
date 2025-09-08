package at.rocworks.extensions.graphql

import java.util.Base64

enum class DataFormat {
    JSON,
    BINARY
}

data class TopicValue(
    val topic: String,
    val payload: String,
    val format: DataFormat,
    val timestamp: Long,
    val qos: Int
)

data class RetainedMessage(
    val topic: String,
    val payload: String,
    val format: DataFormat,
    val timestamp: Long,
    val qos: Int
)

data class ArchivedMessage(
    val topic: String,
    val payload: String,
    val format: DataFormat,
    val timestamp: Long,
    val qos: Int,
    val clientId: String?
)

data class TopicUpdate(
    val topic: String,
    val payload: String,
    val format: DataFormat,
    val timestamp: Long,
    val qos: Int,
    val retained: Boolean,
    val clientId: String?
)

data class PublishInput(
    val topic: String,
    val payload: String,
    val format: DataFormat = DataFormat.JSON,
    val qos: Int = 0,
    val retained: Boolean = false
)

data class PublishResult(
    val success: Boolean,
    val topic: String,
    val timestamp: Long,
    val error: String? = null
)

object PayloadConverter {
    fun encode(payload: ByteArray, format: DataFormat): String {
        return when (format) {
            DataFormat.JSON -> String(payload, Charsets.UTF_8)
            DataFormat.BINARY -> Base64.getEncoder().encodeToString(payload)
        }
    }

    fun decode(payload: String, format: DataFormat): ByteArray {
        return when (format) {
            DataFormat.JSON -> payload.toByteArray(Charsets.UTF_8)
            DataFormat.BINARY -> Base64.getDecoder().decode(payload)
        }
    }

    fun tryParseJson(payload: ByteArray): String? {
        return try {
            val jsonStr = String(payload, Charsets.UTF_8)
            // Basic validation - check if it starts with { or [
            if (jsonStr.trimStart().firstOrNull() in listOf('{', '[')) {
                jsonStr
            } else {
                null
            }
        } catch (e: Exception) {
            null
        }
    }

    fun autoDetectAndEncode(payload: ByteArray, preferredFormat: DataFormat): Pair<String, DataFormat> {
        return when (preferredFormat) {
            DataFormat.JSON -> {
                // Respect user's JSON preference - convert to UTF-8 string
                try {
                    val jsonStr = String(payload, Charsets.UTF_8)
                    jsonStr to DataFormat.JSON
                } catch (e: Exception) {
                    // Only fall back to binary if UTF-8 decoding fails
                    Base64.getEncoder().encodeToString(payload) to DataFormat.BINARY
                }
            }
            DataFormat.BINARY -> {
                Base64.getEncoder().encodeToString(payload) to DataFormat.BINARY
            }
        }
    }
}