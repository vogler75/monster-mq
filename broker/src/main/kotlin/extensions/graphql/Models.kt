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

// User Management Models
data class UserInfo(
    val username: String,
    val enabled: Boolean,
    val canSubscribe: Boolean,
    val canPublish: Boolean,
    val isAdmin: Boolean,
    val createdAt: String?,
    val updatedAt: String?
)

data class AclRuleInfo(
    val id: String,
    val username: String,
    val topicPattern: String,
    val canSubscribe: Boolean,
    val canPublish: Boolean,
    val priority: Int,
    val createdAt: String?
)

data class CreateUserInput(
    val username: String,
    val password: String,
    val enabled: Boolean = true,
    val canSubscribe: Boolean = true,
    val canPublish: Boolean = true,
    val isAdmin: Boolean = false
)

data class UpdateUserInput(
    val username: String,
    val enabled: Boolean? = null,
    val canSubscribe: Boolean? = null,
    val canPublish: Boolean? = null,
    val isAdmin: Boolean? = null
)

data class SetPasswordInput(
    val username: String,
    val password: String
)

data class CreateAclRuleInput(
    val username: String,
    val topicPattern: String,
    val canSubscribe: Boolean = false,
    val canPublish: Boolean = false,
    val priority: Int = 0
)

data class UpdateAclRuleInput(
    val id: String,
    val username: String? = null,
    val topicPattern: String? = null,
    val canSubscribe: Boolean? = null,
    val canPublish: Boolean? = null,
    val priority: Int? = null
)

data class UserManagementResult(
    val success: Boolean,
    val message: String? = null,
    val user: UserInfo? = null,
    val aclRule: AclRuleInfo? = null
)

// Authentication Models
data class LoginInput(
    val username: String,
    val password: String
)

data class LoginResult(
    val success: Boolean,
    val token: String? = null,
    val message: String? = null,
    val username: String? = null,
    val isAdmin: Boolean = false
)

// Metrics data classes
data class BrokerMetrics(
    val messagesIn: Long,
    val messagesOut: Long,
    val nodeSessionCount: Int,
    val clusterSessionCount: Int,
    val queuedMessagesCount: Long
)

data class Broker(
    val nodeId: String,
    val metrics: BrokerMetrics
)

data class SessionMetrics(
    val messagesIn: Long,
    val messagesOut: Long
)

data class MqttSubscription(
    val topicFilter: String,
    val qos: Int
)

data class Session(
    val clientId: String,
    val nodeId: String,
    val metrics: SessionMetrics,
    val subscriptions: List<MqttSubscription>,
    val cleanSession: Boolean,
    val sessionExpiryInterval: Long,
    val clientAddress: String?
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