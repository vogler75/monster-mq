package at.rocworks.extensions.graphql

import java.time.Instant
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

data class TopicUpdateBulk(
    val updates: List<TopicUpdate>,
    val count: Int,
    val timestamp: Long
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
    val updatedAt: String?,
    val aclRules: List<AclRuleInfo>
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

data class PurgeResult(
    val success: Boolean,
    val message: String? = null,
    val deletedCount: Long
)

// Authentication Models
data class LoginResult(
    val success: Boolean,
    val token: String? = null,
    val message: String? = null,
    val username: String? = null,
    val isAdmin: Boolean = false
)

// Metrics data classes
data class BrokerMetrics(
    val messagesIn: Double,
    val messagesOut: Double,
    val nodeSessionCount: Int,
    val clusterSessionCount: Int,
    val queuedMessagesCount: Long,
    val topicIndexSize: Int,
    val clientNodeMappingSize: Int,
    val topicNodeMappingSize: Int,
    val messageBusIn: Double,
    val messageBusOut: Double,
    val mqttClientIn: Double = 0.0,
    val mqttClientOut: Double = 0.0,
    val opcUaClientIn: Double = 0.0,
    val opcUaClientOut: Double = 0.0,
    val kafkaClientIn: Double = 0.0,
    val kafkaClientOut: Double = 0.0,
    val winCCOaClientIn: Double = 0.0,
    val winCCUaClientIn: Double = 0.0,
    val neo4jClientIn: Double = 0.0,
    val timestamp: String
)

// OPC UA Device Metrics
// messagesIn: values received from OPC UA server into broker
// messagesOut: values written from broker to OPC UA server (future use, currently may remain 0)
 data class OpcUaDeviceMetrics(
     val messagesIn: Double,
     val messagesOut: Double,
     val timestamp: String
 )
 
 data class Broker(
     val nodeId: String,
     val version: String
 )

data class SessionMetrics(
    val messagesIn: Double,
    val messagesOut: Double,
    val timestamp: String
)

data class MqttClientMetrics(
    val messagesIn: Double,
    val messagesOut: Double,
    val timestamp: String
)

// Kafka Client Metrics (similar structure to MQTT bridge metrics)
data class KafkaClientMetrics(
    val messagesIn: Double,
    val messagesOut: Double,
    val timestamp: String
)

// WinCC OA Client Metrics
data class WinCCOaClientMetrics(
    val messagesIn: Double,
    val connected: Boolean = false,
    val timestamp: String
)

// WinCC Unified (UA) Client Metrics
data class WinCCUaClientMetrics(
    val messagesIn: Double,
    val connected: Boolean = false,
    val timestamp: String
)

// PLC4X Client Metrics
data class Plc4xDeviceMetrics(
    val messagesInRate: Double,
    val connected: Boolean = false,
    val timestamp: String
)

// Neo4j Client Metrics
data class Neo4jClientMetrics(
    val messagesIn: Double,
    val messagesWritten: Double,
    val messagesSuppressed: Double,
    val errors: Double,
    val pathQueueSize: Int,
    val messagesInRate: Double,
    val messagesWrittenRate: Double,
    val timestamp: String
)

// JDBC Logger Metrics
data class JDBCLoggerMetrics(
    val messagesIn: Double,
    val messagesValidated: Double,
    val messagesWritten: Double,
    val messagesSkipped: Double,
    val validationErrors: Double,
    val writeErrors: Double,
    val queueSize: Int,
    val queueCapacity: Int,
    val queueFull: Boolean,
    val timestamp: String
)

data class ArchiveGroupMetrics(
    val messagesOut: Double,  // Messages written to database per second
    val bufferSize: Int,      // Average size of the write buffer
    val timestamp: String
)

data class MqttSubscription(
    val topicFilter: String,
    val qos: Int
)

data class Session(
    val clientId: String,
    val nodeId: String,
    val subscriptions: List<MqttSubscription>,
    val cleanSession: Boolean,
    val sessionExpiryInterval: Long,
    val clientAddress: String?,
    val connected: Boolean,
    val information: String?
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

object TimestampConverter {
    fun instantToIsoString(instant: Instant): String {
        return instant.toString()
    }

    fun milliToIsoString(millis: Long): String {
        return Instant.ofEpochMilli(millis).toString()
    }

    fun currentTimeIsoString(): String {
        return Instant.now().toString()
    }
}

// System Logs Models
data class SystemLogEntry(
    val timestamp: String,
    val level: String,
    val logger: String,
    val message: String,
    val thread: Long,
    val node: String,
    val sourceClass: String?,
    val sourceMethod: String?,
    val parameters: List<String>?,
    val exception: ExceptionInfo?
)


data class ExceptionInfo(
    val `class`: String,
    val message: String?,
    val stackTrace: String
)