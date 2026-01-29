package at.rocworks.data

import at.rocworks.Utils
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.Future
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.Json
import io.vertx.mqtt.MqttEndpoint
import io.vertx.mqtt.MqttWill
import io.vertx.mqtt.messages.MqttPublishMessage
import java.io.Serializable
import java.time.Instant
import java.util.*

class BrokerMessage(
    val messageUuid: String = Utils.getUuid(),
    val messageId: Int,
    val topicName: String,
    val payload: ByteArray,
    val qosLevel: Int,
    val isRetain: Boolean,
    val isDup: Boolean,
    val isQueued: Boolean,
    val clientId: String,
    val senderId: String? = null,  // Optional sender identification for loop prevention
    val time: Instant = Instant.now(),
    // MQTT 5.0 properties
    val messageExpiryInterval: Long? = null,
    val payloadFormatIndicator: Int? = null,  // 0=unspecified, 1=UTF-8
    val contentType: String? = null,
    val responseTopic: String? = null,
    val correlationData: ByteArray? = null,
    val userProperties: Map<String, String>? = null
): Serializable {
    
    init {
        // MQTT v5.0 Phase 8: Validate UTF-8 payload if Payload Format Indicator = 1
        if (payloadFormatIndicator == 1) {
            try {
                // Attempt to decode as UTF-8 - will throw if invalid
                val decodedString = String(payload, Charsets.UTF_8)
                // Re-encode and compare to detect invalid UTF-8 sequences
                val reencoded = decodedString.toByteArray(Charsets.UTF_8)
                if (!payload.contentEquals(reencoded)) {
                    Utils.getLogger(this::class.java).warning(
                        "Client [$clientId] Payload Format Indicator=1 (UTF-8) but payload contains invalid UTF-8 sequences"
                    )
                }
            } catch (e: Exception) {
                Utils.getLogger(this::class.java).warning(
                    "Client [$clientId] Payload Format Indicator=1 (UTF-8) but payload is not valid UTF-8: ${e.message}"
                )
            }
        }
    }
    
    companion object {
        fun getPayloadFromBase64(s: String): ByteArray = Base64.getDecoder().decode(s)
        
        // Helper function to extract MQTT v5 properties from MqttPublishMessage
        private fun extractProperties(message: MqttPublishMessage): MqttV5Properties {
            val props = message.properties()
            return MqttV5Properties(
                messageExpiryInterval = props?.getProperty(2)?.let {
                    when (val value = it.value()) {
                        is Int -> value.toLong()
                        is Long -> value
                        else -> null
                    }
                },
                payloadFormatIndicator = props?.getProperty(1)?.value() as? Int,
                contentType = props?.getProperty(3)?.value() as? String,
                responseTopic = props?.getProperty(8)?.value() as? String,
                correlationData = props?.getProperty(9)?.value() as? ByteArray,
                userProperties = run {
                    val userProps = mutableMapOf<String, String>()
                    props?.listAll()?.forEach { p ->
                        if (p.propertyId() == 38) {
                            val value = p.value()
                            when (value) {
                                is io.netty.handler.codec.mqtt.MqttProperties.StringPair -> {
                                    userProps[value.key] = value.value
                                }
                                is java.util.ArrayList<*> -> {
                                    // User Properties can be returned as ArrayList of StringPairs
                                    value.forEach { item ->
                                        if (item is io.netty.handler.codec.mqtt.MqttProperties.StringPair) {
                                            userProps[item.key] = item.value
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (userProps.isEmpty()) null else userProps.toMap()
                }
            )
        }
    }
    
    // Helper data class to hold extracted MQTT v5 properties
    private data class MqttV5Properties(
        val messageExpiryInterval: Long?,
        val payloadFormatIndicator: Int?,
        val contentType: String?,
        val responseTopic: String?,
        val correlationData: ByteArray?,
        val userProperties: Map<String, String>?
    )
    
    constructor(clientId: String, message: MqttPublishMessage): this(
        Utils.getUuid(),
        if (message.messageId()<0) 0 else message.messageId(),
        message.topicName(),
        message.payload().bytes,
        message.qosLevel().value(),
        message.isRetain,
        message.isDup,
        false,
        clientId,
        senderId = clientId,  // MQTT v5: Track sender for noLocal filtering
        time = Instant.now(),
        // Parse MQTT v5.0 properties if present
        messageExpiryInterval = extractProperties(message).messageExpiryInterval,
        payloadFormatIndicator = extractProperties(message).payloadFormatIndicator,
        contentType = extractProperties(message).contentType,
        responseTopic = extractProperties(message).responseTopic,
        correlationData = extractProperties(message).correlationData,
        userProperties = extractProperties(message).userProperties
    )

    // Constructor with explicit topic name override (for Topic Alias resolution)
    constructor(clientId: String, message: MqttPublishMessage, topicNameOverride: String): this(
        Utils.getUuid(),
        if (message.messageId()<0) 0 else message.messageId(),
        topicNameOverride,  // Use override instead of message.topicName()
        message.payload().bytes,
        message.qosLevel().value(),
        message.isRetain,
        message.isDup,
        false,
        clientId,
        senderId = clientId,  // MQTT v5: Track sender for noLocal filtering
        time = Instant.now(),
        // Parse MQTT v5.0 properties if present
        messageExpiryInterval = extractProperties(message).messageExpiryInterval,
        payloadFormatIndicator = extractProperties(message).payloadFormatIndicator,
        contentType = extractProperties(message).contentType,
        responseTopic = extractProperties(message).responseTopic,
        correlationData = extractProperties(message).correlationData,
        userProperties = extractProperties(message).userProperties
    )

    constructor(clientId: String, message: MqttWill): this(
        Utils.getUuid(),
        0,
        message.willTopic,
        message.willMessageBytes,
        message.willQos,
        message.isWillRetain,
        false,
        false,
        clientId
    )

    constructor(clientId: String, topic: String, payload: String) : this(
        Utils.getUuid(),
        0,
        topic,
        payload.toByteArray(),
        0,
        false,
        false,
        false,
        clientId
    )

    fun cloneWithNewQoS(qosLevel: Int): BrokerMessage = BrokerMessage(
        messageUuid, messageId, topicName, payload, qosLevel, isRetain, isDup, isQueued, clientId, senderId, time,
        messageExpiryInterval, payloadFormatIndicator, contentType, responseTopic, correlationData, userProperties
    )
    
    fun cloneWithNewMessageId(messageId: Int): BrokerMessage = BrokerMessage(
        messageUuid, messageId, topicName, payload, qosLevel, isRetain, isDup, isQueued, clientId, senderId, time,
        messageExpiryInterval, payloadFormatIndicator, contentType, responseTopic, correlationData, userProperties
    )

    private fun getPayloadAsBuffer(): Buffer = Buffer.buffer(payload)

    fun getPayloadAsJson(): String? {
        return try {
            val jsonString = String(payload, Charsets.UTF_8)
            Json.decodeValue(jsonString) // Check if it is a valid JSON
            jsonString
        } catch (e: Exception) {
            null
        }
    }

    fun getPayloadAsJsonValue(): Any? {
        return try {
            val jsonString = String(payload, Charsets.UTF_8)
            Json.decodeValue(jsonString)
        } catch (e: Exception) {
            null
        }
    }

    fun getPayloadAsBase64(): String = Base64.getEncoder().encodeToString(payload)

    private fun getQoS(): MqttQoS = MqttQoS.valueOf(qosLevel)

    fun publishToEndpoint(endpoint: MqttEndpoint, qos: MqttQoS=getQoS()): Future<Int> {
        // For MQTT v5.0 clients, include properties
        if (endpoint.protocolVersion() == 5) {
            val properties = io.netty.handler.codec.mqtt.MqttProperties()
            
            // Add Message Expiry Interval if present
            // Per MQTT v5 spec: Update expiry interval to reflect time remaining
            messageExpiryInterval?.let { originalInterval ->
                val ageSeconds = (System.currentTimeMillis() - time.toEpochMilli()) / 1000
                val remainingSeconds = (originalInterval - ageSeconds).coerceAtLeast(0)
                if (remainingSeconds > 0) {
                    properties.add(io.netty.handler.codec.mqtt.MqttProperties.IntegerProperty(
                        2, // Property ID for Message Expiry Interval
                        remainingSeconds.toInt()
                    ))
                }
                // If remainingSeconds is 0 or negative, don't include property (message expired)
            }
            
            // Add Payload Format Indicator if present
            payloadFormatIndicator?.let {
                properties.add(io.netty.handler.codec.mqtt.MqttProperties.IntegerProperty(
                    1, // Property ID for Payload Format Indicator
                    it
                ))
            }
            
            // Add Content Type if present
            contentType?.let {
                properties.add(io.netty.handler.codec.mqtt.MqttProperties.StringProperty(
                    3, // Property ID for Content Type
                    it
                ))
            }
            
            // Add Response Topic if present
            responseTopic?.let {
                properties.add(io.netty.handler.codec.mqtt.MqttProperties.StringProperty(
                    8, // Property ID for Response Topic
                    it
                ))
            }
            
            // Add Correlation Data if present
            correlationData?.let {
                properties.add(io.netty.handler.codec.mqtt.MqttProperties.BinaryProperty(
                    9, // Property ID for Correlation Data
                    it
                ))
            }
            
            // Add User Properties if present (Property ID 38)
            userProperties?.forEach { (key, value) ->
                properties.add(io.netty.handler.codec.mqtt.MqttProperties.UserProperty(key, value))
            }
            
            return endpoint.publish(topicName, getPayloadAsBuffer(), qos, isDup, isRetain, messageId, properties)
        } else {
            // MQTT v3.1.1 clients - no properties
            return endpoint.publish(topicName, getPayloadAsBuffer(), qos, isDup, isRetain, messageId)
        }
    }
}