package at.rocworks.data

import io.vertx.core.json.JsonObject
import java.io.Serializable

/**
 * MQTT 5 Properties
 * Based on MQTT 5.0 specification section 2.2.2
 */
data class MqttProperties(
    // Payload Format Indicator (0x01) - byte
    var payloadFormatIndicator: Byte? = null,
    
    // Message Expiry Interval (0x02) - four byte integer
    var messageExpiryInterval: Long? = null,
    
    // Content Type (0x03) - UTF-8 string
    var contentType: String? = null,
    
    // Response Topic (0x08) - UTF-8 string
    var responseTopic: String? = null,
    
    // Correlation Data (0x09) - binary data
    var correlationData: ByteArray? = null,
    
    // Subscription Identifier (0x0B) - variable byte integer
    var subscriptionIdentifier: Int? = null,
    
    // Session Expiry Interval (0x11) - four byte integer
    var sessionExpiryInterval: Long? = null,
    
    // Assigned Client Identifier (0x12) - UTF-8 string
    var assignedClientIdentifier: String? = null,
    
    // Server Keep Alive (0x13) - two byte integer
    var serverKeepAlive: Int? = null,
    
    // Authentication Method (0x15) - UTF-8 string
    var authenticationMethod: String? = null,
    
    // Authentication Data (0x16) - binary data
    var authenticationData: ByteArray? = null,
    
    // Request Problem Information (0x17) - byte
    var requestProblemInformation: Byte? = null,
    
    // Will Delay Interval (0x18) - four byte integer
    var willDelayInterval: Long? = null,
    
    // Request Response Information (0x19) - byte
    var requestResponseInformation: Byte? = null,
    
    // Response Information (0x1A) - UTF-8 string
    var responseInformation: String? = null,
    
    // Server Reference (0x1C) - UTF-8 string
    var serverReference: String? = null,
    
    // Reason String (0x1F) - UTF-8 string
    var reasonString: String? = null,
    
    // Receive Maximum (0x21) - two byte integer
    var receiveMaximum: Int? = null,
    
    // Topic Alias Maximum (0x22) - two byte integer
    var topicAliasMaximum: Int? = null,
    
    // Topic Alias (0x23) - two byte integer
    var topicAlias: Int? = null,
    
    // Maximum QoS (0x24) - byte
    var maximumQoS: Byte? = null,
    
    // Retain Available (0x25) - byte
    var retainAvailable: Byte? = null,
    
    // User Properties (0x26) - UTF-8 string pairs
    var userProperties: MutableMap<String, String>? = null,
    
    // Maximum Packet Size (0x27) - four byte integer
    var maximumPacketSize: Long? = null,
    
    // Wildcard Subscription Available (0x28) - byte
    var wildcardSubscriptionAvailable: Byte? = null,
    
    // Subscription Identifier Available (0x29) - byte
    var subscriptionIdentifierAvailable: Byte? = null,
    
    // Shared Subscription Available (0x2A) - byte
    var sharedSubscriptionAvailable: Byte? = null
): Serializable {
    
    fun addUserProperty(key: String, value: String) {
        if (userProperties == null) {
            userProperties = mutableMapOf()
        }
        userProperties!![key] = value
    }
    
    fun toJson(): JsonObject {
        val json = JsonObject()
        
        payloadFormatIndicator?.let { json.put("payloadFormatIndicator", it.toInt()) }
        messageExpiryInterval?.let { json.put("messageExpiryInterval", it) }
        contentType?.let { json.put("contentType", it) }
        responseTopic?.let { json.put("responseTopic", it) }
        correlationData?.let { json.put("correlationData", java.util.Base64.getEncoder().encodeToString(it)) }
        subscriptionIdentifier?.let { json.put("subscriptionIdentifier", it) }
        sessionExpiryInterval?.let { json.put("sessionExpiryInterval", it) }
        assignedClientIdentifier?.let { json.put("assignedClientIdentifier", it) }
        serverKeepAlive?.let { json.put("serverKeepAlive", it) }
        authenticationMethod?.let { json.put("authenticationMethod", it) }
        authenticationData?.let { json.put("authenticationData", java.util.Base64.getEncoder().encodeToString(it)) }
        requestProblemInformation?.let { json.put("requestProblemInformation", it.toInt()) }
        willDelayInterval?.let { json.put("willDelayInterval", it) }
        requestResponseInformation?.let { json.put("requestResponseInformation", it.toInt()) }
        responseInformation?.let { json.put("responseInformation", it) }
        serverReference?.let { json.put("serverReference", it) }
        reasonString?.let { json.put("reasonString", it) }
        receiveMaximum?.let { json.put("receiveMaximum", it) }
        topicAliasMaximum?.let { json.put("topicAliasMaximum", it) }
        topicAlias?.let { json.put("topicAlias", it) }
        maximumQoS?.let { json.put("maximumQoS", it.toInt()) }
        retainAvailable?.let { json.put("retainAvailable", it.toInt()) }
        userProperties?.let { 
            val userPropsJson = JsonObject()
            it.forEach { (k, v) -> userPropsJson.put(k, v) }
            json.put("userProperties", userPropsJson)
        }
        maximumPacketSize?.let { json.put("maximumPacketSize", it) }
        wildcardSubscriptionAvailable?.let { json.put("wildcardSubscriptionAvailable", it.toInt()) }
        subscriptionIdentifierAvailable?.let { json.put("subscriptionIdentifierAvailable", it.toInt()) }
        sharedSubscriptionAvailable?.let { json.put("sharedSubscriptionAvailable", it.toInt()) }
        
        return json
    }
    
    companion object {
        fun fromJson(json: JsonObject): MqttProperties {
            val props = MqttProperties()
            
            json.getInteger("payloadFormatIndicator")?.let { props.payloadFormatIndicator = it.toByte() }
            json.getLong("messageExpiryInterval")?.let { props.messageExpiryInterval = it }
            json.getString("contentType")?.let { props.contentType = it }
            json.getString("responseTopic")?.let { props.responseTopic = it }
            json.getString("correlationData")?.let { props.correlationData = it.toByteArray() }
            json.getInteger("subscriptionIdentifier")?.let { props.subscriptionIdentifier = it }
            json.getLong("sessionExpiryInterval")?.let { props.sessionExpiryInterval = it }
            json.getString("assignedClientIdentifier")?.let { props.assignedClientIdentifier = it }
            json.getInteger("serverKeepAlive")?.let { props.serverKeepAlive = it }
            json.getString("authenticationMethod")?.let { props.authenticationMethod = it }
            json.getString("authenticationData")?.let { props.authenticationData = it.toByteArray() }
            json.getInteger("requestProblemInformation")?.let { props.requestProblemInformation = it.toByte() }
            json.getLong("willDelayInterval")?.let { props.willDelayInterval = it }
            json.getInteger("requestResponseInformation")?.let { props.requestResponseInformation = it.toByte() }
            json.getString("responseInformation")?.let { props.responseInformation = it }
            json.getString("serverReference")?.let { props.serverReference = it }
            json.getString("reasonString")?.let { props.reasonString = it }
            json.getInteger("receiveMaximum")?.let { props.receiveMaximum = it }
            json.getInteger("topicAliasMaximum")?.let { props.topicAliasMaximum = it }
            json.getInteger("topicAlias")?.let { props.topicAlias = it }
            json.getInteger("maximumQoS")?.let { props.maximumQoS = it.toByte() }
            json.getInteger("retainAvailable")?.let { props.retainAvailable = it.toByte() }
            json.getJsonObject("userProperties")?.let { 
                props.userProperties = it.map.mapValues { entry -> entry.value.toString() }.toMutableMap()
            }
            json.getLong("maximumPacketSize")?.let { props.maximumPacketSize = it }
            json.getInteger("wildcardSubscriptionAvailable")?.let { props.wildcardSubscriptionAvailable = it.toByte() }
            json.getInteger("subscriptionIdentifierAvailable")?.let { props.subscriptionIdentifierAvailable = it.toByte() }
            json.getInteger("sharedSubscriptionAvailable")?.let { props.sharedSubscriptionAvailable = it.toByte() }
            
            return props
        }
    }
}

/**
 * MQTT 5 Reason Codes
 */
enum class MqttReasonCode(val value: Int) {
    // Success codes (0x00)
    SUCCESS(0x00),
    NORMAL_DISCONNECTION(0x00),
    GRANTED_QOS_0(0x00),
    
    // QoS granted codes
    GRANTED_QOS_1(0x01),
    GRANTED_QOS_2(0x02),
    
    // Disconnect with Will Message
    DISCONNECT_WITH_WILL_MESSAGE(0x04),
    
    // No matching subscribers
    NO_MATCHING_SUBSCRIBERS(0x10),
    
    // No subscription existed
    NO_SUBSCRIPTION_EXISTED(0x11),
    
    // Continue authentication
    CONTINUE_AUTHENTICATION(0x18),
    
    // Re-authenticate
    RE_AUTHENTICATE(0x19),
    
    // Error codes (0x80+)
    UNSPECIFIED_ERROR(0x80),
    MALFORMED_PACKET(0x81),
    PROTOCOL_ERROR(0x82),
    IMPLEMENTATION_SPECIFIC_ERROR(0x83),
    UNSUPPORTED_PROTOCOL_VERSION(0x84),
    CLIENT_IDENTIFIER_NOT_VALID(0x85),
    BAD_USER_NAME_OR_PASSWORD(0x86),
    NOT_AUTHORIZED(0x87),
    SERVER_UNAVAILABLE(0x88),
    SERVER_BUSY(0x89),
    BANNED(0x8A),
    SERVER_SHUTTING_DOWN(0x8B),
    BAD_AUTHENTICATION_METHOD(0x8C),
    KEEP_ALIVE_TIMEOUT(0x8D),
    SESSION_TAKEN_OVER(0x8E),
    TOPIC_FILTER_INVALID(0x8F),
    TOPIC_NAME_INVALID(0x90),
    PACKET_IDENTIFIER_IN_USE(0x91),
    PACKET_IDENTIFIER_NOT_FOUND(0x92),
    RECEIVE_MAXIMUM_EXCEEDED(0x93),
    TOPIC_ALIAS_INVALID(0x94),
    PACKET_TOO_LARGE(0x95),
    MESSAGE_RATE_TOO_HIGH(0x96),
    QUOTA_EXCEEDED(0x97),
    ADMINISTRATIVE_ACTION(0x98),
    PAYLOAD_FORMAT_INVALID(0x99),
    RETAIN_NOT_SUPPORTED(0x9A),
    QOS_NOT_SUPPORTED(0x9B),
    USE_ANOTHER_SERVER(0x9C),
    SERVER_MOVED(0x9D),
    SHARED_SUBSCRIPTIONS_NOT_SUPPORTED(0x9E),
    CONNECTION_RATE_EXCEEDED(0x9F),
    MAXIMUM_CONNECT_TIME(0xA0),
    SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED(0xA1),
    WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED(0xA2);
    
    companion object {
        fun fromValue(value: Int): MqttReasonCode? = values().find { it.value == value }
    }
}

/**
 * MQTT 5 Subscription Options
 */
data class MqttSubscriptionOptions(
    val qos: Int = 0,
    val noLocal: Boolean = false,
    val retainAsPublished: Boolean = false,
    val retainHandling: RetainHandling = RetainHandling.SEND_ON_SUBSCRIBE
): Serializable {
    
    enum class RetainHandling(val value: Int) {
        SEND_ON_SUBSCRIBE(0),           // Send retained messages at subscribe time
        SEND_ON_SUBSCRIBE_IF_NEW(1),    // Send retained messages only if subscription doesn't exist
        DO_NOT_SEND(2);                  // Do not send retained messages
        
        companion object {
            fun fromValue(value: Int): RetainHandling = values().find { it.value == value } ?: SEND_ON_SUBSCRIBE
        }
    }
    
    fun toByte(): Byte {
        var optionsByte = qos and 0x03
        if (noLocal) optionsByte = optionsByte or 0x04
        if (retainAsPublished) optionsByte = optionsByte or 0x08
        optionsByte = optionsByte or ((retainHandling.value and 0x03) shl 4)
        return optionsByte.toByte()
    }
    
    companion object {
        fun fromByte(byte: Byte): MqttSubscriptionOptions {
            val b = byte.toInt()
            return MqttSubscriptionOptions(
                qos = b and 0x03,
                noLocal = (b and 0x04) != 0,
                retainAsPublished = (b and 0x08) != 0,
                retainHandling = RetainHandling.fromValue((b shr 4) and 0x03)
            )
        }
    }
}