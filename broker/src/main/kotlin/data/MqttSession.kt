package at.rocworks.data

import java.io.Serializable
import java.util.concurrent.atomic.AtomicLong

data class MqttSession(
    val clientId: String,
    var cleanSession: Boolean,
    var connected: Boolean? = null,
    var lastWill: BrokerMessage? = null,
    val messagesIn: AtomicLong = AtomicLong(0),
    val messagesOut: AtomicLong = AtomicLong(0),
    var nodeId: String? = null,
    var clientAddress: String? = null,
    // MQTT 5.0 properties
    var sessionExpiryInterval: Long = 0L,  // seconds, 0 = session ends on disconnect
    var receiveMaximum: Int = 65535,  // max outstanding QoS 1/2 messages
    var maximumPacketSize: Long = 268435456L,  // max packet size in bytes
    var topicAliasMaximum: Int = 0,  // 0 = not supported
    var willDelayInterval: Long = 0L  // seconds to delay will message
): Serializable
