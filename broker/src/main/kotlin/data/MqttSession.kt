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
    var sessionExpiryInterval: Long = 0L
): Serializable
