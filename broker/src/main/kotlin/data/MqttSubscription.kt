package at.rocworks.data

import io.netty.handler.codec.mqtt.MqttQoS
import java.io.Serializable

data class MqttSubscription(
    val clientId: String,
    val topicName: String,
    val qos: MqttQoS,
    val options: MqttSubscriptionOptions? = null,  // MQTT 5.0 subscription options
    val subscriptionId: Int? = null  // MQTT 5.0 subscription identifier
): Serializable {
    
    /**
     * Constructor for backward compatibility with MQTT 3.1.1
     */
    constructor(clientId: String, topicName: String, qos: MqttQoS) : this(
        clientId, 
        topicName, 
        qos, 
        null, 
        null
    )
    
    /**
     * Get the effective QoS, considering MQTT 5 options
     */
    fun getEffectiveQoS(): MqttQoS {
        return if (options != null) {
            MqttQoS.valueOf(options.qos.coerceAtMost(qos.value()))
        } else {
            qos
        }
    }
    
    /**
     * Check if retained messages should be sent for this subscription
     */
    fun shouldSendRetained(): Boolean {
        return options?.retainHandling != MqttSubscriptionOptions.RetainHandling.DO_NOT_SEND
    }
    
    /**
     * Check if this is a new subscription (for retain handling)
     */
    fun shouldSendRetainedIfNew(isNew: Boolean): Boolean {
        return when (options?.retainHandling) {
            MqttSubscriptionOptions.RetainHandling.SEND_ON_SUBSCRIBE -> true
            MqttSubscriptionOptions.RetainHandling.SEND_ON_SUBSCRIBE_IF_NEW -> isNew
            MqttSubscriptionOptions.RetainHandling.DO_NOT_SEND -> false
            null -> true  // Default MQTT 3.1.1 behavior
        }
    }
}
