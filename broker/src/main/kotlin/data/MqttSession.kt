package at.rocworks.data

import java.io.Serializable

data class MqttSession(
    val clientId: String,
    var cleanSession: Boolean,
    var connected: Boolean? = null,
    var lastWill: MqttMessage? = null,
): Serializable
