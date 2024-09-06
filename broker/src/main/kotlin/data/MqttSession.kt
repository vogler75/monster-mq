package at.rocworks.data

import java.time.Instant

data class MqttSession(
    val clientId: String,
    val cleanSession: Boolean,
    val createDate: Instant
)
