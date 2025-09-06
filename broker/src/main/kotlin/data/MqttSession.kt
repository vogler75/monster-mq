package at.rocworks.data

import java.io.Serializable
import java.time.Instant

data class MqttSession(
    val clientId: String,
    var cleanSession: Boolean,  // MQTT 3.1.1 compatibility
    var cleanStart: Boolean = false,  // MQTT 5.0 Clean Start flag
    var sessionExpiryInterval: Long? = null,  // MQTT 5.0 Session Expiry Interval in seconds
    var sessionExpiryTime: Instant? = null,  // Calculated expiry time
    var connected: Boolean? = null,
    var lastWill: MqttMessage? = null,
    var protocolVersion: Int = 4,  // 4 = MQTT 3.1.1, 5 = MQTT 5.0
    var connectProperties: MqttProperties? = null  // MQTT 5.0 connection properties
): Serializable {
    
    /**
     * Calculate and update the session expiry time based on the expiry interval
     */
    fun updateSessionExpiryTime() {
        sessionExpiryTime = when {
            sessionExpiryInterval == null -> null  // No expiry
            sessionExpiryInterval == 0L -> Instant.now()  // Expire immediately on disconnect
            sessionExpiryInterval == 0xFFFFFFFFL -> null  // Never expire (max value)
            else -> Instant.now().plusSeconds(sessionExpiryInterval!!)
        }
    }
    
    /**
     * Check if the session has expired
     */
    fun isExpired(): Boolean {
        return sessionExpiryTime?.let { Instant.now().isAfter(it) } ?: false
    }
    
    /**
     * For MQTT 5, clean start is used instead of clean session
     * This method provides compatibility between versions
     */
    fun shouldCleanOnConnect(): Boolean {
        return if (protocolVersion == 5) cleanStart else cleanSession
    }
}
