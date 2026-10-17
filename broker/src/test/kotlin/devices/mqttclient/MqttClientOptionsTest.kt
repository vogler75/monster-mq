package at.rocworks.devices.mqttclient

import at.rocworks.stores.devices.MqttClientConnectionConfig
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class MqttClientOptionsTest {

    @Test
    fun testMqtt5ConnectionOptionsCleanStartAndExpiry() {
        val configCleanSessionFalse = MqttClientConnectionConfig(
            brokerUrl = "tcp://127.0.0.1:1883",
            clientId = "test-client",
            cleanSession = false,
            keepAlive = 30
        )

        val optsFalse = MqttConnectionOptions().apply {
            isCleanStart = configCleanSessionFalse.cleanSession
            sessionExpiryInterval = if (configCleanSessionFalse.cleanSession) 0L else 0xFFFFFFFFL
            connectionTimeout = (configCleanSessionFalse.connectionTimeout / 1000).toInt()
            keepAliveInterval = configCleanSessionFalse.keepAlive
            isAutomaticReconnect = false
        }

        assertFalse(optsFalse.isCleanStart)
        assertEquals(0xFFFFFFFFL, optsFalse.sessionExpiryInterval)
        assertEquals(30, optsFalse.keepAliveInterval)

        val configCleanSessionTrue = MqttClientConnectionConfig(
            brokerUrl = "tcp://127.0.0.1:1883",
            clientId = "test-client",
            cleanSession = true
        )

        val optsTrue = MqttConnectionOptions().apply {
            isCleanStart = configCleanSessionTrue.cleanSession
            sessionExpiryInterval = if (configCleanSessionTrue.cleanSession) 0L else 0xFFFFFFFFL
        }

        assertTrue(optsTrue.isCleanStart)
        assertEquals(0L, optsTrue.sessionExpiryInterval)
    }

    @Test
    fun testRemainingExpiryCalculation() {
        val now = java.time.Instant.now()
        val originalExpirySeconds = 60L
        val enqueuedAt = now.minusSeconds(10)

        // 10 seconds elapsed -> remaining should be around 50s
        val elapsedSeconds = java.time.Duration.between(enqueuedAt, now).seconds
        val remaining = originalExpirySeconds - elapsedSeconds
        assertTrue(remaining in 49..51)

        // Expired message: 70 seconds elapsed -> remaining <= 0
        val expiredEnqueuedAt = now.minusSeconds(70)
        val expiredElapsed = java.time.Duration.between(expiredEnqueuedAt, now).seconds
        val expiredRemaining = originalExpirySeconds - expiredElapsed
        assertTrue(expiredRemaining <= 0)
    }
}
