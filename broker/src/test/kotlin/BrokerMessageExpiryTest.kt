package at.rocworks

import at.rocworks.data.BrokerMessage
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.time.Instant

class BrokerMessageExpiryTest {
    @Test
    fun testNoExpiryIntervalNeverExpires() {
        val msg = BrokerMessage(
            messageUuid = "test-1",
            messageId = 1,
            topicName = "test/topic",
            payload = ByteArray(0),
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = "c1",
            time = Instant.now().minusSeconds(10000),
            messageExpiryInterval = null
        )
        assertFalse(msg.isExpired())
    }

    @Test
    fun testUnexpiredMessage() {
        val msg = BrokerMessage(
            messageUuid = "test-2",
            messageId = 2,
            topicName = "test/topic",
            payload = ByteArray(0),
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = "c1",
            time = Instant.now().minusSeconds(5),
            messageExpiryInterval = 60L // expires in 60s
        )
        assertFalse(msg.isExpired())
    }

    @Test
    fun testExpiredMessage() {
        val msg = BrokerMessage(
            messageUuid = "test-3",
            messageId = 3,
            topicName = "test/topic",
            payload = ByteArray(0),
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = "c1",
            time = Instant.now().minusSeconds(100),
            messageExpiryInterval = 30L // expired 70s ago
        )
        assertTrue(msg.isExpired())
    }
}
