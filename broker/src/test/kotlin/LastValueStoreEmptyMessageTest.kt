package at.rocworks

import at.rocworks.data.BrokerMessage
import at.rocworks.handlers.ArchiveGroup
import at.rocworks.handlers.MessageHandler
import at.rocworks.stores.MessageArchiveType
import at.rocworks.stores.MessageStoreMemory
import at.rocworks.stores.MessageStoreType
import io.vertx.core.json.JsonObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Test
import java.time.Instant

class LastValueStoreEmptyMessageTest {

    @Test
    fun testEmptyMessageDeletesEntryFromLastValueStore() {
        val retainedStore = MessageStoreMemory("Retained")
        val archiveGroup = ArchiveGroup(
            name = "Default",
            topicFilter = listOf("#"),
            retainedOnly = false,
            lastValType = MessageStoreType.MEMORY,
            archiveType = MessageArchiveType.NONE,
            databaseConfig = JsonObject()
        )

        // Inject memory lastValStore for testing
        val lastValStore = MessageStoreMemory("DefaultLastval")
        val field = ArchiveGroup::class.java.getDeclaredField("lastValStore")
        field.isAccessible = true
        field.set(archiveGroup, lastValStore)

        val messageHandler = MessageHandler(retainedStore, listOf(archiveGroup))
        messageHandler.registerArchiveGroup(archiveGroup)

        val topic = "sensor/humidity"
        val msg1 = BrokerMessage(
            messageId = 1,
            topicName = topic,
            payload = "65%".toByteArray(),
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = "test-client",
            time = Instant.now()
        )

        messageHandler.saveMessage(msg1)
        assertNotNull("Expected topic to be present in lastValStore", lastValStore.get(topic))
        assertEquals("65%", String(lastValStore.get(topic)!!.payload))

        val emptyMsg = BrokerMessage(
            messageId = 2,
            topicName = topic,
            payload = ByteArray(0),
            qosLevel = 0,
            isRetain = false,
            isDup = false,
            isQueued = false,
            clientId = "test-client",
            time = Instant.now()
        )

        messageHandler.saveMessage(emptyMsg)
        assertNull("Expected topic to be removed from lastValStore on empty message payload", lastValStore.get(topic))
    }
}
