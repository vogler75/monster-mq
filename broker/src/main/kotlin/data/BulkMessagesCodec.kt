package at.rocworks.data

import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec

/**
 * Codec for BulkClientMessage - serializes multiple BrokerMessages for efficient local client delivery
 */
class BulkClientMessageCodec : MessageCodec<BulkClientMessage, BulkClientMessage> {

    private val brokerMessageCodec = BrokerMessageCodec()

    override fun encodeToWire(buffer: Buffer, s: BulkClientMessage) {
        // Write the number of messages
        buffer.appendInt(s.messages.size)

        // Write each message using BrokerMessageCodec
        s.messages.forEach { message ->
            val messageBuffer = Buffer.buffer()
            brokerMessageCodec.encodeToWire(messageBuffer, message)

            // Write the size of the encoded message
            buffer.appendInt(messageBuffer.length())
            // Write the encoded message
            buffer.appendBuffer(messageBuffer)
        }
    }

    override fun decodeFromWire(pos: Int, buffer: Buffer): BulkClientMessage {
        var position = pos

        // Read the number of messages
        val messageCount = buffer.getInt(position)
        position += 4

        val messages = mutableListOf<BrokerMessage>()

        // Read each message
        repeat(messageCount) {
            val messageSize = buffer.getInt(position)
            position += 4

            val message = brokerMessageCodec.decodeFromWire(position, buffer)
            messages.add(message)

            // Advance position by the message size
            position += messageSize
        }

        return BulkClientMessage(messages)
    }

    override fun transform(s: BulkClientMessage): BulkClientMessage {
        return s
    }

    override fun name(): String {
        return this.javaClass.simpleName
    }

    override fun systemCodecID(): Byte {
        return -1 // User codec
    }
}

/**
 * Codec for BulkNodeMessage - serializes multiple BrokerMessages for efficient inter-node delivery
 */
class BulkNodeMessageCodec : MessageCodec<BulkNodeMessage, BulkNodeMessage> {

    private val brokerMessageCodec = BrokerMessageCodec()

    override fun encodeToWire(buffer: Buffer, s: BulkNodeMessage) {
        // Write the number of messages
        buffer.appendInt(s.messages.size)

        // Write each message using BrokerMessageCodec
        s.messages.forEach { message ->
            val messageBuffer = Buffer.buffer()
            brokerMessageCodec.encodeToWire(messageBuffer, message)

            // Write the size of the encoded message
            buffer.appendInt(messageBuffer.length())
            // Write the encoded message
            buffer.appendBuffer(messageBuffer)
        }
    }

    override fun decodeFromWire(pos: Int, buffer: Buffer): BulkNodeMessage {
        var position = pos

        // Read the number of messages
        val messageCount = buffer.getInt(position)
        position += 4

        val messages = mutableListOf<BrokerMessage>()

        // Read each message
        repeat(messageCount) {
            val messageSize = buffer.getInt(position)
            position += 4

            val message = brokerMessageCodec.decodeFromWire(position, buffer)
            messages.add(message)

            // Advance position by the message size
            position += messageSize
        }

        return BulkNodeMessage(messages)
    }

    override fun transform(s: BulkNodeMessage): BulkNodeMessage {
        return s
    }

    override fun name(): String {
        return this.javaClass.simpleName
    }

    override fun systemCodecID(): Byte {
        return -1 // User codec
    }
}
