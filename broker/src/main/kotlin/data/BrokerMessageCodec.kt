package at.rocworks.data

import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec
import java.time.Instant

class BrokerMessageCodec : MessageCodec<BrokerMessage, BrokerMessage> {

    override fun encodeToWire(buffer: Buffer, s: BrokerMessage) {
        fun addString(text: String) {
            val str = text.toByteArray(Charsets.UTF_8)
            buffer.appendInt(str.size)
            buffer.appendBytes(str)
        }

        addString(s.messageUuid)
        buffer.appendInt(s.messageId)

        /*
        buffer.appendByte(s.qosLevel.toByte())
        buffer.appendByte(if (s.isDup) 1 else 0.toByte())
        buffer.appendByte(if (s.isRetain) 1 else 0.toByte())
        buffer.appendByte(if (s.isQueued) 1 else 0.toByte())
        */

        buffer.appendByte((((s.qosLevel and 0x03) shl 3) or
                ((if (s.isDup) 1 else 0) shl 2)
                or ((if (s.isRetain) 1 else 0) shl 1)
                or (if (s.isQueued) 1 else 0)).toByte())
        addString(s.topicName)
        addString(s.clientId)
        buffer.appendLong(s.time.toEpochMilli())
        buffer.appendBuffer(Buffer.buffer(s.payload))
    }

    override fun decodeFromWire(pos: Int, buffer: Buffer): BrokerMessage {
        var position = pos

        fun readString(): String {
            val len = buffer.getInt(position)
            position += 4
            val str = buffer.getString(position, position + len)
            position += len
            return str
        }

        val messageUuid = readString()
        val messageId = buffer.getInt(position)
        position += 4

        val status = buffer.getByte(position)
        val qos = (status.toInt() shr 3) and 0x03
        val isDup = ((status.toInt() shr 2) and 0x01) == 1
        val isRetain = ((status.toInt() shr 1) and 0x01) == 1
        val isQueued = (status.toInt() and 0x01) == 1
        position += 1

        /*
        val qos = buffer.getByte(position).toInt()
        position += 1
        val isDup = (buffer.getByte(position)) == 1.toByte()
        position += 1
        val isRetain = (buffer.getByte(position)) == 1.toByte()
        position += 1
        val isQueued = (buffer.getByte(position)) == 1.toByte()
        position += 1
        */
        val topicName = readString()
        val clientId = readString()
        val time = buffer.getLong(position)
        position += 8
        val payload = buffer.slice(position, buffer.length())

        return BrokerMessage(
            messageUuid,
            messageId,
            topicName,
            payload.bytes,
            qos,
            isRetain,
            isDup,
            isQueued,
            clientId,
            Instant.ofEpochMilli(time)
        )
    }

    override fun transform(s: BrokerMessage): BrokerMessage {
        // Return the original message (no transformation needed)
        return s
    }

    override fun name(): String {
        return this.javaClass.simpleName
    }

    override fun systemCodecID(): Byte {
        return -1 // User codec
    }
}