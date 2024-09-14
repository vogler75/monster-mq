package at.rocworks.data

import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec
import java.time.Instant

class MqttMessageCodec : MessageCodec<MqttMessage, MqttMessage> {

    override fun encodeToWire(buffer: Buffer, s: MqttMessage) {
        fun addString(text: String) {
            val str = text.toByteArray(Charsets.UTF_8)
            buffer.appendInt(str.size)
            buffer.appendBytes(str)
        }

        addString(s.messageUuid)
        buffer.appendInt(s.messageId)
        buffer.appendByte(s.qosLevel.toByte())
        buffer.appendByte(if (s.isDup) 1 else 0.toByte())
        buffer.appendByte(if (s.isRetain) 1 else 0.toByte())
        addString(s.topicName)
        addString(s.clientId)
        buffer.appendLong(s.time.toEpochMilli())
        buffer.appendBuffer(Buffer.buffer(s.payload))
    }

    override fun decodeFromWire(pos: Int, buffer: Buffer): MqttMessage {
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
        val qos = buffer.getByte(position).toInt()
        position += 1
        val isDup = (buffer.getByte(position)) == 1.toByte()
        position += 1
        val isRetain = (buffer.getByte(position)) == 1.toByte()
        position += 1
        val topicName = readString()
        val clientId = readString()
        val time = buffer.getLong(position)
        position += 8
        val payload = buffer.slice(position, buffer.length())

        return MqttMessage(
            messageUuid,
            messageId,
            topicName,
            payload.bytes,
            qos,
            isRetain,
            isDup,
            clientId,
            Instant.ofEpochMilli(time)
        )
    }

    override fun transform(s: MqttMessage): MqttMessage {
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