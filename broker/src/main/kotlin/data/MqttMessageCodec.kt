package at.rocworks.data

import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec

class MqttMessageCodec : MessageCodec<MqttMessage, MqttMessage> {
    override fun encodeToWire(buffer: Buffer, s: MqttMessage) {
        // Serialize MqttPublishMessage fields to the buffer
        buffer.appendInt(s.messageId)
        buffer.appendByte(s.qosLevel.toByte())
        buffer.appendByte(if (s.isDup) 1 else 0.toByte())
        buffer.appendByte(if (s.isRetain) 1 else 0.toByte())
        val topicName = s.topicName.toByteArray(Charsets.UTF_8)
        buffer.appendInt(topicName.size)
        buffer.appendBytes(topicName)
        buffer.appendBuffer(Buffer.buffer(s.payload))
    }

    override fun decodeFromWire(pos: Int, buffer: Buffer): MqttMessage {
        var position = pos
        val messageId = buffer.getInt(position)
        position += 4
        val qos = buffer.getByte(position).toInt()
        position += 1
        val isDup = (buffer.getByte(position)) == 1.toByte()
        position += 1
        val isRetain = (buffer.getByte(position)) == 1.toByte()
        position += 1
        val topicNameLen = buffer.getInt(position)
        position += 4
        val topicName = buffer.getString(position, position + topicNameLen)
        position += topicNameLen
        val payload = buffer.slice(position, buffer.length())

        return MqttMessage(
            messageId,
            topicName,
            payload.bytes,
            qos,
            isRetain,
            isDup,
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