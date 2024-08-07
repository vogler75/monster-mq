package at.rocworks.codecs

import at.rocworks.Const
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec
import io.vertx.mqtt.messages.MqttPublishMessage
import io.vertx.mqtt.messages.impl.MqttPublishMessageImpl

class MqttPublishMessageCodec : MessageCodec<MqttPublishMessageShareable, MqttPublishMessageShareable> {
    override fun encodeToWire(buffer: Buffer, s: MqttPublishMessageShareable) {
        // Serialize MqttPublishMessage fields to the buffer
        buffer.appendInt(s.messageId)
        buffer.appendByte(s.qosLevel.toByte())
        buffer.appendByte(if (s.isDup) 1 else 0)
        buffer.appendByte(if (s.isRetain) 1 else 0)
        val topicName = s.topicName.toByteArray(Charsets.UTF_8)
        buffer.appendInt(topicName.size)
        buffer.appendBytes(topicName)
        buffer.appendBuffer(Buffer.buffer(s.payload))
    }

    override fun decodeFromWire(pos: Int, buffer: Buffer): MqttPublishMessageShareable {
        var position = pos
        val messageId = buffer.getInt(position)
        position += 4
        val qos = buffer.getByte(position).toInt()
        position += 1
        val isDup = buffer.getByte(position) == 1.toByte()
        position += 1
        val isRetain = buffer.getByte(position) == 1.toByte()
        position += 1
        val topicNameLen = buffer.getInt(position)
        position += 4
        val topicName = buffer.getString(position, position + topicNameLen)
        position += topicNameLen
        val payload = buffer.slice(position, buffer.length())

        return MqttPublishMessage.create(
            messageId,
            MqttQoS.valueOf(qos),
            isDup,
            isRetain,
            topicName,
            Const.toByteBuf(payload)
        ) as MqttPublishMessageShareable
    }

    override fun transform(s: MqttPublishMessageShareable): MqttPublishMessageShareable {
        // Return the original message (no transformation needed)
        return s
    }

    override fun name(): String {
        return "MqttPublishMessageCodec"
    }

    override fun systemCodecID(): Byte {
        return -1 // User codec
    }
}