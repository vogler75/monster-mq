package at.rocworks.codecs

import io.netty.buffer.Unpooled
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec
import io.vertx.mqtt.messages.MqttPublishMessage
import io.vertx.mqtt.messages.impl.MqttPublishMessageImpl

class MqttPublishMessageCodec : MessageCodec<MqttPublishMessageImpl, MqttPublishMessageImpl> {
    override fun encodeToWire(buffer: Buffer, s: MqttPublishMessageImpl) {
        // Serialize MqttPublishMessage fields to the buffer
        buffer.appendInt(s.messageId())
        buffer.appendByte(s.qosLevel().value().toByte())
        buffer.appendByte(if (s.isDup) 1 else 0)
        buffer.appendByte(if (s.isRetain) 1 else 0)
        buffer.appendString(s.topicName())
        buffer.appendBuffer(s.payload())
    }

    override fun decodeFromWire(pos: Int, buffer: Buffer): MqttPublishMessageImpl {
        var position = pos
        val messageId = buffer.getInt(position)
        position += 4
        val qos = buffer.getByte(position).toInt()
        position += 1
        val isDup = buffer.getByte(position) == 1.toByte()
        position += 1
        val isRetain = buffer.getByte(position) == 1.toByte()
        position += 1
        val topicName = buffer.getString(position, buffer.length())
        position += buffer.length() - position
        val payload = buffer.slice(position, buffer.length())

        return MqttPublishMessage.create(messageId, MqttQoS.valueOf(qos), isDup, isRetain, topicName, Unpooled.wrappedBuffer(payload.bytes)) as MqttPublishMessageImpl
    }

    override fun transform(s: MqttPublishMessageImpl): MqttPublishMessageImpl {
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