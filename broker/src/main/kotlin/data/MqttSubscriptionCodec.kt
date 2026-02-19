package at.rocworks.data

import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec

class MqttSubscriptionCodec : MessageCodec<MqttSubscription, MqttSubscription> {
    override fun encodeToWire(buffer: Buffer, s: MqttSubscription) {
        val clientId = s.clientId.toByteArray(Charsets.UTF_8)
        buffer.appendInt(clientId.size)
        buffer.appendBytes(clientId)
        val topicName = s.topicName.toByteArray(Charsets.UTF_8)
        buffer.appendInt(topicName.size)
        buffer.appendBytes(topicName)
        buffer.appendInt(s.qos.value())
        buffer.appendByte(if (s.noLocal) 1 else 0)  // noLocal flag
        buffer.appendInt(s.retainHandling)  // retainHandling value (0, 1, or 2)
        buffer.appendByte(if (s.retainAsPublished) 1 else 0)  // retainAsPublished flag
    }

    override fun decodeFromWire(pos: Int, buffer: Buffer): MqttSubscription {
        var position = pos
        val clientIdLen = buffer.getInt(position)
        position += 4
        val clientId = buffer.getString(position, position + clientIdLen)
        position += clientIdLen
        val topicNameLen = buffer.getInt(position)
        position += 4
        val topicName = buffer.getString(position, position + topicNameLen)
        position += topicNameLen
        val qos = MqttQoS.valueOf(buffer.getInt(position))
        position += 4
        val noLocal = buffer.getByte(position) == 1.toByte()  // noLocal flag
        position += 1
        val retainHandling = buffer.getInt(position)  // retainHandling value
        position += 4
        val retainAsPublished = buffer.getByte(position) == 1.toByte()  // retainAsPublished flag
        return MqttSubscription(clientId, topicName, qos, noLocal, retainHandling, retainAsPublished)
    }

    override fun transform(s: MqttSubscription): MqttSubscription {
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