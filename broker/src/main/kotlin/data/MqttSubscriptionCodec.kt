package at.rocworks.data

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
        return MqttSubscription(clientId, topicName)
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