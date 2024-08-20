package at.rocworks.codecs

import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec

class MqttTopicNameCodec : MessageCodec<MqttTopicName, MqttTopicName> {
    override fun encodeToWire(buffer: Buffer, s: MqttTopicName) {
        // Serialize MqttPublishMessage fields to the buffer
        val topicName = s.identifier.toByteArray(Charsets.UTF_8)
        buffer.appendInt(topicName.size)
        buffer.appendBytes(topicName)
    }

    override fun decodeFromWire(pos: Int, buffer: Buffer): MqttTopicName {
        var position = pos
        val topicNameLen = buffer.getInt(position)
        position += 4
        val topicName = buffer.getString(position, position + topicNameLen)

        return MqttTopicName(topicName)
    }

    override fun transform(s: MqttTopicName): MqttTopicName {
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