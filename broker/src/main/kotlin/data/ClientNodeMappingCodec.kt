package at.rocworks.data

import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec

class ClientNodeMappingCodec : MessageCodec<ClientNodeMapping, ClientNodeMapping> {

    override fun encodeToWire(buffer: Buffer, mapping: ClientNodeMapping) {
        val json = mapping.toJson()
        val jsonBytes = json.encode().toByteArray(Charsets.UTF_8)
        buffer.appendInt(jsonBytes.size)
        buffer.appendBytes(jsonBytes)
    }

    override fun decodeFromWire(pos: Int, buffer: Buffer): ClientNodeMapping {
        val length = buffer.getInt(pos)
        val jsonBytes = buffer.getBytes(pos + 4, pos + 4 + length)
        val jsonStr = String(jsonBytes, Charsets.UTF_8)
        return ClientNodeMapping.fromJson(io.vertx.core.json.JsonObject(jsonStr))
    }

    override fun transform(mapping: ClientNodeMapping): ClientNodeMapping = mapping

    override fun name(): String = this.javaClass.simpleName

    override fun systemCodecID(): Byte = -1 // User codec
}