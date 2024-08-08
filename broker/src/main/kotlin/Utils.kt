package at.rocworks

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.vertx.core.buffer.Buffer

object Utils {
    fun toByteBuf(buffer: Buffer): ByteBuf = Unpooled.wrappedBuffer(buffer.bytes)
    fun toByteBuf(bytes: ByteArray): ByteBuf = Unpooled.wrappedBuffer(bytes)
}