package at.rocworks

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.vertx.core.buffer.Buffer
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.util.logging.LogManager

object Utils {
    fun toByteBuf(buffer: Buffer): ByteBuf = Unpooled.wrappedBuffer(buffer.bytes)
    fun toByteBuf(bytes: ByteArray): ByteBuf = Unpooled.wrappedBuffer(bytes)

    fun initLogging() {
        try {
            println("Loading logging.properties...")
            val initialFile = File("logging.properties")
            val targetStream: InputStream = FileInputStream(initialFile)
            LogManager.getLogManager().readConfiguration(targetStream)
        } catch (e: Exception) {
            try {
                println("Using default logging.properties...")
                val stream = this::class.java.classLoader.getResourceAsStream("logging.properties")
                LogManager.getLogManager().readConfiguration(stream)
            } catch (e: Exception) {
                println("Unable to read default logging.properties!")
            }
        }
    }
}