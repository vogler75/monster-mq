package at.rocworks

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.vertx.core.buffer.Buffer

object Const {
    private const val DIST_NAMESPACE = "D"
    private const val TOPIC_NAMESPACE = "T"
    private const val CLIENT_NAMESPACE = "C"

    const val TOPIC_KEY = "Topic"
    const val CLIENT_KEY = "Client"

    fun getTopicBusAddr(deploymentID: String) = "$TOPIC_NAMESPACE" // /${deploymentID}"
    fun getDistBusAddr(deploymentID: String) = "$DIST_NAMESPACE/${deploymentID}"
    fun getClientBusAddr(deploymentID: String) = "$CLIENT_NAMESPACE/${deploymentID}"

    fun isWildCardTopic(topicName: String): Boolean = topicName.contains('+') || topicName.contains('#')

    fun bufferToByteBuf(buffer: Buffer): ByteBuf = Unpooled.wrappedBuffer(buffer.bytes)
}