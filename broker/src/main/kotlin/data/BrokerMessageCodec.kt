package at.rocworks.data

import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.MessageCodec
import java.time.Instant

class BrokerMessageCodec : MessageCodec<BrokerMessage, BrokerMessage> {

    override fun encodeToWire(buffer: Buffer, s: BrokerMessage) {
        fun addString(text: String) {
            val str = text.toByteArray(Charsets.UTF_8)
            buffer.appendInt(str.size)
            buffer.appendBytes(str)
        }

        fun addNullableString(text: String?) {
            if (text == null) {
                buffer.appendInt(-1)
            } else {
                val str = text.toByteArray(Charsets.UTF_8)
                buffer.appendInt(str.size)
                buffer.appendBytes(str)
            }
        }

        addString(s.messageUuid)
        buffer.appendInt(s.messageId)

        buffer.appendByte((((s.qosLevel and 0x03) shl 3) or
                ((if (s.isDup) 1 else 0) shl 2)
                or ((if (s.isRetain) 1 else 0) shl 1)
                or (if (s.isQueued) 1 else 0)).toByte())
        addString(s.topicName)
        addString(s.clientId)
        buffer.appendLong(s.time.toEpochMilli())
        buffer.appendInt(s.payload.size)
        buffer.appendBytes(s.payload)
        addNullableString(s.originNodeId)

        // Versioned trailer (Magic 0x504C0001 = 'P', 'L', 0x00, 0x01)
        buffer.appendInt(0x504C0001)
        addNullableString(s.senderId)
        buffer.appendLong(s.messageExpiryInterval ?: -1L)
        buffer.appendInt(s.payloadFormatIndicator ?: -1)
        addNullableString(s.contentType)
        addNullableString(s.responseTopic)
        if (s.correlationData == null) {
            buffer.appendInt(-1)
        } else {
            buffer.appendInt(s.correlationData.size)
            buffer.appendBytes(s.correlationData)
        }
        if (s.userProperties == null) {
            buffer.appendInt(-1)
        } else {
            buffer.appendInt(s.userProperties.size)
            s.userProperties.forEach { (k, v) ->
                addString(k)
                addString(v)
            }
        }
        buffer.appendByte(if (s.isWill) 1 else 0)
        addNullableString(s.username)
        addNullableString(s.peerSource)
        if (s.peer == null) {
            buffer.appendByte(0)
        } else {
            buffer.appendByte(1)
            addString(s.peer.sourceNode)
            addString(s.peer.clientId)
            addNullableString(s.peer.username)
            buffer.appendLong(s.peer.timeNs)
            buffer.appendLong(s.peer.epoch)
            buffer.appendLong(s.peer.offset)
            val flags = ((if (s.peer.dup) 1 else 0) shl 2) or
                    ((if (s.peer.will) 1 else 0) shl 1) or
                    (if (s.peer.snapshot) 1 else 0)
            buffer.appendByte(flags.toByte())
        }
    }

    override fun decodeFromWire(pos: Int, buffer: Buffer): BrokerMessage {
        var position = pos

        fun readString(): String {
            val len = buffer.getInt(position)
            position += 4
            val str = buffer.getString(position, position + len)
            position += len
            return str
        }

        fun readNullableString(): String? {
            val len = buffer.getInt(position)
            position += 4
            if (len == -1) return null
            val str = buffer.getString(position, position + len)
            position += len
            return str
        }

        val messageUuid = readString()
        val messageId = buffer.getInt(position)
        position += 4

        val status = buffer.getByte(position)
        val qos = (status.toInt() shr 3) and 0x03
        val isDup = ((status.toInt() shr 2) and 0x01) == 1
        val isRetain = ((status.toInt() shr 1) and 0x01) == 1
        val isQueued = (status.toInt() and 0x01) == 1
        position += 1

        val topicName = readString()
        val clientId = readString()
        val time = buffer.getLong(position)
        position += 8
        val payloadLen = buffer.getInt(position)
        position += 4
        val payload = buffer.getBytes(position, position + payloadLen)
        position += payloadLen
        val originNodeId = readNullableString()

        // Check for versioned trailer
        var senderId: String? = null
        var messageExpiryInterval: Long? = null
        var payloadFormatIndicator: Int? = null
        var contentType: String? = null
        var responseTopic: String? = null
        var correlationData: ByteArray? = null
        var userProperties: Map<String, String>? = null
        var isWill = false
        var username: String? = null
        var peerSource: String? = null
        var peer: PeerForward? = null

        if (position + 4 <= buffer.length() && buffer.getInt(position) == 0x504C0001) {
            position += 4
            senderId = readNullableString()
            val expiry = buffer.getLong(position)
            position += 8
            if (expiry != -1L) messageExpiryInterval = expiry

            val pfi = buffer.getInt(position)
            position += 4
            if (pfi != -1) payloadFormatIndicator = pfi

            contentType = readNullableString()
            responseTopic = readNullableString()

            val cDataLen = buffer.getInt(position)
            position += 4
            if (cDataLen != -1) {
                correlationData = buffer.getBytes(position, position + cDataLen)
                position += cDataLen
            }

            val uPropsSize = buffer.getInt(position)
            position += 4
            if (uPropsSize != -1) {
                val map = mutableMapOf<String, String>()
                for (i in 0 until uPropsSize) {
                    val k = readString()
                    val v = readString()
                    map[k] = v
                }
                userProperties = map
            }

            isWill = buffer.getByte(position).toInt() == 1
            position += 1
            username = readNullableString()
            peerSource = readNullableString()

            val hasPeer = buffer.getByte(position).toInt() == 1
            position += 1
            if (hasPeer) {
                val pSourceNode = readString()
                val pClientId = readString()
                val pUsername = readNullableString()
                val pTimeNs = buffer.getLong(position)
                position += 8
                val pEpoch = buffer.getLong(position)
                position += 8
                val pOffset = buffer.getLong(position)
                position += 8
                val pFlags = buffer.getByte(position).toInt()
                position += 1
                peer = PeerForward(
                    sourceNode = pSourceNode,
                    clientId = pClientId,
                    username = pUsername,
                    timeNs = pTimeNs,
                    epoch = pEpoch,
                    offset = pOffset,
                    dup = (pFlags and 0x04) != 0,
                    will = (pFlags and 0x02) != 0,
                    snapshot = (pFlags and 0x01) != 0
                )
            }
        }

        return BrokerMessage(
            messageUuid = messageUuid,
            messageId = messageId,
            topicName = topicName,
            payload = payload,
            qosLevel = qos,
            isRetain = isRetain,
            isDup = isDup,
            isQueued = isQueued,
            clientId = clientId,
            senderId = senderId,
            time = Instant.ofEpochMilli(time),
            messageExpiryInterval = messageExpiryInterval,
            payloadFormatIndicator = payloadFormatIndicator,
            contentType = contentType,
            responseTopic = responseTopic,
            correlationData = correlationData,
            userProperties = userProperties,
            originNodeId = originNodeId,
            peer = peer,
            peerSource = peerSource,
            isWill = isWill,
            username = username
        )
    }

    override fun transform(s: BrokerMessage): BrokerMessage {
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