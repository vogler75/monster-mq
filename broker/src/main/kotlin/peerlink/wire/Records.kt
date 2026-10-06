package at.rocworks.peerlink.wire

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets

// Record constants (record.go: lines 30-53)
const val RecordVersion: Byte = 1
const val RecordHeaderLen: Int = 44
const val TombstoneLen: Int = RecordHeaderLen
const val TLVHeaderLen: Int = 5
const val MaxStringLen: Int = 65535

const val offRecLen: Int = 0
const val offVersion: Int = 4
const val offHdrLen: Int = 5
const val offFlags: Int = 6
const val offWallNs: Int = 8
const val offMonoMs: Int = 16
const val offExpiry: Int = 24
const val offPayloadFormat: Int = 28
const val offTopicLen: Int = 30
const val offClientIDLen: Int = 32
const val offUsernameLen: Int = 34
const val offPropsLen: Int = 36
const val offPayloadLen: Int = 40

// Record flags (record.go: lines 56-65)
const val FlagQoSMask: Int = 0x0003
const val FlagRetain: Int = 1 shl 2
const val FlagDup: Int = 1 shl 3
const val FlagWill: Int = 1 shl 4
const val FlagInline: Int = 1 shl 5
const val FlagPayloadFormat: Int = 1 shl 7
const val FlagSnapshot: Int = 1 shl 8
const val FlagSkipped: Int = 1 shl 9

// Property TLV ids (record.go: lines 68-73)
const val PropContentType: Byte = 0x03
const val PropResponseTopic: Byte = 0x08
const val PropCorrelationData: Byte = 0x09
const val PropUserProperty: Byte = 0x26

// Malformed errors (record.go: lines 77-106)
val errRecordTruncated = MalformedRecordException("recLen beyond buffer")
val errHeaderShort = MalformedRecordException("header shorter than 44 bytes")
val errVersion = MalformedRecordException("unknown recVersion")
val errHdrLen = MalformedRecordException("hdrLen out of range")
val errInvariant = MalformedRecordException("recLen invariant broken")
val errQoS = MalformedRecordException("qos > 2")
val errTopic = MalformedRecordException("invalid topic")
val errClientID = MalformedRecordException("invalid client id")
val errUsername = MalformedRecordException("invalid username")
val errTLV = MalformedRecordException("truncated property TLV")
val errPropString = MalformedRecordException("invalid property string")
val errPropLen = MalformedRecordException("property value too long")
val errUserProp = MalformedRecordException("malformed user property")
val errMonoAhead = MalformedRecordException("captureMonoMs after batch sourceMonoMs")

class RecordOverrunException : WireException("peerlink/wire: record overruns the batch records region")
class BatchCountException : WireException("peerlink/wire: record count does not match the batch count")

data class UserProp(val key: String, val value: String)

// Record input representation (record.go: lines 110-124)
data class Record(
    var flags: Int = 0,
    var publishWallNs: Long = 0L,
    var captureMonoMs: Long = 0L,
    var expirySec: Long = 0L,
    var payloadFormat: Byte = 0,
    var topic: String = "",
    var clientID: String = "",
    var username: ByteArray = ByteArray(0),
    var contentType: String = "",
    var responseTopic: String = "",
    var correlationData: ByteArray = ByteArray(0),
    var user: List<UserProp> = emptyList(),
    var payload: ByteArray = ByteArray(0)
) {
    fun validContent(): Boolean {
        if (!validTopic(topic) || !validString(clientID) || !validStringBytes(username)) return false
        if (!validString(contentType) || !validString(responseTopic) || correlationData.size > MaxStringLen) return false
        for (u in user) {
            if (!validString(u.key) || !validString(u.value)) return false
        }
        return true
    }
}

private fun propsSize(r: Record): Pair<Long, Boolean> {
    var n = 0L
    if (r.contentType.isNotEmpty()) {
        val l = r.contentType.toByteArray(Charsets.UTF_8).size
        if (l > MaxStringLen) return Pair(0L, false)
        n += TLVHeaderLen + l
    }
    if (r.responseTopic.isNotEmpty()) {
        val l = r.responseTopic.toByteArray(Charsets.UTF_8).size
        if (l > MaxStringLen) return Pair(0L, false)
        n += TLVHeaderLen + l
    }
    if (r.correlationData.isNotEmpty()) {
        val l = r.correlationData.size
        if (l > MaxStringLen) return Pair(0L, false)
        n += TLVHeaderLen + l
    }
    for (u in r.user) {
        val k = u.key.toByteArray(Charsets.UTF_8).size
        val v = u.value.toByteArray(Charsets.UTF_8).size
        if (k > MaxStringLen || v > MaxStringLen) return Pair(0L, false)
        n += TLVHeaderLen + 2 + k + v
    }
    return Pair(n, n <= 0xFFFFFFFFL)
}

// RecordSize (record.go: lines 208-222)
fun recordSize(r: Record): Int {
    val topicBytes = r.topic.toByteArray(Charsets.UTF_8)
    val clientIDBytes = r.clientID.toByteArray(Charsets.UTF_8)
    if (topicBytes.size > MaxStringLen || clientIDBytes.size > MaxStringLen || r.username.size > MaxStringLen) {
        return 0
    }
    val (props, ok) = propsSize(r)
    if (!ok) return 0
    val total = RecordHeaderLen.toLong() + topicBytes.size + clientIDBytes.size + r.username.size + props + r.payload.size
    if (total - 4 > 0xFFFFFFFFL || total > Int.MAX_VALUE) {
        return 0
    }
    return total.toInt()
}

// EncodeRecord (record.go: lines 226-266)
fun encodeRecord(dst: ByteArray, r: Record, offset: Int = 0): Int {
    val (props, _) = propsSize(r)
    val topicBytes = r.topic.toByteArray(Charsets.UTF_8)
    val clientIDBytes = r.clientID.toByteArray(Charsets.UTF_8)
    val tl = topicBytes.size
    val cl = clientIDBytes.size
    val ul = r.username.size
    val pl = r.payload.size
    val size = RecordHeaderLen + tl + cl + ul + props.toInt() + pl

    val bb = ByteBuffer.wrap(dst, offset, size).order(ByteOrder.LITTLE_ENDIAN)
    bb.putInt((size - 4))
    bb.put(RecordVersion)
    bb.put(RecordHeaderLen.toByte())
    bb.putShort(r.flags.toShort())
    bb.putLong(r.publishWallNs)
    bb.putLong(r.captureMonoMs)
    bb.putInt((r.expirySec and 0xFFFFFFFFL).toInt())
    bb.put(r.payloadFormat)
    bb.put(0.toByte()) // reserved
    bb.putShort(tl.toShort())
    bb.putShort(cl.toShort())
    bb.putShort(ul.toShort())
    bb.putInt(props.toInt())
    bb.putInt(pl)

    var o = offset + RecordHeaderLen
    System.arraycopy(topicBytes, 0, dst, o, tl)
    o += tl
    System.arraycopy(clientIDBytes, 0, dst, o, cl)
    o += cl
    System.arraycopy(r.username, 0, dst, o, ul)
    o += ul

    if (props > 0) {
        if (r.contentType.isNotEmpty()) {
            val cb = r.contentType.toByteArray(Charsets.UTF_8)
            o = putTLV(dst, o, PropContentType, cb)
        }
        if (r.responseTopic.isNotEmpty()) {
            val rb = r.responseTopic.toByteArray(Charsets.UTF_8)
            o = putTLV(dst, o, PropResponseTopic, rb)
        }
        if (r.correlationData.isNotEmpty()) {
            o = putTLV(dst, o, PropCorrelationData, r.correlationData)
        }
        for (u in r.user) {
            val kb = u.key.toByteArray(Charsets.UTF_8)
            val vb = u.value.toByteArray(Charsets.UTF_8)
            dst[o] = PropUserProperty
            val le = ByteBuffer.wrap(dst, o + 1, 6).order(ByteOrder.LITTLE_ENDIAN)
            le.putInt(2 + kb.size + vb.size)
            le.putShort(kb.size.toShort())
            o += TLVHeaderLen + 2
            System.arraycopy(kb, 0, dst, o, kb.size)
            o += kb.size
            System.arraycopy(vb, 0, dst, o, vb.size)
            o += vb.size
        }
    }
    System.arraycopy(r.payload, 0, dst, o, pl)
    o += pl
    return o - offset
}

private fun putTLV(b: ByteArray, o: Int, id: Byte, v: ByteArray): Int {
    if (v.isEmpty()) return o
    b[o] = id
    val le = ByteBuffer.wrap(b, o + 1, 4).order(ByteOrder.LITTLE_ENDIAN)
    le.putInt(v.size)
    System.arraycopy(v, 0, b, o + TLVHeaderLen, v.size)
    return o + TLVHeaderLen + v.size
}

// AppendRecord (record.go: lines 279-288)
fun appendRecord(dst: ByteArray, r: Record): ByteArray {
    val n = recordSize(r)
    if (n == 0) return dst
    val res = ByteArray(dst.size + n)
    System.arraycopy(dst, 0, res, 0, dst.size)
    encodeRecord(res, r, dst.size)
    return res
}

// RecordFrameLen (record.go: lines 299-310)
fun recordFrameLen(b: ByteArray, offset: Int = 0): Int {
    if (b.size - offset < 4) return -1
    val recLen = (b[offset].toLong() and 0xFFL) or
            ((b[offset + 1].toLong() and 0xFFL) shl 8) or
            ((b[offset + 2].toLong() and 0xFFL) shl 16) or
            ((b[offset + 3].toLong() and 0xFFL) shl 24)
    val total = 4L + recLen
    if (total > Int.MAX_VALUE) return -1
    return total.toInt()
}

// PutTombstone (record.go: lines 315-328)
fun putTombstone(dst: ByteArray, orig: ByteArray, dstOffset: Int = 0, origOffset: Int = 0) {
    java.util.Arrays.fill(dst, dstOffset, dstOffset + TombstoneLen, 0.toByte())
    val bb = ByteBuffer.wrap(dst, dstOffset, TombstoneLen).order(ByteOrder.LITTLE_ENDIAN)
    bb.putInt(TombstoneLen - 4)
    bb.put(RecordVersion)
    bb.put(RecordHeaderLen.toByte())

    var flags = FlagSkipped
    if (orig.size - origOffset >= RecordHeaderLen) {
        val origBB = ByteBuffer.wrap(orig, origOffset + offFlags, 2).order(ByteOrder.LITTLE_ENDIAN)
        flags = flags or (origBB.getShort().toInt() and 0xFFFF)
        System.arraycopy(orig, origOffset + offWallNs, dst, dstOffset + offWallNs, offPayloadFormat + 1 - offWallNs)
    }
    val flagsBB = ByteBuffer.wrap(dst, dstOffset + offFlags, 2).order(ByteOrder.LITTLE_ENDIAN)
    flagsBB.putShort(flags.toShort())
}

// AppendTombstone (record.go: lines 330-336)
fun appendTombstone(dst: ByteArray, orig: ByteArray, origOffset: Int = 0): ByteArray {
    val res = ByteArray(dst.size + TombstoneLen)
    System.arraycopy(dst, 0, res, 0, dst.size)
    putTombstone(res, orig, dst.size, origOffset)
    return res
}

// RecordView (record.go: lines 339-365)
class RecordView {
    var frame: ByteArray = ByteArray(0)
    var version: Byte = 0
    var hdrLen: Int = 0
    var flags: Int = 0
    var publishWallNs: Long = 0L
    var captureMonoMs: Long = 0L
    var expirySec: Long = 0L
    var payloadFormat: Byte = 0
    var topic: ByteArray = ByteArray(0)
    var clientID: ByteArray = ByteArray(0)
    var username: ByteArray = ByteArray(0)
    var props: ByteArray = ByteArray(0)
    var payload: ByteArray = ByteArray(0)
    var unknownProps: Int = 0

    fun qos(): Byte = (flags and FlagQoSMask).toByte()
    fun retain(): Boolean = (flags and FlagRetain) != 0
    fun dup(): Boolean = (flags and FlagDup) != 0
    fun will(): Boolean = (flags and FlagWill) != 0
    fun inline(): Boolean = (flags and FlagInline) != 0
    fun snapshot(): Boolean = (flags and FlagSnapshot) != 0
    fun skipped(): Boolean = (flags and FlagSkipped) != 0
    fun hasPayloadFormat(): Boolean = (flags and FlagPayloadFormat) != 0

    fun topicString(): String = String(topic, Charsets.UTF_8)
    fun clientIDString(): String = String(clientID, Charsets.UTF_8)
    fun usernameString(): String = String(username, Charsets.UTF_8)

    fun propIter(): PropIter = PropIter(props)

    fun toRecord(): Record {
        val r = Record(
            flags = flags,
            publishWallNs = publishWallNs,
            captureMonoMs = captureMonoMs,
            expirySec = expirySec,
            payloadFormat = payloadFormat,
            topic = topicString(),
            clientID = clientIDString(),
            username = username.clone(),
            payload = payload.clone()
        )
        val userList = mutableListOf<UserProp>()
        val it = propIter()
        while (it.hasNext()) {
            val item = it.next() ?: break
            when (item.id) {
                PropContentType -> r.contentType = String(item.value, Charsets.UTF_8)
                PropResponseTopic -> r.responseTopic = String(item.value, Charsets.UTF_8)
                PropCorrelationData -> r.correlationData = item.value.clone()
                PropUserProperty -> {
                    val pair = splitUserProperty(item.value)
                    if (pair != null) {
                        userList.add(UserProp(String(pair.first, Charsets.UTF_8), String(pair.second, Charsets.UTF_8)))
                    }
                }
            }
        }
        r.user = userList
        return r
    }
}

// DecodeRecord (record.go: lines 373-441)
fun decodeRecord(frame: ByteArray, v: RecordView, offset: Int = 0, length: Int = frame.size - offset) {
    if (length < 4) throw errRecordTruncated
    val recLen = (frame[offset].toLong() and 0xFFL) or
            ((frame[offset + 1].toLong() and 0xFFL) shl 8) or
            ((frame[offset + 2].toLong() and 0xFFL) shl 16) or
            ((frame[offset + 3].toLong() and 0xFFL) shl 24)
    val n = 4L + recLen
    if (n > length.toLong()) throw errRecordTruncated

    val nInt = n.toInt()
    val b = ByteArray(nInt)
    System.arraycopy(frame, offset, b, 0, nInt)
    v.frame = b

    if (nInt < RecordHeaderLen) throw errHeaderShort

    val bb = ByteBuffer.wrap(b, 0, RecordHeaderLen).order(ByteOrder.LITTLE_ENDIAN)
    bb.position(offVersion)
    v.version = bb.get()
    v.hdrLen = bb.get().toInt() and 0xFF
    v.flags = bb.getShort().toInt() and 0xFFFF
    v.publishWallNs = bb.getLong()
    v.captureMonoMs = bb.getLong()
    v.expirySec = bb.getInt().toLong() and 0xFFFFFFFFL
    v.payloadFormat = bb.get()
    bb.get() // reserved byte

    val tl = bb.getShort().toLong() and 0xFFFFL
    val cl = bb.getShort().toLong() and 0xFFFFL
    val ul = bb.getShort().toLong() and 0xFFFFL
    val pr = bb.getInt().toLong() and 0xFFFFFFFFL
    val pl = bb.getInt().toLong() and 0xFFFFFFFFL

    if (v.version != RecordVersion) throw errVersion
    val hdr = v.hdrLen.toLong()
    if (hdr < RecordHeaderLen.toLong()) throw errHdrLen
    if (hdr + tl + cl + ul + pr + pl != n) {
        if (hdr > n) throw errHdrLen
        throw errInvariant
    }

    var o = hdr.toInt()
    v.topic = ByteArray(tl.toInt()).also { System.arraycopy(b, o, it, 0, tl.toInt()) }
    o += tl.toInt()
    v.clientID = ByteArray(cl.toInt()).also { System.arraycopy(b, o, it, 0, cl.toInt()) }
    o += cl.toInt()
    v.username = ByteArray(ul.toInt()).also { System.arraycopy(b, o, it, 0, ul.toInt()) }
    o += ul.toInt()
    v.props = ByteArray(pr.toInt()).also { System.arraycopy(b, o, it, 0, pr.toInt()) }
    o += pr.toInt()
    v.payload = ByteArray(pl.toInt()).also { System.arraycopy(b, o, it, 0, pl.toInt()) }

    if ((v.flags and FlagQoSMask) == 3) throw errQoS
    if ((v.flags and FlagSkipped) != 0) return

    if (!validTopicBytes(v.topic)) throw errTopic
    if (!validStringBytes(v.clientID)) throw errClientID
    if (!validStringBytes(v.username)) throw errUsername

    val unknown = validateProps(v.props)
    v.unknownProps = unknown
}

// ValidateProps (record.go: lines 446-481)
fun validateProps(block: ByteArray): Int {
    var unknown = 0
    val it = PropIter(block)
    while (it.hasNext()) {
        val item = it.next() ?: throw errTLV
        when (item.id) {
            PropContentType, PropResponseTopic -> {
                if (item.value.size > MaxStringLen) throw errPropLen
                if (!validStringBytes(item.value)) throw errPropString
            }
            PropCorrelationData -> {
                if (item.value.size > MaxStringLen) throw errPropLen
            }
            PropUserProperty -> {
                val pair = splitUserProperty(item.value) ?: throw errUserProp
                if (pair.second.size > MaxStringLen) throw errPropLen
                if (!validStringBytes(pair.first) || !validStringBytes(pair.second)) throw errPropString
            }
            else -> unknown++
        }
    }
    return unknown
}

data class PropItem(val id: Byte, val value: ByteArray)

// PropIter (record.go: lines 484-505)
class PropIter(val b: ByteArray, var offset: Int = 0) {
    fun hasNext(): Boolean = offset < b.size

    fun next(): PropItem? {
        if (b.size - offset < TLVHeaderLen) {
            offset = b.size
            return null
        }
        val id = b[offset]
        val l = (b[offset + 1].toLong() and 0xFFL) or
                ((b[offset + 2].toLong() and 0xFFL) shl 8) or
                ((b[offset + 3].toLong() and 0xFFL) shl 16) or
                ((b[offset + 4].toLong() and 0xFFL) shl 24)
        val end = TLVHeaderLen.toLong() + l
        if (end > (b.size - offset).toLong()) {
            offset = b.size
            return null
        }
        val value = ByteArray(l.toInt())
        System.arraycopy(b, offset + TLVHeaderLen, value, 0, l.toInt())
        offset += (TLVHeaderLen + l).toInt()
        return PropItem(id, value)
    }
}

// SplitUserProperty (record.go: lines 508-517)
fun splitUserProperty(value: ByteArray): Pair<ByteArray, ByteArray>? {
    if (value.size < 2) return null
    val kLen = (value[0].toInt() and 0xFF) or ((value[1].toInt() and 0xFF) shl 8)
    if (2 + kLen > value.size) return null
    val key = ByteArray(kLen)
    System.arraycopy(value, 2, key, 0, kLen)
    val vLen = value.size - 2 - kLen
    val v = ByteArray(vLen)
    System.arraycopy(value, 2 + kLen, v, 0, vLen)
    return Pair(key, v)
}

// RecordIter (record.go: lines 579-641)
class RecordIter(val region: ByteArray, val count: Int) {
    private var offset = 0
    private var remainingCount = count
    private var maxMonoMs = 0L
    private var checkMono = false
    private var fault: Throwable? = null

    fun checkMono(sourceMonoMs: Long) {
        maxMonoMs = sourceMonoMs
        checkMono = true
    }

    fun remaining(): Int = remainingCount

    // Returns: Pair(hasRecord: Boolean, malformedExceptionOrNull)
    // Throws RecordOverrunException or BatchCountException on structural faults
    fun next(v: RecordView): Pair<Boolean, MalformedRecordException?> {
        fault?.let { throw it }
        if (remainingCount == 0) {
            if (offset != region.size) {
                val e = BatchCountException()
                fault = e
                throw e
            }
            return Pair(false, null)
        }
        if (offset == region.size) {
            val e = BatchCountException()
            fault = e
            throw e
        }
        val n = recordFrameLen(region, offset)
        if (n < 0 || n > region.size - offset) {
            val e = RecordOverrunException()
            fault = e
            throw e
        }
        val frameOffset = offset
        offset += n
        remainingCount--

        var malformed: MalformedRecordException? = null
        try {
            decodeRecord(region, v, frameOffset, n)
            if (checkMono && v.captureMonoMs > maxMonoMs) {
                malformed = errMonoAhead
            }
        } catch (e: MalformedRecordException) {
            malformed = e
        }
        return Pair(true, malformed)
    }
}

// Content validators (record.go: lines 643-693)
fun validTopic(s: String): Boolean {
    val b = s.toByteArray(Charsets.UTF_8)
    return validTopicBytes(b)
}

fun validTopicBytes(b: ByteArray): Boolean {
    if (b.isEmpty() || b.size > MaxStringLen) return false
    for (byte in b) {
        val c = byte.toInt() and 0xFF
        if (c == 0 || c == '+'.code || c == '#'.code) return false
    }
    return isValidUTF8(b)
}

fun validString(s: String): Boolean {
    val b = s.toByteArray(Charsets.UTF_8)
    return validStringBytes(b)
}

fun validStringBytes(b: ByteArray): Boolean {
    if (b.size > MaxStringLen) return false
    for (byte in b) {
        if (byte.toInt() == 0) return false
    }
    return isValidUTF8(b)
}

private fun isValidUTF8(b: ByteArray): Boolean {
    if (b.isEmpty()) return true
    val decoder = StandardCharsets.UTF_8.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
    try {
        decoder.decode(ByteBuffer.wrap(b))
        return true
    } catch (_: Exception) {
        return false
    }
}
