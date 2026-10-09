package at.rocworks.peerlink.wire

import java.io.EOFException
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32C

// Protocol constants (frame.go: lines 22-50)
const val Magic = "MMQP"
const val VersionMajor: Int = 1
const val VersionMinor: Int = 0
const val ALPN = "mmq-peer/1"
const val PreambleLen = 8

const val FrameHeaderLen = 5
const val MaxPreAuthFrame = 4 shl 10
const val MaxConsumerFrame = 64 shl 10
const val FrameSlack = 64 shl 10
const val DefaultMaxFrameBytes = (16 shl 20) + FrameSlack

const val BatchHeaderLen = 68
const val BatchPrefixLen = FrameHeaderLen + BatchHeaderLen
const val BatchCRCCovered = BatchHeaderLen - 4

const val NonceLen = 32
const val MACLen = 32
const val MinRecordFrame = 4

// FrameType (frame.go: lines 53-100)
enum class FrameType(val code: Byte, val frameName: String) {
    ServerHello(0x01, "SERVER_HELLO"),
    Hello(0x02, "HELLO"),
    HelloOK(0x03, "HELLO_OK"),
    GoAway(0x04, "GOAWAY"),
    Fetch(0x10, "FETCH"),
    Batch(0x11, "BATCH"),
    Commit(0x12, "COMMIT"),
    Ping(0x13, "PING"),
    Pong(0x14, "PONG"),
    // C→S, only with the agreed CapInterest (plan-peerlink-interest-routing section 4)
    InterestSnapshot(0x20, "INTEREST_SNAPSHOT"),
    InterestDelta(0x21, "INTEREST_DELTA");

    override fun toString(): String = frameName

    companion object {
        fun fromCode(code: Byte): FrameType? = when (code) {
            0x01.toByte() -> ServerHello
            0x02.toByte() -> Hello
            0x03.toByte() -> HelloOK
            0x04.toByte() -> GoAway
            0x10.toByte() -> Fetch
            0x11.toByte() -> Batch
            0x12.toByte() -> Commit
            0x13.toByte() -> Ping
            0x14.toByte() -> Pong
            0x20.toByte() -> InterestSnapshot
            0x21.toByte() -> InterestDelta
            else -> null
        }
    }
}

// Capability bits (frame.go: lines 102-109)
const val CapBatchCRC: Long = 1L shl 0
const val CapSnapshotFill: Long = 1L shl 1
const val CapResyncNewer: Long = 1L shl 2
const val CapTombstone: Long = 1L shl 3
const val CapsV1: Long = CapBatchCRC or CapSnapshotFill or CapResyncNewer or CapTombstone
// Reserved for the redundancy role extension (plan-peerlink-redundancy).
const val CapRole: Long = 1L shl 4
// INTEREST_SNAPSHOT/INTEREST_DELTA and sparse batches; offered only with PeerLink.Interest.Enabled,
// usable only when HELLO_OK.capabilities (the final agreement) carries it.
const val CapInterest: Long = 1L shl 5

// SERVER_HELLO authModes bits (frame.go: lines 112-115)
const val AuthClientCertRequested: Byte = (1 shl 0).toByte()
const val AuthSharedSecret: Byte = (1 shl 1).toByte()

// HELLO flags (frame.go: line 118)
const val HelloFlagMAC: Int = 1 shl 0

// HELLO_OK flags (frame.go: lines 121-125)
const val HelloOKSourceReset: Int = 1 shl 0
const val HelloOKConsumerStateUsed: Int = 1 shl 1
const val HelloOKSnapshotAvailable: Int = 1 shl 2

// FETCH flags (frame.go: line 128)
const val FetchFlagSnapshot: Int = 1 shl 0

// BATCH flags (frame.go: lines 131-138)
const val BatchFlagGap: Int = 1 shl 0
const val BatchFlagEmpty: Int = 1 shl 1
const val BatchFlagCRC: Int = 1 shl 2
const val BatchFlagSnapshot: Int = 1 shl 3
const val BatchFlagSnapshotEnd: Int = 1 shl 4
const val BatchFlagTruncated: Int = 1 shl 5
// A u32 span and u32 deltas[count] follow the header (needs CapInterest).
const val BatchFlagSparse: Int = 1 shl 6

// Interest classes of INTEREST_SNAPSHOT/INTEREST_DELTA entries.
const val InterestNone: Int = 0 // delta only: the filter is withdrawn
const val InterestVol: Int = 1
const val InterestPer: Int = 2

// expirySec of a PER entry that never expires (u32 0xFFFFFFFF).
const val InterestExpiryNever: Long = 0xFFFFFFFFL

// INTEREST_SNAPSHOT flags.
const val InterestFlagFirst: Int = 1 shl 0
const val InterestFlagLast: Int = 1 shl 1

// Fixed body before the entries, and the fixed part of one entry.
const val InterestSnapshotHeaderLen = 9
const val InterestDeltaHeaderLen = 8
const val InterestEntryOverhead = 7

// RetainedClass (frame.go: lines 141-159)
enum class RetainedClass(val code: Byte, val stringName: String) {
    Memory(0, "MEMORY"),
    DB(1, "DB"),
    WinCCOA(2, "WINCCOA");

    override fun toString(): String = stringName

    companion object {
        fun fromCode(code: Byte): RetainedClass = when (code) {
            0.toByte() -> Memory
            1.toByte() -> DB
            2.toByte() -> WinCCOA
            else -> DB // default fallback
        }
    }
}

// GoAwayCode (frame.go: lines 162-221)
enum class GoAwayCode(val code: Short, val codeName: String, val isConfigError: Boolean) {
    Version(1, "version", true),
    UnknownPeer(2, "unknown_peer", true),
    NotAllowed(3, "not_allowed", true),
    AuthFailed(4, "auth_failed", true),
    IdentityMismatch(5, "identity_mismatch", true),
    SelfConnection(6, "self_connection", true),
    WrongNode(7, "wrong_node", true),
    Superseded(8, "superseded", false),
    Shutdown(9, "shutdown", false),
    Protocol(10, "protocol", false),
    OffsetOutOfRange(11, "offset_out_of_range", false),
    Busy(12, "busy", false),
    DuplicateNode(13, "duplicate_node", true);

    override fun toString(): String = codeName

    companion object {
        fun fromCode(code: Short): GoAwayCode = entries.firstOrNull { it.code == code } ?: Protocol
    }
}

// Framing errors (frame.go: lines 225-235)
open class WireException(message: String) : Exception(message)
class BadMagicException : WireException("peerlink/wire: bad preamble magic")
class FrameTooLargeException : WireException("peerlink/wire: frame exceeds the size cap")
class FrameEmptyException : WireException("peerlink/wire: frame without a type byte")
class ShortFrameException : WireException("peerlink/wire: frame body shorter than its fields")
class UnknownFrameException : WireException("peerlink/wire: unknown frame type")
class BatchRecordsException : WireException("peerlink/wire: recordsBytes exceeds the frame body")
class BatchCountRangeException : WireException("peerlink/wire: batch count exceeds what the records region can hold")
class BatchSparseException : WireException("peerlink/wire: invalid sparse batch span or deltas")
class InterestCountException : WireException("peerlink/wire: interest entry count does not match the frame body")
class MalformedRecordException(val what: String) : WireException("peerlink/wire: malformed record: $what")


// UTF-8 truncation helper (frame.go: lines 428-437)
fun truncUTF8(s: String, maxBytes: Int): String {
    val b = s.toByteArray(Charsets.UTF_8)
    if (b.size <= maxBytes) return s
    var i = maxBytes
    while (i > 0 && (b[i].toInt() and 0xC0) == 0x80) {
        i--
    }
    return String(b, 0, i, Charsets.UTF_8)
}

// WireBuffer helper for efficient encoding
class WireBuffer(initialCapacity: Int = 128) {
    var buf = ByteArray(initialCapacity)
    var length = 0

    fun ensure(n: Int) {
        if (buf.size - length < n) {
            var newCap = buf.size * 2
            if (newCap < length + n) newCap = length + n
            val nb = ByteArray(newCap)
            System.arraycopy(buf, 0, nb, 0, length)
            buf = nb
        }
    }

    fun putByte(v: Byte): WireBuffer {
        ensure(1)
        buf[length++] = v
        return this
    }

    fun putShortLE(v: Short): WireBuffer {
        ensure(2)
        buf[length++] = (v.toInt() and 0xFF).toByte()
        buf[length++] = ((v.toInt() shr 8) and 0xFF).toByte()
        return this
    }

    fun putIntLE(v: Int): WireBuffer {
        ensure(4)
        buf[length++] = (v and 0xFF).toByte()
        buf[length++] = ((v shr 8) and 0xFF).toByte()
        buf[length++] = ((v shr 16) and 0xFF).toByte()
        buf[length++] = ((v shr 24) and 0xFF).toByte()
        return this
    }

    fun putLongLE(v: Long): WireBuffer {
        ensure(8)
        buf[length++] = (v and 0xFF).toByte()
        buf[length++] = ((v shr 8) and 0xFF).toByte()
        buf[length++] = ((v shr 16) and 0xFF).toByte()
        buf[length++] = ((v shr 24) and 0xFF).toByte()
        buf[length++] = ((v shr 32) and 0xFF).toByte()
        buf[length++] = ((v shr 40) and 0xFF).toByte()
        buf[length++] = ((v shr 48) and 0xFF).toByte()
        buf[length++] = ((v shr 56) and 0xFF).toByte()
        return this
    }

    fun putBytes(src: ByteArray, offset: Int = 0, len: Int = src.size): WireBuffer {
        ensure(len)
        System.arraycopy(src, offset, buf, length, len)
        length += len
        return this
    }

    fun putStr8(s: String): WireBuffer {
        val truncated = truncUTF8(s, 0xFF)
        val bytes = truncated.toByteArray(Charsets.UTF_8)
        putByte(bytes.size.toByte())
        putBytes(bytes)
        return this
    }

    fun putStr16(s: String): WireBuffer {
        val truncated = truncUTF8(s, 0xFFFF)
        val bytes = truncated.toByteArray(Charsets.UTF_8)
        putShortLE(bytes.size.toShort())
        putBytes(bytes)
        return this
    }

    fun toByteArray(): ByteArray {
        val result = ByteArray(length)
        System.arraycopy(buf, 0, result, 0, length)
        return result
    }
}

// Decoder helper (frame.go: lines 454-518)
class WireDecoder(val b: ByteArray, var offset: Int = 0, val limit: Int = b.size) {
    var short = false

    fun remaining(): Int = if (short) 0 else limit - offset

    fun take(n: Int): ByteArray? {
        if (short || remaining() < n) {
            short = true
            return null
        }
        val res = ByteArray(n)
        System.arraycopy(b, offset, res, 0, n)
        offset += n
        return res
    }

    fun u8(): Byte {
        if (short || remaining() < 1) {
            short = true
            return 0
        }
        return b[offset++]
    }

    fun u16(): Short {
        if (short || remaining() < 2) {
            short = true
            return 0
        }
        val v0 = b[offset++].toInt() and 0xFF
        val v1 = b[offset++].toInt() and 0xFF
        return (v0 or (v1 shl 8)).toShort()
    }

    fun u32(): Int {
        if (short || remaining() < 4) {
            short = true
            return 0
        }
        val v0 = b[offset++].toInt() and 0xFF
        val v1 = b[offset++].toInt() and 0xFF
        val v2 = b[offset++].toInt() and 0xFF
        val v3 = b[offset++].toInt() and 0xFF
        return v0 or (v1 shl 8) or (v2 shl 16) or (v3 shl 24)
    }

    fun u64(): Long {
        if (short || remaining() < 8) {
            short = true
            return 0L
        }
        var res = 0L
        for (i in 0 until 8) {
            res = res or ((b[offset++].toLong() and 0xFFL) shl (i * 8))
        }
        return res
    }

    fun arr32(dst: ByteArray) {
        if (short || remaining() < 32) {
            short = true
            return
        }
        System.arraycopy(b, offset, dst, 0, 32)
        offset += 32
    }

    fun str8(): String {
        val n = u8().toInt() and 0xFF
        val bytes = take(n) ?: return ""
        return String(bytes, Charsets.UTF_8)
    }

    fun str16(): String {
        val n = u16().toInt() and 0xFFFF
        val bytes = take(n) ?: return ""
        return String(bytes, Charsets.UTF_8)
    }

    fun checkErr() {
        if (short) throw ShortFrameException()
    }
}

// Preamble functions (frame.go: lines 243-279)
fun appendPreamble(dst: ByteArray = ByteArray(0)): ByteArray {
    return appendPreambleVersion(dst, VersionMajor, VersionMinor)
}

fun appendPreambleVersion(dst: ByteArray, major: Int, minor: Int): ByteArray {
    val wb = WireBuffer(dst.size + PreambleLen)
    wb.putBytes(dst)
    wb.putBytes(Magic.toByteArray(Charsets.ISO_8859_1))
    wb.putShortLE(major.toShort())
    wb.putShortLE(minor.toShort())
    return wb.toByteArray()
}

fun writePreamble(out: OutputStream) {
    out.write(appendPreamble())
}

fun parsePreamble(b: ByteArray): Pair<Int, Int> {
    if (b.size < PreambleLen) throw EOFException("unexpected EOF parsing preamble")
    val magicStr = String(b, 0, 4, Charsets.ISO_8859_1)
    if (magicStr != Magic) throw BadMagicException()
    val major = (b[4].toInt() and 0xFF) or ((b[5].toInt() and 0xFF) shl 8)
    val minor = (b[6].toInt() and 0xFF) or ((b[7].toInt() and 0xFF) shl 8)
    return Pair(major, minor)
}

fun readPreamble(input: InputStream): Pair<Int, Int> {
    val b = ByteArray(PreambleLen)
    var read = 0
    while (read < PreambleLen) {
        val n = input.read(b, read, PreambleLen - read)
        if (n < 0) throw EOFException("unexpected EOF reading preamble")
        read += n
    }
    return parsePreamble(b)
}

// Frame interface & decoding (frame.go: lines 282-326)
interface Frame {
    fun type(): FrameType
    fun appendFrame(wb: WireBuffer)
    fun encode(): ByteArray {
        val wb = WireBuffer()
        appendFrame(wb)
        return wb.toByteArray()
    }
    fun decode(body: ByteArray)
}

fun decodeFrame(t: FrameType, body: ByteArray): Frame {
    val f: Frame = when (t) {
        FrameType.ServerHello -> ServerHello()
        FrameType.Hello -> Hello()
        FrameType.HelloOK -> HelloOK()
        FrameType.GoAway -> GoAway()
        FrameType.Fetch -> Fetch()
        FrameType.Batch -> Batch()
        FrameType.Commit -> Commit()
        FrameType.Ping -> Ping()
        FrameType.Pong -> Pong()
        FrameType.InterestSnapshot -> InterestSnapshot()
        FrameType.InterestDelta -> InterestDelta()
    }
    f.decode(body)
    return f
}

fun writeFrame(out: OutputStream, f: Frame) {
    out.write(f.encode())
}

private fun beginFrame(wb: WireBuffer, t: FrameType, bodyHint: Int): Int {
    wb.ensure(FrameHeaderLen + bodyHint)
    val start = wb.length
    wb.putIntLE(0) // placeholder for frameLen
    wb.putByte(t.code)
    return start
}

private fun endFrame(wb: WireBuffer, start: Int) {
    val frameLen = wb.length - start - 4
    val b = wb.buf
    b[start] = (frameLen and 0xFF).toByte()
    b[start + 1] = ((frameLen shr 8) and 0xFF).toByte()
    b[start + 2] = ((frameLen shr 16) and 0xFF).toByte()
    b[start + 3] = ((frameLen shr 24) and 0xFF).toByte()
}

// ServerHello (frame.go: lines 520-550)
data class ServerHello(
    var versionMajor: Int = VersionMajor,
    var versionMinor: Int = VersionMinor,
    var capabilities: Long = CapsV1,
    var authModes: Byte = 0,
    var nonceS: ByteArray = ByteArray(NonceLen)
) : Frame {
    override fun type(): FrameType = FrameType.ServerHello

    override fun appendFrame(wb: WireBuffer) {
        val s = beginFrame(wb, FrameType.ServerHello, 45)
        wb.putShortLE(versionMajor.toShort())
        wb.putShortLE(versionMinor.toShort())
        wb.putLongLE(capabilities)
        wb.putByte(authModes)
        wb.putBytes(nonceS, 0, NonceLen)
        endFrame(wb, s)
    }

    override fun decode(body: ByteArray) {
        val d = WireDecoder(body)
        versionMajor = d.u16().toInt() and 0xFFFF
        versionMinor = d.u16().toInt() and 0xFFFF
        capabilities = d.u64()
        authModes = d.u8()
        d.arr32(nonceS)
        d.checkErr()
    }
}

// Hello (frame.go: lines 552-610)
data class Hello(
    var flags: Int = 0,
    var capabilities: Long = CapsV1,
    var instanceID: Long = 0L,
    var lastEpoch: Long = 0L,
    var resumeOffset: Long = 0L,
    var lastSeenLeo: Long = 0L,
    var maxRecordBytes: Int = 0,
    var retainedClass: RetainedClass = RetainedClass.DB,
    var nonceC: ByteArray = ByteArray(NonceLen),
    var mac: ByteArray = ByteArray(MACLen),
    var consumerNodeID: String = "",
    var expectedSourceNodeID: String = "",
    var topicRoot: String = "",
    var oaSystem: String = ""
) : Frame {
    override fun type(): FrameType = FrameType.Hello

    override fun appendFrame(wb: WireBuffer) {
        val hint = 111 + 4 + consumerNodeID.length + expectedSourceNodeID.length + topicRoot.length + oaSystem.length + 1
        val s = beginFrame(wb, FrameType.Hello, hint)
        wb.putShortLE(flags.toShort())
        wb.putLongLE(capabilities)
        wb.putLongLE(instanceID)
        wb.putLongLE(lastEpoch)
        wb.putLongLE(resumeOffset)
        wb.putLongLE(lastSeenLeo)
        wb.putIntLE(maxRecordBytes)
        wb.putByte(retainedClass.code)
        wb.putBytes(nonceC, 0, NonceLen)
        wb.putBytes(mac, 0, MACLen)
        wb.putStr8(consumerNodeID)
        wb.putStr8(expectedSourceNodeID)
        wb.putStr16(topicRoot)
        wb.putStr8(oaSystem)
        endFrame(wb, s)
    }

    override fun decode(body: ByteArray) {
        val d = WireDecoder(body)
        flags = d.u16().toInt() and 0xFFFF
        capabilities = d.u64()
        instanceID = d.u64()
        lastEpoch = d.u64()
        resumeOffset = d.u64()
        lastSeenLeo = d.u64()
        maxRecordBytes = d.u32()
        retainedClass = RetainedClass.fromCode(d.u8())
        d.arr32(nonceC)
        d.arr32(mac)
        consumerNodeID = d.str8()
        expectedSourceNodeID = d.str8()
        topicRoot = d.str16()
        oaSystem = d.str8()
        d.checkErr()
    }
}

// HelloOK (frame.go: lines 612-675)
data class HelloOK(
    var flags: Int = 0,
    var capabilities: Long = CapsV1,
    var epoch: Long = 0L,
    var resumeAt: Long = 0L,
    var logStart: Long = 0L,
    var leo: Long = 0L,
    var committed: Long = 0L,
    var lostOnResume: Long = 0L,
    var wallNowMs: Long = 0L,
    var monoNowMs: Long = 0L,
    var maxRecordBytes: Int = 0,
    var retainedClass: RetainedClass = RetainedClass.DB,
    var macS: ByteArray = ByteArray(MACLen),
    var sourceNodeID: String = "",
    var topicRoot: String = "",
    var oaSystem: String = ""
) : Frame {
    override fun type(): FrameType = FrameType.HelloOK

    override fun appendFrame(wb: WireBuffer) {
        val hint = 111 + 4 + sourceNodeID.length + topicRoot.length + oaSystem.length
        val s = beginFrame(wb, FrameType.HelloOK, hint)
        wb.putShortLE(flags.toShort())
        wb.putLongLE(capabilities)
        wb.putLongLE(epoch)
        wb.putLongLE(resumeAt)
        wb.putLongLE(logStart)
        wb.putLongLE(leo)
        wb.putLongLE(committed)
        wb.putLongLE(lostOnResume)
        wb.putLongLE(wallNowMs)
        wb.putLongLE(monoNowMs)
        wb.putIntLE(maxRecordBytes)
        wb.putByte(retainedClass.code)
        wb.putBytes(macS, 0, MACLen)
        wb.putStr8(sourceNodeID)
        wb.putStr16(topicRoot)
        wb.putStr8(oaSystem)
        endFrame(wb, s)
    }

    override fun decode(body: ByteArray) {
        val d = WireDecoder(body)
        flags = d.u16().toInt() and 0xFFFF
        capabilities = d.u64()
        epoch = d.u64()
        resumeAt = d.u64()
        logStart = d.u64()
        leo = d.u64()
        committed = d.u64()
        lostOnResume = d.u64()
        wallNowMs = d.u64()
        monoNowMs = d.u64()
        maxRecordBytes = d.u32()
        retainedClass = RetainedClass.fromCode(d.u8())
        d.arr32(macS)
        sourceNodeID = d.str8()
        topicRoot = d.str16()
        oaSystem = d.str8()
        d.checkErr()
    }
}

// GoAway (frame.go: lines 677-697)
data class GoAway(
    var code: GoAwayCode = GoAwayCode.Protocol,
    var reason: String = ""
) : Frame {
    override fun type(): FrameType = FrameType.GoAway

    override fun appendFrame(wb: WireBuffer) {
        val s = beginFrame(wb, FrameType.GoAway, 4 + reason.length)
        wb.putShortLE(code.code)
        wb.putStr16(reason)
        endFrame(wb, s)
    }

    override fun decode(body: ByteArray) {
        val d = WireDecoder(body)
        code = GoAwayCode.fromCode(d.u16())
        reason = d.str16()
        d.checkErr()
    }
}

// Fetch (frame.go: lines 699-741)
data class Fetch(
    var fetchID: Int = 0,
    var flags: Int = 0,
    var lingerMs: Int = 0,
    var offset: Long = 0L,
    var commit: Long = 0L,
    var maxRecords: Int = 0,
    var maxBytes: Int = 0,
    var minRecords: Int = 0,
    var maxWaitMs: Int = 0
) : Frame {
    override fun type(): FrameType = FrameType.Fetch

    override fun appendFrame(wb: WireBuffer) {
        val s = beginFrame(wb, FrameType.Fetch, 40)
        wb.putIntLE(fetchID)
        wb.putShortLE(flags.toShort())
        wb.putShortLE(lingerMs.toShort())
        wb.putLongLE(offset)
        wb.putLongLE(commit)
        wb.putIntLE(maxRecords)
        wb.putIntLE(maxBytes)
        wb.putIntLE(minRecords)
        wb.putIntLE(maxWaitMs)
        endFrame(wb, s)
    }

    override fun decode(body: ByteArray) {
        val d = WireDecoder(body)
        fetchID = d.u32()
        flags = d.u16().toInt() and 0xFFFF
        lingerMs = d.u16().toInt() and 0xFFFF
        offset = d.u64()
        commit = d.u64()
        maxRecords = d.u32()
        maxBytes = d.u32()
        minRecords = d.u32()
        maxWaitMs = d.u32()
        d.checkErr()
    }
}

// BatchHeader (frame.go: lines 743-798)
data class BatchHeader(
    var fetchID: Int = 0,
    var flags: Int = 0,
    var reserved: Int = 0,
    var baseOffset: Long = 0L,
    var count: Int = 0,
    var recordsBytes: Int = 0,
    var logStart: Long = 0L,
    var leo: Long = 0L,
    var lost: Long = 0L,
    var sourceMonoMs: Long = 0L,
    var sourceWallMs: Long = 0L,
    var crc32c: Int = 0
) {
    fun put(b: ByteArray, offset: Int = 0) {
        val bb = ByteBuffer.wrap(b, offset, BatchHeaderLen).order(ByteOrder.LITTLE_ENDIAN)
        bb.putInt(fetchID)
        bb.putShort(flags.toShort())
        bb.putShort(reserved.toShort())
        bb.putLong(baseOffset)
        bb.putInt(count)
        bb.putInt(recordsBytes)
        bb.putLong(logStart)
        bb.putLong(leo)
        bb.putLong(lost)
        bb.putLong(sourceMonoMs)
        bb.putLong(sourceWallMs)
        bb.putInt(crc32c)
    }

    fun parse(b: ByteArray, offset: Int = 0) {
        val bb = ByteBuffer.wrap(b, offset, BatchHeaderLen).order(ByteOrder.LITTLE_ENDIAN)
        fetchID = bb.getInt()
        flags = bb.getShort().toInt() and 0xFFFF
        reserved = bb.getShort().toInt() and 0xFFFF
        baseOffset = bb.getLong()
        count = bb.getInt()
        recordsBytes = bb.getInt()
        logStart = bb.getLong()
        leo = bb.getLong()
        lost = bb.getLong()
        sourceMonoMs = bb.getLong()
        sourceWallMs = bb.getLong()
        crc32c = bb.getInt()
    }
}

// Length of the sparse table of a batch with count records: u32 span and u32 deltas[count].
fun sparseTableLen(count: Int): Int = 4 + 4 * count

// Sparse table length h announces: 0 without BatchFlagSparse.
fun BatchHeader.sparseLen(): Int = if (flags and BatchFlagSparse == 0) 0 else sparseTableLen(count)

// Encodes the sparse table: u32 span and one u32 delta per record.
fun encodeSparseTable(span: Int, deltas: IntArray, count: Int = deltas.size): ByteArray {
    val bb = ByteBuffer.allocate(sparseTableLen(count)).order(ByteOrder.LITTLE_ENDIAN)
    bb.putInt(span)
    for (i in 0 until count) bb.putInt(deltas[i])
    return bb.array()
}

// EncodeBatchPrefix (frame.go: lines 791-798). frameLen includes the sparse table when
// BatchFlagSparse is set; the caller writes the table after the prefix, before the records.
fun encodeBatchPrefix(dst: ByteArray, h: BatchHeader, offset: Int = 0) {
    val bb = ByteBuffer.wrap(dst, offset, BatchPrefixLen).order(ByteOrder.LITTLE_ENDIAN)
    val frameLen = 1 + BatchHeaderLen + h.sparseLen() + h.recordsBytes
    bb.putInt(frameLen)
    bb.put(FrameType.Batch.code)
    h.put(dst, offset + FrameHeaderLen)
}

// SetBatchCRC (frame.go: lines 800-810)
fun setBatchCRC(prefix: ByteArray, records: List<ByteArray>, prefixOffset: Int = 0, sparse: ByteArray? = null): Int {
    val crc = CRC32C()
    crc.update(prefix, prefixOffset + FrameHeaderLen, BatchCRCCovered)
    if (sparse != null) crc.update(sparse, 0, sparse.size)
    for (r in records) {
        crc.update(r, 0, r.size)
    }
    val crcVal = crc.value.toInt()
    val bb = ByteBuffer.wrap(prefix, prefixOffset + BatchPrefixLen - 4, 4).order(ByteOrder.LITTLE_ENDIAN)
    bb.putInt(crcVal)
    return crcVal
}

// Batch (frame.go: lines 812-877)
data class Batch(
    var header: BatchHeader = BatchHeader(),
    var records: ByteArray = ByteArray(0)
) : Frame {
    var rawHeader: ByteArray? = null
    // Raw sparse table (u32 span, u32 deltas[count]) of a BatchFlagSparse batch, null otherwise.
    var sparse: ByteArray? = null

    override fun type(): FrameType = FrameType.Batch

    // The header is written as given (recordsBytes and crc32c are not recomputed, so tests can
    // craft faulty frames), followed by the sparse table and the records.
    override fun appendFrame(wb: WireBuffer) {
        val sp = sparse
        val s = beginFrame(wb, FrameType.Batch, BatchHeaderLen + (sp?.size ?: 0) + records.size)
        val hBytes = ByteArray(BatchHeaderLen)
        header.put(hBytes)
        wb.putBytes(hBytes)
        if (sp != null) wb.putBytes(sp)
        wb.putBytes(records)
        endFrame(wb, s)
    }

    override fun decode(body: ByteArray) {
        rawHeader = null
        sparse = null
        if (body.size < BatchHeaderLen) throw ShortFrameException()
        header.parse(body)
        var start = BatchHeaderLen.toLong()
        if (header.flags and BatchFlagSparse != 0) {
            val end = start + 4L + 4L * (header.count.toLong() and 0xFFFFFFFFL)
            if (end > body.size.toLong()) {
                records = ByteArray(0)
                throw BatchSparseException()
            }
            sparse = body.copyOfRange(start.toInt(), end.toInt())
            start = end
        }
        val end = start + (header.recordsBytes.toLong() and 0xFFFFFFFFL)
        if (end > body.size.toLong()) {
            records = ByteArray(0)
            sparse = null
            throw BatchRecordsException()
        }
        val hb = ByteArray(BatchHeaderLen)
        System.arraycopy(body, 0, hb, 0, BatchHeaderLen)
        rawHeader = hb
        records = ByteArray(header.recordsBytes)
        System.arraycopy(body, start.toInt(), records, 0, header.recordsBytes)
        if ((header.count.toLong() and 0xFFFFFFFFL) * MinRecordFrame > (header.recordsBytes.toLong() and 0xFFFFFFFFL)) {
            throw BatchCountRangeException()
        }
        if (sparse != null) checkSparse()
    }

    // Deltas strictly increasing and below span; span >= count and span > 0.
    private fun checkSparse() {
        val span = span()
        val count = header.count.toLong() and 0xFFFFFFFFL
        if (span == 0L || span < count) throw BatchSparseException()
        var prev = -1L
        for (i in 0 until count.toInt()) {
            val d = delta(i)
            if (d <= prev || d >= span) throw BatchSparseException()
            prev = d
        }
    }

    fun isSparse(): Boolean = sparse != null

    // Number of offsets the batch covers: the sparse span, or count for a dense batch.
    fun span(): Long {
        val sp = sparse ?: return header.count.toLong() and 0xFFFFFFFFL
        return ByteBuffer.wrap(sp, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt().toLong() and 0xFFFFFFFFL
    }

    // Offset of record i relative to baseOffset.
    fun delta(i: Int): Long {
        val sp = sparse ?: return i.toLong()
        return ByteBuffer.wrap(sp, 4 + 4 * i, 4).order(ByteOrder.LITTLE_ENDIAN).getInt().toLong() and 0xFFFFFFFFL
    }

    // Sets the sparse table and BatchFlagSparse (tests and test peers).
    fun setSparse(span: Int, deltas: IntArray) {
        sparse = encodeSparseTable(span, deltas)
        header.flags = header.flags or BatchFlagSparse
    }

    fun computeCRC(): Int {
        val hb = rawHeader ?: ByteArray(BatchHeaderLen).also { header.put(it) }
        val crc = CRC32C()
        crc.update(hb, 0, BatchCRCCovered)
        sparse?.let { crc.update(it, 0, it.size) }
        crc.update(records, 0, records.size)
        return crc.value.toInt()
    }

    fun isCRCValid(): Boolean {
        return (header.flags and BatchFlagCRC == 0) || (computeCRC() == header.crc32c)
    }

    fun crcValid(): Boolean = isCRCValid()

    fun iter(): RecordIter {
        val it = RecordIter(records, header.count)
        it.checkMono(header.sourceMonoMs)
        return it
    }
}

// Commit (frame.go: lines 879-895)
data class Commit(var commit: Long = 0L) : Frame {
    override fun type(): FrameType = FrameType.Commit

    override fun appendFrame(wb: WireBuffer) {
        val s = beginFrame(wb, FrameType.Commit, 8)
        wb.putLongLE(commit)
        endFrame(wb, s)
    }

    override fun decode(body: ByteArray) {
        val d = WireDecoder(body)
        commit = d.u64()
        d.checkErr()
    }
}

// Ping (frame.go: lines 897-912)
data class Ping(var token: Long = 0L) : Frame {
    override fun type(): FrameType = FrameType.Ping

    override fun appendFrame(wb: WireBuffer) {
        val s = beginFrame(wb, FrameType.Ping, 8)
        wb.putLongLE(token)
        endFrame(wb, s)
    }

    override fun decode(body: ByteArray) {
        val d = WireDecoder(body)
        token = d.u64()
        d.checkErr()
    }
}

// Pong (frame.go: lines 914-929)
data class Pong(var token: Long = 0L) : Frame {
    override fun type(): FrameType = FrameType.Pong

    override fun appendFrame(wb: WireBuffer) {
        val s = beginFrame(wb, FrameType.Pong, 8)
        wb.putLongLE(token)
        endFrame(wb, s)
    }

    override fun decode(body: ByteArray) {
        val d = WireDecoder(body)
        token = d.u64()
        d.checkErr()
    }
}

// One entry of INTEREST_SNAPSHOT/INTEREST_DELTA: the absolute class of a filter. The decoder does
// not validate class or filter; the receiver ignores invalid entries (section 4). filterBytes keeps
// the raw bytes so that invalid UTF-8 can be detected (filter is then decoded with replacements).
class InterestEntry(
    var cls: Int = InterestVol,
    var expirySec: Long = 0L, // PER only; 0 for VOL/NONE; InterestExpiryNever for no expiry
    filter: String = ""
) {
    var filterBytes: ByteArray = filter.toByteArray(Charsets.UTF_8)
    val filter: String get() = String(filterBytes, Charsets.UTF_8)

    // Encoded size of the entry.
    fun len(): Int = InterestEntryOverhead + filterBytes.size

    // Whether filterBytes are well-formed UTF-8.
    fun validUtf8(): Boolean = try {
        Charsets.UTF_8.newDecoder()
            .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
            .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
            .decode(ByteBuffer.wrap(filterBytes))
        true
    } catch (e: java.nio.charset.CharacterCodingException) {
        false
    }

    override fun equals(other: Any?): Boolean = other is InterestEntry && other.cls == cls &&
        other.expirySec == expirySec && other.filterBytes.contentEquals(filterBytes)
    override fun hashCode(): Int = (cls * 31 + expirySec.hashCode()) * 31 + filterBytes.contentHashCode()
    override fun toString(): String = "InterestEntry(cls=$cls, expirySec=$expirySec, filter=$filter)"
}

private fun appendInterestEntries(wb: WireBuffer, es: List<InterestEntry>) {
    wb.putIntLE(es.size)
    for (e in es) {
        wb.putByte(e.cls.toByte())
        wb.putIntLE(e.expirySec.toInt())
        wb.putShortLE(e.filterBytes.size.toShort())
        wb.putBytes(e.filterBytes)
    }
}

// Reads u32 count and the entries. A body shorter than count entries or with bytes left after them
// is InterestCountException.
private fun decodeInterestEntries(d: WireDecoder): MutableList<InterestEntry> {
    val n = d.u32().toLong() and 0xFFFFFFFFL
    d.checkErr()
    if (n * InterestEntryOverhead > d.remaining()) throw InterestCountException()
    val res = ArrayList<InterestEntry>(n.toInt())
    for (i in 0 until n.toInt()) {
        val e = InterestEntry()
        e.cls = d.u8().toInt() and 0xFF
        e.expirySec = d.u32().toLong() and 0xFFFFFFFFL
        val len = d.u16().toInt() and 0xFFFF
        e.filterBytes = d.take(len) ?: throw InterestCountException()
        if (d.short) throw InterestCountException()
        res.add(e)
    }
    if (d.remaining() != 0) throw InterestCountException()
    return res
}

private fun interestBodyLen(es: List<InterestEntry>): Int = es.sumOf { it.len() }

// INTEREST_SNAPSHOT (0x20, C→S): the consumer's full interest set at generation, possibly split over
// several frames from FIRST to LAST that carry the same generation.
class InterestSnapshot(
    var generation: Long = 0L, // u32
    var flags: Int = 0,
    var entries: MutableList<InterestEntry> = ArrayList()
) : Frame {
    override fun type(): FrameType = FrameType.InterestSnapshot

    override fun appendFrame(wb: WireBuffer) {
        val s = beginFrame(wb, FrameType.InterestSnapshot, InterestSnapshotHeaderLen + interestBodyLen(entries))
        wb.putIntLE(generation.toInt())
        wb.putByte(flags.toByte())
        appendInterestEntries(wb, entries)
        endFrame(wb, s)
    }

    // Unlike other frames, bytes after the entries are an error, as is a body shorter than count.
    override fun decode(body: ByteArray) {
        val d = WireDecoder(body)
        generation = d.u32().toLong() and 0xFFFFFFFFL
        flags = d.u8().toInt() and 0xFF
        d.checkErr()
        entries = decodeInterestEntries(d)
    }
}

// INTEREST_DELTA (0x21, C→S): absolute classes of changed filters; generations strictly increase.
class InterestDelta(
    var generation: Long = 0L, // u32
    var entries: MutableList<InterestEntry> = ArrayList()
) : Frame {
    override fun type(): FrameType = FrameType.InterestDelta

    override fun appendFrame(wb: WireBuffer) {
        val s = beginFrame(wb, FrameType.InterestDelta, InterestDeltaHeaderLen + interestBodyLen(entries))
        wb.putIntLE(generation.toInt())
        appendInterestEntries(wb, entries)
        endFrame(wb, s)
    }

    override fun decode(body: ByteArray) {
        val d = WireDecoder(body)
        generation = d.u32().toLong() and 0xFFFFFFFFL
        d.checkErr()
        entries = decodeInterestEntries(d)
    }
}

// FrameReader (frame.go: lines 328-410)
class FrameReader(
    private val r: InputStream,
    var max: Long = DefaultMaxFrameBytes.toLong(),
    var progress: (() -> Unit)? = null
) {
    private val hdr = ByteArray(FrameHeaderLen)
    private var buf = ByteArray(0)
    private val readChunk = 64 shl 10

    fun readHeader(): Pair<FrameType, Int> {
        var read = 0
        while (read < FrameHeaderLen) {
            val n = r.read(hdr, read, FrameHeaderLen - read)
            if (n < 0) {
                if (read == 0) throw EOFException("EOF at frame boundary")
                throw EOFException("unexpected EOF in frame header")
            }
            read += n
        }
        val n = ((hdr[0].toLong() and 0xFFL) or
                ((hdr[1].toLong() and 0xFFL) shl 8) or
                ((hdr[2].toLong() and 0xFFL) shl 16) or
                ((hdr[3].toLong() and 0xFFL) shl 24))
        if (n == 0L) throw FrameEmptyException()
        if (n > max) throw FrameTooLargeException()

        val typeCode = hdr[4]
        val frameType = FrameType.fromCode(typeCode) ?: throw UnknownFrameException()
        return Pair(frameType, (n - 1).toInt())
    }

    fun readBody(dst: ByteArray, offset: Int = 0, length: Int = dst.size - offset) {
        var remaining = length
        var off = offset
        while (remaining > 0) {
            val c = minOf(remaining, readChunk)
            var read = 0
            while (read < c) {
                val n = r.read(dst, off + read, c - read)
                if (n < 0) throw EOFException("unexpected EOF reading frame body")
                read += n
            }
            off += c
            remaining -= c
            progress?.invoke()
        }
    }

    fun body(n: Int): ByteArray {
        if (buf.size < n) {
            buf = ByteArray(n)
        }
        readBody(buf, 0, n)
        val res = ByteArray(n)
        System.arraycopy(buf, 0, res, 0, n)
        return res
    }

    fun discard(n: Int) {
        var remaining = n
        val temp = ByteArray(minOf(remaining, readChunk))
        while (remaining > 0) {
            val c = minOf(remaining, temp.size)
            var read = 0
            while (read < c) {
                val rLen = r.read(temp, read, c - read)
                if (rLen < 0) throw EOFException("unexpected EOF discarding frame body")
                read += rLen
            }
            remaining -= c
            progress?.invoke()
        }
    }

    fun readFrame(): Pair<FrameType, ByteArray> {
        val (t, len) = readHeader()
        val b = body(len)
        return Pair(t, b)
    }
}
