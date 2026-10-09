package at.rocworks.peerlink.wire

import org.junit.Assert.*
import org.junit.Test
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.Random

// Interest routing wire additions (plan-peerlink-interest-routing section 4).
class InterestWireTest {

    private fun hex(s: String): ByteArray {
        val c = s.replace(" ", "")
        return ByteArray(c.length / 2) { c.substring(2 * it, 2 * it + 2).toInt(16).toByte() }
    }

    private fun roundTrip(f: Frame): Frame {
        val enc = f.encode()
        val (t, body) = FrameReader(ByteArrayInputStream(enc), 1 shl 20).readFrame()
        assertEquals(f.type(), t)
        val d = decodeFrame(t, body)
        assertArrayEquals(enc, d.encode())
        return d
    }

    @Test
    fun capabilityBitsAndCodes() {
        assertEquals(1L shl 4, CapRole)
        assertEquals(1L shl 5, CapInterest)
        assertEquals(0L, CapsV1 and (CapRole or CapInterest))
        assertEquals(1 shl 6, BatchFlagSparse)
        assertEquals(FrameType.InterestSnapshot, FrameType.fromCode(0x20))
        assertEquals(FrameType.InterestDelta, FrameType.fromCode(0x21))
    }

    @Test
    fun snapshotGoldenBytes() {
        val f = InterestSnapshot(
            generation = 7L,
            flags = InterestFlagFirst or InterestFlagLast,
            entries = mutableListOf(
                InterestEntry(InterestVol, 0L, "a/#"),
                InterestEntry(InterestPer, InterestExpiryNever, "b/+"),
                InterestEntry(InterestPer, 3600L, "ü")
            )
        )
        val expected = hex(
            "27000000" + "20" +             // frame length 39 (type + body), type
                "07000000" + "03" + "03000000" + // generation, FIRST|LAST, count
                "01" + "00000000" + "0300" + "612f23" +
                "02" + "ffffffff" + "0300" + "622f2b" +
                "02" + "100e0000" + "0200" + "c3bc"
        )
        assertArrayEquals(expected, f.encode())
        val d = roundTrip(f) as InterestSnapshot
        assertEquals(7L, d.generation)
        assertEquals(3, d.flags)
        assertEquals(f.entries, d.entries)
        assertEquals("ü", d.entries[2].filter)
        assertEquals(InterestExpiryNever, d.entries[1].expirySec)
        assertTrue(d.entries.all { it.validUtf8() })
    }

    @Test
    fun deltaGoldenBytes() {
        val f = InterestDelta(
            generation = 0xFFFFFFFEL,
            entries = mutableListOf(InterestEntry(InterestNone, 0L, "x"))
        )
        val expected = hex("11000000" + "21" + "feffffff" + "01000000" + "00" + "00000000" + "0100" + "78")
        assertArrayEquals(expected, f.encode())
        val d = roundTrip(f) as InterestDelta
        assertEquals(0xFFFFFFFEL, d.generation)
        assertEquals(f.entries, d.entries)
        roundTrip(InterestDelta(generation = 1L))
    }

    @Test
    fun interestCountErrors() {
        val enc = InterestDelta(1L, mutableListOf(InterestEntry(InterestVol, 0L, "ab"))).encode()
        val body = enc.copyOfRange(FrameHeaderLen, enc.size)
        // trailing bytes after the entries
        assertThrows(InterestCountException::class.java) { decodeFrame(FrameType.InterestDelta, body + byteArrayOf(0)) }
        // count larger than the body
        val big = body.clone()
        ByteBuffer.wrap(big, 4, 4).order(ByteOrder.LITTLE_ENDIAN).putInt(2)
        assertThrows(InterestCountException::class.java) { decodeFrame(FrameType.InterestDelta, big) }
        // short entry: filter cut off
        assertThrows(InterestCountException::class.java) {
            decodeFrame(FrameType.InterestDelta, body.copyOf(body.size - 1))
        }
        // count field itself short
        assertThrows(ShortFrameException::class.java) { decodeFrame(FrameType.InterestDelta, body.copyOf(6)) }
        assertThrows(ShortFrameException::class.java) { decodeFrame(FrameType.InterestSnapshot, byteArrayOf(1, 0, 0, 0, 1)) }
        // count of zero with no trailing bytes is fine
        val empty = InterestSnapshot(3L, InterestFlagFirst).encode()
        assertEquals(InterestSnapshotHeaderLen, empty.size - FrameHeaderLen)
    }

    @Test
    fun decoderDoesNotValidateClassOrFilter() {
        val e = InterestEntry(9, 5L, "")
        e.filterBytes = byteArrayOf(0xC3.toByte(), 0x28)
        val d = roundTrip(InterestSnapshot(1L, 0, mutableListOf(e))) as InterestSnapshot
        assertEquals(9, d.entries[0].cls)
        assertFalse(d.entries[0].validUtf8())
        assertArrayEquals(e.filterBytes, d.entries[0].filterBytes)
    }

    private fun records(vararg topics: String): List<ByteArray> = topics.map { t ->
        val r = Record(topic = t, payload = byteArrayOf(1))
        ByteArray(recordSize(r)).also { encodeRecord(it, r) }
    }

    private fun sparseBatch(span: Int, deltas: IntArray, recs: List<ByteArray>): Batch {
        val region = recs.fold(ByteArray(0)) { a, b -> a + b }
        val b = Batch(BatchHeader(fetchID = 3, flags = BatchFlagCRC, baseOffset = 100L, count = recs.size, recordsBytes = region.size), region)
        b.setSparse(span, deltas)
        b.header.crc32c = b.computeCRC()
        return b
    }

    @Test
    fun sparseBatchRoundTripAndCRC() {
        val recs = records("a", "b")
        val b = sparseBatch(10, intArrayOf(2, 7), recs)
        val enc = b.encode()
        assertEquals(FrameHeaderLen + BatchHeaderLen + sparseTableLen(2) + recs.sumOf { it.size }, enc.size)
        val d = roundTrip(b) as Batch
        assertTrue(d.isSparse())
        assertEquals(10L, d.span())
        assertEquals(2L, d.delta(0))
        assertEquals(7L, d.delta(1))
        assertTrue(d.isCRCValid())

        // The prefix path used by the server produces the same bytes.
        val prefix = ByteArray(BatchPrefixLen)
        val h = b.header.copy(crc32c = 0)
        encodeBatchPrefix(prefix, h)
        val sparse = encodeSparseTable(10, intArrayOf(2, 7))
        val crc = setBatchCRC(prefix, recs, sparse = sparse)
        assertEquals(b.header.crc32c, crc)
        assertArrayEquals(enc, prefix + sparse + recs[0] + recs[1])

        // CRC covers the sparse table.
        val bad = enc.clone()
        bad[FrameHeaderLen + BatchHeaderLen + 4] = 3
        val (t, body) = FrameReader(ByteArrayInputStream(bad), 1 shl 20).readFrame()
        assertFalse((decodeFrame(t, body) as Batch).isCRCValid())

        // A pure skip batch: count 0, span > 0.
        val skip = sparseBatch(5, IntArray(0), emptyList())
        val ds = roundTrip(skip) as Batch
        assertEquals(5L, ds.span())
        assertEquals(0, ds.header.count)

        // Dense accessors.
        val dense = Batch(BatchHeader(count = 2, recordsBytes = recs.sumOf { it.size }), recs[0] + recs[1])
        assertFalse(dense.isSparse())
        assertEquals(2L, dense.span())
        assertEquals(1L, dense.delta(1))
    }

    private fun decodeBody(b: Batch): Batch {
        val enc = b.encode()
        return decodeFrame(FrameType.Batch, enc.copyOfRange(FrameHeaderLen, enc.size)) as Batch
    }

    @Test
    fun sparseViolations() {
        val recs = records("a", "b")
        for ((span, deltas) in listOf(
            0 to intArrayOf(0, 1),     // span == 0
            1 to intArrayOf(0, 1),     // span < count
            10 to intArrayOf(3, 3),    // not strictly increasing
            10 to intArrayOf(4, 2),
            10 to intArrayOf(2, 10)    // delta >= span
        )) {
            assertThrows("span=$span deltas=${deltas.toList()}", BatchSparseException::class.java) {
                decodeBody(sparseBatch(span, deltas, recs))
            }
        }
        assertThrows(BatchSparseException::class.java) { decodeBody(sparseBatch(0, IntArray(0), emptyList())) }

        // Table that does not fit → BatchSparse; records that do not fit → BatchRecords.
        val enc = sparseBatch(10, intArrayOf(2, 7), recs).encode()
        val body = enc.copyOfRange(FrameHeaderLen, enc.size)
        assertThrows(BatchSparseException::class.java) {
            decodeFrame(FrameType.Batch, body.copyOf(BatchHeaderLen + sparseTableLen(2) - 1))
        }
        assertThrows(BatchRecordsException::class.java) {
            decodeFrame(FrameType.Batch, body.copyOf(body.size - 1))
        }
    }

    @Test
    fun fuzzInterestAndSparse() {
        val rng = Random(7)
        val frames = listOf(
            InterestSnapshot(1L, InterestFlagFirst, mutableListOf(InterestEntry(InterestVol, 0L, "a/b"), InterestEntry(InterestPer, 9L, "#"))),
            InterestDelta(2L, mutableListOf(InterestEntry(InterestNone, 0L, "x/+/y"))),
            sparseBatch(9, intArrayOf(1, 4, 8), records("t1", "t2", "t3"))
        )
        for (f in frames) {
            val enc = f.encode()
            for (i in 0 until 2000) {
                val m = enc.clone()
                repeat(rng.nextInt(3) + 1) {
                    val p = FrameHeaderLen + rng.nextInt(m.size - FrameHeaderLen)
                    m[p] = (m[p].toInt() xor (rng.nextInt(255) + 1)).toByte()
                }
                val body = m.copyOfRange(FrameHeaderLen, FrameHeaderLen + rng.nextInt(m.size - FrameHeaderLen + 1))
                try {
                    val d = decodeFrame(f.type(), body)
                    if (d is Batch) d.isCRCValid()
                } catch (e: WireException) {
                    // expected
                }
            }
        }
    }
}
