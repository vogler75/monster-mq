package at.rocworks.peerlink.wire

import org.junit.Assert.*
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.Random

class WireTest {

    private fun nonce(seed: Byte): ByteArray {
        val n = ByteArray(32)
        for (i in 0 until 32) {
            n[i] = (seed + i).toByte()
        }
        return n
    }

    private fun sampleFrames(): List<Frame> {
        return listOf(
            ServerHello(
                versionMajor = 1,
                versionMinor = 3,
                capabilities = CapsV1 or (1L shl 40),
                authModes = (AuthClientCertRequested.toInt() or AuthSharedSecret.toInt()).toByte(),
                nonceS = nonce(1)
            ),
            Hello(
                flags = HelloFlagMAC,
                capabilities = CapsV1,
                instanceID = 0xdeadbeefcafeL,
                lastEpoch = 77L,
                resumeOffset = 1000L,
                lastSeenLeo = 2000L,
                maxRecordBytes = (1 shl 20) + (64 shl 10),
                retainedClass = RetainedClass.WinCCOA,
                nonceC = nonce(2),
                mac = nonce(3),
                consumerNodeID = "oa-b",
                expectedSourceNodeID = "oa-a",
                topicRoot = "winccoa",
                oaSystem = "System1",
                brokerType = BrokerTypeEdge,
                brokerVersion = "1.4.2+abc"
            ),
            Hello(consumerNodeID = "b", expectedSourceNodeID = "a"),
            HelloOK(
                flags = HelloOKSourceReset or HelloOKSnapshotAvailable,
                capabilities = CapTombstone or CapSnapshotFill,
                epoch = 0x1234567890abcdefL,
                resumeAt = 5L,
                logStart = 3L,
                leo = 9L,
                committed = 5L,
                lostOnResume = 2L,
                wallNowMs = -1L,
                monoNowMs = 42L,
                maxRecordBytes = 1 shl 20,
                retainedClass = RetainedClass.DB,
                macS = nonce(4),
                sourceNodeID = "oa-a",
                topicRoot = "",
                oaSystem = "",
                brokerType = BrokerTypeFull,
                brokerVersion = "1.8.33"
            ),
            GoAway(code = GoAwayCode.Shutdown, reason = "source stopping"),
            GoAway(code = GoAwayCode.AuthFailed),
            Fetch(
                fetchID = 7,
                flags = FetchFlagSnapshot,
                lingerMs = 5,
                offset = 100L,
                commit = 99L,
                maxRecords = 4096,
                maxBytes = 1 shl 20,
                minRecords = 1,
                maxWaitMs = 1000
            ),
            Batch(
                header = BatchHeader(
                    fetchID = 7,
                    flags = BatchFlagGap or BatchFlagTruncated,
                    reserved = 3,
                    baseOffset = 105L,
                    count = 2,
                    recordsBytes = 8,
                    logStart = 105L,
                    leo = 200L,
                    lost = 5L,
                    sourceMonoMs = 9000L,
                    sourceWallMs = 1759500000000L,
                    crc32c = 0xabcdef01.toInt()
                ),
                records = "abcdefgh".toByteArray(Charsets.ISO_8859_1)
            ),
            Batch(
                header = BatchHeader(flags = BatchFlagEmpty, baseOffset = 1L),
                records = ByteArray(0)
            ),
            Commit(commit = 123456789L),
            Ping(token = 1L),
            Pong(token = -1L)
        )
    }

    @Test
    fun testFrameRoundTrips() {
        for (f in sampleFrames()) {
            val enc = f.encode()
            val fr = FrameReader(ByteArrayInputStream(enc), 1 shl 20)
            val (t, body) = fr.readFrame()
            assertEquals("Frame type mismatch", f.type(), t)

            val decoded = decodeFrame(t, body)
            val reEnc = decoded.encode()
            assertArrayEquals("Re-encoded bytes must match for ${f.type()}", enc, reEnc)

            // Forward compatibility: trailing bytes ignored
            val trailing = ByteArray(enc.size + 4)
            System.arraycopy(enc, 0, trailing, 0, enc.size)
            trailing[enc.size] = 0x11.toByte()
            trailing[enc.size + 1] = 0x22.toByte()
            trailing[enc.size + 2] = 0x33.toByte()
            trailing[enc.size + 3] = 0x44.toByte()
            // Update frame length
            val bb = ByteBuffer.wrap(trailing, 0, 4).order(ByteOrder.LITTLE_ENDIAN)
            bb.putInt(trailing.size - 4)

            val fr2 = FrameReader(ByteArrayInputStream(trailing), 1 shl 20)
            val (t2, body2) = fr2.readFrame()
            val decoded2 = decodeFrame(t2, body2)
            assertArrayEquals("Re-encoded with trailing ignored", enc, decoded2.encode())
        }
    }

    @Test
    fun testFrameShortBodies() {
        for (f in sampleFrames()) {
            val enc = f.encode()
            val bodyLen = enc.size - FrameHeaderLen
            for (cut in 0 until bodyLen) {
                val shortBody = ByteArray(cut)
                System.arraycopy(enc, FrameHeaderLen, shortBody, 0, cut)
                try {
                    val decoded = decodeFrame(f.type(), shortBody)
                    // If decode succeeded, it can only happen if all fields were already read
                } catch (e: Exception) {
                    assertTrue("Expected ShortFrameException but got ${e.javaClass.name}", e is ShortFrameException || e is WireException)
                }
            }
        }
    }

    // A HELLO or HELLO_OK from a peer that predates brokerType/brokerVersion decodes with both empty;
    // a body cut inside the pair is still short.
    @Test
    fun testHelloWithoutBrokerInfo() {
        val frames = listOf<Frame>(
            Hello(consumerNodeID = "b", expectedSourceNodeID = "a", oaSystem = "S", brokerType = BrokerTypeEdge, brokerVersion = "1.0"),
            HelloOK(sourceNodeID = "a", oaSystem = "S", brokerType = BrokerTypeFull, brokerVersion = "2.0")
        )
        for (f in frames) {
            val enc = f.encode()
            val legacyLen = enc.size - FrameHeaderLen - 2 - 4 - 3
            val decoded = decodeFrame(f.type(), enc.copyOfRange(FrameHeaderLen, FrameHeaderLen + legacyLen))
            when (decoded) {
                is Hello -> assertEquals(listOf("S", "", ""), listOf(decoded.oaSystem, decoded.brokerType, decoded.brokerVersion))
                is HelloOK -> assertEquals(listOf("S", "", ""), listOf(decoded.oaSystem, decoded.brokerType, decoded.brokerVersion))
                else -> fail("unexpected ${decoded.type()}")
            }
            try {
                decodeFrame(f.type(), enc.copyOfRange(FrameHeaderLen, FrameHeaderLen + legacyLen + 3))
                fail("body cut inside brokerVersion must be short")
            } catch (_: ShortFrameException) {
            }
        }
        assertEquals("1.0", protocolVersion(VersionMajor, VersionMinor))
    }

    @Test
    fun testUnknownFrame() {
        val b = byteArrayOf(0x01, 0x00, 0x00, 0x00, 0x7F) // len=1, type=0x7F
        val fr = FrameReader(ByteArrayInputStream(b))
        try {
            fr.readHeader()
            fail("Expected UnknownFrameException")
        } catch (e: UnknownFrameException) {
            // Success
        }
    }

    @Test
    fun testRecordRoundTrips() {
        val records = listOf(
            Record(topic = "a"),
            Record(flags = 2 or FlagWill, topic = "clients/c1/state", clientID = "c1", payload = "offline".toByteArray()),
            Record(flags = FlagRetain, topic = "a/b", clientID = "inline"),
            Record(flags = FlagSnapshot or FlagRetain, topic = "x", payload = byteArrayOf(0), expirySec = 1),
            Record(flags = FlagPayloadFormat, payloadFormat = 0, topic = "t"),
            Record(
                topic = "werk/größe/ü€",
                clientID = "ç",
                username = "ñ".toByteArray(Charsets.UTF_8),
                user = listOf(UserProp("ä", "😀"))
            ),
            Record(topic = "t", user = listOf(UserProp("a", "b"))),
            Record(
                flags = 1 or FlagRetain or FlagDup or FlagInline or FlagPayloadFormat,
                publishWallNs = 1759500000123456789L,
                captureMonoMs = 123456L,
                expirySec = 60L,
                payloadFormat = 1,
                topic = "plant/line1/temp",
                clientID = "client-1",
                username = "alice".toByteArray(Charsets.UTF_8),
                contentType = "application/json",
                responseTopic = "reply/here",
                correlationData = byteArrayOf(1, 2, 3, 0),
                user = listOf(
                    UserProp("k", "v1"),
                    UserProp("k", "v2"),
                    UserProp("a", ""),
                    UserProp("", "x"),
                    UserProp("k", "v1")
                ),
                payload = """{"v":1}""".toByteArray(Charsets.UTF_8)
            )
        )

        for (r in records) {
            val sz = recordSize(r)
            assertTrue(sz > 0)
            val dst = ByteArray(sz)
            val written = encodeRecord(dst, r)
            assertEquals(sz, written)

            val view = RecordView()
            decodeRecord(dst, view)

            assertEquals(r.topic, view.topicString())
            assertEquals(r.clientID, view.clientIDString())
            assertArrayEquals(r.username, view.username)
            assertEquals(r.flags, view.flags)
            assertEquals(r.expirySec, view.expirySec)
            assertEquals(r.payloadFormat, view.payloadFormat)
            assertArrayEquals(r.payload, view.payload)

            val recCopy = view.toRecord()
            assertEquals(r.contentType, recCopy.contentType)
            assertEquals(r.responseTopic, recCopy.responseTopic)
            assertArrayEquals(r.correlationData, recCopy.correlationData)
            assertEquals(r.user.size, recCopy.user.size)
            for (i in r.user.indices) {
                assertEquals(r.user[i].key, recCopy.user[i].key)
                assertEquals(r.user[i].value, recCopy.user[i].value)
            }
        }
    }

    @Test
    fun testRecordTombstone() {
        val orig = Record(
            flags = 1 or FlagRetain,
            publishWallNs = 999999L,
            captureMonoMs = 88888L,
            expirySec = 120L,
            payloadFormat = 1,
            topic = "test/topic",
            payload = "data".toByteArray()
        )
        val origBytes = ByteArray(recordSize(orig))
        encodeRecord(origBytes, orig)

        val tomb = ByteArray(TombstoneLen)
        putTombstone(tomb, origBytes)

        val view = RecordView()
        decodeRecord(tomb, view)

        assertTrue(view.skipped())
        assertEquals(orig.flags or FlagSkipped, view.flags)
        assertEquals(orig.publishWallNs, view.publishWallNs)
        assertEquals(orig.captureMonoMs, view.captureMonoMs)
        assertEquals(orig.expirySec, view.expirySec)
        assertEquals(orig.payloadFormat, view.payloadFormat)
        assertEquals(0, view.topic.size)
        assertEquals(0, view.payload.size)
    }

    @Test
    fun testBatchIteration() {
        val r1 = Record(topic = "topic1", payload = "hello".toByteArray())
        val r2 = Record(topic = "topic2", payload = "world".toByteArray())
        val b1 = ByteArray(recordSize(r1)).also { encodeRecord(it, r1) }
        val b2 = ByteArray(recordSize(r2)).also { encodeRecord(it, r2) }

        val region = ByteArray(b1.size + b2.size)
        System.arraycopy(b1, 0, region, 0, b1.size)
        System.arraycopy(b2, 0, region, b1.size, b2.size)

        val iter = RecordIter(region, 2)
        val v = RecordView()

        val (has1, err1) = iter.next(v)
        assertTrue(has1)
        assertNull(err1)
        assertEquals("topic1", v.topicString())

        val (has2, err2) = iter.next(v)
        assertTrue(has2)
        assertNull(err2)
        assertEquals("topic2", v.topicString())

        val (has3, err3) = iter.next(v)
        assertFalse(has3)
        assertNull(err3)
    }

    @Test
    fun testBatchOverrunAndCountFaults() {
        val r = Record(topic = "t", payload = "p".toByteArray())
        val b = ByteArray(recordSize(r)).also { encodeRecord(it, r) }

        // Region truncated
        val shortRegion = b.copyOfRange(0, b.size - 2)
        val iter1 = RecordIter(shortRegion, 1)
        val v = RecordView()
        try {
            iter1.next(v)
            fail("Expected RecordOverrunException")
        } catch (e: RecordOverrunException) {
            // Success
        }

        // Expected count 2 but region only has 1 record
        val iter2 = RecordIter(b, 2)
        iter2.next(v)
        try {
            iter2.next(v)
            fail("Expected BatchCountException")
        } catch (e: BatchCountException) {
            // Success
        }

        // Expected count 1 but region has trailing bytes
        val extraRegion = ByteArray(b.size + 4)
        System.arraycopy(b, 0, extraRegion, 0, b.size)
        val iter3 = RecordIter(extraRegion, 1)
        iter3.next(v)
        try {
            iter3.next(v)
            fail("Expected BatchCountException for extra bytes")
        } catch (e: BatchCountException) {
            // Success
        }
    }

    @Test
    fun testFuzzMutationSafety() {
        val rng = Random(42)
        val frames = sampleFrames()
        for (f in frames) {
            val enc = f.encode()
            for (i in 0 until 50) {
                val mutated = enc.clone()
                val mutations = rng.nextInt(3) + 1
                for (m in 0 until mutations) {
                    val pos = rng.nextInt(mutated.size)
                    mutated[pos] = (mutated[pos].toInt() xor (rng.nextInt(255) + 1)).toByte()
                }
                // Random truncate
                val cutLen = rng.nextInt(mutated.size + 1)
                val testBytes = mutated.copyOf(cutLen)

                try {
                    val fr = FrameReader(ByteArrayInputStream(testBytes))
                    val (typ, body) = fr.readFrame()
                    decodeFrame(typ, body)
                } catch (e: Exception) {
                    // Only declared WireExceptions or EOFExceptions allowed!
                    assertTrue(
                        "Fuzz mutation must only throw WireException or EOFException, but got ${e.javaClass.name}",
                        e is WireException || e is java.io.EOFException
                    )
                }
            }
        }
    }
}
