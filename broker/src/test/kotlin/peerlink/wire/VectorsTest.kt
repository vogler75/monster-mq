package at.rocworks.peerlink.wire

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.junit.Assert.*
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.HexFormat

class VectorsTest {

    private val hexFormat = HexFormat.of()

    private fun loadVectors(): JsonObject {
        val stream = javaClass.getResourceAsStream("/peerlink/vectors.json")
            ?: error("Missing /peerlink/vectors.json test resource")
        val text = stream.bufferedReader().use { it.readText() }
        return JsonObject(text)
    }

    @Test
    fun testPreamble() {
        val vj = loadVectors()
        val expectedHex = vj.getString("preambleHex")
        val preamble = appendPreamble()
        assertEquals(expectedHex, hexFormat.formatHex(preamble))

        val (major, minor) = parsePreamble(preamble)
        assertEquals(VersionMajor, major)
        assertEquals(VersionMinor, minor)
    }

    @Test
    fun testFrames() {
        val vj = loadVectors()
        val frames = vj.getJsonArray("frames")
        for (i in 0 until frames.size()) {
            val fObj = frames.getJsonObject(i)
            val name = fObj.getString("name")
            val typeCode = fObj.getInteger("type").toByte()
            val hex = fObj.getString("hex")
            val raw = hexFormat.parseHex(hex)

            val frameType = FrameType.fromCode(typeCode)
            assertNotNull("Known frame type for $name", frameType)

            // Split header and body
            assertTrue(raw.size >= FrameHeaderLen)
            val body = raw.copyOfRange(FrameHeaderLen, raw.size)

            val decoded = decodeFrame(frameType!!, body)
            val reEncoded = decoded.encode()

            assertEquals("Frame byte-identical re-encode for $name ($i)", hex, hexFormat.formatHex(reEncoded))
        }
    }

    @Test
    fun testRecords() {
        val vj = loadVectors()
        val records = vj.getJsonArray("records")
        for (i in 0 until records.size()) {
            val rObj = records.getJsonObject(i)
            val name = rObj.getString("name")
            val hex = rObj.getString("hex")
            val raw = hexFormat.parseHex(hex)

            val view = RecordView()
            decodeRecord(raw, view)

            assertEquals("Flags for $name", rObj.getInteger("flags"), view.flags)
            assertEquals("PublishWallNs for $name", rObj.getLong("publishWallNs"), view.publishWallNs)
            assertEquals("CaptureMonoMs for $name", rObj.getLong("captureMonoMs"), view.captureMonoMs)
            assertEquals("ExpirySec for $name", rObj.getLong("expirySec"), view.expirySec)
            assertEquals("PayloadFormat for $name", rObj.getInteger("payloadFormat").toByte(), view.payloadFormat)

            if (!view.skipped()) {
                assertEquals("Topic for $name", rObj.getString("topic"), view.topicString())
                assertEquals("ClientID for $name", rObj.getString("clientId"), view.clientIDString())
                assertEquals("Username for $name", rObj.getString("username"), view.usernameString())
                assertEquals("ContentType for $name", rObj.getString("contentType"), view.toRecord().contentType)
                assertEquals("ResponseTopic for $name", rObj.getString("responseTopic"), view.toRecord().responseTopic)

                val corrHex = rObj.getString("correlationDataHex") ?: ""
                assertEquals("CorrelationData for $name", corrHex, hexFormat.formatHex(view.toRecord().correlationData))

                val payloadHex = rObj.getString("payloadHex") ?: ""
                assertEquals("Payload for $name", payloadHex, hexFormat.formatHex(view.payload))

                val r = view.toRecord()
                val encodedSize = recordSize(r)
                assertEquals("Encoded size for $name", raw.size, encodedSize)

                val dst = ByteArray(encodedSize)
                encodeRecord(dst, r)
                assertEquals("Record byte-identical re-encode for $name", hex, hexFormat.formatHex(dst))
            } else {
                // Tombstone re-encode
                val tomb = ByteArray(TombstoneLen)
                putTombstone(tomb, raw)
                assertEquals("Tombstone byte-identical re-encode for $name", hex, hexFormat.formatHex(tomb))
            }
        }
    }

    @Test
    fun testBatches() {
        val vj = loadVectors()
        val batches = vj.getJsonArray("batches")
        for (i in 0 until batches.size()) {
            val bObj = batches.getJsonObject(i)
            val name = bObj.getString("name")
            val hex = bObj.getString("hex")
            val raw = hexFormat.parseHex(hex)

            val body = raw.copyOfRange(FrameHeaderLen, raw.size)
            val b = Batch()
            b.decode(body)

            assertEquals("BaseOffset for $name", bObj.getLong("baseOffset"), b.header.baseOffset)
            assertEquals("Count for $name", bObj.getInteger("count"), b.header.count)
            assertEquals("RecordsBytes for $name", bObj.getInteger("recordsBytes"), b.header.recordsBytes)
            assertEquals("Leo for $name", bObj.getLong("leo"), b.header.leo)
            assertEquals("CRC32C for $name", bObj.getLong("crc32c").toInt(), b.header.crc32c)

            assertTrue("CRC valid for $name", b.isCRCValid())

            val reEncoded = b.encode()
            assertEquals("Batch re-encode for $name", hex, hexFormat.formatHex(reEncoded))

            // Iterate records in batch
            val it = b.iter()
            var count = 0
            val rv = RecordView()
            while (true) {
                val (has, err) = it.next(rv)
                if (!has) break
                assertNull("Record error in valid batch", err)
                count++
            }
            assertEquals("Batch iterated record count", b.header.count, count)
        }
    }

    @Test
    fun testMACs() {
        val vj = loadVectors()
        val macs = vj.getJsonArray("macs")
        for (i in 0 until macs.size()) {
            val mObj = macs.getJsonObject(i)
            val name = mObj.getString("name")
            val isConsumer = mObj.getBoolean("isConsumer")
            val secret = hexFormat.parseHex(mObj.getString("secretHex"))
            val nonceS = hexFormat.parseHex(mObj.getString("nonceSHex"))
            val nonceC = hexFormat.parseHex(mObj.getString("nonceCHex"))
            val consumerId = mObj.getString("consumerId")
            val sourceId = mObj.getString("sourceId")
            val exporter = hexFormat.parseHex(mObj.getString("exporterHex"))
            val expectedInputHex = mObj.getString("inputHex")
            val expectedMacHex = mObj.getString("expectedMacHex")

            val input = if (isConsumer) {
                consumerMACInput(nonceS, nonceC, consumerId, sourceId, exporter)
            } else {
                sourceMACInput(nonceC, nonceS, sourceId, consumerId, exporter)
            }

            assertEquals("Input hex for $name", expectedInputHex, hexFormat.formatHex(input))

            val computedMac = mac(secret, input)
            assertEquals("MAC hex for $name", expectedMacHex, hexFormat.formatHex(computedMac))

            val matchIdx = matchMAC(listOf(secret), input, computedMac)
            assertEquals("MatchMAC index for $name", 0, matchIdx)
        }
    }

    @Test
    fun testInvalidCases() {
        val vj = loadVectors()
        val invalid = vj.getJsonArray("invalid")
        for (i in 0 until invalid.size()) {
            val inv = invalid.getJsonObject(i)
            val category = inv.getString("category")
            val name = inv.getString("name")
            val hex = inv.getString("hex")
            val raw = hexFormat.parseHex(hex)

            if (category == "frame") {
                val reader = FrameReader(ByteArrayInputStream(raw))
                try {
                    reader.readFrame()
                    fail("Expected exception for frame $name")
                } catch (e: Exception) {
                    // Frame empty or short
                    assertTrue(
                        "Expected WireException or EOFException for $name but got ${e.javaClass.name}",
                        e is WireException || e is java.io.EOFException
                    )
                }
            } else if (category == "record") {
                val view = RecordView()
                try {
                    decodeRecord(raw, view)
                    fail("Expected exception for record $name")
                } catch (e: MalformedRecordException) {
                    // Success: malformed record exception caught
                }
            }
        }
    }

    @Test
    fun testTruncUTF8() {
        assertEquals("abc", truncUTF8("abc", 5))
        assertEquals("abc", truncUTF8("abc", 3))
        assertEquals("ab", truncUTF8("abc", 2))

        // Multi-byte: "€" is 3 bytes (0xE2, 0x82, 0xAC)
        val euro = "€"
        assertEquals("€", truncUTF8(euro, 3))
        assertEquals("", truncUTF8(euro, 2))
        assertEquals("", truncUTF8(euro, 1))

        val combined = "a€b"
        assertEquals("a€b", truncUTF8(combined, 5))
        assertEquals("a€", truncUTF8(combined, 4)) // fits "a" (1) + "€" (3) = 4 bytes, drops "b"
        assertEquals("a", truncUTF8(combined, 3))  // drops partial euro
        assertEquals("a", truncUTF8(combined, 2))
        assertEquals("a", truncUTF8(combined, 1))
    }
}
