package at.rocworks

import at.rocworks.data.BrokerMessage
import at.rocworks.stores.PayloadFormat
import at.rocworks.stores.sqlite.MessageArchiveSQLite
import at.rocworks.stores.sqlite.MessageStoreSQLite
import at.rocworks.stores.sqlite.SQLiteVerticle
import io.vertx.core.AbstractVerticle
import io.vertx.core.Vertx
import org.junit.After
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.io.File
import java.sql.DriverManager
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Base64
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * Byte array parameters reach SQLite as BLOBs, not as the base64 text a Vert.x JsonArray turns
 * them into when iterated. Same storage layout as monster-mq-edge internal/stores/sqlite.
 */
class SQLiteBinaryPayloadTest {

    private lateinit var vertx: Vertx
    private lateinit var dbFile: File

    // Not valid UTF-8, and its base64url form contains '-' and '_'.
    private val binary = byteArrayOf(0xFB.toByte(), 0xFF.toByte(), 0x00, 0x3E, 0x3F)

    private val day = Instant.now().truncatedTo(ChronoUnit.DAYS)

    @Before
    fun setUp() {
        vertx = Vertx.vertx()
        dbFile = File.createTempFile("monstermq-blob-", ".db")
        dbFile.deleteOnExit()
        deploy(SQLiteVerticle(), "sqlite verticle")
    }

    @After
    fun tearDown() {
        val latch = CountDownLatch(1)
        vertx.close().onComplete { latch.countDown() }
        latch.await(10, TimeUnit.SECONDS)
    }

    @Test
    fun archiveStoresDefaultFormatPayloadAsBlob() {
        val archive = MessageArchiveSQLite("BlobTestArchive", dbFile.absolutePath, PayloadFormat.DEFAULT)
        deploy(archive, "sqlite archive")
        val time = day.plus(12, ChronoUnit.HOURS)
        archive.addHistory(listOf(message("blob/number", "42".toByteArray(), time)))

        assertEquals("blob" to "42", storedBlob("blobtestarchive", "payload_blob", "blob/number")
            .let { (type, bytes) -> type to String(bytes) })

        val history = archive.getHistory("blob/number", null, null, 10)
        assertEquals(1, history.size())
        assertEquals("42", history.getJsonObject(0).getValue("payload"))

        val agg = archive.getAggregatedHistory(listOf("blob/number"), day, day.plus(1, ChronoUnit.DAYS), 1440,
            listOf("AVG", "COUNT"), emptyList())
        assertEquals(listOf("timestamp", "blob/number_avg", "blob/number_count"), agg.getJsonArray("columns").list)
        val row = agg.getJsonArray("rows").getJsonArray(0)
        assertEquals(42.0, row.getDouble(1), 0.0)
        assertEquals(1.0, row.getDouble(2), 0.0)
    }

    @Test
    fun archiveRoundTripsBinaryPayload() {
        val archive = MessageArchiveSQLite("BlobTestBinaryArchive", dbFile.absolutePath, PayloadFormat.JSON)
        deploy(archive, "sqlite archive")
        archive.addHistory(listOf(message("blob/binary", binary, day.plus(12, ChronoUnit.HOURS))))

        val (type, bytes) = storedBlob("blobtestbinaryarchive", "payload_blob", "blob/binary")
        assertEquals("blob", type)
        assertArrayEquals(binary, bytes)

        val row = archive.getHistory("blob/binary", null, null, 10).getJsonObject(0)
        assertEquals(Base64.getEncoder().encodeToString(binary), row.getString("payload_base64"))
    }

    @Test
    fun retainedStoreRoundTripsBinaryPayload() {
        val store = MessageStoreSQLite("BlobTestRetained", dbFile.absolutePath)
        deploy(store, "message store")
        store.addAll(listOf(message("retained/binary", binary, Instant.now())))

        val fetched = poll { store.get("retained/binary") }
        assertNotNull("retained message not found", fetched)
        assertArrayEquals(binary, fetched!!.payload)
        val (type, bytes) = storedBlob("blobtestretained", "payload_blob", "retained/binary")
        assertEquals("blob", type)
        assertArrayEquals(binary, bytes)
    }

    private fun message(topic: String, payload: ByteArray, time: Instant) = BrokerMessage(
        topicName = topic,
        payload = payload,
        qosLevel = 1,
        isRetain = true,
        clientId = "blob-test",
        time = time
    )

    // typeof() and raw bytes of the payload column, read straight from the file.
    private fun storedBlob(table: String, column: String, topic: String): Pair<String, ByteArray> =
        DriverManager.getConnection("jdbc:sqlite:${dbFile.absolutePath}").use { conn ->
            conn.prepareStatement("SELECT typeof($column), $column FROM $table WHERE topic = ?").use { stmt ->
                stmt.setString(1, topic)
                val rs = stmt.executeQuery()
                assertTrue("no row for $topic in $table", rs.next())
                rs.getString(1) to rs.getBytes(2)
            }
        }

    private fun <T> poll(read: () -> T?): T? {
        val deadline = System.currentTimeMillis() + 5_000
        while (System.currentTimeMillis() < deadline) {
            read()?.let { return it }
            Thread.sleep(25)
        }
        return null
    }

    private fun deploy(verticle: AbstractVerticle, label: String) {
        val latch = CountDownLatch(1)
        val errorRef = AtomicReference<Throwable?>()
        vertx.deployVerticle(verticle).onComplete { result ->
            if (result.failed()) errorRef.set(result.cause())
            latch.countDown()
        }
        assertTrue("Timed out deploying $label", latch.await(30, TimeUnit.SECONDS))
        errorRef.get()?.let { throw AssertionError("Failed to deploy $label", it) }
    }
}
