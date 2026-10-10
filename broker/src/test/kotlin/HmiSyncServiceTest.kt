package at.rocworks

import at.rocworks.data.BrokerMessage
import at.rocworks.handlers.HmiSyncService
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.nio.file.Files
import java.security.MessageDigest
import java.util.Base64
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream

class HmiSyncServiceTest {

    private lateinit var vertx: Vertx
    private lateinit var tempDir: File
    private lateinit var hmiDir: File
    private lateinit var syncService: HmiSyncService
    private val publishedDownstream = mutableListOf<Pair<String, JsonObject>>()

    @Before
    fun setUp() {
        vertx = Vertx.vertx()
        tempDir = Files.createTempDirectory("hmi-sync-test").toFile()
        hmiDir = File(tempDir, "data/hmi")
        hmiDir.mkdirs()

        val config = JsonObject().put("HMI", JsonObject().put("Path", hmiDir.absolutePath))
        syncService = HmiSyncService(
            vertx = vertx,
            config = config,
            sessionHandler = null,
            deviceStore = null,
            nodeId = "test-node",
            baseTopic = "monstermq/hmi/sync",
            publishFn = { topic, payload ->
                synchronized(publishedDownstream) {
                    publishedDownstream.add(Pair(topic, JsonObject(String(payload, Charsets.UTF_8))))
                }
            }
        )
        syncService.ensureInit()
    }

    @After
    fun tearDown() {
        val latch = CountDownLatch(1)
        vertx.close().onComplete { latch.countDown() }
        latch.await(5, TimeUnit.SECONDS)
        tempDir.deleteRecursively()
    }

    @Test
    fun testEnsureInit() {
        val metaFile = File(hmiDir, "metadata.json")
        assertTrue(metaFile.exists())
        val indexFile = File(hmiDir, "main/index.html")
        assertTrue(indexFile.exists())
        assertTrue(indexFile.readText().contains("MonsterMQ HMI Dashboard"))
        assertEquals("main", syncService.getMainDashboardName())
        assertEquals(listOf("main"), syncService.listDashboards())
    }

    @Test
    fun testPing() {
        val req = JsonObject().put("action", "ping").put("reqId", "p1")
        val resp = syncService.processRequestDirect("sess1", req)

        assertTrue(resp.getBoolean("success"))
        assertEquals("p1", resp.getString("reqId"))
        assertEquals("ping", resp.getString("action"))
        assertEquals("test-node", resp.getString("nodeId"))
        assertEquals("main", resp.getString("mainDashboard"))
        assertNotNull(resp.getString("brokerVersion"))
        val dashboards = resp.getJsonArray("dashboards")
        assertTrue(dashboards.contains("main"))
    }

    @Test
    fun testList() {
        val req = JsonObject().put("action", "list").put("reqId", "l1").put("dashboard", "main")
        val resp = syncService.processRequestDirect("sess1", req)

        assertTrue(resp.getBoolean("success"))
        assertEquals("main", resp.getString("dashboard"))
        val files = resp.getJsonArray("files")
        assertTrue(files.size() >= 1)
        val firstFile = files.getJsonObject(0)
        assertEquals("index.html", firstFile.getString("path"))
        assertTrue(firstFile.getLong("sizeBytes") > 0)
        assertNotNull(firstFile.getString("sha256"))
    }

    @Test
    fun testExport() {
        val req = JsonObject().put("action", "export").put("reqId", "e1").put("dashboard", "main")
        val resp = syncService.processRequestDirect("sess1", req)

        assertTrue(resp.getBoolean("success"))
        val zipB64 = resp.getString("zipBase64")
        assertNotNull(zipB64)
        val zipBytes = Base64.getDecoder().decode(zipB64)
        assertTrue(zipBytes.isNotEmpty())

        var foundIndex = false
        ZipInputStream(ByteArrayInputStream(zipBytes)).use { zis ->
            var entry = zis.nextEntry
            while (entry != null) {
                if (entry.name == "index.html") foundIndex = true
                entry = zis.nextEntry
            }
        }
        assertTrue("index.html must be in exported zip", foundIndex)
    }

    @Test
    fun testWriteAndRead() {
        val content = "console.log('sensor widget active');"
        val contentB64 = Base64.getEncoder().encodeToString(content.toByteArray())
        val sha256 = calculateSha256Hex(content.toByteArray())

        // 1. Write file
        val writeReq = JsonObject()
            .put("action", "write")
            .put("reqId", "w1")
            .put("dashboard", "main")
            .put("path", "widget.js")
            .put("contentBase64", contentB64)
            .put("sha256", sha256)
        val writeResp = syncService.processRequestDirect("sess1", writeReq)

        assertTrue(writeResp.getBoolean("success"))
        assertEquals(content.length.toLong(), writeResp.getLong("bytesWritten"))

        // Verify on disk
        val diskFile = File(hmiDir, "main/widget.js")
        assertTrue(diskFile.exists())
        assertEquals(content, diskFile.readText())

        // 2. Read file
        val readReq = JsonObject()
            .put("action", "read")
            .put("reqId", "r1")
            .put("dashboard", "main")
            .put("path", "widget.js")
        val readResp = syncService.processRequestDirect("sess1", readReq)

        assertTrue(readResp.getBoolean("success"))
        assertEquals(contentB64, readResp.getString("contentBase64"))
        assertEquals(sha256, readResp.getString("sha256"))
        assertEquals(content.length.toLong(), readResp.getLong("sizeBytes"))
    }

    @Test
    fun testWriteShaMismatch() {
        val content = "test content"
        val contentB64 = Base64.getEncoder().encodeToString(content.toByteArray())
        val writeReq = JsonObject()
            .put("action", "write")
            .put("reqId", "w2")
            .put("dashboard", "main")
            .put("path", "mismatch.js")
            .put("contentBase64", contentB64)
            .put("sha256", "0000000000000000000000000000000000000000000000000000000000000000")
        val writeResp = syncService.processRequestDirect("sess1", writeReq)

        assertFalse(writeResp.getBoolean("success"))
        assertTrue(writeResp.getString("error").contains("sha256 mismatch"))
    }

    @Test
    fun testDirectoryTraversalDefense() {
        val writeReq = JsonObject()
            .put("action", "write")
            .put("reqId", "t1")
            .put("dashboard", "main")
            .put("path", "../../outside.txt")
            .put("contentBase64", Base64.getEncoder().encodeToString("bad".toByteArray()))
        val writeResp = syncService.processRequestDirect("sess1", writeReq)

        assertFalse(writeResp.getBoolean("success"))
        assertTrue(writeResp.getString("error").contains("access denied"))
    }

    @Test
    fun testDelete() {
        val diskFile = File(hmiDir, "main/delete-me.txt")
        diskFile.writeText("to be deleted")
        assertTrue(diskFile.exists())

        val delReq = JsonObject()
            .put("action", "delete")
            .put("reqId", "d1")
            .put("dashboard", "main")
            .put("path", "delete-me.txt")
        val delResp = syncService.processRequestDirect("sess1", delReq)

        assertTrue(delResp.getBoolean("success"))
        assertFalse(diskFile.exists())
    }

    @Test
    fun testImport() {
        // Create in-memory zip with 2 files
        val baos = ByteArrayOutputStream()
        ZipOutputStream(baos).use { zos ->
            zos.putNextEntry(ZipEntry("page.html"))
            zos.write("<h1>Imported</h1>".toByteArray())
            zos.closeEntry()

            zos.putNextEntry(ZipEntry("sub/style.css"))
            zos.write("body { background: black; }".toByteArray())
            zos.closeEntry()
        }
        val zipB64 = Base64.getEncoder().encodeToString(baos.toByteArray())

        val importReq = JsonObject()
            .put("action", "import")
            .put("reqId", "i1")
            .put("dashboard", "imported-dash")
            .put("zipBase64", zipB64)
            .put("setAsMain", false)
        val importResp = syncService.processRequestDirect("sess1", importReq)

        assertTrue(importResp.getBoolean("success"))
        val importedPage = File(hmiDir, "imported-dash/page.html")
        assertTrue(importedPage.exists())
        assertEquals("<h1>Imported</h1>", importedPage.readText())

        val importedCss = File(hmiDir, "imported-dash/sub/style.css")
        assertTrue(importedCss.exists())
        assertEquals("body { background: black; }", importedCss.readText())

        assertTrue(syncService.listDashboards().contains("imported-dash"))
    }

    @Test
    fun testHandlePacketEndToEnd() {
        val latch = CountDownLatch(1)
        val sessionUUID = "sess-full-123"
        val req = JsonObject().put("action", "ping").put("reqId", "req-test-99")
        val topic = "monstermq/hmi/sync/$sessionUUID/upstream"
        val msg = BrokerMessage("cli-test", topic, req.encode())

        syncService.handlePacket(msg)

        // Wait for async execution
        var receivedTopic: String? = null
        var receivedResp: JsonObject? = null
        for (i in 0 until 50) {
            synchronized(publishedDownstream) {
                if (publishedDownstream.isNotEmpty()) {
                    val p = publishedDownstream.first()
                    receivedTopic = p.first
                    receivedResp = p.second
                    latch.countDown()
                }
            }
            if (latch.count == 0L) break
            Thread.sleep(50)
        }

        assertEquals("monstermq/hmi/sync/$sessionUUID/downstream", receivedTopic)
        assertNotNull(receivedResp)
        assertTrue(receivedResp!!.getBoolean("success"))
        assertEquals("req-test-99", receivedResp.getString("reqId"))
        assertEquals("ping", receivedResp.getString("action"))
    }

    private fun calculateSha256Hex(bytes: ByteArray): String {
        val md = MessageDigest.getInstance("SHA-256")
        val digest = md.digest(bytes)
        return digest.joinToString("") { "%02x".format(it) }
    }
}
