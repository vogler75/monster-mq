package at.rocworks.handlers

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.Version
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import java.util.Base64
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream

/**
 * Native MQTT file synchronization service for HMI dashboards.
 * Implements bidirectional file sync with `mmq hmi sync` CLI matching the edge broker protocol.
 */
class HmiSyncService(
    private val vertx: Vertx,
    private val config: JsonObject,
    private val sessionHandler: SessionHandler? = null,
    private val deviceStore: IDeviceConfigStore? = null,
    private val nodeId: String = "local",
    private val baseTopic: String = "monstermq/hmi/sync",
    private val publishFn: ((topic: String, payload: ByteArray) -> Unit)? = null
) {
    private val logger: Logger = Utils.getLogger(HmiSyncService::class.java)
    private val started = AtomicBoolean(false)
    private val listenerId = "hmi-sync-${Utils.getUuid()}"
    private val normalizedBaseTopic = baseTopic.trim().trimEnd('/')
    private val hmiPath: String = Monster.getHmiPath(config) ?: "./data/hmi"
    private val dashboardNameRegex = Regex("^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")

    companion object {
        const val DEFAULT_INDEX_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MonsterMQ HMI Dashboard</title>
    <style>
        * { box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; padding: 2rem; background: #0f172a; color: #f8fafc; margin: 0; }
        .card { background: #1e293b; padding: 1.5rem; border-radius: 8px; max-width: 640px; margin: 0 auto; box-shadow: 0 4px 6px rgba(0,0,0,0.3); border: 1px solid #334155; }
        h1 { margin-top: 0; color: #38bdf8; font-size: 1.5rem; }
        p { color: #94a3b8; font-size: 0.95rem; line-height: 1.5; }
        pre { background: #090d16; padding: 1rem; border-radius: 6px; overflow-x: auto; color: #a7f3d0; border: 1px solid #1e293b; font-size: 0.875rem; }
        button { background: #0284c7; color: white; border: none; padding: 0.6rem 1.2rem; border-radius: 6px; cursor: pointer; font-size: 0.95rem; font-weight: 500; transition: background 0.2s; }
        button:hover { background: #0369a1; }
    </style>
</head>
<body>
    <div class="card">
        <h1>MonsterMQ HMI Dashboard</h1>
        <p>This is the default HMI application served directly by MonsterMQ.</p>
        <button onclick="checkStatus()">Check Broker Status</button>
        <pre id="output">Click button to test GraphQL connection...</pre>
    </div>
    <script>
        async function checkStatus() {
            try {
                const res = await fetch('/graphql', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ query: '{ broker { nodeId version userManagementEnabled isLeader isCurrent enabledFeatures } }' })
                });
                const data = await res.json();
                document.getElementById('output').textContent = JSON.stringify(data, null, 2);
            } catch (err) {
                document.getElementById('output').textContent = 'Error: ' + err.message;
            }
        }
    </script>
</body>
</html>"""
    }

    fun start() {
        if (!started.compareAndSet(false, true)) return
        ensureInit()
        val filter = "$normalizedBaseTopic/+/upstream"
        if (sessionHandler != null) {
            sessionHandler.registerMessageListener(listenerId, listOf(filter)) { msg ->
                handlePacket(msg)
            }
        }
        logger.info("HMI MQTT file sync service started, filter: $filter")
    }

    fun stop() {
        if (!started.compareAndSet(true, false)) return
        if (sessionHandler != null) {
            sessionHandler.unregisterMessageListener(listenerId)
        }
        logger.info("HMI MQTT file sync service stopped")
    }

    fun ensureInit() {
        try {
            val baseDir = File(hmiPath).canonicalFile
            if (!baseDir.exists()) {
                baseDir.mkdirs()
            }
            val metaFile = File(baseDir, "metadata.json")
            if (!metaFile.exists()) {
                val meta = JsonObject().put("mainDashboard", "main")
                metaFile.writeText(meta.encodePrettily())
            }
            val mainDir = File(baseDir, "main")
            if (!mainDir.exists()) {
                mainDir.mkdirs()
            }
            val indexFile = File(mainDir, "index.html")
            if (!indexFile.exists()) {
                indexFile.writeText(DEFAULT_INDEX_HTML)
            }
            if (deviceStore != null) {
                deviceStore.getDevice("main").onComplete { res ->
                    if (res.succeeded() && res.result() == null) {
                        val dev = DeviceConfig(
                            name = "main",
                            namespace = "main",
                            nodeId = "local",
                            type = DeviceConfig.DEVICE_TYPE_HMI,
                            enabled = true,
                            config = JsonObject()
                                .put("isMain", true)
                                .put("urlPath", "")
                                .put("entryPoint", "index.html")
                                .put("title", "main")
                        )
                        deviceStore.saveDevice(dev)
                    }
                }
            }
        } catch (e: Exception) {
            logger.warning("HmiSyncService ensureInit error: ${e.message}")
        }
    }

    fun getMainDashboardName(): String {
        val metaFile = File(hmiPath, "metadata.json")
        if (metaFile.exists()) {
            try {
                val json = JsonObject(metaFile.readText())
                val name = json.getString("mainDashboard")
                if (!name.isNullOrBlank()) return name
            } catch (_: Exception) {}
        }
        return "main"
    }

    fun setMainDashboardName(dashName: String) {
        try {
            val metaFile = File(hmiPath, "metadata.json")
            val metaJson = if (metaFile.exists()) {
                try { JsonObject(metaFile.readText()) } catch (_: Exception) { JsonObject() }
            } else JsonObject()
            metaJson.put("mainDashboard", dashName)
            metaFile.writeText(metaJson.encodePrettily())
        } catch (e: Exception) {
            logger.warning("Failed to save metadata.json: ${e.message}")
        }
    }

    fun listDashboards(): List<String> {
        val baseDir = File(hmiPath).canonicalFile
        if (!baseDir.exists() || !baseDir.isDirectory) return listOf("main")
        val dirs = baseDir.listFiles()
            ?.filter { it.isDirectory && dashboardNameRegex.matches(it.name) }
            ?.map { it.name }
            ?.sorted() ?: emptyList()
        return if (dirs.isEmpty()) listOf("main") else dirs
    }

    fun handlePacket(msg: BrokerMessage) {
        val topic = msg.topicName
        val prefix = "$normalizedBaseTopic/"
        val suffix = "/upstream"
        if (!topic.startsWith(prefix) || !topic.endsWith(suffix)) return

        val sessionUUID = topic.substring(prefix.length, topic.length - suffix.length).trim()
        if (sessionUUID.isEmpty() || sessionUUID.contains('/')) return

        val payloadStr = try {
            msg.getPayloadAsString()
        } catch (e: Exception) {
            sendDownstream(sessionUUID, JsonObject()
                .put("action", "error")
                .put("success", false)
                .put("error", "invalid payload: ${e.message}"))
            return
        }

        val req = try {
            JsonObject(payloadStr)
        } catch (e: Exception) {
            logger.warning("Invalid HMI sync JSON payload from session $sessionUUID: ${e.message}")
            sendDownstream(sessionUUID, JsonObject()
                .put("action", "error")
                .put("success", false)
                .put("error", "invalid JSON payload: ${e.message}"))
            return
        }

        vertx.executeBlocking(java.util.concurrent.Callable {
            try {
                processRequest(sessionUUID, req)
            } catch (e: Exception) {
                logger.warning("Error processing HMI sync request: ${e.message}")
                val resp = JsonObject()
                    .put("action", req.getString("action", "error"))
                    .put("reqId", req.getString("reqId", ""))
                    .put("success", false)
                    .put("error", e.message ?: "Internal error")
                sendDownstream(sessionUUID, resp)
            }
        })
    }

    fun processRequestDirect(sessionUUID: String, req: JsonObject): JsonObject {
        val action = req.getString("action", "")
        val reqId = req.getString("reqId", "")
        val resp = JsonObject()
            .put("action", action)
            .put("reqId", reqId)

        when (action.lowercase()) {
            "ping" -> handlePing(resp)
            "list" -> handleList(req, resp)
            "export" -> handleExport(req, resp)
            "read" -> handleRead(req, resp)
            "write" -> handleWrite(req, resp)
            "delete" -> handleDelete(req, resp)
            "import" -> handleImport(req, resp)
            else -> {
                resp.put("success", false)
                resp.put("error", "unsupported action \"$action\"")
            }
        }
        return resp
    }

    private fun processRequest(sessionUUID: String, req: JsonObject) {
        val resp = processRequestDirect(sessionUUID, req)
        sendDownstream(sessionUUID, resp)
    }

    private fun handlePing(resp: JsonObject) {
        resp.put("success", true)
        resp.put("nodeId", nodeId)
        resp.put("brokerVersion", Version.getVersion())
        resp.put("mainDashboard", getMainDashboardName())
        resp.put("dashboards", JsonArray(listDashboards()))
    }

    private fun handleList(req: JsonObject, resp: JsonObject) {
        var dashName = req.getString("dashboard")?.trim() ?: ""
        if (dashName.isEmpty()) {
            dashName = getMainDashboardName()
        }
        resp.put("dashboard", dashName)

        val dashDir = try {
            resolveDashboardDir(dashName)
        } catch (e: Exception) {
            resp.put("success", false)
            resp.put("error", e.message)
            return
        }

        if (!dashDir.exists() || !dashDir.isDirectory) {
            resp.put("success", false)
            resp.put("error", "dashboard \"$dashName\" not found")
            return
        }

        val entries = JsonArray()
        val baseCanonical = dashDir.canonicalFile
        dashDir.walkTopDown().filter { it.isFile }.forEach { file ->
            val canonical = file.canonicalFile
            val relPath = canonical.path
                .removePrefix(baseCanonical.path)
                .removePrefix(File.separator)
                .replace('\\', '/')
            val sizeBytes = file.length()
            val modTime = file.lastModified() / 1000
            val sha256 = calculateSha256Hex(file.readBytes())

            entries.add(JsonObject()
                .put("path", relPath)
                .put("sizeBytes", sizeBytes)
                .put("sha256", sha256)
                .put("modTime", modTime)
            )
        }

        resp.put("success", true)
        resp.put("files", entries)
        resp.put("fileCount", entries.size())
    }

    private fun handleExport(req: JsonObject, resp: JsonObject) {
        var dashName = req.getString("dashboard")?.trim() ?: ""
        if (dashName.isEmpty()) {
            dashName = getMainDashboardName()
        }
        resp.put("dashboard", dashName)

        val dashDir = try {
            resolveDashboardDir(dashName)
        } catch (e: Exception) {
            resp.put("success", false)
            resp.put("error", e.message)
            return
        }

        if (!dashDir.exists() || !dashDir.isDirectory) {
            resp.put("success", false)
            resp.put("error", "dashboard \"$dashName\" not found")
            return
        }

        val baos = ByteArrayOutputStream()
        var fileCount = 0
        var totalBytes = 0L
        val baseCanonical = dashDir.canonicalFile
        ZipOutputStream(baos).use { zos ->
            dashDir.walkTopDown().filter { it.isFile }.forEach { file ->
                val canonical = file.canonicalFile
                val relPath = canonical.path
                    .removePrefix(baseCanonical.path)
                    .removePrefix(File.separator)
                    .replace('\\', '/')
                val entry = ZipEntry(relPath)
                zos.putNextEntry(entry)
                file.inputStream().use { input -> input.copyTo(zos) }
                zos.closeEntry()
                fileCount++
                totalBytes += file.length()
            }
        }

        val zipB64 = Base64.getEncoder().encodeToString(baos.toByteArray())
        resp.put("success", true)
        resp.put("zipBase64", zipB64)
        resp.put("fileCount", fileCount)
        resp.put("sizeBytes", totalBytes)
    }

    private fun handleRead(req: JsonObject, resp: JsonObject) {
        var dashName = req.getString("dashboard")?.trim() ?: ""
        if (dashName.isEmpty()) {
            dashName = getMainDashboardName()
        }
        val path = req.getString("path", "")
        resp.put("dashboard", dashName)
        resp.put("path", path)

        if (path.isBlank()) {
            resp.put("success", false)
            resp.put("error", "path cannot be empty")
            return
        }

        val file = try {
            resolveSafeFile(dashName, path)
        } catch (e: Exception) {
            resp.put("success", false)
            resp.put("error", e.message)
            return
        }

        if (!file.exists() || !file.isFile) {
            resp.put("success", false)
            resp.put("error", "file not found: $path")
            return
        }

        val data = file.readBytes()
        val sha256 = calculateSha256Hex(data)
        resp.put("success", true)
        resp.put("contentBase64", Base64.getEncoder().encodeToString(data))
        resp.put("sha256", sha256)
        resp.put("sizeBytes", data.size.toLong())
    }

    private fun handleWrite(req: JsonObject, resp: JsonObject) {
        var dashName = req.getString("dashboard")?.trim() ?: ""
        if (dashName.isEmpty()) {
            dashName = getMainDashboardName()
        }
        val path = req.getString("path", "")
        resp.put("dashboard", dashName)
        resp.put("path", path)

        if (path.isBlank()) {
            resp.put("success", false)
            resp.put("error", "path cannot be empty")
            return
        }

        val contentB64 = req.getString("contentBase64", "")
        val data = try {
            Base64.getDecoder().decode(contentB64)
        } catch (e: Exception) {
            resp.put("success", false)
            resp.put("error", "invalid base64 content: ${e.message}")
            return
        }

        val expectedSha = req.getString("sha256")?.trim()?.lowercase()
        if (!expectedSha.isNullOrEmpty()) {
            val actualSha = calculateSha256Hex(data).lowercase()
            if (actualSha != expectedSha) {
                resp.put("success", false)
                resp.put("error", "sha256 mismatch: expected $expectedSha, got $actualSha")
                return
            }
        }

        val targetFile = try {
            resolveSafeFile(dashName, path)
        } catch (e: Exception) {
            resp.put("success", false)
            resp.put("error", e.message)
            return
        }

        try {
            targetFile.parentFile?.mkdirs()
            val tempFile = File(targetFile.parentFile, ".tmp." + Utils.getUuid())
            tempFile.writeBytes(data)
            try {
                Files.move(tempFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
            } catch (_: Exception) {
                Files.move(tempFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING)
            }
        } catch (e: Exception) {
            resp.put("success", false)
            resp.put("error", e.message ?: "Failed to write file")
            return
        }

        ensureDeviceConfig(dashName)
        resp.put("success", true)
        resp.put("bytesWritten", data.size.toLong())
    }

    private fun handleDelete(req: JsonObject, resp: JsonObject) {
        var dashName = req.getString("dashboard")?.trim() ?: ""
        if (dashName.isEmpty()) {
            dashName = getMainDashboardName()
        }
        val path = req.getString("path", "")
        resp.put("dashboard", dashName)
        resp.put("path", path)

        if (path.isBlank()) {
            resp.put("success", false)
            resp.put("error", "path cannot be empty")
            return
        }

        val targetFile = try {
            resolveSafeFile(dashName, path)
        } catch (e: Exception) {
            resp.put("success", false)
            resp.put("error", e.message)
            return
        }

        if (targetFile.exists()) {
            val ok = if (targetFile.isDirectory) targetFile.deleteRecursively() else targetFile.delete()
            if (!ok) {
                resp.put("success", false)
                resp.put("error", "failed to delete file: $path")
                return
            }
        }

        resp.put("success", true)
    }

    private fun handleImport(req: JsonObject, resp: JsonObject) {
        var dashName = req.getString("dashboard")?.trim() ?: ""
        if (dashName.isEmpty()) {
            dashName = getMainDashboardName()
        }
        val zipB64 = req.getString("zipBase64", "")
        val setAsMain = req.getBoolean("setAsMain", false)
        resp.put("dashboard", dashName)

        if (zipB64.isBlank()) {
            resp.put("success", false)
            resp.put("error", "zipBase64 cannot be empty")
            return
        }

        val zipBytes = try {
            Base64.getDecoder().decode(zipB64)
        } catch (e: Exception) {
            resp.put("success", false)
            resp.put("error", "invalid base64 zip: ${e.message}")
            return
        }

        val dashDir = try {
            resolveDashboardDir(dashName)
        } catch (e: Exception) {
            resp.put("success", false)
            resp.put("error", e.message)
            return
        }

        if (dashDir.exists()) {
            dashDir.deleteRecursively()
        }
        dashDir.mkdirs()

        val canonicalDashDir = dashDir.canonicalFile
        try {
            ZipInputStream(ByteArrayInputStream(zipBytes)).use { zis ->
                var entry = zis.nextEntry
                while (entry != null) {
                    val entryFile = File(dashDir, entry.name).canonicalFile
                    if (!entryFile.path.startsWith(canonicalDashDir.path)) {
                        entry = zis.nextEntry
                        continue
                    }
                    if (entry.isDirectory) {
                        entryFile.mkdirs()
                    } else {
                        entryFile.parentFile?.mkdirs()
                        entryFile.outputStream().use { os -> zis.copyTo(os) }
                    }
                    zis.closeEntry()
                    entry = zis.nextEntry
                }
            }
        } catch (e: Exception) {
            resp.put("success", false)
            resp.put("error", "failed to extract zip: ${e.message}")
            return
        }

        if (setAsMain) {
            setMainDashboardName(dashName)
            unsetOtherMainHmis(dashName)
        }

        saveHmiDevice(dashName, setAsMain)
        resp.put("success", true)
    }

    private fun sendDownstream(sessionUUID: String, resp: JsonObject) {
        val topic = "$normalizedBaseTopic/$sessionUUID/downstream"
        val payload = resp.encode().toByteArray(Charsets.UTF_8)
        if (publishFn != null) {
            publishFn.invoke(topic, payload)
        } else if (sessionHandler != null) {
            val brokerMsg = BrokerMessage(
                messageUuid = Utils.getUuid(),
                messageId = 0,
                topicName = topic,
                payload = payload,
                qosLevel = 1,
                isRetain = false,
                isDup = false,
                isQueued = false,
                clientId = "hmi-sync"
            )
            sessionHandler.publishMessage(brokerMsg)
        }
    }

    private fun validateDashboardName(name: String) {
        if (!dashboardNameRegex.matches(name)) {
            throw IllegalArgumentException("invalid dashboard name: \"$name\"")
        }
    }

    private fun resolveDashboardDir(dashName: String): File {
        validateDashboardName(dashName)
        val base = File(hmiPath).canonicalFile
        val dashDir = File(base, dashName).canonicalFile
        if (!dashDir.toPath().startsWith(base.toPath()) || dashDir == base) {
            throw SecurityException("access denied: outside of HMI base directory")
        }
        return dashDir
    }

    private fun resolveSafeFile(dashName: String, relPath: String): File {
        val dashDir = resolveDashboardDir(dashName)
        val cleanPath = relPath.replace('\\', '/').trim().trimStart('/')
        if (cleanPath.isEmpty()) {
            throw IllegalArgumentException("path cannot be empty")
        }
        val targetFile = File(dashDir, cleanPath).canonicalFile
        if (!targetFile.toPath().startsWith(dashDir.toPath())) {
            throw SecurityException("access denied: outside of dashboard directory")
        }
        return targetFile
    }

    private fun ensureDeviceConfig(dashName: String) {
        if (deviceStore == null) return
        deviceStore.getDevice(dashName).onComplete { res ->
            if (res.succeeded() && res.result() == null) {
                val isMain = dashName == getMainDashboardName()
                val dev = DeviceConfig(
                    name = dashName,
                    namespace = dashName,
                    nodeId = "local",
                    type = DeviceConfig.DEVICE_TYPE_HMI,
                    enabled = true,
                    config = JsonObject()
                        .put("isMain", isMain)
                        .put("urlPath", if (isMain) "" else dashName)
                        .put("entryPoint", "index.html")
                        .put("title", dashName)
                )
                deviceStore.saveDevice(dev)
            }
        }
    }

    private fun saveHmiDevice(dashName: String, isMain: Boolean) {
        if (deviceStore == null) return
        val config = JsonObject()
            .put("isMain", isMain)
            .put("urlPath", if (isMain) "" else dashName)
            .put("entryPoint", "index.html")
            .put("title", dashName)

        val device = DeviceConfig(
            name = dashName,
            namespace = dashName,
            nodeId = "local",
            type = DeviceConfig.DEVICE_TYPE_HMI,
            enabled = true,
            config = config
        )
        deviceStore.saveDevice(device)
    }

    private fun unsetOtherMainHmis(currentName: String) {
        if (deviceStore == null) return
        deviceStore.getDevicesByType(DeviceConfig.DEVICE_TYPE_HMI).onComplete { res ->
            if (res.succeeded()) {
                res.result().forEach { dev ->
                    if (dev.name != currentName && dev.config.getBoolean("isMain", false)) {
                        dev.config.put("isMain", false)
                        if (dev.config.getString("urlPath").isNullOrEmpty()) {
                            dev.config.put("urlPath", dev.name)
                        }
                        deviceStore.saveDevice(dev)
                    }
                }
            }
        }
    }

    private fun calculateSha256Hex(bytes: ByteArray): String {
        val md = MessageDigest.getInstance("SHA-256")
        val digest = md.digest(bytes)
        return digest.joinToString("") { "%02x".format(it) }
    }
}
