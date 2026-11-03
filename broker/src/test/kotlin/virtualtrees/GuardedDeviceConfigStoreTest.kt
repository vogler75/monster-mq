package at.rocworks.virtualtrees

import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.ImportDeviceConfigResult
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.util.concurrent.TimeUnit

/** Generic device store operations on tree rows must go through [VirtualTopicTreeManager]. */
class GuardedDeviceConfigStoreTest {

    private class MemoryDeviceStore : IDeviceConfigStore {
        val rows = LinkedHashMap<String, DeviceConfig>()
        override fun initialize(): Future<Void> = Future.succeededFuture()
        override fun getAllDevices() = Future.succeededFuture(rows.values.toList())
        override fun getDevicesByType(type: String) = Future.succeededFuture(rows.values.filter { it.type == type })
        override fun getDevicesByNode(nodeId: String) = Future.succeededFuture(rows.values.filter { it.nodeId == nodeId })
        override fun getDevice(name: String): Future<DeviceConfig?> = Future.succeededFuture(rows[name])
        override fun isNamespaceInUse(namespace: String, excludeName: String?) =
            Future.succeededFuture(rows.values.any { it.namespace == namespace && it.name != excludeName })
        override fun saveDevice(device: DeviceConfig): Future<DeviceConfig> { rows[device.name] = device; return Future.succeededFuture(device) }
        override fun deleteDevice(name: String) = Future.succeededFuture(rows.remove(name) != null)
        override fun toggleDevice(name: String, enabled: Boolean): Future<DeviceConfig?> =
            Future.succeededFuture(rows[name]?.copy(enabled = enabled)?.also { rows[name] = it })
        override fun reassignDevice(name: String, nodeId: String): Future<DeviceConfig?> =
            Future.succeededFuture(rows[name]?.copy(nodeId = nodeId)?.also { rows[name] = it })
        override fun getEnabledDevicesByNode(nodeId: String) = Future.succeededFuture(rows.values.filter { it.enabled && it.nodeId == nodeId })
        override fun close(): Future<Void> = Future.succeededFuture()
        override fun exportConfigs(names: List<String>?) = Future.succeededFuture(emptyList<Map<String, Any?>>())
        override fun importConfigs(configs: List<Map<String, Any?>>): Future<ImportDeviceConfigResult> {
            configs.forEach { c -> val n = c["name"] as String; rows[n] = DeviceConfig(n, "ns/$n", "local", JsonObject(), false, c["type"] as String) }
            return Future.succeededFuture(ImportDeviceConfigResult.success(configs.size))
        }
    }

    private lateinit var vertx: Vertx
    private lateinit var raw: MemoryDeviceStore
    private lateinit var manager: VirtualTopicTreeManager
    private lateinit var store: GuardedDeviceConfigStore

    private fun <T> Future<T>.await(): T = toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS)
    private fun <T> Future<T>.awaitFailure(): Throwable {
        val e = runCatching { await() }.exceptionOrNull() ?: throw AssertionError("expected failure")
        return if (e is java.util.concurrent.ExecutionException && e.cause != null) e.cause!! else e
    }

    private val tree = VtTree(VtTree.SCHEMA_VERSION, "plant", "Plant", "v", null, true,
        listOf(VtNode("a", null, "area", null, VtBinding(VtTree.SUBTREE, "plc/line1", VtTree.READ_WRITE))))

    @Before
    fun setUp() {
        vertx = Vertx.vertx()
        raw = MemoryDeviceStore()
        store = GuardedDeviceConfigStore(raw) { manager }
        manager = VirtualTopicTreeManager(vertx, store)
        manager.start().await()
        assertTrue(manager.save(tree, null).await().success)
        raw.saveDevice(DeviceConfig("bridge", "mqtt/bridge", "local", JsonObject(), true, DeviceConfig.DEVICE_TYPE_MQTT_CLIENT)).await()
    }

    @After
    fun tearDown() {
        vertx.close().await()
    }

    @Test
    fun toggleGoesThroughManager() {
        val before = manager.tree("plant")!!.revision
        val result = store.toggleDevice("plant", false).await()
        assertEquals(false, result?.enabled)
        val after = manager.tree("plant")!!
        assertFalse(after.enabled)
        assertNotEquals(before, after.revision)
        // Disabled tree still reserves its root but no longer routes
        assertEquals(VtCodes.REJECT_DISABLED, manager.current().publish("v/area/x").reject)
    }

    @Test
    fun deleteGoesThroughManager() {
        assertTrue(store.deleteDevice("plant").await())
        assertNull(manager.tree("plant"))
        assertNull(raw.rows["plant"])
        assertEquals("v/area/x", manager.current().publish("v/area/x").canonical)
    }

    @Test
    fun overwritingTreeRowIsRejected() {
        val e = store.saveDevice(DeviceConfig("plant", "x", "local", JsonObject(), true, "Redfish")).awaitFailure()
        assertTrue(e.message!!.contains("saveVirtualTopicTree"))
        assertTrue(store.saveDevice(DeviceConfig("other", "v", "local", JsonObject(), true, VirtualTopicTreeManager.DEVICE_TYPE))
            .awaitFailure().message!!.contains("virtual topic tree"))
        assertTrue(store.reassignDevice("plant", "node2").awaitFailure().message!!.contains("virtual topic tree"))
        assertEquals(VirtualTopicTreeManager.DEVICE_TYPE, raw.rows["plant"]!!.type)
    }

    @Test
    fun otherDevicesPassThrough() {
        assertEquals(false, store.toggleDevice("bridge", false).await()?.enabled)
        assertTrue(store.deleteDevice("bridge").await())
    }

    @Test
    fun importRoutesTreesToManagerAndRejectsClashes() {
        val doc = tree.copy(id = "line", rootPath = "w").toDocument().map
        val result = store.importConfigs(listOf(
            mapOf("name" to "line", "type" to VirtualTopicTreeManager.DEVICE_TYPE, "config" to doc),
            mapOf("name" to "plant", "type" to DeviceConfig.DEVICE_TYPE_MQTT_CLIENT),
            mapOf("name" to "opc", "type" to DeviceConfig.DEVICE_TYPE_OPCUA_CLIENT)
        )).await()
        assertEquals(2, result.imported)
        assertEquals(1, result.failed)
        assertFalse(manager.tree("line")!!.enabled)
        assertEquals(VirtualTopicTreeManager.DEVICE_TYPE, raw.rows["plant"]!!.type)
        assertTrue(raw.rows.containsKey("opc"))
    }

    @Test
    fun withoutManagerTreeRowsAreRejected() {
        val bare = GuardedDeviceConfigStore(raw) { null }
        assertTrue(bare.deleteDevice("plant").awaitFailure().message!!.contains("deleteVirtualTopicTree"))
        assertTrue(raw.rows.containsKey("plant"))
    }
}
