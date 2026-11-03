package at.rocworks.virtualtrees

import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import at.rocworks.stores.ImportDeviceConfigResult
import io.vertx.core.Future

/**
 * Device config store decorator that keeps virtual topic tree rows consistent with
 * [VirtualTopicTreeManager]. Generic device operations (typed device mutations, file based config
 * loading, device import) share the device table with the trees, so:
 *  - delete and toggle of a tree row are executed by the manager (validation, new revision, reload),
 *  - imports of tree documents are handed to the manager (validated, stored disabled),
 *  - saves or reassignments that would overwrite or move a tree row are rejected.
 * The manager itself works on [inner].
 */
class GuardedDeviceConfigStore(
    val inner: IDeviceConfigStore,
    private val managerProvider: () -> VirtualTopicTreeManager? = { VirtualTopicTreeManager.getInstance() }
) : IDeviceConfigStore by inner {

    companion object {
        private const val TYPE = VirtualTopicTreeManager.DEVICE_TYPE

        fun unwrap(store: IDeviceConfigStore?): IDeviceConfigStore? = (store as? GuardedDeviceConfigStore)?.inner ?: store

        fun managedMessage(name: String) =
            "Device '$name' is a virtual topic tree; use the saveVirtualTopicTree/deleteVirtualTopicTree mutations"
    }

    private fun isTree(name: String): Future<Boolean> = inner.getDevice(name).map { it?.type == TYPE }

    private fun manager(name: String): VirtualTopicTreeManager =
        managerProvider() ?: throw IllegalStateException("${managedMessage(name)} (VirtualTopicTrees feature is not enabled)")

    private fun failure(r: VirtualTopicTreeManager.MutationResult): Throwable =
        IllegalStateException(r.errors.joinToString("; ") { "${it.code}: ${it.message}" })

    override fun deleteDevice(name: String): Future<Boolean> = isTree(name).compose { tree ->
        if (!tree) return@compose inner.deleteDevice(name)
        val m = manager(name)
        m.delete(name, m.tree(name)?.revision).compose { r ->
            if (r.success) Future.succeededFuture(true) else Future.failedFuture(failure(r))
        }
    }

    override fun toggleDevice(name: String, enabled: Boolean): Future<DeviceConfig?> = isTree(name).compose { tree ->
        if (!tree) return@compose inner.toggleDevice(name, enabled)
        val m = manager(name)
        val current = m.tree(name) ?: return@compose Future.failedFuture(IllegalStateException("Virtual topic tree '$name' is not loaded"))
        m.save(current.copy(enabled = enabled), current.revision).compose { r ->
            if (r.success) inner.getDevice(name) else Future.failedFuture(failure(r))
        }
    }

    override fun saveDevice(device: DeviceConfig): Future<DeviceConfig> =
        if (device.type == TYPE) Future.failedFuture(IllegalStateException(managedMessage(device.name)))
        else isTree(device.name).compose { tree ->
            if (tree) Future.failedFuture(IllegalStateException(managedMessage(device.name))) else inner.saveDevice(device)
        }

    override fun reassignDevice(name: String, nodeId: String): Future<DeviceConfig?> = isTree(name).compose { tree ->
        if (tree) Future.failedFuture(IllegalStateException(managedMessage(name))) else inner.reassignDevice(name, nodeId)
    }

    override fun importConfigs(configs: List<Map<String, Any?>>): Future<ImportDeviceConfigResult> =
        inner.getDevicesByType(TYPE).compose { rows ->
            val treeNames = rows.map { it.name }.toSet()
            val (treeConfigs, deviceConfigs) = configs.partition { it["type"] == TYPE }
            val (clashing, plain) = deviceConfigs.partition { it["name"] in treeNames }
            val clashErrors = clashing.map { managedMessage(it["name"].toString()) }

            val treeImport: Future<VirtualTopicTreeManager.ImportResult> = when {
                treeConfigs.isEmpty() -> Future.succeededFuture(VirtualTopicTreeManager.ImportResult(0, 0, emptyList()))
                else -> managerProvider()?.importTrees(treeConfigs.map { (it["name"] as? String) to it["config"] })
                    ?: Future.succeededFuture(VirtualTopicTreeManager.ImportResult(0, treeConfigs.size,
                        listOf("VirtualTopicTrees feature is not enabled; ${treeConfigs.size} tree(s) not imported")))
            }
            val deviceImport: Future<ImportDeviceConfigResult> =
                if (plain.isEmpty()) Future.succeededFuture(ImportDeviceConfigResult(true)) else inner.importConfigs(plain)

            Future.all(treeImport, deviceImport).map {
                val t = treeImport.result()
                val d = deviceImport.result()
                val failed = d.failed + t.failed + clashing.size
                ImportDeviceConfigResult(
                    success = d.success && failed == 0,
                    imported = d.imported + t.imported,
                    failed = failed,
                    errors = d.errors + t.errors + clashErrors
                )
            }
        }
}
