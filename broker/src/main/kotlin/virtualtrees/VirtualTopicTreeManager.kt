package at.rocworks.virtualtrees

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.logging.Logger

/**
 * Cluster-aware owner of the virtual topic tree configuration (issue #217).
 *
 * Trees are persisted as [DeviceConfig] rows of type [DEVICE_TYPE]. The compiled [VtResolver] is an
 * immutable snapshot swapped atomically, so routing always sees one complete configuration.
 * Saves and deletes are serialized cluster-wide by a shared-data lock and re-read the store under
 * that lock, so concurrent root claims on different nodes cannot both succeed.
 */
class VirtualTopicTreeManager(
    private val vertx: Vertx,
    store: IDeviceConfigStore?
) {
    // Bypass the guard: this manager is the only writer of tree rows
    private val deviceStore: IDeviceConfigStore? = GuardedDeviceConfigStore.unwrap(store)

    data class MutationResult(val success: Boolean, val tree: VtTree?, val errors: List<VtIssue>)

    data class ImportResult(val imported: Int, val failed: Int, val errors: List<String>)

    companion object {
        const val DEVICE_TYPE = "VirtualTopicTree"
        private const val LOCK_NAME = "monster.virtualtopictrees.lock"

        @Volatile
        private var instance: VirtualTopicTreeManager? = null

        fun getInstance(): VirtualTopicTreeManager? = instance

        /** Current resolver; empty when the feature is disabled or not started. */
        fun resolver(): VtResolver = instance?.snapshot?.get() ?: VtResolver.EMPTY
    }

    private val logger: Logger = Utils.getLogger(VirtualTopicTreeManager::class.java)
    private val snapshot = AtomicReference(VtResolver.EMPTY)
    private val reloadSeq = AtomicLong(0)
    private val nodeId: String by lazy { runCatching { Monster.getClusterNodeId(vertx) }.getOrDefault("local") }

    /** Trees held in memory when no device config store is configured. */
    private val memoryTrees = LinkedHashMap<String, VtTree>()

    fun start(): Future<Void> {
        instance = this
        vertx.eventBus().consumer<JsonObject>(EventBusAddresses.VirtualTopicTrees.RELOAD) { msg ->
            if (msg.body()?.getString("node") != nodeId) reload()
        }
        return reload().mapEmpty()
    }

    fun current(): VtResolver = snapshot.get()

    fun trees(): List<VtTree> = snapshot.get().trees

    fun tree(id: String): VtTree? = snapshot.get().tree(id)

    /** Reloads all trees from the store and swaps the routing snapshot. */
    fun reload(): Future<VtResolver> {
        val seq = reloadSeq.incrementAndGet()
        return loadTrees().map { trees ->
            val r = VtResolver(trees)
            if (seq == reloadSeq.get()) snapshot.set(r)
            logger.fine { "Virtual topic trees loaded: ${trees.size}" }
            r
        }.onFailure { logger.warning("Failed to load virtual topic trees: ${it.message}") }
    }

    private fun loadTrees(): Future<List<VtTree>> {
        val store = deviceStore ?: return Future.succeededFuture(synchronized(memoryTrees) { memoryTrees.values.toList() })
        return store.getDevicesByType(DEVICE_TYPE).map { devices ->
            devices.mapNotNull { d ->
                try {
                    VtTree.fromJson(d.config).copy(id = d.name, enabled = d.enabled)
                } catch (e: Exception) {
                    logger.warning("Ignoring invalid virtual topic tree '${d.name}': ${e.message}")
                    null
                }
            }.sortedBy { it.id }
        }
    }

    private fun applyLocal(trees: List<VtTree>) {
        reloadSeq.incrementAndGet()
        snapshot.set(VtResolver(trees))
    }

    private fun broadcast() {
        vertx.eventBus().publish(EventBusAddresses.VirtualTopicTrees.RELOAD, JsonObject().put("node", nodeId))
    }

    private fun <T> withLock(action: () -> Future<T>): Future<T> {
        return vertx.sharedData().getLockWithTimeout(LOCK_NAME, 30_000).compose { lock ->
            val f = try { action() } catch (e: Exception) { Future.failedFuture(e) }
            f.onComplete { lock.release() }
        }
    }

    /** Validation of a candidate against the currently active configuration (no side effects). */
    fun validate(candidate: VtTree): List<VtIssue> =
        VtRules.validate(candidate, trees().filter { it.id != candidate.id })

    /** Warnings for a candidate: retained physical data inside the claimed root. */
    fun warnings(candidate: VtTree): Future<List<VtIssue>> {
        val retained = Monster.getRetainedStore() ?: return Future.succeededFuture(emptyList())
        if (!VtRules.validName(candidate.rootPath) || candidate.rootPath.contains('+') || candidate.rootPath.contains('#')) {
            return Future.succeededFuture(emptyList())
        }
        return vertx.executeBlocking(Callable {
            var found: String? = null
            try {
                for (f in listOf(candidate.rootPath, candidate.rootPath + "/#")) {
                    retained.findMatchingMessages(f) { m -> found = m.topicName; false }
                    if (found != null) break
                }
            } catch (e: Exception) {
                logger.fine { "Physical conflict check failed: ${e.message}" }
            }
            found?.let {
                listOf(VtIssue(VtCodes.PHYSICAL_CONFLICT, null, it,
                    "Retained physical topic '$it' lies in the virtual root and will be hidden"))
            } ?: emptyList()
        })
    }

    fun save(candidate: VtTree, expectedRevision: String?): Future<MutationResult> = withLock {
        loadTrees().compose { stored ->
            val existing = stored.firstOrNull { it.id == candidate.id }
            when {
                existing != null && expectedRevision.isNullOrEmpty() ->
                    return@compose Future.succeededFuture(conflict(candidate.id, "Tree '${candidate.id}' already exists; expectedRevision is required"))
                existing != null && existing.revision != expectedRevision ->
                    return@compose Future.succeededFuture(conflict(candidate.id, "Tree '${candidate.id}' was changed (revision ${existing.revision})"))
                existing == null && !expectedRevision.isNullOrEmpty() ->
                    return@compose Future.succeededFuture(MutationResult(false, null,
                        listOf(VtIssue(VtCodes.NOT_FOUND, null, null, "Tree '${candidate.id}' does not exist"))))
            }
            val others = stored.filter { it.id != candidate.id }
            val errors = VtRules.validate(candidate, others)
            if (errors.isNotEmpty()) return@compose Future.succeededFuture(MutationResult(false, null, errors))
            val tree = candidate.copy(revision = Utils.getUuid())
            persist(tree).map {
                applyLocal((others + tree).sortedBy { it.id })
                broadcast()
                MutationResult(true, tree, emptyList())
            }
        }
    }

    fun delete(id: String, expectedRevision: String?): Future<MutationResult> = withLock {
        loadTrees().compose { stored ->
            val existing = stored.firstOrNull { it.id == id }
                ?: return@compose Future.succeededFuture(MutationResult(false, null,
                    listOf(VtIssue(VtCodes.NOT_FOUND, null, null, "Tree '$id' does not exist"))))
            if (existing.revision != expectedRevision) {
                return@compose Future.succeededFuture(conflict(id, "Tree '$id' was changed (revision ${existing.revision})"))
            }
            remove(id).map {
                applyLocal(stored.filter { it.id != id })
                broadcast()
                MutationResult(true, existing, emptyList())
            }
        }
    }

    /**
     * Imports portable tree documents (device export format). Imported trees are always stored disabled
     * so that an import never changes live routing; they are validated like a save.
     */
    fun importTrees(entries: List<Pair<String?, Any?>>): Future<ImportResult> = withLock {
        loadTrees().compose { stored ->
            val current = stored.associateBy { it.id }.toMutableMap()
            val errors = ArrayList<String>()
            val accepted = ArrayList<VtTree>()
            for ((name, raw) in entries) {
                try {
                    val json = when (raw) {
                        is JsonObject -> raw
                        is Map<*, *> -> JsonObject(@Suppress("UNCHECKED_CAST") (raw as Map<String, Any?>))
                        is String -> JsonObject(raw)
                        else -> throw IllegalArgumentException("config must be a JSON object")
                    }
                    var tree = VtTree.fromJson(json).copy(enabled = false, revision = null)
                    if (tree.id.isEmpty() && name != null) tree = tree.copy(id = name)
                    if (name != null && name != tree.id) throw IllegalArgumentException("device name '$name' differs from tree id '${tree.id}'")
                    val issues = VtRules.validate(tree, current.values.filter { it.id != tree.id })
                    if (issues.isNotEmpty()) throw IllegalArgumentException(issues.joinToString { "${it.code}: ${it.message}" })
                    tree = tree.copy(revision = Utils.getUuid())
                    current[tree.id] = tree
                    accepted.add(tree)
                } catch (e: Exception) {
                    errors.add("Virtual topic tree '${name ?: "?"}': ${e.message}")
                }
            }
            var chain: Future<Void> = Future.succeededFuture()
            accepted.forEach { t -> chain = chain.compose { persist(t) } }
            chain.map {
                if (accepted.isNotEmpty()) {
                    applyLocal(current.values.sortedBy { it.id })
                    broadcast()
                }
                ImportResult(accepted.size, entries.size - accepted.size, errors)
            }
        }
    }

    private fun conflict(id: String, msg: String) =
        MutationResult(false, null, listOf(VtIssue(VtCodes.REVISION_CONFLICT, null, null, msg)))
            .also { logger.fine { "Virtual topic tree '$id': $msg" } }

    private fun persist(tree: VtTree): Future<Void> {
        val store = deviceStore ?: run {
            synchronized(memoryTrees) { memoryTrees[tree.id] = tree }
            return Future.succeededFuture()
        }
        return store.getDevice(tree.id).compose { dev ->
            if (dev != null && dev.type != DEVICE_TYPE) {
                Future.failedFuture("A device named '${tree.id}' of type '${dev.type}' already exists")
            } else {
                store.saveDevice(
                    DeviceConfig(
                        name = tree.id,
                        namespace = tree.rootPath,
                        nodeId = "*",
                        config = tree.toStored(),
                        enabled = tree.enabled,
                        type = DEVICE_TYPE,
                        createdAt = dev?.createdAt ?: Instant.now(),
                        updatedAt = Instant.now()
                    )
                ).mapEmpty()
            }
        }
    }

    private fun remove(id: String): Future<Void> {
        val store = deviceStore ?: run {
            synchronized(memoryTrees) { memoryTrees.remove(id) }
            return Future.succeededFuture()
        }
        return store.deleteDevice(id).mapEmpty()
    }
}
