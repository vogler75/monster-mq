package at.rocworks.virtualtrees

import at.rocworks.Utils
import at.rocworks.data.BrokerMessage

/**
 * Pure virtual topic tree rules (validation and routing), see issue #217.
 * Mirrors the reference model used by the acceptance tests.
 */
object VtRules {
    const val MAX_TOPIC_LENGTH = 65535

    /** MQTT filter match, including: filters starting with a wildcard never match '$' topics. */
    fun match(filter: String, topic: String): Boolean {
        if (topic.startsWith("$") && (filter.startsWith("+") || filter.startsWith("#"))) return false
        val f = filter.split('/')
        val t = topic.split('/')
        for (i in f.indices) {
            val part = f[i]
            if (part == "#") return i == f.size - 1
            if (i >= t.size) return false
            if (part != "+" && part != t[i]) return false
        }
        return f.size == t.size
    }

    /** Topic equals prefix or lies below it (whole levels only). */
    fun under(topic: String, prefix: String): Boolean =
        topic == prefix || (topic.length > prefix.length && topic.startsWith(prefix) && topic[prefix.length] == '/')

    /** Filter matches the prefix itself or any topic below it. */
    fun intersects(filter: String, prefix: String): Boolean {
        val f = filter.split('/')
        val p = prefix.split('/')
        for (i in f.indices) {
            val part = f[i]
            if (part == "#") return true
            if (i >= p.size) return true
            if (part != "+" && part != p[i]) return false
        }
        return f.size >= p.size
    }

    fun utf8Length(s: String): Int {
        var n = 0
        var i = 0
        while (i < s.length) {
            val c = s[i]
            n += when {
                c.code < 0x80 -> 1
                c.code < 0x800 -> 2
                Character.isHighSurrogate(c) && i + 1 < s.length && Character.isLowSurrogate(s[i + 1]) -> { i++; 4 }
                else -> 3
            }
            i++
        }
        return n
    }

    private fun validUtf16(s: String): Boolean {
        var i = 0
        while (i < s.length) {
            val c = s[i]
            if (Character.isHighSurrogate(c)) {
                if (i + 1 >= s.length || !Character.isLowSurrogate(s[i + 1])) return false
                i++
            } else if (Character.isLowSurrogate(c)) return false
            i++
        }
        return true
    }

    fun validName(s: String): Boolean =
        s.isNotEmpty() && s.indexOf('\u0000') < 0 && validUtf16(s) && utf8Length(s) <= MAX_TOPIC_LENGTH

    fun validSegment(s: String): Boolean =
        validName(s) && s.none { it == '/' || it == '+' || it == '#' }

    private fun hasWildcard(s: String) = s.indexOf('+') >= 0 || s.indexOf('#') >= 0

    /**
     * Validates [candidate] against the other configured trees ([others] must not contain the candidate).
     * Only issues concerning the candidate are reported, so pre-existing problems of other trees
     * never block an unrelated save.
     */
    fun validate(candidate: VtTree, others: Collection<VtTree>): List<VtIssue> {
        val issues = ArrayList<VtIssue>()
        fun add(code: String, node: String?, path: String?, msg: String) = issues.add(VtIssue(code, node, path, msg))
        val t = candidate
        if (t.id.isEmpty()) add(VtCodes.INVALID_TOPIC, null, null, "Tree id must not be empty")
        if (t.schemaVersion != VtTree.SCHEMA_VERSION) {
            add(VtCodes.UNSUPPORTED_SCHEMA_VERSION, null, t.rootPath, "Unsupported schemaVersion ${t.schemaVersion}")
        }
        when {
            !validName(t.rootPath) || hasWildcard(t.rootPath) ->
                add(VtCodes.INVALID_TOPIC, null, t.rootPath, "Invalid root path '${t.rootPath}'")
            t.rootPath.startsWith("$") ->
                add(VtCodes.RESERVED_TOPIC, null, t.rootPath, "Root path must not start with '$'")
        }
        val ids = HashSet<String>()
        for (n in t.nodes) {
            if (!ids.add(n.id)) add(VtCodes.DUPLICATE_ID, n.id, null, "Duplicate node id '${n.id}'")
        }
        val allRoots = others.map { it.rootPath } + t.rootPath
        val paths = t.paths()
        val seen = HashMap<String, String>()
        for (n in t.nodes) {
            if (n.id.isEmpty()) add(VtCodes.INVALID_SEGMENT, n.id, null, "Node id must not be empty")
            if (!validSegment(n.segment)) add(VtCodes.INVALID_SEGMENT, n.id, null, "Invalid segment '${n.segment}'")
            if (n.parentId != null && n.parentId !in ids) {
                add(VtCodes.INVALID_PARENT, n.id, null, "Unknown parent '${n.parentId}'")
                continue
            }
            val p = paths[n.id]
            if (p == null) {
                add(VtCodes.CYCLE, n.id, null, "Node is part of a cycle")
                continue
            }
            if (utf8Length(p) > MAX_TOPIC_LENGTH) add(VtCodes.INVALID_TOPIC, n.id, null, "Derived path is too long")
            val other = seen[p]
            if (other != null && other != n.id) add(VtCodes.DUPLICATE_PATH, n.id, p, "Path '$p' is already used by node '$other'")
            seen[p] = n.id
            val b = n.binding ?: continue
            when {
                (b.kind != VtTree.EXACT && b.kind != VtTree.SUBTREE) ||
                    (b.access != VtTree.READ_ONLY && b.access != VtTree.READ_WRITE) ->
                    add(VtCodes.INVALID_BINDING, n.id, p, "Invalid binding kind '${b.kind}' or access '${b.access}'")
                hasWildcard(b.target) ->
                    add(VtCodes.UNSUPPORTED_WILDCARD, n.id, p, "Binding target '${b.target}' must not contain wildcards")
                !validName(b.target) ->
                    add(VtCodes.INVALID_TOPIC, n.id, p, "Invalid binding target '${b.target}'")
            }
            for (root in allRoots) {
                if (under(b.target, root)) {
                    add(VtCodes.TARGET_IN_VIRTUAL_ROOT, n.id, p, "Binding target '${b.target}' lies in virtual root '$root'")
                }
            }
        }
        for (o in others) {
            if (under(o.rootPath, t.rootPath) || under(t.rootPath, o.rootPath)) {
                add(VtCodes.ROOT_CONFLICT, null, t.rootPath, "Root '${t.rootPath}' overlaps root '${o.rootPath}' of tree '${o.id}'")
            }
            for (n in o.nodes) {
                val b = n.binding ?: continue
                if (under(b.target, t.rootPath)) {
                    add(VtCodes.TARGET_IN_VIRTUAL_ROOT, null, t.rootPath, "Root '${t.rootPath}' contains target '${b.target}' of tree '${o.id}'")
                }
            }
        }
        return issues
    }
}

/** Result of resolving a publish topic. */
data class VtPublish(val canonical: String?, val reject: String?, val nodeId: String? = null, val access: String? = null)

/** One virtual presentation of a canonical topic. */
data class VtPresentation(val virtual: String, val nodeId: String, val treeId: String)

/**
 * Compiled, immutable routing table for a set of valid trees.
 * All lookups walk the topic levels and use hash maps, so cost is independent of the number of bindings.
 */
class VtResolver(val trees: List<VtTree>) {
    class Route(val nodeId: String, val path: String, val binding: VtBinding, val tree: Compiled)

    class Compiled(val tree: VtTree) {
        val exact = HashMap<String, Route>()
        val mounts = HashMap<String, Route>()
        val folders = HashSet<String>()
        val paths: Map<String, String> = tree.paths()

        init {
            for (n in tree.nodes) {
                val p = paths[n.id] ?: continue
                val b = n.binding
                when {
                    b == null -> folders.add(p)
                    b.kind == VtTree.EXACT -> exact[p] = Route(n.id, p, b, this)
                    else -> mounts[p] = Route(n.id, p, b, this)
                }
            }
        }

        fun winner(v: String): Route? {
            exact[v]?.let { return it }
            if (mounts.isEmpty()) return null
            var p = v
            while (true) {
                mounts[p]?.let { return it }
                val i = p.lastIndexOf('/')
                if (i < 0) return null
                p = p.substring(0, i)
            }
        }
    }

    private val compiled: List<Compiled> = trees.map { Compiled(it) }
    private val byRoot = HashMap<String, Compiled>().also { m -> compiled.forEach { m.putIfAbsent(it.tree.rootPath, it) } }
    private val exactByTarget = HashMap<String, MutableList<Route>>()
    private val mountsByTarget = HashMap<String, MutableList<Route>>()

    init {
        for (c in compiled) {
            if (!c.tree.enabled) continue
            c.exact.values.forEach { exactByTarget.getOrPut(it.binding.target) { ArrayList() }.add(it) }
            c.mounts.values.forEach { mountsByTarget.getOrPut(it.binding.target) { ArrayList() }.add(it) }
        }
    }

    val isEmpty: Boolean get() = compiled.isEmpty()

    fun tree(id: String): VtTree? = trees.firstOrNull { it.id == id }

    private inline fun forEachPrefix(topic: String, action: (String) -> Boolean) {
        var p = topic
        while (true) {
            if (!action(p)) return
            val i = p.lastIndexOf('/')
            if (i < 0) return
            p = p.substring(0, i)
        }
    }

    private fun owner(topic: String): Compiled? {
        if (byRoot.isEmpty()) return null
        var found: Compiled? = null
        forEachPrefix(topic) { p -> val c = byRoot[p]; if (c != null) { found = c; false } else true }
        return found
    }

    /** True if the topic lies in a reserved root (enabled or disabled). */
    fun inVirtualNamespace(topic: String): Boolean = owner(topic) != null

    private fun canonical(v: String, r: Route): String =
        if (r.binding.kind == VtTree.EXACT) r.binding.target else r.binding.target + v.substring(r.path.length)

    /** Resolves a concrete publish topic; physical topics resolve to themselves. */
    fun publish(topic: String): VtPublish {
        val c = owner(topic) ?: return VtPublish(topic, null)
        if (!c.tree.enabled) return VtPublish(null, VtCodes.REJECT_DISABLED)
        val r = c.winner(topic) ?: return VtPublish(null, VtCodes.REJECT_UNBOUND)
        if (r.binding.access != VtTree.READ_WRITE) return VtPublish(null, VtCodes.REJECT_READ_ONLY, r.nodeId, r.binding.access)
        return VtPublish(canonical(topic, r), null, r.nodeId, r.binding.access)
    }

    /** Winning binding for a virtual topic regardless of access (used by preview). */
    fun winnerRoute(topic: String): Route? {
        val c = owner(topic) ?: return null
        if (!c.tree.enabled) return null
        return c.winner(topic)
    }

    /**
     * Canonical topic backing a concrete topic for reads; physical topics resolve to themselves.
     * Returns null for virtual topics without a backing source (unbound, disabled tree).
     */
    fun readResolve(topic: String): String? {
        val c = owner(topic) ?: return topic
        if (!c.tree.enabled) return null
        val r = c.winner(topic) ?: return null
        return canonical(topic, r)
    }

    /** All virtual presentations of a canonical topic (enabled trees only), sorted by virtual topic. */
    fun project(canonicalTopic: String): List<VtPresentation> {
        if (exactByTarget.isEmpty() && mountsByTarget.isEmpty()) return emptyList()
        val out = ArrayList<VtPresentation>(2)
        exactByTarget[canonicalTopic]?.forEach { out.add(VtPresentation(it.path, it.nodeId, it.tree.tree.id)) }
        if (mountsByTarget.isNotEmpty()) {
            forEachPrefix(canonicalTopic) { target ->
                mountsByTarget[target]?.forEach { m ->
                    val v = m.path + canonicalTopic.substring(target.length)
                    if (m.tree.winner(v) === m) out.add(VtPresentation(v, m.nodeId, m.tree.tree.id))
                }
                true
            }
        }
        if (out.size > 1) out.sortBy { it.virtual }
        return out
    }

    /**
     * Topic names under which a message on [canonicalTopic] is presented to subscribers:
     * the topic itself (unless it lies in a reserved root) plus all virtual presentations.
     */
    fun presentations(canonicalTopic: String): List<String> {
        val proj = project(canonicalTopic)
        val physical = !inVirtualNamespace(canonicalTopic)
        if (proj.isEmpty()) return if (physical) listOf(canonicalTopic) else emptyList()
        val out = ArrayList<String>(proj.size + 1)
        if (physical) out.add(canonicalTopic)
        proj.forEach { if (it.virtual !in out) out.add(it.virtual) }
        return out
    }

    /** Node ids whose binding can contribute deliveries to a virtual filter (all trees). */
    fun subscribeRoutes(filter: String): List<Route> {
        val out = ArrayList<Route>()
        for (c in compiled) {
            c.exact.values.forEach { if (VtRules.match(filter, it.path)) out.add(it) }
            c.mounts.values.forEach { if (VtRules.intersects(filter, it.path)) out.add(it) }
        }
        out.sortBy { it.nodeId }
        return out
    }

    /** Canonical filters a virtual filter maps to through one binding route (empty if none). */
    fun canonicalFilters(filter: String, r: Route): List<String> {
        if (r.binding.kind == VtTree.EXACT) {
            return if (VtRules.match(filter, r.path)) listOf(r.binding.target) else emptyList()
        }
        val f = filter.split('/')
        val p = r.path.split('/')
        val target = r.binding.target
        for (i in p.indices) {
            if (i >= f.size) return emptyList()
            if (f[i] == "#") return listOf("$target/#", target)
            if (f[i] != "+" && f[i] != p[i]) return emptyList()
        }
        if (f.size == p.size) return listOf(target)
        val rest = f.subList(p.size, f.size).joinToString("/")
        return if (rest == "#") listOf("$target/#", target) else listOf("$target/$rest")
    }

    /** Distinct canonical filters needed to look up stored data for a (possibly virtual) filter. */
    fun canonicalQueries(filter: String): List<String> {
        val out = LinkedHashSet<String>()
        for (c in compiled) {
            if (!c.tree.enabled) continue
            for (r in c.exact.values) out.addAll(canonicalFilters(filter, r))
            for (r in c.mounts.values) out.addAll(canonicalFilters(filter, r))
        }
        return out.toList()
    }

    /**
     * Looks up stored items for a subscription filter as the subscriber sees them: physical items
     * outside reserved roots plus virtual presentations of canonical items matching the filter.
     * [finder] runs a store query for a filter and calls its callback per item until it returns false.
     * [emit] receives the presented topic and the stored item; return false to stop.
     */
    fun <T> findPresented(
        filter: String,
        finder: (String, (T) -> Boolean) -> Unit,
        topicOf: (T) -> String,
        emit: (String, T) -> Boolean
    ) {
        val seen = HashSet<String>()
        var stop = false
        finder(filter) { item ->
            val t = topicOf(item)
            if (!inVirtualNamespace(t) && seen.add(t)) {
                if (!emit(t, item)) stop = true
            }
            !stop
        }
        if (stop || compiled.isEmpty()) return
        for (q in canonicalQueries(filter)) {
            finder(q) { item ->
                val c = topicOf(item)
                for (p in project(c)) {
                    if (VtRules.match(filter, p.virtual) && seen.add(p.virtual)) {
                        if (!emit(p.virtual, item)) { stop = true; break }
                    }
                }
                !stop
            }
            if (stop) return
        }
    }

    /** All virtual folder and binding node paths of enabled trees, including the roots. */
    fun structuralPaths(): List<String> {
        val out = ArrayList<String>()
        for (c in compiled) {
            if (!c.tree.enabled) continue
            out.add(c.tree.rootPath)
            out.addAll(c.paths.values)
        }
        return out
    }

    companion object {
        val EMPTY = VtResolver(emptyList())

        /** Copy of a canonical message presented under a virtual topic name. */
        fun virtualCopy(msg: BrokerMessage, virtualTopic: String): BrokerMessage =
            msg.copy(messageUuid = Utils.getUuid(), topicName = virtualTopic)
    }
}
