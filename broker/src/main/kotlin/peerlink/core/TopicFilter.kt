package at.rocworks.peerlink.core

// Ported from edge/internal/peerlink/filter.go

class TopicFilter(
    val all: Boolean = false,
    val exact: Set<String> = emptySet(),
    val prefixes: List<String> = emptyList(),
    val trie: FilterNode? = null
) {
    fun match(t: String): Boolean {
        if (all) return true
        if (exact.contains(t)) return true
        for (i in prefixes.indices) {
            if (underRoot(t, prefixes[i])) return true
        }
        return trie?.match(t, 0) ?: false
    }

    companion object {
        fun compile(list: List<String>): TopicFilter {
            var all = false
            val exact = mutableSetOf<String>()
            val prefixes = mutableListOf<String>()
            var trie: FilterNode? = null

            for (raw in list) {
                validFilter(raw)
                when {
                    raw == "#" -> all = true
                    !raw.contains('+') && !raw.contains('#') -> exact.add(raw)
                    raw.endsWith("/#") && !raw.substring(0, raw.length - 2).contains('+') && !raw.substring(0, raw.length - 2).contains('#') -> {
                        prefixes.add(raw.substring(0, raw.length - 2))
                    }
                    else -> {
                        if (trie == null) trie = FilterNode()
                        trie.add(raw)
                    }
                }
            }
            return TopicFilter(all, exact, prefixes, trie)
        }

        fun validFilter(s: String) {
            if (s.isEmpty()) throw IllegalArgumentException("empty topic filter")
            val levels = s.split('/')
            for ((i, l) in levels.withIndex()) {
                if (l.contains('#') && (l != "#" || i != levels.size - 1)) {
                    throw IllegalArgumentException("invalid topic filter \"$s\": '#' must be the last level on its own")
                }
                if (l.contains('+') && l != "+") {
                    throw IllegalArgumentException("invalid topic filter \"$s\": '+' must be a level on its own")
                }
            }
        }

        fun underRoot(t: String, root: String): Boolean {
            if (root.isEmpty() || t.length < root.length || !t.startsWith(root)) return false
            return t.length == root.length || t[root.length] == '/'
        }
    }
}

class FilterNode {
    var children: MutableMap<String, FilterNode>? = null
    var plus: FilterNode? = null
    var hash: Boolean = false
    var end: Boolean = false

    fun add(filter: String) {
        var cur = this
        val levels = filter.split('/')
        for (l in levels) {
            when (l) {
                "#" -> {
                    cur.hash = true
                    return
                }
                "+" -> {
                    if (cur.plus == null) cur.plus = FilterNode()
                    cur = cur.plus!!
                }
                else -> {
                    if (cur.children == null) cur.children = mutableMapOf()
                    cur = cur.children!!.getOrPut(l) { FilterNode() }
                }
            }
        }
        cur.end = true
    }

    fun match(t: String, i: Int): Boolean {
        if (hash) return true
        if (i > t.length) return end

        val slash = t.indexOf('/', i)
        val level: String
        val next: Int
        if (slash >= 0) {
            level = t.substring(i, slash)
            next = slash + 1
        } else {
            level = t.substring(i)
            next = t.length + 1
        }

        val c = children?.get(level)
        if (c != null && c.match(t, next)) return true
        return plus?.match(t, next) ?: false
    }
}

// MaskNode is a level of the interest union trie (plan-peerlink-interest-routing 6.1). endMask holds the
// consumers with a filter ending at this node, hashMask those with a "#" below it. A node and its kind
// identify exactly one filter, so a consumer's bit is set or cleared without counting. match does not
// allocate (G-IR1: skipped publishes cost no allocations on the capture path).
class MaskNode {
    var children: LevelMap? = null
    var plus: MaskNode? = null
    var endMask: Long = 0L
    var hashMask: Long = 0L

    fun isEmpty(): Boolean = endMask == 0L && hashMask == 0L && plus == null && (children?.size ?: 0) == 0

    // set sets (on) or clears bit in the node of filter, creating nodes on set and pruning empty ones on clear.
    fun set(filter: String, bit: Long, on: Boolean) {
        setLevels(filter, 0, bit, on)
    }

    private fun setLevels(f: String, i: Int, bit: Long, on: Boolean) {
        val j = f.indexOf('/', i)
        val last = j < 0
        val level = if (last) f.substring(i) else f.substring(i, j)
        if (level == "#") {
            hashMask = if (on) hashMask or bit else hashMask and bit.inv()
            return
        }
        var child = if (level == "+") plus else children?.get(level)
        if (child == null) {
            if (!on) return
            child = MaskNode()
            if (level == "+") {
                plus = child
            } else {
                val m = children ?: LevelMap().also { children = it }
                m.put(level, child)
            }
        }
        if (last) {
            child.endMask = if (on) child.endMask or bit else child.endMask and bit.inv()
        } else {
            child.setLevels(f, j + 1, bit, on)
        }
        if (!on && child.isEmpty()) {
            if (level == "+") plus = null else children?.remove(level)
        }
    }

    // match returns the union of the masks of all filters matching the topic name t. Topics starting with
    // '$' are not matched by a leading wildcard (MQTT 4.7.2).
    fun match(t: String): Long {
        if (t.isNotEmpty() && t[0] == '$') {
            val j = t.indexOf('/')
            val end = if (j < 0) t.length else j
            val c = children?.get(t, 0, end) ?: return 0L
            return c.hashMask or c.matchFrom(t, end + 1)
        }
        return matchFrom(t, 0) or hashMask
    }

    // matchFrom matches the levels of t from index i against the children of this node; i > t.length
    // means every level was consumed by the node itself.
    private fun matchFrom(t: String, i: Int): Long {
        if (i > t.length) return endMask
        val j = t.indexOf('/', i)
        val end = if (j < 0) t.length else j
        val next = end + 1
        var m = 0L
        val c = children?.get(t, i, end)
        if (c != null) m = m or c.hashMask or c.matchFrom(t, next)
        val p = plus
        if (p != null) m = m or p.hashMask or p.matchFrom(t, next)
        return m
    }
}

// LevelMap maps the topic levels below a MaskNode to its children: an open-addressing table whose lookup
// takes a region of the topic, so matching needs no substrings. The hashes are kept beside the keys, so
// a probe compares the key only when the hash matches.
class LevelMap {
    private var keys = arrayOfNulls<String>(4)
    private var hashes = IntArray(4)
    private var vals = arrayOfNulls<MaskNode>(4)
    var size = 0
        private set

    fun get(key: String): MaskNode? = get(key, 0, key.length)

    // get returns the child for the level t[from, to).
    fun get(t: String, from: Int, to: Int): MaskNode? {
        val h = hash(t, from, to)
        val mask = keys.size - 1
        val n = to - from
        var i = h and mask
        while (true) {
            val k = keys[i] ?: return null
            if (hashes[i] == h && k.length == n && k.regionMatches(0, t, from, n)) return vals[i]
            i = (i + 1) and mask
        }
    }

    fun put(key: String, v: MaskNode) {
        if ((size + 1) * 4 > keys.size * 3) resize(keys.size * 2)
        insert(key, v)
    }

    fun remove(key: String) {
        val mask = keys.size - 1
        var i = hash(key, 0, key.length) and mask
        while (true) {
            val k = keys[i] ?: return
            if (k == key) break
            i = (i + 1) and mask
        }
        keys[i] = null
        vals[i] = null
        size--
        // Reinsert the rest of the probe run so that no lookup stops early at the freed slot.
        var j = (i + 1) and mask
        while (true) {
            val k = keys[j] ?: break
            val v = vals[j]!!
            keys[j] = null
            vals[j] = null
            size--
            insert(k, v)
            j = (j + 1) and mask
        }
        if (keys.size > 4 && size * 8 < keys.size) resize(keys.size / 2)
    }

    private fun insert(key: String, v: MaskNode) {
        val h = hash(key, 0, key.length)
        val mask = keys.size - 1
        var i = h and mask
        while (true) {
            val k = keys[i]
            if (k == null) {
                keys[i] = key
                hashes[i] = h
                vals[i] = v
                size++
                return
            }
            if (k == key) {
                vals[i] = v
                return
            }
            i = (i + 1) and mask
        }
    }

    private fun resize(cap: Int) {
        val ok = keys
        val ov = vals
        keys = arrayOfNulls(cap)
        hashes = IntArray(cap)
        vals = arrayOfNulls(cap)
        size = 0
        for (x in ok.indices) {
            val k = ok[x] ?: continue
            insert(k, ov[x]!!)
        }
    }

    private fun hash(t: String, from: Int, to: Int): Int {
        var h = 0
        for (x in from until to) h = 31 * h + t[x].code
        return h xor (h ushr 16)
    }
}

class IncludeExclude(
    val include: TopicFilter,
    val exclude: TopicFilter
) {
    fun accept(t: String): Boolean = include.match(t) && !exclude.match(t)

    companion object {
        fun create(include: List<String>, exclude: List<String>): IncludeExclude {
            val inList = if (include.isEmpty()) listOf("#") else include
            val inFilter = TopicFilter.compile(inList)
            val exFilter = TopicFilter.compile(exclude)
            return IncludeExclude(inFilter, exFilter)
        }
    }
}
