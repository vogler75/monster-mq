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
        for (p in prefixes) {
            if (underRoot(t, p)) return true
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
