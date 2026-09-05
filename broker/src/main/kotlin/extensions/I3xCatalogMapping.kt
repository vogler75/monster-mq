package at.rocworks.extensions

import at.rocworks.data.TopicTree

/** Catalog aliases resolve at topic boundaries; the longest matching instance ID wins. */
internal object I3xCatalogMapping {
    fun topic(mappings: Map<String, String>, id: String): String {
        val key = mappings.keys.filter { id == it || id.startsWith("$it/") }.maxByOrNull { it.length }
        return if (key == null) id else mappings.getValue(key) + id.removePrefix(key)
    }

    fun subscriptionBindings(mappings: Map<String, String>, ids: Set<String>, maxDepth: Int): Map<String, Int> {
        val bindings = ids.associateWith { maxDepth }.toMutableMap()
        for (id in ids) {
            for (child in mappings.keys.filter { it.startsWith("$id/") }) {
                val distance = child.removePrefix(id).count { it == '/' }
                if (maxDepth == 0 || distance < maxDepth) {
                    val depth = if (maxDepth == 0) 0 else maxDepth - distance
                    val previous = bindings[child]
                    bindings[child] = if (previous == 0 || depth == 0) 0 else maxOf(previous ?: depth, depth)
                }
            }
        }
        return bindings
    }

    fun subscriptionIds(mappings: Map<String, String>, ids: Set<String>, messageTopic: String, maxDepth: Int): List<String> =
        subscriptionBindings(mappings, ids, maxDepth).mapNotNull { (id, depthLimit) ->
            val base = topic(mappings, id)
            if (base == id) {
                val clean = id.trim().trim('/')
                val filters = mutableListOf(clean)
                if (depthLimit == 0) filters.add("$clean/#")
                else for (depth in 2..depthLimit) filters.add(clean + "/+".repeat(depth - 1))
                return@mapNotNull if (filters.any { TopicTree.matches(it, messageTopic) } && topic(mappings, messageTopic) == messageTopic) messageTopic else null
            }
            val suffix = messageTopic.removePrefix(base)
            val matches = messageTopic == base || (messageTopic.startsWith("$base/") &&
                (depthLimit == 0 || suffix.count { it == '/' } < depthLimit))
            if (matches && topic(mappings, id + suffix) == messageTopic) id + suffix else null
        }.distinct()
}
