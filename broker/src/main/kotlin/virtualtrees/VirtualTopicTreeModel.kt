package at.rocworks.virtualtrees

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/**
 * Portable virtual topic tree document (schemaVersion 1), see issue #217.
 *
 * A tree reserves [VtTree.rootPath] as a virtual namespace. Nodes form a hierarchy below the root;
 * a node without binding is a folder, an EXACT binding presents one canonical topic under the
 * node path and a SUBTREE binding mounts a canonical subtree (suffix preserved).
 */
data class VtBinding(val kind: String, val target: String, val access: String) {
    fun toJson(): JsonObject = JsonObject().put("kind", kind).put("target", target).put("access", access)
}

data class VtNode(
    val id: String,
    val parentId: String?,
    val segment: String,
    val description: String?,
    val binding: VtBinding?
) {
    fun toJson(): JsonObject {
        val o = JsonObject().put("id", id).put("parentId", parentId).put("segment", segment)
        if (!description.isNullOrEmpty()) o.put("description", description)
        if (binding != null) o.put("binding", binding.toJson())
        return o
    }
}

data class VtTree(
    val schemaVersion: Int,
    val id: String,
    val name: String,
    val rootPath: String,
    val description: String?,
    val enabled: Boolean,
    val nodes: List<VtNode>,
    val revision: String? = null
) {
    /** Portable document without the server revision. */
    fun toDocument(): JsonObject {
        val o = JsonObject()
            .put("schemaVersion", schemaVersion)
            .put("id", id)
            .put("name", name)
            .put("rootPath", rootPath)
        if (!description.isNullOrEmpty()) o.put("description", description)
        o.put("enabled", enabled)
        o.put("nodes", JsonArray(nodes.map { it.toJson() }))
        return o
    }

    /** Stored representation: the document plus its revision. */
    fun toStored(): JsonObject = toDocument().also { if (revision != null) it.put("revision", revision) }

    /** Map shape used by the GraphQL VirtualTopicTree type. */
    fun toGraphQL(): Map<String, Any?> = mapOf(
        "schemaVersion" to schemaVersion,
        "id" to id,
        "name" to name,
        "rootPath" to rootPath,
        "description" to description?.takeIf { it.isNotEmpty() },
        "enabled" to enabled,
        "revision" to (revision ?: ""),
        "nodes" to nodes.map { n ->
            mapOf(
                "id" to n.id,
                "parentId" to n.parentId,
                "segment" to n.segment,
                "description" to n.description?.takeIf { it.isNotEmpty() },
                "binding" to n.binding?.let { b -> mapOf("kind" to b.kind, "target" to b.target, "access" to b.access) }
            )
        }
    )

    /** Derived virtual path per node id; nodes in a cycle or below an unknown parent are omitted. */
    fun paths(): Map<String, String> {
        val byId = HashMap<String, VtNode>()
        nodes.forEach { byId[it.id] = it }
        val out = HashMap<String, String>()
        fun resolve(id: String, seen: MutableSet<String>): String? {
            out[id]?.let { return it }
            val n = byId[id] ?: return null
            if (!seen.add(id)) return null
            val parent = n.parentId
            val base = if (parent != null) resolve(parent, seen) ?: return null else rootPath
            val p = "$base/${n.segment}"
            out[id] = p
            return p
        }
        nodes.forEach { resolve(it.id, HashSet()) }
        return out
    }

    companion object {
        const val SCHEMA_VERSION = 1
        const val EXACT = "EXACT"
        const val SUBTREE = "SUBTREE"
        const val READ_ONLY = "READ_ONLY"
        const val READ_WRITE = "READ_WRITE"

        /**
         * Parses a tree document from a GraphQL input map or a stored JSON object.
         * Throws [IllegalArgumentException] if the document is structurally malformed.
         * A SUBTREE target written with the UI syntax "x/#" is normalized to "x".
         */
        fun fromMap(map: Map<String, Any?>, revision: String? = null): VtTree {
            val nodeList: List<Any?> = when (val nodesRaw = map["nodes"]) {
                null -> emptyList()
                is List<*> -> nodesRaw
                is JsonArray -> nodesRaw.list
                else -> throw IllegalArgumentException("nodes must be a list")
            }
            val nodes = nodeList.mapIndexed { i, raw ->
                val n = asMap(raw) ?: throw IllegalArgumentException("node $i must be an object")
                val b = n["binding"]?.let { asMap(it) ?: throw IllegalArgumentException("binding of node $i must be an object") }
                VtNode(
                    id = n["id"]?.toString() ?: throw IllegalArgumentException("node $i has no id"),
                    parentId = n["parentId"]?.toString(),
                    segment = n["segment"]?.toString() ?: "",
                    description = n["description"]?.toString()?.takeIf { it.isNotEmpty() },
                    binding = b?.let {
                        val kind = it["kind"]?.toString() ?: ""
                        var target = it["target"]?.toString() ?: ""
                        if (kind == SUBTREE && target.length > 2 && target.endsWith("/#")) {
                            target = target.substring(0, target.length - 2)
                        }
                        VtBinding(kind, target, it["access"]?.toString() ?: READ_ONLY)
                    }
                )
            }
            val id = map["id"]?.toString() ?: ""
            val sv = map["schemaVersion"]
            return VtTree(
                schemaVersion = (sv as? Number)?.toInt() ?: sv?.toString()?.toIntOrNull()
                    ?: throw IllegalArgumentException("schemaVersion is required"),
                id = id,
                name = map["name"]?.toString()?.takeIf { it.isNotEmpty() } ?: id,
                rootPath = map["rootPath"]?.toString() ?: "",
                description = map["description"]?.toString()?.takeIf { it.isNotEmpty() },
                enabled = (map["enabled"] as? Boolean) ?: true,
                nodes = nodes,
                revision = revision ?: map["revision"]?.toString()
            )
        }

        fun fromJson(json: JsonObject): VtTree = fromMap(json.map)

        @Suppress("UNCHECKED_CAST")
        private fun asMap(raw: Any?): Map<String, Any?>? = when (raw) {
            is JsonObject -> raw.map
            is Map<*, *> -> raw as Map<String, Any?>
            else -> null
        }
    }
}

/** Validation issue reported through GraphQL. */
data class VtIssue(val code: String, val nodeId: String?, val path: String?, val message: String) {
    fun toGraphQL(): Map<String, Any?> = mapOf("code" to code, "nodeId" to nodeId, "path" to path, "message" to message)
}

object VtCodes {
    const val DUPLICATE_PATH = "DUPLICATE_PATH"
    const val DUPLICATE_ID = "DUPLICATE_ID"
    const val INVALID_PARENT = "INVALID_PARENT"
    const val CYCLE = "CYCLE"
    const val INVALID_SEGMENT = "INVALID_SEGMENT"
    const val UNSUPPORTED_WILDCARD = "UNSUPPORTED_WILDCARD"
    const val TARGET_IN_VIRTUAL_ROOT = "TARGET_IN_VIRTUAL_ROOT"
    const val ROOT_CONFLICT = "ROOT_CONFLICT"
    const val INVALID_TOPIC = "INVALID_TOPIC"
    const val UNSUPPORTED_SCHEMA_VERSION = "UNSUPPORTED_SCHEMA_VERSION"
    const val RESERVED_TOPIC = "RESERVED_TOPIC"
    const val INVALID_BINDING = "INVALID_BINDING"
    const val REVISION_CONFLICT = "REVISION_CONFLICT"
    const val PHYSICAL_CONFLICT = "PHYSICAL_CONFLICT"
    const val NOT_FOUND = "NOT_FOUND"

    const val REJECT_READ_ONLY = "READ_ONLY"
    const val REJECT_UNBOUND = "UNBOUND"
    const val REJECT_DISABLED = "TREE_DISABLED"
}
