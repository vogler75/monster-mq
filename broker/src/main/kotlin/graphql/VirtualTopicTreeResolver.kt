package at.rocworks.extensions.graphql

import at.rocworks.Features
import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.virtualtrees.VirtualTopicTreeManager
import at.rocworks.virtualtrees.VtIssue
import at.rocworks.virtualtrees.VtResolver
import at.rocworks.virtualtrees.VtTree
import graphql.GraphQLException
import graphql.schema.DataFetcher
import java.util.concurrent.CompletableFuture
import java.util.logging.Logger

/**
 * GraphQL queries and mutations for virtual MQTT topic trees (issue #217).
 * The schema (schema-virtualtopictrees.graphqls) is shared with MonsterMQ Edge.
 */
class VirtualTopicTreeResolver(private val authContext: GraphQLAuthContext) {
    private val logger: Logger = Utils.getLogger(VirtualTopicTreeResolver::class.java)

    private fun enabled() = Monster.isFeatureEnabled(Features.VirtualTopicTrees)

    private fun manager(): VirtualTopicTreeManager? = if (enabled()) VirtualTopicTreeManager.getInstance() else null

    private fun issue(code: String, message: String) = VtIssue(code, null, null, message).toGraphQL()

    @Suppress("UNCHECKED_CAST")
    private fun parseInput(raw: Any?): VtTree {
        val map = raw as? Map<String, Any?> ?: throw IllegalArgumentException("input is required")
        return VtTree.fromMap(map, revision = null)
    }

    fun virtualTopicTrees(): DataFetcher<List<Map<String, Any?>>> = DataFetcher {
        manager()?.trees()?.map { it.toGraphQL() } ?: emptyList()
    }

    fun virtualTopicTree(): DataFetcher<Map<String, Any?>?> = DataFetcher { env ->
        val id = env.getArgument<String>("id") ?: return@DataFetcher null
        manager()?.tree(id)?.toGraphQL()
    }

    fun validateVirtualTopicTree(): DataFetcher<CompletableFuture<Map<String, Any?>>> = DataFetcher { env ->
        val future = CompletableFuture<Map<String, Any?>>()
        val mgr = manager()
        if (mgr == null) {
            future.complete(mapOf("valid" to false, "errors" to listOf(issue("FEATURE_DISABLED", "VirtualTopicTrees feature is disabled")), "warnings" to emptyList<Any>()))
            return@DataFetcher future
        }
        val tree = try {
            parseInput(env.getArgument("input"))
        } catch (e: Exception) {
            future.complete(mapOf("valid" to false, "errors" to listOf(issue("INVALID_INPUT", e.message ?: "Invalid input")), "warnings" to emptyList<Any>()))
            return@DataFetcher future
        }
        val errors = mgr.validate(tree)
        mgr.warnings(tree).onComplete { ar ->
            val warnings = if (ar.succeeded()) ar.result() else emptyList()
            future.complete(mapOf(
                "valid" to errors.isEmpty(),
                "errors" to errors.map { it.toGraphQL() },
                "warnings" to warnings.map { it.toGraphQL() }
            ))
        }
        future
    }

    fun previewVirtualTopicTree(): DataFetcher<Map<String, Any?>> = DataFetcher { env ->
        val mgr = manager()
            ?: return@DataFetcher mapOf("routes" to emptyList<Any>(), "errors" to listOf(issue("FEATURE_DISABLED", "VirtualTopicTrees feature is disabled")))
        val tree = try {
            parseInput(env.getArgument("input"))
        } catch (e: Exception) {
            return@DataFetcher mapOf("routes" to emptyList<Any>(), "errors" to listOf(issue("INVALID_INPUT", e.message ?: "Invalid input")))
        }
        val topic = env.getArgument<String>("topicOrFilter") ?: ""
        val operation = env.getArgument<Any>("operation")?.toString() ?: ""
        val errors = mgr.validate(tree)
        if (errors.isNotEmpty()) {
            return@DataFetcher mapOf("routes" to emptyList<Any>(), "errors" to errors.map { it.toGraphQL() })
        }
        mapOf("routes" to preview(tree, topic, operation), "errors" to emptyList<Any>())
    }

    /** Routes of a candidate tree evaluated in isolation; never touches the active configuration. */
    internal fun preview(tree: VtTree, topic: String, operation: String): List<Map<String, Any?>> {
        val r = VtResolver(listOf(tree))
        fun route(nodeId: String, virtual: String, canonical: String, access: String) =
            mapOf("nodeId" to nodeId, "virtualPath" to virtual, "canonicalPath" to canonical, "access" to access)
        return when (operation) {
            "PUBLISH" -> {
                val w = r.winnerRoute(topic) ?: return emptyList()
                val canonical = if (w.binding.kind == VtTree.EXACT) w.binding.target else w.binding.target + topic.substring(w.path.length)
                listOf(route(w.nodeId, topic, canonical, w.binding.access))
            }
            "PROJECT" -> {
                val access = tree.nodes.associate { it.id to (it.binding?.access ?: VtTree.READ_ONLY) }
                r.project(topic).map { route(it.nodeId, it.virtual, topic, access[it.nodeId] ?: VtTree.READ_ONLY) }
            }
            "SUBSCRIBE" -> r.subscribeRoutes(topic).map { rt ->
                route(rt.nodeId, rt.path, r.canonicalFilters(topic, rt).firstOrNull() ?: rt.binding.target, rt.binding.access)
            }
            else -> throw GraphQLException("Unsupported operation '$operation'")
        }
    }

    private fun mutationResult(res: VirtualTopicTreeManager.MutationResult): Map<String, Any?> = mapOf(
        "success" to res.success,
        "tree" to res.tree?.toGraphQL(),
        "appliedRevision" to if (res.success) res.tree?.revision else null,
        "errors" to res.errors.map { it.toGraphQL() }
    )

    private fun failure(code: String, message: String): Map<String, Any?> =
        mapOf("success" to false, "tree" to null, "appliedRevision" to null, "errors" to listOf(issue(code, message)))

    private fun checkAdmin(env: graphql.schema.DataFetchingEnvironment) {
        val auth = authContext.validateFieldAccess(env)
        if (!auth.allowed) throw GraphQLException(auth.errorMessage ?: "Unauthorized")
    }

    fun saveVirtualTopicTree(): DataFetcher<CompletableFuture<Map<String, Any?>>> = DataFetcher { env ->
        checkAdmin(env)
        val future = CompletableFuture<Map<String, Any?>>()
        val mgr = manager()
        if (mgr == null) {
            future.complete(failure("FEATURE_DISABLED", "VirtualTopicTrees feature is disabled"))
            return@DataFetcher future
        }
        val tree = try {
            parseInput(env.getArgument("input"))
        } catch (e: Exception) {
            future.complete(failure("INVALID_INPUT", e.message ?: "Invalid input"))
            return@DataFetcher future
        }
        mgr.save(tree, env.getArgument<String>("expectedRevision")).onComplete { ar ->
            if (ar.succeeded()) future.complete(mutationResult(ar.result()))
            else {
                logger.warning("Saving virtual topic tree '${tree.id}' failed: ${ar.cause()?.message}")
                future.complete(failure("STORE_ERROR", ar.cause()?.message ?: "Save failed"))
            }
        }
        future
    }

    fun deleteVirtualTopicTree(): DataFetcher<CompletableFuture<Map<String, Any?>>> = DataFetcher { env ->
        checkAdmin(env)
        val future = CompletableFuture<Map<String, Any?>>()
        val mgr = manager()
        if (mgr == null) {
            future.complete(failure("FEATURE_DISABLED", "VirtualTopicTrees feature is disabled"))
            return@DataFetcher future
        }
        val id = env.getArgument<String>("id") ?: ""
        mgr.delete(id, env.getArgument<String>("expectedRevision")).onComplete { ar ->
            if (ar.succeeded()) future.complete(mutationResult(ar.result()))
            else {
                logger.warning("Deleting virtual topic tree '$id' failed: ${ar.cause()?.message}")
                future.complete(failure("STORE_ERROR", ar.cause()?.message ?: "Delete failed"))
            }
        }
        future
    }
}
