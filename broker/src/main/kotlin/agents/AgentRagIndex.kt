package at.rocworks.agents

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.stores.IMessageArchiveExtended
import dev.langchain4j.agent.tool.P
import dev.langchain4j.agent.tool.Tool
import dev.langchain4j.data.document.Metadata
import dev.langchain4j.data.segment.TextSegment
import dev.langchain4j.model.embedding.EmbeddingModel
import dev.langchain4j.store.embedding.EmbeddingSearchRequest
import dev.langchain4j.store.embedding.inmemory.InMemoryEmbeddingStore
import io.vertx.core.json.JsonArray
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger

/**
 * Semantic index over an archive group, used for retrieval-augmented generation (RAG).
 *
 * Indexed documents:
 *  - the current value of every topic in the last-value store that matches one of the configured topic filters
 *  - the archived history of these topics within the lookback window, in chunks of [HISTORY_CHUNK_ROWS] rows (CSV)
 *
 * Refreshes are incremental: every document has a stable key and a content hash, only new or changed
 * documents are embedded, and documents that disappeared are removed. The vector store is in-memory
 * and rebuilt after a broker restart.
 */
class AgentRagIndex(
    private val agentName: String,
    private val embeddingModel: EmbeddingModel,
    private val config: AgentConfig
) {
    private val logger: Logger = Utils.getLogger(AgentRagIndex::class.java)
    private val store = InMemoryEmbeddingStore<TextSegment>()
    // document key -> (content hash, embedding id)
    private val indexed = HashMap<String, Pair<Int, String>>()
    private val refreshing = AtomicBoolean(false)

    data class Hit(val score: Double, val text: String, val topic: String?, val kind: String?)

    companion object {
        const val HISTORY_CHUNK_ROWS = 20
        const val MAX_HISTORY_ROWS_PER_TOPIC = 1000
        const val MAX_DOCUMENTS = 2000
        private const val EMBED_BATCH = 64
    }

    fun size(): Int = synchronized(indexed) { indexed.size }

    /** Collects the documents to index. Blocking, runs on a worker thread. */
    internal fun collectDocuments(): LinkedHashMap<String, TextSegment> {
        val docs = LinkedHashMap<String, TextSegment>()
        val group = Monster.getArchiveHandler()?.getDeployedArchiveGroups()?.get(config.ragArchiveGroup) ?: run {
            logger.warning("Agent $agentName RAG: archive group '${config.ragArchiveGroup}' not found")
            return docs
        }
        val filters = config.ragTopics.ifEmpty { listOf("#") }

        // Current values
        val topics = LinkedHashSet<String>()
        val lastValStore = group.lastValStore
        if (lastValStore != null) {
            for (filter in filters) {
                lastValStore.findMatchingMessages(filter) { msg ->
                    if (topics.add(msg.topicName)) {
                        val text = "Topic ${msg.topicName} current value: ${msg.getPayloadAsString()} (at ${msg.time})"
                        docs["lastval:${msg.topicName}"] = segment(text, msg.topicName, "current")
                    }
                    docs.size < MAX_DOCUMENTS
                }
            }
        }

        // History chunks
        val archive = group.archiveStore as? IMessageArchiveExtended
        if (archive != null && config.ragLookbackSeconds > 0) {
            val end = Instant.now()
            val start = end.minusSeconds(config.ragLookbackSeconds)
            for (topic in topics) {
                if (docs.size >= MAX_DOCUMENTS) break
                val rows = try {
                    archive.getHistory(topic, start, end, MAX_HISTORY_ROWS_PER_TOPIC)
                } catch (e: Exception) {
                    logger.fine { "Agent $agentName RAG: history for $topic failed: ${e.message}" }
                    continue
                }
                chunkHistory(topic, rows).forEachIndexed { i, text ->
                    if (docs.size < MAX_DOCUMENTS) docs["history:$topic:$i"] = segment(text, topic, "history")
                }
            }
        }
        return docs
    }

    private fun segment(text: String, topic: String, kind: String) =
        TextSegment.from(text, Metadata().put("topic", topic).put("kind", kind))

    internal fun chunkHistory(topic: String, rows: JsonArray): List<String> {
        if (rows.isEmpty) return emptyList()
        val keys = rows.getJsonObject(0)?.fieldNames()?.toList() ?: return emptyList()
        val header = keys.joinToString(",")
        return (0 until rows.size()).chunked(HISTORY_CHUNK_ROWS).map { indices ->
            val csv = indices.mapNotNull { rows.getJsonObject(it) }
                .joinToString("\n") { row -> keys.joinToString(",") { row.getValue(it)?.toString() ?: "" } }
            "History of topic $topic:\n$header\n$csv"
        }
    }

    /** Synchronizes the vector store with the archive. Blocking; concurrent calls are skipped. */
    fun refresh() {
        if (!refreshing.compareAndSet(false, true)) return
        try {
            val docs = collectDocuments()
            val toEmbed = ArrayList<Pair<String, TextSegment>>()
            val stale = ArrayList<String>()
            synchronized(indexed) {
                for ((key, segment) in docs) {
                    val existing = indexed[key]
                    if (existing == null || existing.first != segment.text().hashCode()) toEmbed.add(key to segment)
                }
                indexed.keys.filter { it !in docs }.forEach { key -> indexed.remove(key)?.let { stale.add(it.second) } }
            }
            if (stale.isNotEmpty()) store.removeAll(stale)

            for (batch in toEmbed.chunked(EMBED_BATCH)) {
                val embeddings = embeddingModel.embedAll(batch.map { it.second }).content()
                batch.forEachIndexed { i, (key, segment) ->
                    val id = Utils.getUuid()
                    store.add(id, embeddings[i], segment)
                    val replaced = synchronized(indexed) { indexed.put(key, segment.text().hashCode() to id) }
                    if (replaced != null) store.remove(replaced.second)
                }
            }
            if (toEmbed.isNotEmpty() || stale.isNotEmpty()) {
                logger.fine { "Agent $agentName RAG index: ${toEmbed.size} embedded, ${stale.size} removed, ${size()} total" }
            }
        } finally {
            refreshing.set(false)
        }
    }

    fun search(query: String, maxResults: Int): List<Hit> {
        if (size() == 0) return emptyList()
        val queryEmbedding = embeddingModel.embed(query).content()
        val request = EmbeddingSearchRequest.builder()
            .queryEmbedding(queryEmbedding)
            .maxResults(maxResults.coerceIn(1, 50))
            .build()
        return store.search(request).matches().mapNotNull { match ->
            val segment = match.embedded() ?: return@mapNotNull null
            Hit(match.score(), segment.text(), segment.metadata().getString("topic"), segment.metadata().getString("kind"))
        }
    }
}

/** LLM tool exposing the RAG index of an agent. */
class AgentRagTools(
    private val index: AgentRagIndex,
    private val defaultMaxResults: Int,
    private val toolLogger: (String, String, String) -> Unit
) {
    @Tool("Semantic search over the archived MQTT topic values and history of this agent's archive group. " +
        "Use it to find topics, values or past events by meaning when you do not know the exact topic name.")
    fun semanticSearch(
        @P("Natural language description of what to look for") query: String,
        @P("Maximum number of results (optional, default from agent config)", required = false) maxResults: Int?
    ): String {
        val result = try {
            val hits = index.search(query, maxResults?.takeIf { it > 0 } ?: defaultMaxResults)
            if (hits.isEmpty()) "No matching data found (the index may still be building)."
            else hits.joinToString("\n\n") { hit -> "[score=${"%.3f".format(hit.score)}]\n${hit.text}" }
        } catch (e: Exception) {
            "Error: semantic search failed: ${e.message}"
        }
        toolLogger("semanticSearch", "query=$query, maxResults=$maxResults", result)
        return result
    }
}
