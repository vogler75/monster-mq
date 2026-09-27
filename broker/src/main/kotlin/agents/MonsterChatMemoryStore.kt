package at.rocworks.agents

import at.rocworks.Const
import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.stores.DatabaseConnection
import at.rocworks.stores.sqlite.SQLiteClient
import at.rocworks.stores.sqlite.SQLiteDatabasePath
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.Indexes
import com.mongodb.client.model.ReplaceOptions
import dev.langchain4j.data.message.ChatMessage
import dev.langchain4j.data.message.ChatMessageDeserializer
import dev.langchain4j.data.message.ChatMessageSerializer
import dev.langchain4j.store.memory.chat.ChatMemoryStore
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.bson.Document
import java.sql.Connection
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

/**
 * Persistence contract for MonsterMQ agent conversational memory.
 */
interface IChatMemoryPersistence {
    fun init(vertx: Vertx? = null): Future<Boolean>
    fun loadMessages(agentName: String, sessionId: String): List<ChatMessage>?
    fun saveMessages(agentName: String, sessionId: String, messagesJson: String, messageCount: Int)
    fun deleteMessages(agentName: String, sessionId: String)
    fun close() {}
}

/**
 * In-memory persistence implementation for tests or memory-only mode.
 */
class InMemoryChatMemoryPersistence : IChatMemoryPersistence {
    private val store = ConcurrentHashMap<String, String>()

    override fun init(vertx: Vertx?): Future<Boolean> = Future.succeededFuture(true)

    override fun loadMessages(agentName: String, sessionId: String): List<ChatMessage>? {
        val json = store["$agentName:$sessionId"] ?: return null
        return ChatMessageDeserializer.messagesFromJson(json)
    }

    override fun saveMessages(agentName: String, sessionId: String, messagesJson: String, messageCount: Int) {
        store["$agentName:$sessionId"] = messagesJson
    }

    override fun deleteMessages(agentName: String, sessionId: String) {
        store.remove("$agentName:$sessionId")
    }
}

/**
 * SQLite persistence for agent conversational memory.
 */
class SqliteChatMemoryPersistence(
    private val vertx: Vertx,
    private val dbPath: String
) : IChatMemoryPersistence {
    private val logger = Utils.getLogger(SqliteChatMemoryPersistence::class.java)
    private val sqliteClient = SQLiteClient(vertx, dbPath)

    override fun init(vertx: Vertx?): Future<Boolean> {
        val ddl = """
            CREATE TABLE IF NOT EXISTS agent_chat_memory (
                agent_name TEXT NOT NULL,
                session_id TEXT NOT NULL,
                messages TEXT NOT NULL,
                message_count INTEGER NOT NULL,
                updated_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (agent_name, session_id)
            )
        """.trimIndent()
        val indexDdl = "CREATE INDEX IF NOT EXISTS idx_agent_chat_memory_updated ON agent_chat_memory(updated_at)"
        return sqliteClient.executeUpdate(ddl).compose {
            sqliteClient.executeUpdate(indexDdl)
        }.map { true }
    }

    override fun loadMessages(agentName: String, sessionId: String): List<ChatMessage>? {
        return try {
            val sql = "SELECT messages FROM agent_chat_memory WHERE agent_name = ? AND session_id = ?"
            val result = sqliteClient.executeQuerySync(sql, JsonArray().add(agentName).add(sessionId))
            if (result.size() > 0) {
                val json = result.getJsonObject(0).getString("messages")
                if (!json.isNullOrBlank()) {
                    ChatMessageDeserializer.messagesFromJson(json)
                } else null
            } else null
        } catch (e: Exception) {
            logger.warning("Error loading chat memory for $agentName:$sessionId: ${e.message}")
            null
        }
    }

    override fun saveMessages(agentName: String, sessionId: String, messagesJson: String, messageCount: Int) {
        val sql = """
            INSERT INTO agent_chat_memory (agent_name, session_id, messages, message_count, updated_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(agent_name, session_id) DO UPDATE SET
                messages = excluded.messages,
                message_count = excluded.message_count,
                updated_at = datetime('now')
        """.trimIndent()
        sqliteClient.executeUpdate(sql, JsonArray().add(agentName).add(sessionId).add(messagesJson).add(messageCount))
            .onFailure { e -> logger.warning("Error saving chat memory for $agentName:$sessionId: ${e.message}") }
    }

    override fun deleteMessages(agentName: String, sessionId: String) {
        val sql = "DELETE FROM agent_chat_memory WHERE agent_name = ? AND session_id = ?"
        sqliteClient.executeUpdate(sql, JsonArray().add(agentName).add(sessionId))
            .onFailure { e -> logger.warning("Error deleting chat memory for $agentName:$sessionId: ${e.message}") }
    }
}

/**
 * PostgreSQL persistence for agent conversational memory.
 */
class PostgresChatMemoryPersistence(
    private val url: String,
    private val user: String,
    private val pass: String,
    private val schema: String? = null
) : DatabaseConnection(Utils.getLogger(PostgresChatMemoryPersistence::class.java), url, user, pass), IChatMemoryPersistence {

    private val logger = Utils.getLogger(PostgresChatMemoryPersistence::class.java)
    private val tableName = if (schema.isNullOrBlank()) "agent_chat_memory" else "$schema.agent_chat_memory"

    override fun init(connection: Connection): Future<Void> {
        val promise = Promise.promise<Void>()
        try {
            connection.createStatement().use { stmt ->
                stmt.execute("""
                    CREATE TABLE IF NOT EXISTS $tableName (
                        agent_name VARCHAR(128) NOT NULL,
                        session_id VARCHAR(128) NOT NULL,
                        messages TEXT NOT NULL,
                        message_count INT NOT NULL,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (agent_name, session_id)
                    );
                    CREATE INDEX IF NOT EXISTS idx_agent_chat_memory_updated ON $tableName(updated_at);
                """.trimIndent())
            }
            promise.complete()
        } catch (e: Exception) {
            logger.warning("Error initializing Postgres chat memory table: ${e.message}")
            promise.fail(e)
        }
        return promise.future()
    }

    override fun init(vertx: Vertx?): Future<Boolean> {
        val promise = Promise.promise<Void>()
        val v = vertx ?: Monster.getVertx() ?: return Future.succeededFuture(false)
        start(v, promise)
        return promise.future().map { true }
    }

    // One JDBC connection is shared by all agents (each runs on its own worker thread), so access is serialized.
    override fun loadMessages(agentName: String, sessionId: String): List<ChatMessage>? = synchronized(this) {
        try {
            val sql = "SELECT messages FROM $tableName WHERE agent_name = ? AND session_id = ?"
            connection?.prepareStatement(sql)?.use { ps ->
                ps.setString(1, agentName)
                ps.setString(2, sessionId)
                ps.executeQuery().use { rs ->
                    if (rs.next()) {
                        val json = rs.getString("messages")
                        if (!json.isNullOrBlank()) {
                            ChatMessageDeserializer.messagesFromJson(json)
                        } else null
                    } else null
                }
            }
        } catch (e: Exception) {
            logger.warning("Error loading Postgres chat memory for $agentName:$sessionId: ${e.message}")
            null
        }
    }

    override fun saveMessages(agentName: String, sessionId: String, messagesJson: String, messageCount: Int): Unit = synchronized(this) {
        try {
            val sql = """
                INSERT INTO $tableName (agent_name, session_id, messages, message_count, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (agent_name, session_id) DO UPDATE SET
                    messages = EXCLUDED.messages,
                    message_count = EXCLUDED.message_count,
                    updated_at = CURRENT_TIMESTAMP
            """.trimIndent()
            connection?.prepareStatement(sql)?.use { ps ->
                ps.setString(1, agentName)
                ps.setString(2, sessionId)
                ps.setString(3, messagesJson)
                ps.setInt(4, messageCount)
                ps.executeUpdate()
            }
        } catch (e: Exception) {
            logger.warning("Error saving Postgres chat memory for $agentName:$sessionId: ${e.message}")
        }
    }

    override fun deleteMessages(agentName: String, sessionId: String): Unit = synchronized(this) {
        try {
            val sql = "DELETE FROM $tableName WHERE agent_name = ? AND session_id = ?"
            connection?.prepareStatement(sql)?.use { ps ->
                ps.setString(1, agentName)
                ps.setString(2, sessionId)
                ps.executeUpdate()
            }
        } catch (e: Exception) {
            logger.warning("Error deleting Postgres chat memory for $agentName:$sessionId: ${e.message}")
        }
    }

    override fun close() {
        stop()
    }
}

/**
 * MongoDB persistence for agent conversational memory.
 */
class MongoChatMemoryPersistence(
    private val uri: String,
    private val database: String
) : IChatMemoryPersistence {
    private val logger = Utils.getLogger(MongoChatMemoryPersistence::class.java)
    private var collection: MongoCollection<Document>? = null
    private var clientAcquired = false

    override fun init(vertx: Vertx?): Future<Boolean> {
        val promise = Promise.promise<Boolean>()
        try {
            val client = at.rocworks.stores.mongodb.MongoClientPool.getClient(uri)
            clientAcquired = true
            val db = client.getDatabase(database)
            val col = db.getCollection("agent_chat_memory")
            col.createIndex(Indexes.ascending("updated_at"), IndexOptions())
            collection = col
            promise.complete(true)
        } catch (e: Exception) {
            logger.warning("Error initializing Mongo chat memory collection: ${e.message}")
            promise.complete(false)
        }
        return promise.future()
    }

    override fun loadMessages(agentName: String, sessionId: String): List<ChatMessage>? {
        return try {
            val doc = collection?.find(Filters.and(Filters.eq("agent_name", agentName), Filters.eq("session_id", sessionId)))?.first()
            val json = doc?.getString("messages")
            if (!json.isNullOrBlank()) {
                ChatMessageDeserializer.messagesFromJson(json)
            } else null
        } catch (e: Exception) {
            logger.warning("Error loading Mongo chat memory for $agentName:$sessionId: ${e.message}")
            null
        }
    }

    override fun saveMessages(agentName: String, sessionId: String, messagesJson: String, messageCount: Int) {
        try {
            val filter = Filters.and(Filters.eq("agent_name", agentName), Filters.eq("session_id", sessionId))
            val doc = Document()
                .append("agent_name", agentName)
                .append("session_id", sessionId)
                .append("messages", messagesJson)
                .append("message_count", messageCount)
                .append("updated_at", Instant.now())
            collection?.replaceOne(filter, doc, ReplaceOptions().upsert(true))
        } catch (e: Exception) {
            logger.warning("Error saving Mongo chat memory for $agentName:$sessionId: ${e.message}")
        }
    }

    override fun deleteMessages(agentName: String, sessionId: String) {
        try {
            collection?.deleteOne(Filters.and(Filters.eq("agent_name", agentName), Filters.eq("session_id", sessionId)))
        } catch (e: Exception) {
            logger.warning("Error deleting Mongo chat memory for $agentName:$sessionId: ${e.message}")
        }
    }

    override fun close() {
        collection = null
        if (clientAcquired) {
            clientAcquired = false
            at.rocworks.stores.mongodb.MongoClientPool.releaseClient(uri)
        }
    }
}

/**
 * MonsterMQ implementation of LangChain4j ChatMemoryStore.
 * Caches active memory windows in memory for fast synchronous LLM access,
 * and persists to backing database stores (SQLite, PostgreSQL, MongoDB).
 */
class MonsterChatMemoryStore(
    private val persistence: IChatMemoryPersistence? = null
) : ChatMemoryStore {

    private val cache = ConcurrentHashMap<String, MutableList<ChatMessage>>()

    override fun getMessages(memoryId: Any): List<ChatMessage> {
        val key = memoryId.toString()
        val cached = cache[key]
        if (cached != null) {
            return ArrayList(cached)
        }

        if (persistence != null) {
            val (agentName, sessionId) = parseMemoryId(key)
            val loaded = persistence.loadMessages(agentName, sessionId)
            if (loaded != null) {
                val list = ArrayList(loaded)
                cache[key] = list
                return list
            }
        }

        val empty = ArrayList<ChatMessage>()
        cache[key] = empty
        return empty
    }

    override fun updateMessages(memoryId: Any, messages: List<ChatMessage>) {
        val key = memoryId.toString()
        cache[key] = ArrayList(messages)

        if (persistence != null) {
            val (agentName, sessionId) = parseMemoryId(key)
            val json = ChatMessageSerializer.messagesToJson(messages)
            persistence.saveMessages(agentName, sessionId, json, messages.size)
        }
    }

    override fun deleteMessages(memoryId: Any) {
        val key = memoryId.toString()
        cache.remove(key)

        if (persistence != null) {
            val (agentName, sessionId) = parseMemoryId(key)
            persistence.deleteMessages(agentName, sessionId)
        }
    }

    /** Drops a memory from the in-memory cache only; persisted messages are reloaded on next access. */
    fun evictFromCache(memoryId: Any) {
        cache.remove(memoryId.toString())
    }

    /** Drops all cached memories of an agent (e.g. when the agent stops). */
    fun evictAgentFromCache(agentName: String) {
        cache.keys.removeIf { it.startsWith("$agentName:") }
    }

    fun close() {
        cache.clear()
        persistence?.close()
    }

    companion object {
        private val logger: Logger = Utils.getLogger(MonsterChatMemoryStore::class.java)
        private val sharedLock = Any()
        private var sharedInstance: MonsterChatMemoryStore? = null
        private var sharedRefCount = 0

        /**
         * Returns the process-wide store (created on first use). Every call must be paired with [release];
         * the backing persistence is closed when the last agent releases it.
         */
        fun acquire(storeType: String?, config: JsonObject, vertx: Vertx): MonsterChatMemoryStore {
            synchronized(sharedLock) {
                val store = sharedInstance ?: create(storeType, config, vertx).also { sharedInstance = it }
                sharedRefCount++
                return store
            }
        }

        fun release(store: MonsterChatMemoryStore) {
            synchronized(sharedLock) {
                if (store !== sharedInstance) return
                sharedRefCount--
                if (sharedRefCount <= 0) {
                    sharedRefCount = 0
                    sharedInstance = null
                    store.close()
                }
            }
        }

        fun parseMemoryId(memoryId: String): Pair<String, String> {
            val parts = memoryId.split(":", limit = 2)
            val agentName = parts[0]
            val sessionId = if (parts.size > 1 && parts[1].isNotBlank()) parts[1] else "default"
            return Pair(agentName, sessionId)
        }

        fun create(storeType: String?, config: JsonObject, vertx: Vertx): MonsterChatMemoryStore {
            val persistence: IChatMemoryPersistence = when (storeType?.uppercase()) {
                "SQLITE" -> {
                    val sqliteConfig = config.getJsonObject("SQLite")
                    val directory = sqliteConfig?.getString("Path", Const.SQLITE_DEFAULT_PATH) ?: Const.SQLITE_DEFAULT_PATH
                    val dbPath = "$directory/monstermq.db"
                    SqliteChatMemoryPersistence(vertx, dbPath)
                }
                "POSTGRES" -> {
                    val postgresConfig = config.getJsonObject("Postgres")
                    if (postgresConfig != null) {
                        val url = postgresConfig.getString("Url")
                        val user = postgresConfig.getString("User")
                        val pass = postgresConfig.getString("Pass")
                        val schema = postgresConfig.getString("Schema")
                        if (url != null && user != null && pass != null) {
                            PostgresChatMemoryPersistence(url, user, pass, schema)
                        } else InMemoryChatMemoryPersistence()
                    } else InMemoryChatMemoryPersistence()
                }
                "MONGODB" -> {
                    val mongoConfig = config.getJsonObject("MongoDB")
                    if (mongoConfig != null) {
                        val uri = mongoConfig.getString("Url") ?: mongoConfig.getString("Uri")
                        val database = mongoConfig.getString("Database")
                        if (uri != null && database != null) {
                            MongoChatMemoryPersistence(uri, database)
                        } else InMemoryChatMemoryPersistence()
                    } else InMemoryChatMemoryPersistence()
                }
                else -> InMemoryChatMemoryPersistence()
            }

            persistence.init(vertx).onComplete { result ->
                if (result.failed() || result.result() != true) {
                    logger.warning("Agent chat memory persistence (${storeType ?: "MEMORY"}) failed to initialize: ${result.cause()?.message ?: "not available"}; memory will not survive restarts")
                } else {
                    logger.info("Agent chat memory persistence initialized (${storeType ?: "MEMORY"})")
                }
            }
            return MonsterChatMemoryStore(persistence)
        }
    }
}
