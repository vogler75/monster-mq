# Agent Improvement Code Review Findings (Issues #189 - #194)

Date: 2026-09-27  
Status: Resolved — findings 1–3 fixed; 4–5 need no code change. Also fixed: dashboard edits moved to the monster-mq-dashboard repo.

This document records the code review findings across the implementation of GitHub issues #189, #190, #191, #192, #193, and #194, excluding the Python SDK example script (`monster_agent.py`).

---

## 1. PostgreSQL Chat Memory Store Concurrency (High Priority)

- **File**: `broker/src/main/kotlin/agents/MonsterChatMemoryStore.kt`
- **Class**: `PostgresChatMemoryPersistence` (lines 169–224)
- **Component**: Issue #190 (Conversation Memory Persistence)

### Finding
`PostgresChatMemoryPersistence` extends `DatabaseConnection`, which maintains a single `var connection: Connection?`. In `MonsterChatMemoryStore`, a single process-wide singleton (`sharedInstance`) is shared across all agents in the broker.

Because each `AgentExecutor` runs on its own dedicated single-thread executor (`worker = Executors.newFixedThreadPool(1)` per agent), multiple agents executing in parallel or handling concurrent tasks will invoke:
```kotlin
connection?.prepareStatement(sql)?.use { ps -> ... }
```
simultaneously on the **same underlying JDBC `Connection` object** without any synchronization.

JDBC `java.sql.Connection` instances are **not thread-safe**. Concurrent operations on a single PostgreSQL connection will result in `org.postgresql.util.PSQLException` (such as *"A result was returned when none was expected"*, socket errors, or transaction state corruption).

### Recommended Fix
Synchronize database operations within `PostgresChatMemoryPersistence` on `this` (or a dedicated mutex lock):

```kotlin
override fun loadMessages(agentName: String, sessionId: String): List<ChatMessage>? = synchronized(this) {
    return try {
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

override fun saveMessages(agentName: String, sessionId: String, messagesJson: String, messageCount: Int) = synchronized(this) {
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

override fun deleteMessages(agentName: String, sessionId: String) = synchronized(this) {
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
```

---

## 2. Reactive Streams `Long` Overflow in `agentStream` Subscription (Medium Priority)

- **File**: `broker/src/main/kotlin/graphql/SubscriptionResolver.kt`
- **Class**: `AgentStreamSubscription.request(n: Long)` (lines 173–184)
- **Component**: Issue #193 (Token Streaming over GraphQL)

### Finding
In `AgentStreamSubscription.request(n: Long)`:
```kotlin
override fun request(n: Long) {
    if (cancelled.get()) return
    synchronized(this) {
        requested.addAndGet(n)
        while (requested.get() > 0 && pending.isNotEmpty()) {
            subscriber.onNext(pending.removeFirst())
            requested.decrementAndGet()
        }
    }
}
```
GraphQL subscription clients (such as Apollo Client, Relay, or `graphql-ws`) commonly send `request(Long.MAX_VALUE)` to signal unbounded demand. If `requested` already has a small positive count (e.g. 1), executing `requested.addAndGet(Long.MAX_VALUE)` causes standard Java signed 64-bit integer overflow into `Long.MIN_VALUE` (negative).

When `requested.get()` becomes negative, `requested.get() > 0` evaluates to `false`, permanently blocking delivery of token chunks to the subscriber.

### Recommended Fix
Cap `requested` at `Long.MAX_VALUE` before addition:

```kotlin
override fun request(n: Long) {
    if (cancelled.get()) return
    synchronized(this) {
        if (n == Long.MAX_VALUE || requested.get() > Long.MAX_VALUE - n) {
            requested.set(Long.MAX_VALUE)
        } else {
            requested.addAndGet(n)
        }
        while (requested.get() > 0 && pending.isNotEmpty()) {
            subscriber.onNext(pending.removeFirst())
            requested.decrementAndGet()
        }
    }
}
```

---

## 3. Kotlin Compiler Warnings in `AgentExecutor.kt` (Low Priority)

- **File**: `broker/src/main/kotlin/agents/AgentExecutor.kt`
- **Method**: `setupRagIndex()` (lines 650–652)
- **Component**: Issue #191 (RAG Integration)

### Finding
The Kotlin compiler emits three compiler warnings during build:
```
[WARNING] AgentExecutor.kt:650:48 Unnecessary safe call on a non-null receiver of type 'ChatModelConfig'.
[WARNING] AgentExecutor.kt:651:50 Unnecessary safe call on a non-null receiver of type 'ChatModelConfig'.
[WARNING] AgentExecutor.kt:652:56 Unnecessary safe call on a non-null receiver of type 'ChatModelConfig'.
```

Code:
```kotlin
val base = modelConfig
val provider = agentConfig.embeddingProvider?.takeIf { it.isNotBlank() } ?: base?.provider ?: agentConfig.provider
val sameProvider = base != null && provider.equals(base.provider, ignoreCase = true)
val embeddingModel = LangChain4jFactory.createEmbeddingModel(
    provider = provider,
    model = agentConfig.embeddingModel?.takeIf { it.isNotBlank() },
    apiKey = if (sameProvider) base?.apiKey else null,
    endpoint = if (sameProvider) base?.endpoint else null,
    serviceVersion = if (sameProvider) base?.serviceVersion else null,
    globalConfig = globalConfig ?: JsonObject()
)
```

Because `sameProvider` explicitly checks `base != null`, smart-casting applies, making `base?.apiKey` redundant.

### Recommended Fix
Replace `base?.` with `base.`:
```kotlin
    apiKey = if (sameProvider) base.apiKey else null,
    endpoint = if (sameProvider) base.endpoint else null,
    serviceVersion = if (sameProvider) base.serviceVersion else null,
```

---

## 4. GraphQL Schema Parity & Edge Broker Awareness (Informational / Guidelines)

- **Files**:
  - `broker/src/main/resources/schema-subscriptions.graphqls`
  - `broker/src/main/resources/schema-agents.graphqls`
- **Cross-Repo Impact**: `monster-mq-edge` (Go Edge Broker) and `monster-mq-dashboard` (Web UI)

### Finding
1. **Schema Additions**:
   - `schema-agents.graphqls` added 12 fields to `type Agent` and `input AgentInput`: `persistMemory`, `maxCallDepth`, `streamingEnabled`, `contextMaxTokens`, `ragEnabled`, `ragArchiveGroup`, `ragTopics`, `ragLookbackSeconds`, `ragRefreshSeconds`, `ragMaxResults`, `embeddingProvider`, `embeddingModel`.
   - `schema-subscriptions.graphqls` added `subscription { agentStream(...) }` and `type AgentStreamChunk`.
2. **Parity Assessment**:
   - The Go edge broker (`monster-mq-edge`) omits the entire `schema-agents.graphqls` file, which is permitted under the "droppable subsystem" rule (`agents` is not implemented on the edge).
   - However, `schema-subscriptions.graphqls` in `monster-mq-edge` does not have `agentStream`.
3. **Dashboard Requirement**:
   - The web dashboard (`monster-mq-dashboard`) must check `Broker.enabledFeatures.includes("AiAgents")` (or feature flags) before invoking `subscription { agentStream(...) }`. This prevents GraphQL query validation errors when the dashboard is connected to a Go edge broker.

---

## 5. Stream Correlation & Wildcard Subscriptions (Design Clarification)

- **File**: `broker/src/main/kotlin/agents/AgentExecutor.kt` (lines 1822–1825)
- **Component**: Issue #193 (Token Streaming)

### Finding
In `invokeStreaming`:
```kotlin
val streamId = request.taskId ?: currentTransactionId ?: Utils.getUuid()
val topic = a2aAgentTopic("stream/$streamId")
```
When an agent is triggered manually or via an MQTT input topic (no explicit `taskId`), `streamId` falls back to the generated transaction ID (`txId`).

In `schema-subscriptions.graphqls`:
```graphql
agentStream(
    agentName: String!
    taskId: String = "+"
    org: String = "default"
    site: String = "default"
): AgentStreamChunk!
```

### Recommendation
Dashboard views and external GraphQL subscribers should rely on the default `taskId: "+"` wildcard when monitoring live agent output, unless they are tracking an explicit A2A task whose `taskId` they already know.

---

## Verification & Test Results

The suite was verified with Maven:
- `mvn test-compile`: **0 errors**
- `mvn test -Dtest="AgentToolsTest,ContextBudgetTest,MonsterChatMemoryStoreTest,TurnAwareChatMemoryTest"`:
  - `MonsterChatMemoryStoreTest`: 3 passed, 0 failures
  - `TurnAwareChatMemoryTest`: 7 passed, 0 failures
  - `ContextBudgetTest`: 5 passed, 0 failures
  - `AgentToolsTest`: 7 passed, 0 failures
  - **Total**: 22 passed, 0 failures, 0 errors
