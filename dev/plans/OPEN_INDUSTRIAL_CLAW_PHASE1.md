# OpenClaw Phase 1: AI Agent Runtime Foundation

## Context

MonsterMQ already has every building block for AI agents (MQTT pub/sub, archive stores, MCP server, GenAI provider, flow engine, device management). What's missing is the **agent lifecycle layer** — a way to deploy autonomous LLM-powered agents as first-class broker entities that can subscribe to topics, call tools, reason via ReAct loops, and publish results.

This plan implements Phase 1 from `plans/OPEN_INDUSTRIAL_CLAW.md`: in-broker agents running as Vert.x verticles, managed via GraphQL, with LLM tool calling and access to the broker's own MCP tools.

---

## Framework Decision: LangChain4j

**Use LangChain4j** (v1.0.0-beta4 or latest stable) as the agent framework.

**Why:**
- Built-in ReAct loop via AI Services — no need to hand-code the tool-calling cycle
- MCP client module (`langchain4j-mcp`) — agents can call broker's own MCP tools or external MCP servers
- Multi-provider out of the box: Anthropic, OpenAI, Google Gemini, Ollama (local models)
- Conversation memory with configurable windowing
- Tool dispatch handled automatically from `@Tool` annotations or `ToolSpecification` objects
- Modular Maven deps — add only what we need

**Vert.x integration:** LangChain4j calls are blocking I/O (HTTP to LLMs). Use `vertx.executeBlocking()` — the same pattern already proven for GraalVM script execution in `FlowInstanceExecutor`.

**Impact on existing GenAI system:** The current `IGenAiProvider` is text-in/text-out with no tool calling. Rather than extend it, create a `LangChain4jProviderAdapter` implementing `IGenAiProvider` so `GenAiResolver` (dashboard code assistant) keeps working. Over time, LangChain4j replaces the custom Gemini SDK entirely.

**Alternative considered:** Direct LLM SDKs (anthropic-java, openai-java, google-genai) + MCP Kotlin SDK + manual ReAct loop (~200 lines). More control, smaller footprint, but significantly more work and no built-in memory/tool orchestration.

---

## Architecture

Follows the exact Extension + Connector pattern used by all device bridges:

```
AgentExtension (cluster-aware coordinator, one per broker node)
    |
    +-- AgentExecutor (per-agent verticle)
            +-- LangChain4j ChatLanguageModel (LLM client)
            +-- LangChain4j AiServices (ReAct tool loop)
            +-- AgentTools (MCP tools + MQTT publish exposed as @Tool)
            +-- MQTT subscriptions (via SessionHandler internal client)
            +-- Persistent state (published as retained msg on agents/{name}/state)
```

Agents stored in `IDeviceConfigStore` with `type = "Agent"` — same store as all other devices.

---

## Files to Create

### 1. `broker/src/main/kotlin/agents/AgentConfig.kt`
Agent-specific config data class, parsed from `DeviceConfig.config` JsonObject.

```kotlin
data class AgentConfig(
    val description: String,
    val skills: List<AgentSkill>,
    val inputTopics: List<String>,
    val outputTopics: List<String>,
    val triggerType: TriggerType,          // MQTT, CRON, MANUAL
    val cronExpression: String?,
    val cronIntervalMs: Long?,
    val provider: String,                  // "gemini", "claude", "openai", "ollama"
    val model: String?,                    // null = provider default
    val apiKey: String?,                   // null = use global GenAI config
    val systemPrompt: String,
    val maxTokens: Int?,
    val temperature: Double,
    val maxToolIterations: Int,            // ReAct loop safety limit (default 10)
    val memoryWindowSize: Int,             // Conversation memory (default 20)
    val stateEnabled: Boolean
)
enum class TriggerType { MQTT, CRON, MANUAL }
data class AgentSkill(val name: String, val description: String, val inputSchema: JsonObject?)
```

### 2. `broker/src/main/kotlin/agents/AgentExtension.kt`
Cluster-aware coordinator. Pattern: `flowengine/FlowEngineExtension.kt`.

- `start()`: init device store, load agents where `type == "Agent"` and `nodeId == currentNode`, deploy one `AgentExecutor` per enabled agent
- EventBus listener on `agent.config.changed` for add/update/delete/toggle/reassign
- Tracks `ConcurrentHashMap<String, String>` (agentName -> deploymentId)
- `stop()`: undeploy all agent verticles

### 3. `broker/src/main/kotlin/agents/AgentExecutor.kt`
Per-agent verticle. Pattern: `flowengine/FlowInstanceExecutor.kt`.

**On start():**
1. Parse `AgentConfig` from deployment config
2. Create LangChain4j `ChatLanguageModel` via `LangChain4jFactory`
3. Create `AgentTools` instance (MCP tools + MQTT publish)
4. Build `AiServices` proxy with model, tools, memory
5. Subscribe to input MQTT topics via `SessionHandler.subscribeInternalClient("agent-{name}", ...)`
6. Register EventBus consumer for incoming messages
7. If CRON trigger: set up `vertx.setPeriodic()` timer
8. Publish Agent Card as retained message on `agents/{name}/card`

**On MQTT message / timer tick:**
1. Build user message from topic + payload
2. `vertx.executeBlocking { aiService.chat(userMessage) }` — LangChain4j handles full ReAct loop internally
3. On complete: publish response to output topics, persist state, update metrics

**On stop():**
- Unsubscribe MQTT client, cancel timers, publish offline health status

### 4. `broker/src/main/kotlin/agents/AgentTools.kt`
Exposes broker capabilities as LangChain4j `@Tool` methods:

- `publishMessage(topic, payload)` — publish MQTT message via SessionHandler
- `getTopicValue(topic)` — delegate to McpHandler's get-topic-value tool
- `queryHistory(topic, startTime, endTime, limit)` — delegate to query-message-archive
- `findTopics(pattern)` — delegate to find-topics-by-name
- `queryArchiveSQL(sql)` — delegate to query-message-archive-by-sql
- `listArchiveGroups()` — delegate to list-archive-groups

Reuses existing `McpHandler` / `ArchiveHandler` logic directly (call the same methods, not HTTP).

### 5. `broker/src/main/kotlin/agents/LangChain4jFactory.kt`
Factory creating `ChatLanguageModel` from provider name + config:

- `"gemini"` -> `GoogleAiGeminiChatModel`
- `"claude"` -> `AnthropicChatModel`
- `"openai"` -> `OpenAiChatModel`
- `"ollama"` -> `OllamaChatModel`

Resolves API keys: per-agent config > global GenAI config > environment variable.

### 6. `broker/src/main/kotlin/agents/LangChain4jProviderAdapter.kt`
Adapter: `LangChain4j ChatLanguageModel` -> `IGenAiProvider` interface. Keeps existing `GenAiResolver` working without changes.

### 7. `broker/src/main/resources/schema-agents.graphqls`
GraphQL schema for agents:

```graphql
type Agent {
    name: String!
    description: String
    namespace: String!
    nodeId: String!
    enabled: Boolean!
    status: AgentStatus!
    skills: [AgentSkill!]!
    inputTopics: [String!]!
    outputTopics: [String!]!
    triggerType: AgentTriggerType!
    cronExpression: String
    provider: String
    model: String
    systemPrompt: String
    maxTokens: Int
    temperature: Float
    stateEnabled: Boolean!
    createdAt: String!
    updatedAt: String!
}

type AgentSkill {
    name: String!
    description: String!
    inputSchema: JSON
}

enum AgentStatus { STARTING, RUNNING, STOPPED, ERROR }
enum AgentTriggerType { MQTT, CRON, MANUAL }

input AgentInput {
    name: String!
    namespace: String!
    nodeId: String!
    description: String
    skills: [AgentSkillInput!]
    inputTopics: [String!]
    outputTopics: [String!]
    triggerType: AgentTriggerType
    cronExpression: String
    provider: String
    model: String
    apiKey: String
    systemPrompt: String
    maxTokens: Int
    temperature: Float
    maxToolIterations: Int
    memoryWindowSize: Int
    stateEnabled: Boolean
    enabled: Boolean
}

input AgentSkillInput {
    name: String!
    description: String!
    inputSchema: JSON
}

extend type Query {
    agents(nodeId: String, enabled: Boolean): [Agent!]!
    agent(name: String!): Agent
}

extend type Mutation {
    agent: AgentMutations
}

type AgentMutations {
    create(input: AgentInput!): Agent!
    update(name: String!, input: AgentInput!): Agent!
    delete(name: String!): Boolean!
    start(name: String!): Agent!
    stop(name: String!): Agent!
    invoke(name: String!, input: JSON!): JSON
}
```

### 8. `broker/src/main/kotlin/graphql/AgentQueries.kt`
Query resolvers: `agents(nodeId, enabled)`, `agent(name)`. Filter `IDeviceConfigStore` by `type == "Agent"`.

### 9. `broker/src/main/kotlin/graphql/AgentMutations.kt`
Mutation resolvers: create, update, delete, start, stop, invoke. Notify `agent.config.changed` EventBus address.

### 10. `dashboard/src/pages/agents.html` + `dashboard/src/js/agents.js`
Agent management page. Pattern: `mqtt-clients.html` / `mqtt-clients.js`.
- Table: Name, Status, Provider/Model, Trigger Type, Input Topics, Node
- Create/Edit modal with all AgentInput fields
- System prompt as textarea/code editor
- Start/Stop toggle, Delete button
- "Test Agent" button calling invoke mutation

---

## Files to Modify

| File | Change |
|------|--------|
| `broker/pom.xml` | Add LangChain4j deps: `langchain4j`, `langchain4j-google-ai-gemini`, `langchain4j-anthropic`, `langchain4j-open-ai`, `langchain4j-ollama` |
| `broker/src/main/kotlin/stores/DeviceConfig.kt` | Add `const val DEVICE_TYPE_AGENT = "Agent"` |
| `broker/src/main/kotlin/Monster.kt` | Deploy `AgentExtension` in the startup compose chain (after FlowEngineExtension) |
| `broker/src/main/kotlin/graphql/GraphQLServer.kt` | Add `"schema-agents.graphqls"` to `schemaFiles` list, wire `AgentQueries`/`AgentMutations` in `buildRuntimeWiring()` |
| `broker/src/main/kotlin/genai/GenAiProviderFactory.kt` | Use `LangChain4jProviderAdapter` for all providers (backward-compatible) |
| `dashboard/src/js/sidebar.js` | Add "AI Agents" menu item under Configuration section |

---

## Agent Execution Loop (Detail)

```
MQTT msg arrives on input topic
  -> EventBus delivers to AgentExecutor
  -> Extract topic + payload -> build user message
  -> vertx.executeBlocking {
       // LangChain4j AiServices handles ReAct loop:
       // 1. Send system prompt + memory + message + tool specs to LLM
       // 2. LLM returns: final text OR tool call request
       // 3. If tool call: dispatch to AgentTools method (e.g., queryHistory)
       //    -> calls ArchiveHandler directly (in-process, not HTTP)
       // 4. Feed tool result back to LLM
       // 5. Repeat until final answer or maxToolIterations hit
       response = aiService.chat(userMessage)
     }
  -> Publish response to each output topic
  -> Persist state to agents/{name}/state (retained)
  -> Update metrics (messagesProcessed++, llmCalls++)
```

---

## MQTT Topic Namespace

```
agents/{agent-name}/
├── card                    — Agent Card (retained, JSON)
├── health                  — Heartbeat/status (retained)
├── state                   — Persistent state snapshot (retained)
├── logs                    — Agent log output
└── tasks/
    ├── new                 — Submit new task (for MANUAL trigger / A2A Phase 3)
    └── {task-id}/
        ├── status          — Task status updates
        └── response        — Task result
```

---

## Implementation Order

| Step | Work | Est. |
|------|------|------|
| 1 | Maven deps + `LangChain4jFactory` + `LangChain4jProviderAdapter` + migrate `GenAiProviderFactory` | 1-2d |
| 2 | `AgentConfig.kt` + add `DEVICE_TYPE_AGENT` to `DeviceConfig.kt` | 0.5d |
| 3 | `AgentExtension.kt` + deploy from `Monster.kt` | 1-2d |
| 4 | `AgentTools.kt` — wrap MCP tools as LangChain4j @Tool methods | 2-3d |
| 5 | `AgentExecutor.kt` — MQTT subscription, ReAct loop, state, CRON trigger | 3-4d |
| 6 | `schema-agents.graphqls` + `AgentQueries.kt` + `AgentMutations.kt` + wire into `GraphQLServer.kt` | 2-3d |
| 7 | Dashboard: `agents.html` + `agents.js` + sidebar entry | 2-3d |

---

## Verification

1. **Build**: `cd broker && mvn clean package` — compiles with LangChain4j deps
2. **Existing GenAI**: Dashboard "Code Assistant" / topic analysis still works via adapter
3. **Agent CRUD**: Create/list/update/delete agents via GraphQL playground
4. **Agent execution**: Create a simple agent with `triggerType: MQTT`, subscribe to a test topic, publish a message, verify the agent processes it and publishes a response
5. **Tool calling**: Create an agent with a system prompt like "When asked about sensor data, query the archive". Publish a question, verify the agent calls `queryHistory` tool and responds with archive data
6. **CRON agent**: Create agent with `triggerType: CRON, cronIntervalMs: 60000`, verify it fires periodically
7. **Dashboard**: Agents page loads, shows agents, can create/edit/start/stop
8. **Cluster**: In cluster mode, agents deploy only on their assigned node

---

## Key Reference Files

These existing files serve as patterns to follow during implementation:

- `broker/src/main/kotlin/flowengine/FlowEngineExtension.kt` — Primary pattern for `AgentExtension`
- `broker/src/main/kotlin/flowengine/FlowInstanceExecutor.kt` — Primary pattern for `AgentExecutor`
- `broker/src/main/kotlin/extensions/McpHandler.kt` — Tool implementations to reuse in `AgentTools`
- `broker/src/main/kotlin/genai/IGenAiProvider.kt` — Interface for backward-compatible adapter
- `broker/src/main/kotlin/genai/GeminiProvider.kt` — Only working provider (to be replaced)
- `broker/src/main/kotlin/graphql/GraphQLServer.kt` — Schema loading and wiring
- `broker/src/main/kotlin/graphql/FlowQueries.kt` / `FlowMutations.kt` — GraphQL resolver patterns
- `broker/src/main/kotlin/stores/DeviceConfig.kt` — Device type constants
- `broker/src/main/kotlin/stores/IDeviceConfigStore.kt` — Config store interface
- `dashboard/src/pages/mqtt-clients.html` / `js/mqtt-clients.js` — Dashboard page pattern
