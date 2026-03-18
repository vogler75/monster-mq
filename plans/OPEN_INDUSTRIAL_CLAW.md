# OpenClaw on MonsterMQ — Architecture & Implementation Plan

## Vision

Turn MonsterMQ from an industrial MQTT broker into an **industrial AI agent platform** — "OpenClaw" — where intelligent agents are first-class citizens of the broker, communicate through MQTT topics, persist their state in the existing database backends, and are managed, monitored, and orchestrated through the same dashboard and GraphQL API that already manages devices and flows.

The core insight: MonsterMQ already has every building block needed. It stores messages in databases, bridges to industrial protocols (OPC UA, PLC4X, Kafka, SparkplugB), runs scripted flows triggered by MQTT events, exposes an MCP server for AI tool access, and has a GenAI provider abstraction. What's missing is the **agent lifecycle layer** that ties these pieces together and adds agent-to-agent coordination.

---

## 1. What MonsterMQ Already Has

Before adding anything, it's worth cataloguing the existing capabilities that OpenClaw builds on:

**Message transport & persistence.** MQTT pub/sub with QoS 0/1/2, retained messages, wildcard subscriptions. Archive groups store every message in PostgreSQL, CrateDB, MongoDB, or SQLite with time-range queries and SQL access.

**Industrial protocol bridges.** OPC UA, PLC4X, Kafka, NATS, SparkplugB, WinCC OA/Unified, Neo4j — all following the Extension + Connector pattern, cluster-aware, deployed as Vert.x verticles.

**Flow engine.** GraalVM polyglot scripting (JavaScript/Python) triggered by MQTT messages. Flows have inputs, outputs, mutable state, and database access. They already behave like simple reactive agents.

**MCP server.** Exposes 7 tools (topic discovery, value retrieval, archive queries, SQL queries, aggregation) over HTTP with SSE streaming. AI models can already read the broker's data.

**GenAI provider abstraction.** `IGenAiProvider` interface with Gemini implemented, Claude and OpenAI stubbed. Supports prompt/context/docs in, text/metadata out.

**GraphQL API & dashboard.** Full CRUD for devices, flows, sessions, archives, users. The iX dashboard visualizes everything.

**Auth & ACL.** Per-user topic-level permissions, JWT for API access, bcrypt/SCRAM passwords.

**Clustering.** Hazelcast-backed cluster with per-node device assignment, distributed session tracking, subscription mapping.

---

## 2. The Agent Model

An OpenClaw agent is a **named, stateful, autonomous entity** that:

- Has an identity (name, type, version, description of capabilities)
- Subscribes to MQTT topics (inputs)
- Publishes to MQTT topics (outputs)
- Has persistent state stored in the broker's database
- Can call LLMs via the GenAI provider interface
- Can use MCP tools to query the broker's own data
- Can communicate with other agents via MQTT (A2A over MQTT)
- Has a lifecycle managed by the broker (deploy, start, pause, stop, delete)
- Reports health, metrics, and logs through standard broker mechanisms

### Agent Types

**Reactive agents** — triggered by incoming MQTT messages. Simplest form: an evolved flow node with LLM access. Example: anomaly detector that reads sensor data from `plant/line1/temperature` and publishes alerts to `agents/anomaly-detector/alerts`.

**Scheduled agents** — triggered by cron expressions or time intervals. Example: daily report generator that queries the archive for yesterday's data, calls an LLM to summarize, and publishes to `agents/daily-report/output`.

**Conversational agents** — long-running, maintain a conversation thread. Interact via request/response topics. Example: a maintenance advisor that operators query through the dashboard.

**Orchestrator agents** — coordinate other agents. Subscribe to multiple agents' output topics, decide what to trigger next. Example: a production optimizer that listens to anomaly, quality, and scheduling agents.

---

## 3. Architecture Decisions

### 3.1 Agents In-Broker vs. Coupled by MQTT

**Decision: Both.** Lightweight agents run as Vert.x verticles inside the broker (like flows and device connectors do today). Heavy agents (GPU inference, large context windows, complex toolchains) run as external processes that connect via MQTT.

**In-broker agents** follow the existing Extension + Connector pattern:
- `AgentExtension` (coordinator) — cluster-aware, deploys agent verticles, manages lifecycle
- `AgentExecutor` (per-agent verticle) — subscribes to topics, runs logic, publishes results
- Stored in `IDeviceConfigStore` with `type = "Agent"`

**External agents** connect as regular MQTT clients with a well-known topic convention. The broker tracks them via their client sessions and a registration topic.

This hybrid approach means agents can start simple (in-broker JavaScript/Python) and graduate to external processes when they need more resources, without changing the communication model.

### 3.2 Agent-to-Agent Communication: A2A Over MQTT

**Decision: Implement A2A protocol semantics over MQTT topics,** not HTTP. This keeps everything in the broker's domain and gives us persistence, QoS, and monitoring for free.

**Topic namespace:**
```
agents/{agent-id}/card          — Agent Card (retained, JSON)
agents/{agent-id}/tasks/new     — Submit a new task
agents/{agent-id}/tasks/{id}/status   — Task status updates
agents/{agent-id}/tasks/{id}/messages — Task messages
agents/{agent-id}/tasks/{id}/artifacts — Task artifacts
agents/{agent-id}/health        — Heartbeat / health status
agents/{agent-id}/metrics       — Agent metrics
agents/{agent-id}/logs          — Agent log output
```

**Agent Card** (published as retained message on `agents/{agent-id}/card`):
```json
{
  "name": "anomaly-detector",
  "version": "1.0.0",
  "description": "Detects anomalies in sensor data using statistical and LLM-based analysis",
  "skills": [
    {
      "name": "detect-anomalies",
      "description": "Analyze a topic's recent data for anomalies",
      "inputSchema": { "topic": "string", "window": "string" }
    }
  ],
  "inputTopics": ["plant/+/temperature", "plant/+/pressure"],
  "outputTopics": ["agents/anomaly-detector/alerts"],
  "provider": "gemini",
  "model": "gemini-2.0-flash",
  "status": "running",
  "nodeId": "node-1"
}
```

**Task lifecycle** maps to A2A states (submitted → working → input-required → completed/failed) via messages on `agents/{agent-id}/tasks/{id}/status`.

**Why not HTTP-based A2A?** Because: (a) we already have the transport, (b) MQTT gives us QoS guarantees and persistence, (c) all communication flows through the broker where it can be archived, monitored, and replayed, (d) external agents don't need additional HTTP servers.

### 3.3 Do We Need Full A2A?

**Decision: Start with A2A-lite over MQTT, keep the door open for full A2A HTTP.** The initial implementation uses A2A's conceptual model (Agent Cards, Tasks, Messages, Artifacts) but carries them over MQTT. For interoperability with the broader A2A ecosystem, the MCP/GraphQL server can expose an A2A HTTP endpoint later that translates between HTTP A2A and MQTT A2A internally.

### 3.4 Agent State Storage

**Decision: Reuse the existing store infrastructure.** Agent configs go in `IDeviceConfigStore` (type="Agent"). Agent conversation history and state go in a dedicated archive group (`agents/#`). This means all agent activity is queryable through the same GraphQL/MCP interfaces.

---

## 4. Implementation Phases

### Phase 1: Agent Runtime Foundation

**Goal:** Agents run inside the broker as first-class verticles, managed through GraphQL.

**New files:**
```
broker/src/main/kotlin/agents/
├── AgentExtension.kt          — Cluster-aware coordinator (like FlowEngineExtension)
├── AgentExecutor.kt           — Per-agent verticle
├── AgentConfig.kt             — Agent configuration data class
├── AgentScript.kt             — Script execution with LLM access
├── AgentContext.kt            — Runtime context (state, tools, LLM)
└── AgentRegistry.kt           — In-memory registry of running agents
```

**AgentExtension** follows the Extension + Connector pattern:
- Reads agent configs from `IDeviceConfigStore` where `type = "Agent"`
- Deploys one `AgentExecutor` verticle per enabled agent
- Handles cluster node assignment (same as device extensions)
- Listens on EventBus for config changes

**AgentExecutor** is the per-agent runtime:
- Subscribes to configured MQTT input topics
- On message: executes agent logic (script or LLM call)
- Publishes results to output topics
- Maintains persistent state (loaded from/saved to database)
- Publishes Agent Card as retained message
- Reports health on `agents/{id}/health`

**AgentConfig** extends DeviceConfig:
```kotlin
data class AgentConfig(
    val name: String,
    val description: String,
    val skills: List<AgentSkill>,
    val inputTopics: List<String>,
    val outputTopics: List<String>,
    val triggerType: TriggerType,       // MQTT, CRON, MANUAL, TASK
    val cronExpression: String?,
    val scriptLanguage: String?,        // "javascript", "python"
    val script: String?,                // Inline script
    val provider: String?,              // GenAI provider name
    val model: String?,                 // GenAI model name
    val systemPrompt: String?,          // LLM system prompt
    val maxTokens: Int?,
    val temperature: Double?,
    val stateEnabled: Boolean,          // Persist state between invocations
    val taskQueueEnabled: Boolean       // Accept A2A tasks
)

enum class TriggerType { MQTT, CRON, MANUAL, TASK }
```

**Agent script bindings** (extending flow engine bindings):
```javascript
// Existing flow bindings
inputs, msg, state, flow, outputs, console, dbs

// New agent bindings
agent.name                    // This agent's name
agent.config                  // Agent configuration
agent.llm(prompt)             // Call configured LLM provider
agent.llm(prompt, context)    // Call with additional context
agent.mcp(tool, params)       // Call MCP tool (query archive, find topics, etc.)
agent.publish(topic, payload)  // Publish MQTT message
agent.subscribe(topic)         // Dynamic subscription
agent.task(agentId, skill, input) // Submit task to another agent
agent.log(level, message)     // Structured logging
agent.state.get(key)          // Persistent state
agent.state.set(key, value)   // Persistent state
```

**GraphQL additions:**
```graphql
type Agent {
  name: String!
  description: String
  status: AgentStatus!
  nodeId: String
  skills: [AgentSkill!]
  inputTopics: [String!]
  outputTopics: [String!]
  triggerType: TriggerType!
  provider: String
  model: String
  metrics: AgentMetrics
  lastActivity: DateTime
}

type AgentSkill {
  name: String!
  description: String!
  inputSchema: JSON
}

type AgentMetrics {
  messagesProcessed: Long!
  llmCalls: Long!
  errors: Long!
  avgResponseTimeMs: Float!
  uptime: Long!
}

enum AgentStatus { STARTING, RUNNING, PAUSED, STOPPED, ERROR }
enum TriggerType { MQTT, CRON, MANUAL, TASK }

extend type Query {
  agents: [Agent!]!
  agent(name: String!): Agent
  agentLogs(name: String!, limit: Int): [LogEntry!]
}

extend type Mutation {
  createAgent(input: AgentInput!): Agent
  updateAgent(name: String!, input: AgentInput!): Agent
  deleteAgent(name: String!): Boolean
  startAgent(name: String!): Agent
  stopAgent(name: String!): Agent
  pauseAgent(name: String!): Agent
  invokeAgent(name: String!, skill: String!, input: JSON!): AgentTaskResult
}
```

**Config YAML section:**
```yaml
Agents:
  Enabled: true
  ArchiveGroup: "AgentArchive"  # Archive group for agent messages
  DefaultProvider: "gemini"
  DefaultModel: "gemini-2.0-flash"
  MaxConcurrentLLMCalls: 10
  StateStoreType: "POSTGRES"    # Where agent state is persisted
```

---

### Phase 2: Scheduled & Periodic Agents

**Goal:** Agents can trigger on time schedules, not just MQTT events.

**Implementation:**
- Add Vert.x periodic timers to `AgentExecutor` when `triggerType = CRON`
- Parse cron expressions using a lightweight cron library (e.g., `com.cronutils:cron-utils`)
- On timer fire: execute agent logic with empty input (agent queries archive for data it needs)
- Support both interval-based (`every: "5m"`) and cron-based (`cron: "0 8 * * 1-5"`) scheduling
- Scheduled agents can still receive MQTT messages for ad-hoc triggers

**Example config:**
```yaml
# Daily production report agent
name: daily-report
triggerType: CRON
cronExpression: "0 6 * * *"
provider: gemini
model: gemini-2.0-flash
systemPrompt: |
  You are a production report generator. Query the archive for
  yesterday's sensor data, calculate KPIs, and produce a summary.
outputTopics:
  - reports/daily/production
  - notifications/shift-lead
```

---

### Phase 3: Agent-to-Agent Communication (A2A over MQTT)

**Goal:** Agents can discover each other's capabilities and delegate tasks.

**Implementation:**

**Agent discovery:** Each running agent publishes its Agent Card as a retained message on `agents/{id}/card`. Any agent can discover available agents by subscribing to `agents/+/card`.

**Task submission:**
```json
// Published to agents/anomaly-detector/tasks/new
{
  "taskId": "task-abc-123",
  "requestor": "production-optimizer",
  "skill": "detect-anomalies",
  "input": {
    "topic": "plant/line1/temperature",
    "window": "1h"
  },
  "replyTo": "agents/production-optimizer/tasks/task-abc-123/response"
}
```

**Task status tracking:**
```json
// Published to agents/anomaly-detector/tasks/task-abc-123/status
{
  "taskId": "task-abc-123",
  "status": "completed",
  "artifacts": [
    {
      "name": "anomaly-report",
      "contentType": "application/json",
      "data": { "anomalies": [...], "severity": "warning" }
    }
  ]
}
```

**AgentContext additions:**
```javascript
// In agent script
const detectorCard = agent.discover("anomaly-detector");
const result = await agent.task("anomaly-detector", "detect-anomalies", {
  topic: "plant/line1/temperature",
  window: "1h"
});
// result.artifacts[0].data.anomalies...
```

**Built-in orchestration patterns:**
- **Sequential**: Agent A completes → triggers Agent B → triggers Agent C
- **Parallel fan-out**: Orchestrator sends tasks to N agents, collects results
- **Voting/consensus**: Multiple agents analyze same data, orchestrator aggregates
- **Escalation**: If Agent A can't handle it, delegate to Agent B

---

### Phase 4: GenAI Provider Completion & Tool Use

**Goal:** Full LLM integration with tool calling (function calling) support.

**Implementation:**
- Complete `ClaudeProvider` and `OpenAiProvider` implementations
- Add tool-calling support to `IGenAiProvider`:
```kotlin
interface IGenAiProvider {
    // ... existing methods ...
    fun generateWithTools(
        request: GenAiRequest,
        tools: List<ToolDefinition>
    ): CompletableFuture<GenAiResponse>
}
```
- Expose MCP tools as LLM function-calling tools so agents can autonomously query archives, discover topics, and read values
- Add support for streaming responses (SSE from LLM → MQTT publish incremental updates)

**Agent loop (ReAct pattern):**
1. Receive input (MQTT message or scheduled trigger)
2. Build prompt with system prompt + input + state context
3. Call LLM with available tools
4. If LLM returns tool call → execute tool → feed result back to LLM
5. Repeat until LLM returns final response
6. Publish response to output topics
7. Update agent state

---

### Phase 5: External Agent Support

**Goal:** Agents running outside the broker participate in the same ecosystem.

**External Agent SDK (Kotlin/Python/TypeScript):**
```python
# Python example
from openclaw import Agent, AgentCard

agent = Agent(
    broker="mqtt://localhost:1883",
    name="vision-inspector",
    description="Inspects product images for defects using computer vision",
    skills=[{
        "name": "inspect-image",
        "description": "Analyze product image for defects",
        "inputSchema": {"imageUrl": "string", "productType": "string"}
    }]
)

@agent.on_task("inspect-image")
async def inspect(task):
    image = await download(task.input["imageUrl"])
    defects = model.predict(image)
    task.complete(artifacts=[{
        "name": "inspection-result",
        "data": {"defects": defects, "pass": len(defects) == 0}
    }])

agent.run()
```

**Registration protocol:**
1. External agent connects to MQTT with client ID `agent:{name}`
2. Publishes Agent Card to `agents/{name}/card` (retained)
3. Subscribes to `agents/{name}/tasks/new` for incoming tasks
4. Broker detects agent connection via client ID prefix
5. Dashboard shows external agents alongside internal ones

**Monitoring:**
- External agents publish heartbeats on `agents/{name}/health` (retained, with will message to mark offline)
- Will message on disconnect: `{"status": "offline", "reason": "connection-lost"}`
- Broker tracks external agent sessions in the same session store

---

### Phase 6: Dashboard & Monitoring

**Goal:** Full visibility into agent operations from the iX dashboard.

**Dashboard pages:**

**Agent Overview** — list of all agents (internal + external) with status indicators, last activity, message rates. Table view with filters for status, type, node.

**Agent Detail** — single agent view showing: configuration, current state (JSON viewer), real-time log stream, metrics charts (messages/sec, LLM calls, latency), input/output topic activity, task history.

**Agent Orchestration View** — graph visualization showing agent interconnections: which agents talk to which, task flows, message rates on edges. Uses the existing iX echarts integration.

**Agent Chat** — for conversational agents, a chat interface in the dashboard. Publishes to `agents/{name}/tasks/new`, subscribes to response topics.

**Implementation approach:**
- New pages in `dashboard/pages/`: `agents.html`, `agent-detail.html`, `agent-orchestration.html`
- GraphQL subscriptions for real-time agent status updates
- Reuse existing `GraphQLDashboardClient` and iX components

---

### Phase 7: Advanced Orchestration

**Goal:** Declarative multi-agent workflows.

**Workflow definition** (stored as agent config with `type = "Orchestrator"`):
```yaml
name: quality-control-pipeline
type: Orchestrator
workflow:
  trigger:
    topic: "production/line1/batch-complete"
  steps:
    - name: inspect
      agent: vision-inspector
      skill: inspect-image
      input:
        imageUrl: "{{msg.payload.imageUrl}}"
        productType: "{{msg.payload.productType}}"

    - name: analyze
      agent: anomaly-detector
      skill: detect-anomalies
      input:
        topic: "plant/line1/temperature"
        window: "{{msg.payload.batchDuration}}"

    - name: decide
      agent: quality-decision
      skill: evaluate
      input:
        inspection: "{{steps.inspect.artifacts}}"
        anomalies: "{{steps.analyze.artifacts}}"

    - name: notify
      condition: "{{steps.decide.output.action == 'reject'}}"
      publish:
        topic: "alerts/quality/reject"
        payload: "{{steps.decide.output}}"
```

**Human-in-the-loop:**
- Any workflow step can require human approval
- Step publishes to `agents/{orchestrator}/approval-required/{taskId}`
- Dashboard shows approval queue
- Operator approves/rejects via dashboard or MQTT publish
- Timeout and escalation policies

---

## 5. Topic Namespace Convention

```
agents/                                 — Root namespace for all agent activity
├── {agent-id}/
│   ├── card                            — Agent Card (retained)
│   ├── health                          — Health/heartbeat (retained)
│   ├── metrics                         — Agent metrics
│   ├── logs                            — Log output
│   ├── state                           — State snapshots
│   ├── tasks/
│   │   ├── new                         — Submit new task
│   │   └── {task-id}/
│   │       ├── status                  — Task status updates
│   │       ├── messages                — Task conversation messages
│   │       ├── artifacts               — Task output artifacts
│   │       └── cancel                  — Cancel task request
│   └── config/
│       ├── update                      — Config update commands
│       └── current                     — Current config (retained)
├── $discovery                          — Agent discovery requests
├── $orchestrator/
│   ├── {workflow-id}/status            — Workflow execution status
│   └── {workflow-id}/approval-required — Human approval queue
└── $system/
    ├── registry                        — Agent registry events
    ├── alerts                          — System-level agent alerts
    └── metrics                         — Aggregated agent metrics
```

---

## 6. Archive Group for Agents

```yaml
ArchiveGroups:
  - Name: "AgentArchive"
    TopicFilter:
      - "agents/#"
    RetainedOnly: false
    LastValType: "POSTGRES"
    ArchiveType: "POSTGRES"
    LastValRetention: "30d"
    ArchiveRetention: "90d"
    PurgeInterval: "1h"
    PayloadFormat: "JSON"
```

This gives us: full history of all agent communication queryable via GraphQL and MCP, last-value store for current agent states and cards, SQL access for analytics on agent activity, and retention management so agent logs don't grow unbounded.

---

## 7. What About MCP?

The existing MCP server already lets external AI models (Claude, GPT) query the broker's data. With OpenClaw, we extend MCP in two directions:

**Agents as MCP clients.** In-broker agents call MCP tools to query archives, discover topics, read values. This is already wired — `McpHandler` has the tools, agents just need the `agent.mcp()` binding.

**Agents as MCP servers.** Each agent can expose its skills as MCP tools, making agents callable from external AI systems. The broker's MCP server proxies these: when an external model calls `invoke-agent`, the MCP server publishes a task to the agent's topic and waits for the response.

New MCP tools:
```
list-agents                    — List all registered agents and their skills
invoke-agent                   — Submit a task to an agent and wait for result
get-agent-status               — Get agent health and metrics
query-agent-history            — Query an agent's past task results
```

---

## 8. Security Model

Agents inherit the existing auth/ACL system:

- Each agent gets its own MQTT credentials (or uses the broker's internal bus)
- ACL rules restrict which topics an agent can subscribe to and publish on
- In-broker agents bypass MQTT auth (they use EventBus) but respect topic-level ACL checks
- External agents authenticate like any MQTT client
- LLM API keys are stored in agent config, encrypted at rest
- Agent-to-agent task delegation respects ACLs: agent A can only submit tasks to agent B if it has publish access to `agents/B/tasks/new`
- Dashboard role-based access: who can create/modify/delete agents, who can view agent data

---

## 9. Implementation Priority & Effort Estimates

| Phase | Description | Effort | Dependencies |
|-------|-------------|--------|--------------|
| 1 | Agent Runtime Foundation | 3-4 weeks | Flow Engine, DeviceConfigStore |
| 2 | Scheduled Agents | 1 week | Phase 1 |
| 3 | A2A over MQTT | 2-3 weeks | Phase 1 |
| 4 | GenAI Provider Completion | 2 weeks | Phase 1 |
| 5 | External Agent SDK | 2-3 weeks | Phase 3 |
| 6 | Dashboard & Monitoring | 3-4 weeks | Phase 1, 3 |
| 7 | Advanced Orchestration | 3-4 weeks | Phase 3, 4 |

**MVP (Phases 1 + 2 + partial 4):** ~6 weeks. In-broker agents triggered by MQTT or cron, with LLM access and MCP tool use, managed via GraphQL.

**Full Platform (all phases):** ~16-20 weeks. Complete industrial agent platform with A2A, external agents, dashboard, and declarative orchestration.

---

## 10. Key Design Principles

**MQTT-native.** Everything flows through MQTT. No sidecar protocols, no separate coordination service. MQTT is the universal bus for agent communication, and the broker is the single point of truth.

**Progressive complexity.** Start with a simple script-based agent (like a flow node with LLM access). Graduate to full A2A task coordination when needed. No forced complexity for simple use cases.

**Operational visibility.** Every agent interaction is an MQTT message, archived in the database, queryable via GraphQL and MCP. Nothing happens in the dark.

**Industrial-grade reliability.** QoS 2 for critical agent-to-agent tasks. Persistent state survives broker restarts. Cluster-aware agent distribution. Will messages for external agent failure detection.

**Human-in-the-loop.** Agents propose, operators approve. The dashboard is always the control plane. Emergency stop for any agent, any time.

**Existing patterns.** Agents follow the same Extension + Connector pattern as devices. Same config store, same GraphQL CRUD, same dashboard integration. The codebase stays consistent.
