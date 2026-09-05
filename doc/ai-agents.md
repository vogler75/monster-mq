# AI Agents

MonsterMQ includes an AI agent framework that lets you deploy autonomous LLM-powered agents directly inside the broker. Agents subscribe to MQTT topics, process messages through an LLM with tool-calling (ReAct loop), and publish responses back to MQTT. They can also invoke each other for multi-agent orchestration.

## Architecture

Each agent runs as a Vert.x verticle (`AgentExecutor`) managed by a cluster-aware `AgentExtension`. The extension loads agent configurations from the device config store and deploys/undeploys agents based on node assignment and enabled state.

```
AgentExtension (per cluster node)
  └── AgentExecutor (per agent)
        ├── LangChain4j AiServices (ReAct loop)
        ├── AgentTools (built-in @Tool methods)
        ├── MCP Tool Providers (optional)
        └── Chat Memory (sliding window)
```

## Configuration

Agents are configured via the GraphQL API or dashboard. Key fields and runtime defaults (the linked schema contains the complete input):

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | String | required | Unique agent name |
| `org` | String | `"default"` | A2A organization identifier |
| `site` | String | `"default"` | A2A site identifier |
| `description` | String | `""` | Human-readable description |
| `version` | String | `"1.0.0"` | Agent version |
| `namespace` | String | required | Organizational namespace |
| `nodeId` | String | required | Cluster node to run on (`local` for standalone; `*` deploys on every eligible node) |
| `enabled` | Boolean | `false` | Whether the agent is deployed |
| `provider` | String | `"gemini"` | AI provider ID (see supported providers below) |
| `model` | String | required here or in provider configuration | Provider model ID |
| `apiKey` | String | global config | Per-agent API key (supports `${ENV_VAR}` syntax) |
| `systemPrompt` | String | `""` | System prompt sent to the LLM |
| `temperature` | Double | `0.7` | LLM temperature (0.0-2.0) |
| `maxTokens` | Int | provider default | Max output tokens |
| `maxToolIterations` | Int | `10` | Max tool calls per invocation |
| `memoryWindowSize` | Int | `40` | Chat messages retained in memory |
| `triggerType` | Enum | `MQTT` | `MQTT`, `CRON`, or `MANUAL` |
| `cronExpression` | String | null | Quartz cron expression (for CRON trigger) |
| `cronIntervalMs` | Long | null | Periodic interval in ms (for CRON trigger) |
| `inputTopics` | List | `[]` | MQTT topics that trigger the agent |
| `cronPrompt` | String | null | Custom prompt for scheduled executions (CRON trigger) |
| `outputTopics` | List | `[]` | Where responses are published (default: `a2a/v1/{org}/{site}/agents/{name}/response`) |
| `stateEnabled` | Boolean | `true` | Enable persistent state |
| `mcpServers` | List | `[]` | External MCP server names to connect |
| `useMonsterMqMcp` | Boolean | `false` | Connect to MonsterMQ's built-in MCP server |
| `defaultArchiveGroup` | String | `"Default"` | Archive group for built-in data tools |
| `contextLastvalTopics` | Map | `{}` | Last-value topics injected as context (see [Context Data](#context-data)) |
| `contextRetainedTopics` | List | `[]` | Retained topics injected as context |
| `contextHistoryQueries` | List | `[]` | Historical data queries injected as context |
| `taskTimeoutSeconds` | Long | `60` | Timeout (seconds) for LLM calls and sub-agent invocations. Overrides `GenAI.Providers.Ollama.TimeoutSeconds` when larger. |
| `subAgents` | List | `[]` | Restrict which agents this orchestrator can invoke (empty = all) |
| `skills` | List | `[]` | Declared agent skills for A2A discovery |

### AI Providers

Supported provider IDs are `gemini`, `claude`, `openai`, `ollama`,
`azure-openai`, and `llamacpp`. Configure an explicit model on the agent/stored
provider or in `GenAI.Providers.<Provider>.Model`; the factory no longer supplies
hard-coded default model names. Use `providerName` to reference a saved provider.

API keys are resolved in order: agent-level `apiKey` field > global `config.yaml` > environment variable.

Global config example (`config.yaml`):
```yaml
GenAI:
  Providers:
    Gemini:
      Model: "your-available-model"
      ApiKey: "${GEMINI_API_KEY}"
    Claude:
      Model: "your-available-model"
      ApiKey: "${ANTHROPIC_API_KEY}"
    Ollama:
      Model: "your-installed-model"
      BaseUrl: "http://localhost:11434"
```

### Trigger Types

- **MQTT** - Agent is triggered when messages arrive on `inputTopics`. The message payload becomes the user message for the LLM.
- **CRON** - Agent runs on a schedule. Use `cronExpression` (Quartz format, e.g., `0 0 22 * * ? *` for daily at 22:00) or `cronIntervalMs` (e.g., `300000` for every 5 minutes).
- **MANUAL** - Agent only runs when invoked via A2A orchestration (`invokeAgent`) or direct task messages.

### Skills

Skills declare what an agent can do, used for A2A discovery:

```json
{
  "skills": [
    {
      "name": "analyze-temperature",
      "description": "Analyze temperature sensor data and detect anomalies",
      "inputSchema": {
        "type": "object",
        "properties": {
          "topic": { "type": "string" },
          "threshold": { "type": "number" }
        }
      }
    }
  ]
}
```

## Built-in Tools

All agents have access to these tools via LangChain4j `@Tool` annotations:

### MQTT

| Tool | Parameters | Description |
|------|-----------|-------------|
| `publishMessage` | `topic`, `payload` | Publish a message, restricted by `allowedPublishTopics` when configured |

### Data Queries

| Tool | Parameters | Description |
|------|-----------|-------------|
| `getTopicValues` | `topics`, `archiveGroup?` | Get current/last-known values for comma-separated topics |
| `findTopics` | `pattern`, `archiveGroup?` | Search topics by MQTT wildcard pattern (`+`/`#`) |
| `queryHistory` | `topic`, `startTime?`, `endTime?`, `limit?`, `archiveGroup?` | Query archived messages with ISO-8601 time range |

### Agent Notes (Persistent Memory)

Notes are stored as retained MQTT messages under `a2a/v1/{org}/{site}/agents/{name}/memory/{key}`. Restart durability depends on using a persistent retained store.

| Tool | Parameters | Description |
|------|-----------|-------------|
| `saveNote` | `key`, `content` | Save text/JSON to persistent memory. Keys can be hierarchical (e.g., `decisions/heater`) |
| `recallNote` | `key` | Retrieve a note by exact key |
| `searchNotes` | `pattern` | Search notes using MQTT wildcards (e.g., `#` for all, `decisions/#`) |
| `deleteNote` | `key` | Delete a note by key |

### Agent Orchestration (A2A)

| Tool | Parameters | Description |
|------|-----------|-------------|
| `listAgents` | - | List all available agents (filtered by `subAgents` if configured) |
| `getAgentCard` | `agentName` | Get full Agent Card with capabilities and skills |
| `invokeAgent` | `targetAgent`, `input`, `skill?`, `timeoutSeconds?` | Invoke another agent and wait for its response |
| `getAgentHealth` | `agentName` | Get health status, uptime metrics, and error counts |

## Context Data

Context data is fetched before every LLM invocation and prepended to the user message. This gives the agent situational awareness without needing tool calls.

### Last-Value Topics (`contextLastvalTopics`)

Fetches current values from archive group last-value stores:

```json
{
  "Default": ["sensors/+/temperature", "sensors/+/humidity"],
  "SCADA": ["enterprise/+/value"]
}
```

### Retained Message Topics (`contextRetainedTopics`)

Fetches retained messages matching topic filters:

```json
["config/limits", "status/+/health"]
```

### History Data Queries (`contextHistoryQueries`)

Fetches historical data with optional aggregation:

```json
[
  {
    "archiveGroup": "Default",
    "topics": ["sensors/plant1/temperature"],
    "lastSeconds": 3600,
    "interval": "FIVE_MINUTES",
    "function": "AVG",
    "fields": ["temperature"]
  }
]
```

**Interval options**: `RAW`, `ONE_MINUTE`, `FIVE_MINUTES`, `FIFTEEN_MINUTES`, `ONE_HOUR`, `ONE_DAY`
**Aggregation functions**: `AVG`, `MIN`, `MAX` (ignored for `RAW`)

The injected context looks like:

```
--- Context Data (current values for your reference) ---
[Archive:Default] sensors/plant1/temperature = 22.5 (2024-01-01T12:00:00Z)
[Retained] config/limits = {"maxTemp": 30}
[History:Default:FIVE_MINUTES:AVG] sensors/plant1/temperature (last 3600s, 12 rows):
  Columns: ["timestamp","avg"]
  [2024-01-01T11:00:00Z, 21.8]
  ...
--- End Context Data ---
```

Context is capped at 500 lines.

## Agent-to-Agent Communication (A2A)

Agents communicate using the [HiveMQ A2A over MQTT](https://www.hivemq.com/a2a-mqtt/) topic hierarchy. The `org` and `site` fields on each agent define the topic namespace.

### Topic Hierarchy

```
a2a/v1/{org}/{site}/
├── discovery/{agentId}                    # (retained) Agent Card
└── agents/{agentId}/
    ├── inbox                              # Incoming task requests
    ├── status/{taskId}                    # Task state and results
    └── cancel/{taskId}                    # Task cancellation (reserved)
```

With default `org`/`site` values, topics look like:
```
a2a/v1/default/default/discovery/agent1
a2a/v1/default/default/agents/agent1/inbox
a2a/v1/default/default/agents/agent1/status/67
```

### Discovery

Every running agent publishes an **Agent Card** as a retained message to `a2a/v1/{org}/{site}/discovery/{agentId}`. Other agents discover available agents by calling `listAgents()`, which reads these retained cards.

### Task Request Format

To invoke an agent, publish a message to its inbox topic:

```
a2a/v1/{org}/{site}/agents/{targetAgent}/inbox
```

**JSON payload:**

```json
{
  "taskId": "unique-task-id",
  "input": "Analyze the temperature trend for the last hour",
  "replyTo": "a2a/v1/default/default/agents/callerAgent/inbox/unique-task-id",
  "callerAgent": "orchestrator",
  "skill": "analyze-temperature"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `taskId` | No | Unique task identifier (auto-generated if omitted) |
| `input` | Yes | The task instruction/input text |
| `replyTo` | No | MQTT topic for the response (default: agent's status topic) |
| `callerAgent` | No | Name of the calling agent (default: `"unknown"`) |
| `skill` | No | Specific skill to invoke on the target agent |

**Plain-text payloads** are also accepted: if the message is not valid JSON, the entire payload is treated as the task input. The agent publishes its response to its configured output topics.

### Task Response Format

The target agent publishes the result to the `replyTo` topic (or to `a2a/v1/{org}/{site}/agents/{agentId}/status/{taskId}` by default):

**Success:**
```json
{
  "taskId": "unique-task-id",
  "status": "completed",
  "result": "The temperature has been stable at 22.5°C..."
}
```

**Failure:**
```json
{
  "taskId": "unique-task-id",
  "status": "failed",
  "error": "Error message describing what went wrong"
}
```

### Task Status Updates

During execution, status updates are published to `a2a/v1/{org}/{site}/agents/{agentId}/status/{taskId}`:

```json
{
  "taskId": "unique-task-id",
  "status": "working",
  "agent": "agent-name",
  "timestamp": "2024-01-01T12:00:00Z"
}
```

Status values: `working`, `completed`, `failed`.

### Sequence Diagram

```
Agent A (Orchestrator)                    Agent B (Worker)
    │                                          │
    │  1. invokeAgent("B", "analyze temps")    │
    │                                          │
    │  2. Subscribe to                         │
    │     a2a/v1/../agents/A/inbox/{taskId}    │
    │                                          │
    │  3. Publish to                           │
    │     a2a/v1/../agents/B/inbox ───────────>│
    │     {taskId, input, replyTo,             │
    │      callerAgent: "A"}                   │
    │                                          │ 4. Publish status "working"
    │                                          │    to a2a/v1/../agents/B/status/{taskId}
    │                                          │
    │                                          │ 5. Execute LLM with tools
    │                                          │    (ReAct loop)
    │                                          │
    │                                          │ 6. Publish result to
    │  <───────────────────────────────────────│    a2a/v1/../agents/A/inbox/{taskId}
    │  {taskId, status: "completed",           │
    │   result: "Temperature analysis..."}     │
    │                                          │ 7. Publish status "completed"
    │  8. Future completes,                    │
    │     unsubscribe inbox topic              │
    │                                          │
    │  9. Return result to LLM                 │
    │     (continues ReAct loop)               │
    └──────────────────────────────────────────┘
```

### Sub-Agent Scoping

By default, an agent can discover and invoke any other agent. To restrict this, set `subAgents` to a list of allowed agent names:

```json
{
  "subAgents": ["data-analyst", "alert-manager"]
}
```

When `subAgents` is non-empty and `subAgentsAllowAll` is false:
- `listAgents()` only returns agents in the list (self is always excluded)
- `invokeAgent()` rejects targets not in the list with an error message
- When empty (default), all agents are visible (backward-compatible)

Additional controls: `isolatedAgent: true` prevents discovery/invocation of other
agents; `visibleAgentTags` restricts targets by tag. `allowedPublishTopics` limits
the built-in `publishMessage` tool when non-empty. `timezone` controls scheduled
execution; `conversationLogEnabled` enables full conversation logging. See
[schema-agents.graphqls](../broker/src/main/resources/schema-agents.graphqls).

## MQTT Topic Structure

```
a2a/v1/{org}/{site}/
├── discovery/
│   └── {agentId}                     # (retained) Agent Card with capabilities
└── agents/{agentId}/
    ├── inbox                         # Incoming task requests
    ├── status/{taskId}               # Task state/result updates
    ├── cancel/{taskId}               # Task cancellation (reserved)
    ├── health                        # (retained) Health status + metrics
    ├── response                      # Default output topic
    ├── memory/
    │   └── {key}                     # (retained) Persistent agent notes
    └── logs/
        ├── tools                     # Native tool execution logs
        ├── mcp                       # MCP tool execution logs
        ├── llm                       # LLM request/response logs
        └── errors                    # Error logs
```

## GraphQL API

### Queries

```graphql
query Agents {
  agents { name nodeId enabled provider model }
  agent(name: "temp-monitor") { name enabled inputTopics outputTopics }
}
```

### Mutations

The `agent` group exposes `create(input)`, `update(name, input)`, `delete(name)`,
`start(name)`, and `stop(name)`. Create/update return the agent directly; delete
returns a boolean. Input must include `name`, `namespace`, and `nodeId`.

### Example: Create an Agent

```graphql
mutation {
  agent {
    create(input: {
      name: "temp-monitor"
      namespace: "production"
      nodeId: "local"
      description: "Monitors temperature sensors and alerts on anomalies"
      enabled: true
      provider: "gemini"
      model: "your-available-model"
      triggerType: "MQTT"
      inputTopics: ["sensors/+/temperature"]
      outputTopics: ["alerts/temperature"]
      systemPrompt: "You monitor temperature sensors. Alert if values exceed 30°C."
      temperature: 0.3
      maxToolIterations: 5
      contextLastvalTopics: { Default: ["sensors/+/temperature"] }
    }) {
      name
      enabled
    }
  }
}
```

## MCP Server Integration

Agents can connect to MCP (Model Context Protocol) servers for additional tools:

- **MonsterMQ MCP** (`useMonsterMqMcp: true`) - Connects to the broker's built-in MCP server with auto-generated JWT authentication. Provides topic discovery, archive queries, value retrieval, and SQL queries.
- **External MCP Servers** (`mcpServers: ["server-name"]`) - Connects to MCP servers configured as devices in MonsterMQ. Uses Streamable HTTP transport.

## Message Processing Flow

```
1. Message arrives (MQTT topic, CRON tick, or A2A task)
       │
2. Build context data (lastval + retained + history queries)
       │
3. Prepend context to user message
       │
4. Send to LLM via LangChain4j AiServices
       │
5. ReAct loop: LLM may call tools (built-in or MCP)
   │   └── Tool results fed back to LLM
   │   └── Repeat until final answer or maxToolIterations
       │
6. Publish response to output topics (or replyTo for A2A)
```

## Dashboard

Open **Agents** in the dashboard sidebar (served on the configured GraphQL port):

- **Agent List** - Overview of all agents with status, provider, trigger type, and enable/disable toggles
- **Agent Detail** - Full configuration editor with sections for:
  - Agent configuration (name, namespace, node, enabled)
  - AI provider (model, temperature, tokens, memory)
  - Tools & MCP servers (built-in tools reference, MCP checkboxes, sub-agent checkboxes)
  - Trigger configuration (MQTT topics, CRON schedule)
  - Context data (last-value topics, retained topics, history queries)
  - System prompt editor
