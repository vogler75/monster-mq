# MonsterMQ External Agents (Python)

Build autonomous AI agents that connect to MonsterMQ via MQTT. External agents work exactly like the built-in agents — they subscribe to topics, process messages through an LLM with tool calling, publish responses, and participate in A2A (Agent-to-Agent) orchestration.

## Quick Start

```bash
cd agents

# Create virtual environment
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\activate    # Windows

# Install dependencies
pip install -r requirements.txt

# Set your API key
export GEMINI_API_KEY="your-key-here"
# Or for other providers:
# export ANTHROPIC_API_KEY="your-key-here"
# export OPENAI_API_KEY="your-key-here"

# Run the example agent
python system_monitor.py
```

## Interact with the Agent

Once running, send requests via MQTT:

```bash
# Ask about system health
mosquitto_pub -t "agents/system-monitor/request" -m "What's the current CPU and memory usage?"

# Read responses
mosquitto_sub -t "agents/system-monitor/response"

# Invoke via A2A protocol (from another agent or script)
mosquitto_pub -t "a2a/v1/default/default/agents/system-monitor/inbox" \
  -m '{"taskId":"1","input":"Check disk space","replyTo":"a2a/v1/default/default/agents/my-caller/inbox/1"}'
```

## Configuration

Edit `config.yaml` to configure:

- **MQTT connection** — broker host, port, credentials
- **AI provider** — `gemini` (default), `claude`, `openai`, or `ollama`
- **Input/output topics** — what triggers the agent and where responses go
- **System prompt** — the agent's personality and instructions
- **Skills** — capabilities advertised for A2A discovery

### Using Different Providers

```yaml
# Gemini (default)
ai:
  provider: gemini
  model: gemini-2.0-flash

# Claude
ai:
  provider: claude
  model: claude-sonnet-4-20250514

# OpenAI
ai:
  provider: openai
  model: gpt-4o

# Ollama (local, no API key needed)
ai:
  provider: ollama
  model: llama3
```

Install the provider package you need:
```bash
pip install langchain-google-genai    # Gemini
pip install langchain-anthropic       # Claude
pip install langchain-openai          # OpenAI
pip install langchain-ollama          # Ollama
```

## Building Your Own Agent

Create a new file and subclass `MonsterAgent`:

```python
from langchain_core.tools import tool
from monster_agent import MonsterAgent

@tool
def my_custom_tool(query: str) -> str:
    """Description of what this tool does."""
    return f"Result for: {query}"

class MyAgent(MonsterAgent):
    def get_tools(self):
        return [my_custom_tool]

if __name__ == "__main__":
    agent = MyAgent("my-config.yaml")
    agent.run()
```

### Available Built-in Tools

Every agent automatically gets these MQTT tools:

| Tool | Description |
|------|-------------|
| `publish_message` | Publish a message to any MQTT topic (sensitive topics need approval, see below) |
| `save_note` | Save a persistent note as a retained MQTT message |

### Custom LangGraph Graphs

By default the agent runs LangGraph's prebuilt ReAct agent. Override `build_graph()` to use your own
`StateGraph`. Compile it with the given checkpointer so memory and human-in-the-loop interrupts work;
the graph is invoked with `{"messages": [HumanMessage]}` and the last AI message is the answer:

```python
from langgraph.graph import StateGraph, MessagesState, START, END
from langgraph.prebuilt import ToolNode, tools_condition

class MyAgent(MonsterAgent):
    def get_tools(self):
        return [my_custom_tool]

    def build_graph(self, llm, tools, checkpointer):
        model = llm.bind_tools(tools)

        def call_model(state: MessagesState):
            return {"messages": [model.invoke([("system", self.system_prompt)] + state["messages"])]}

        graph = StateGraph(MessagesState)
        graph.add_node("model", call_model)
        graph.add_node("tools", ToolNode(tools))
        graph.add_edge(START, "model")
        graph.add_conditional_edges("model", tools_condition, {"tools": "tools", END: END})
        graph.add_edge("tools", "model")
        return graph.compile(checkpointer=checkpointer)
```

### Human-in-the-Loop Approval

Publishes to topics matching `hitl.sensitive_topics` pause the graph with LangGraph's `interrupt()`.
The agent publishes a retained approval request and continues when an operator answers:

| Topic | Payload |
|-------|---------|
| `a2a/v1/{org}/{site}/agents/{name}/approval/request/{id}` | `{"approvalId", "agent", "taskId", "request": {"action": "publish", "topic", "payload"}, "responseTopic", ...}` (retained until decided) |
| `a2a/v1/{org}/{site}/agents/{name}/approval/response/{id}` | `{"approved": true, "reason": "optional", "by": "operator"}` or plain `approve` / `reject` |

Answer from any MQTT client or from the MonsterMQ dashboard: the Agent Monitor page shows pending
requests of an agent with Approve / Reject buttons. Without an answer the request is rejected after
`hitl.approval_timeout_seconds`, and the LLM is told that the publish was rejected. Your own tools and
graph nodes can call `interrupt({...})` too; every interrupt is sent as an approval request, and the
decision `{"approved", "reason", "by"}` is the return value of `interrupt()`.

While a task waits for approval its status is `input-required`. Runs are executed one at a time on a
worker thread, so the agent keeps receiving MQTT messages while it works or waits.

### Conversation State

| `memory.checkpointer` | Storage | Extra packages |
|-----------------------|---------|----------------|
| `memory` (default) | In-process, lost on restart | – |
| `sqlite` | `memory.sqlite_path` | `langgraph-checkpoint-sqlite` |
| `postgres` | `memory.postgres_url` or env `AGENT_POSTGRES_URL` | `langgraph-checkpoint-postgres`, `psycopg[binary]` |

Messages on input topics share one conversation (unless `agent.state_enabled: false`). A2A tasks share
a conversation only when they carry the same `sessionId`; other tasks start with an empty history.
`ai.max_tool_iterations` limits the tool round trips per run.

### A2A Protocol

External agents participate in the same A2A protocol as internal agents:

- **Discovery**: Agent card published to `a2a/v1/{org}/{site}/discovery/{name}` (retained)
- **Inbox**: Task requests received on `a2a/v1/{org}/{site}/agents/{name}/inbox` and `.../inbox/{taskId}`
- **Status**: Task status (`working`, `input-required`, `completed`, `failed`) on `a2a/v1/{org}/{site}/agents/{name}/status/{taskId}`
- **Health**: Status published to `a2a/v1/{org}/{site}/agents/{name}/health` (retained)
- **Notes**: Persistent memory at `a2a/v1/{org}/{site}/agents/{name}/memory/{key}` (retained)

Internal agents can invoke external agents (and vice versa) using the standard A2A task format:
```json
{
  "taskId": "unique-id",
  "input": "What is the CPU usage?",
  "replyTo": "a2a/v1/default/default/agents/caller/inbox/unique-id",
  "callerAgent": "orchestrator",
  "skill": "check-system-health",
  "sessionId": "optional-conversation-id"
}
```

## Architecture

```
MonsterMQ Broker
    │
    ├── MQTT ──── External Agent (Python)
    │               ├── MonsterAgent base class
    │               │     ├── MQTT client (paho-mqtt)
    │               │     ├── A2A protocol (discovery, inbox, health)
    │               │     ├── LangGraph graph (ReAct or custom build_graph())
    │               │     ├── HITL approvals (interrupt / resume over MQTT)
    │               │     └── Checkpointer (memory, SQLite, Postgres)
    │               └── Your tools (get_tools())
    │
    └── Internal ── Built-in Agent (Kotlin)
                      ├── AgentExecutor verticle
                      │     ├── LangChain4j AiServices
                      │     └── AgentTools (@Tool methods)
                      └── MCP tool providers
```

## Example: System Monitor Agent

The included `system_monitor.py` demonstrates an agent with these tools:

| Tool | Description |
|------|-------------|
| `get_cpu_usage` | CPU percentage, per-core breakdown, frequency |
| `get_memory_usage` | RAM and swap usage |
| `get_disk_usage` | Disk space for all mounted partitions |
| `get_top_processes` | Top processes by CPU or memory usage |
| `get_network_info` | Network interfaces and I/O counters |
| `get_system_info` | OS, hostname, uptime, architecture |
