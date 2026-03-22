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
| `publish_message` | Publish a message to any MQTT topic |
| `save_note` | Save a persistent note as a retained MQTT message |

### A2A Protocol

External agents participate in the same A2A protocol as internal agents:

- **Discovery**: Agent card published to `a2a/v1/{org}/{site}/discovery/{name}` (retained)
- **Inbox**: Task requests received on `a2a/v1/{org}/{site}/agents/{name}/inbox`
- **Health**: Status published to `a2a/v1/{org}/{site}/agents/{name}/health` (retained)
- **Notes**: Persistent memory at `a2a/v1/{org}/{site}/agents/{name}/memory/{key}` (retained)

Internal agents can invoke external agents (and vice versa) using the standard A2A task format:
```json
{
  "taskId": "unique-id",
  "input": "What is the CPU usage?",
  "replyTo": "a2a/v1/default/default/agents/caller/inbox/unique-id",
  "callerAgent": "orchestrator",
  "skill": "check-system-health"
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
    │               │     └── LLM ReAct loop (LangChain)
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
