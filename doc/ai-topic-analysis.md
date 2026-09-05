# AI Topic Analysis

The AI Topic Analysis feature provides LLM-powered analysis of your MQTT topic tree directly from the Dashboard's Topic Browser. Ask questions about your IoT data structure, detect anomalies, or get insights about naming patterns.

## Prerequisites

1. **GenAI Configuration** - Configure an LLM provider in `config.yaml`:

```yaml
GenAI:
  Enabled: true
  Assistant:
    Provider: gemini
    Model: "your-available-model"
    Temperature: 0.7
  Providers:
    Gemini:
      ApiKey: "${GEMINI_API_KEY}"
Features:
  GenAi: true
```

2. **Archive Group with LastValueStore** - The analysis reads current topic values from a LastValueStore:

```yaml
ArchiveGroups:
  - Name: Default
    TopicFilter: ["#"]
    LastValType: MEMORY
    LastValRetention: "50k"
    ArchiveType: NONE
```

## Using the Feature

1. Open the **Topic Browser** page in the Dashboard
2. Click the **✨ AI** button in the header
3. Select a topic in the tree (or leave at root for all topics)
4. Use quick action buttons or type your own question
5. View the AI analysis in the chat panel

### Quick Actions

Quick action buttons provide pre-configured prompts for common analysis tasks. These are loaded from a configuration file and can be customized.

**Default actions:**
- **Summarize** - High-level overview of the topic tree
- **Find Structure** - Analyze naming patterns and data models
- **Detect Anomalies** - Find unusual values or outliers

### Configuration

Quick actions are defined in `/config/prompts.json`:

```json
{
  "quickActions": [
    {
      "id": "summarize",
      "label": "Summarize",
      "title": "Summarize the topic tree",
      "prompt": "Provide a high-level summary of this topic tree..."
    },
    {
      "id": "find-structure",
      "label": "Find Structure",
      "title": "Analyze naming patterns and hierarchy",
      "prompt": "Analyze the MQTT topic tree to identify..."
    }
  ]
}
```

**Adding custom actions:**

```json
{
  "quickActions": [
    {
      "id": "sparkplug",
      "label": "Sparkplug",
      "title": "Analyze Sparkplug B compliance",
      "prompt": "Analyze this topic tree for Sparkplug B compliance:\n\n1. **Namespace Detection:**\n   - Identify spBv1.0 namespace topics..."
    }
  ]
}
```

### Context Management

- **Context indicator** shows whether topic data is loaded (● Context loaded / ○ No context)
- **Clear button** resets the conversation and reloads topic data on next question
- **Max topics** dropdown limits the number of topics sent to the LLM (default: 100)
- Topic data is sent as a hierarchical ASCII tree to reduce token usage

### System Prompt

Click "System Prompt" to expand and customize the default instructions sent to the LLM. Changes are saved to browser localStorage.

## GraphQL API

The feature is available via GraphQL for programmatic access:

```graphql
query AnalyzeTopics {
  genai {
    analyzeTopics(
      archiveGroup: "Default"
      topicPattern: "#"
      question: "Summarize this topic tree"
      maxTopics: 100
      maxValueLength: 1000
      systemPrompt: null  # uses default if null
      chatHistory: null   # for follow-up questions
    ) {
      response
      topicsAnalyzed
      model
      error
    }
  }
}
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `archiveGroup` | String! | - | Name of the archive group to query |
| `topicPattern` | String! | - | MQTT wildcard pattern (e.g., `#`, `sensors/#`) |
| `question` | String! | - | The question to ask about the topics |
| `systemPrompt` | String | Built-in | Custom system prompt for the LLM |
| `maxTopics` | Int | 100 | Maximum topics to include in context |
| `maxValueLength` | Int | 1000 | Truncate values longer than this |
| `chatHistory` | [ChatMessage] | null | Previous messages for follow-up questions |

### Follow-up Questions

For multi-turn conversations, pass the chat history:

```graphql
query FollowUp {
  genai {
    analyzeTopics(
      archiveGroup: "Default"
      topicPattern: "#"
      question: "Tell me more about the temperature sensors"
      chatHistory: [
        { role: "user", content: "Summarize this topic tree" },
        { role: "assistant", content: "This topic tree contains..." }
      ]
    ) {
      response
      topicsAnalyzed
      model
      error
    }
  }
}
```

## Implementation Details

### Topic Data Format

Topics are formatted as a hierarchical ASCII tree to reduce token usage:

```
├── sensors/
│   ├── temperature/
│   │   ├── living-room = 22.5
│   │   └── bedroom = 21.0
│   └── humidity/
│       └── living-room = 45
└── actuators/
    └── thermostat = {"setpoint": 22, "mode": "heat"}
```

### Token Optimization

- Topic hierarchy is collapsed (shared path segments not repeated)
- Binary payloads are skipped
- Large values (>10KB) are excluded
- Long values are truncated with `...[truncated]`
- ASCII tree format is more compact than flat `topic: value` lists

### Supported LLM Providers

- Gemini, Claude, OpenAI, Ollama, Azure OpenAI, and llama.cpp through the shared provider factory.
- A model must be configured explicitly. Availability depends on the selected provider deployment.

Providers use the shared LangChain4j factory and `LangChain4jProviderAdapter`. The API is stateless - full context is sent with each request.

## Files

| File | Purpose |
|------|---------|
| `monster-mq-dashboard/src/config/prompts.json` | Quick action button configuration |
| `monster-mq-dashboard/src/js/topic-browser.js` | Frontend AI panel (`TopicBrowserAI` class) |
| `monster-mq-dashboard/src/pages/topic-browser.html` | AI panel HTML/CSS |
| `broker/src/main/kotlin/graphql/GenAiResolver.kt` | GraphQL resolver (`analyzeTopics`) |
| `broker/src/main/resources/schema-genai.graphqls` | GraphQL schema |
| `broker/src/main/kotlin/agents/LangChain4jFactory.kt` | Shared provider configuration |

Dashboard files belong to the separate [monster-mq-dashboard repository](https://github.com/vogler75/monster-mq-dashboard); do not edit generated broker resources.
