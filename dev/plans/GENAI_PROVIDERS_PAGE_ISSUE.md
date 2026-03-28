# GenAI Providers Page — Separate CRUD for AI Providers

## Overview

Decouple AI provider configuration from individual agent configuration. Instead of each agent storing its own `provider`, `model`, `apiKey`, `endpoint`, and `serviceVersion` fields, agents will reference a named **GenAI Provider** stored separately in the database.

This mirrors the existing pattern used by MCP Servers (`DeviceConfig` type `"MCP-Server"`) and Agents (`"Agent"`).

---

## Motivation

- Avoid duplicating API keys/endpoints across many agents
- Change provider config (e.g. rotate API key, swap deployment) in one place
- Cleaner agent form — just pick a provider by name
- Backward compatible: agents without `providerName` keep working with their own fields

---

## Architecture

### Data Flow (After)

```
Agent → providerName: "prod-azure" → GenAiProvider(DeviceConfig type="GenAiProvider")
                                          ↓
                                   LangChain4jFactory.createChatModel(providerConfig, agentConfig)
```

### Data Flow (Before / Fallback)

```
Agent → provider="azure-openai", model=..., apiKey=..., endpoint=...
                                          ↓
                                   LangChain4jFactory.createChatModel(agentConfig, globalConfig)
```

---

## Backend Changes

### 1. `broker/src/main/kotlin/stores/DeviceConfig.kt`

Add constant:
```kotlin
const val DEVICE_TYPE_GENAI_PROVIDER = "GenAiProvider"
```

### 2. New: `broker/src/main/kotlin/agents/GenAiProviderConfig.kt`

Data class for provider config stored in `DeviceConfig.config` JSON:

```kotlin
data class GenAiProviderConfig(
    val type: String = "gemini",           // gemini | claude | openai | ollama | azure-openai
    val model: String? = null,
    val apiKey: String? = null,
    val endpoint: String? = null,          // Azure OpenAI endpoint URL
    val serviceVersion: String? = null,    // Azure OpenAI API version
    val baseUrl: String? = null,           // Ollama base URL
    val temperature: Double = 0.7,
    val maxTokens: Int? = null
)
```

With `fromJsonObject()` and `toJsonObject()` methods.

### 3. `broker/src/main/kotlin/agents/AgentConfig.kt`

Add field:
```kotlin
val providerName: String? = null   // references a stored GenAiProvider by name
```

Update `fromJsonObject()` and `toJsonObject()`.

### 4. `broker/src/main/kotlin/agents/AgentExecutor.kt`

At model creation (line ~100), before calling `LangChain4jFactory.createChatModel(agentConfig, ...)`, check if `agentConfig.providerName != null`:

```kotlin
chatModel = if (!agentConfig.providerName.isNullOrBlank()) {
    val store = DeviceConfigStoreFactory.getSharedInstance()
    // synchronous get via CompletableFuture
    val providerDevice = store.getDevice(agentConfig.providerName!!).toCompletionStage().toCompletableFuture().get()
    val providerConfig = GenAiProviderConfig.fromJsonObject(providerDevice!!.config)
    LangChain4jFactory.createChatModel(providerConfig, agentConfig, globalConfig!!, listOf(llmListener))
} else {
    LangChain4jFactory.createChatModel(agentConfig, globalConfig!!, listOf(llmListener))
}
```

### 5. `broker/src/main/kotlin/agents/LangChain4jFactory.kt`

Add new overload:
```kotlin
fun createChatModel(
    providerConfig: GenAiProviderConfig,
    agentConfig: AgentConfig,           // for agent-level overrides (maxTokens, temperature, enableThinking)
    globalConfig: JsonObject,
    listeners: List<ChatModelListener> = emptyList()
): ChatModel
```

Merges: **provider defaults** (type, model, apiKey, endpoint, serviceVersion) with **agent overrides** (temperature, maxTokens, enableThinking), then delegates to `createChatModel(ChatModelConfig, globalConfig, listeners)`.

### 6. New: `broker/src/main/resources/schema-genai-providers.graphqls`

```graphql
type GenAiProvider {
    name: String!
    type: String!
    model: String
    apiKey: String
    endpoint: String
    serviceVersion: String
    baseUrl: String
    temperature: Float
    maxTokens: Int
    enabled: Boolean!
    createdAt: String!
    updatedAt: String!
}

input GenAiProviderInput {
    type: String!
    model: String
    apiKey: String
    endpoint: String
    serviceVersion: String
    baseUrl: String
    temperature: Float
    maxTokens: Int
    enabled: Boolean
}

type GenAiProviderMutations {
    create(name: String!, input: GenAiProviderInput!): GenAiProvider!
    update(name: String!, input: GenAiProviderInput!): GenAiProvider!
    delete(name: String!): Boolean!
}

extend type Query {
    genAiProviders: [GenAiProvider!]!
    genAiProvider(name: String!): GenAiProvider
}

extend type Mutation {
    genAiProvider: GenAiProviderMutations
}
```

### 7. New: `broker/src/main/kotlin/graphql/GenAiProviderQueries.kt`

- `genAiProviders()` — filters `getAllDevices()` by type `GenAiProvider`
- `genAiProvider(name)` — `getDevice(name)` filtered by type

### 8. New: `broker/src/main/kotlin/graphql/GenAiProviderMutations.kt`

- `createGenAiProvider()` — validates name format, saves `DeviceConfig(type="GenAiProvider")`
- `updateGenAiProvider()` — merges existing + input
- `deleteGenAiProvider()` — `deleteDevice(name)`

### 9. `broker/src/main/kotlin/graphql/GraphQLServer.kt`

- Load `schema-genai-providers.graphqls`
- Instantiate `GenAiProviderQueries` and `GenAiProviderMutations`
- Register `genAiProviders`, `genAiProvider` on `Query` type
- Register `GenAiProviderMutations` type (create/update/delete)

### 10. `broker/src/main/resources/schema-agents.graphqls`

Add to `Agent` type:
```graphql
providerName: String
```

Add to `AgentInput`:
```graphql
providerName: String
```

### 11. `broker/src/main/kotlin/graphql/AgentMutations.kt`

Add `providerName` field mapping in `inputToDeviceConfig()` and `mergeAgentConfig()`.

### 12. `broker/src/main/kotlin/graphql/AgentQueries.kt`

Add `"providerName" to agentConfig.providerName` in `agentToMap()`.

---

## Dashboard Changes

### 13. New: `dashboard/src/pages/genai-providers.html`

List page — table of all GenAI providers with columns: Name, Type, Model, Endpoint, Actions (Edit, Delete).
Add button → `/pages/genai-provider-detail.html?new=true`.

### 14. New: `dashboard/src/js/genai-providers.js`

- Queries `genAiProviders` on load
- Renders table rows
- Delete with confirmation
- Navigate to detail page on row click / edit button

### 15. New: `dashboard/src/pages/genai-provider-detail.html`

Form fields:
- Name (read-only if editing)
- Type dropdown (gemini/claude/openai/ollama/azure-openai)
- Model / Deployment
- API Key (password field)
- Endpoint (shown when azure-openai)
- API Version / Service Version (shown when azure-openai)
- Base URL (shown when ollama)
- Temperature
- Max Tokens
- Enabled toggle

### 16. New: `dashboard/src/js/genai-provider-detail.js`

- On load: if `?name=X` → fetch and populate form; else show new form
- Show/hide Azure/Ollama fields based on type selection
- Save → `createGenAiProvider` or `updateGenAiProvider` mutation
- Cancel → back to list

### 17. `dashboard/src/js/sidebar.js`

Add "AI Providers" link to the Agents section:

```js
{ href: '/pages/genai-providers.html', icon: 'hierarchy', text: 'AI Providers' },
```

Place before "AI Agents".

### 18. `dashboard/src/pages/agent-detail.html`

Replace the provider form group (Provider select + Model + API Key + Azure Endpoint + Azure API Version) with:
- **Provider** dropdown: populated from `genAiProviders` query — shows `name (type)` per option + blank/manual option
- If blank selected, show the original manual fields (provider type, model, apiKey, endpoint, serviceVersion) for backward compat

### 19. `dashboard/src/js/agent-detail.js`

- On load, query `genAiProviders` to populate provider dropdown
- If agent has `providerName`, pre-select it and hide manual fields
- If blank / no providerName, show manual fields
- Include `providerName` in save mutation

---

## Backward Compatibility

- Existing agents without `providerName` continue to work unchanged
- `AgentExecutor`: only looks up provider if `providerName != null`
- `AgentConfig.fromJsonObject`: `providerName` defaults to `null`
- Agent detail form still shows manual fields when no provider is selected

---

## Implementation Order

1. `DeviceConfig.kt` — add constant
2. `GenAiProviderConfig.kt` — new data class
3. `AgentConfig.kt` — add `providerName` field
4. `LangChain4jFactory.kt` — new overload
5. `AgentExecutor.kt` — provider lookup
6. `schema-genai-providers.graphqls` — GraphQL schema
7. `GenAiProviderQueries.kt` + `GenAiProviderMutations.kt`
8. `GraphQLServer.kt` — register resolvers
9. `schema-agents.graphqls` — add `providerName`
10. `AgentMutations.kt` + `AgentQueries.kt` — handle `providerName`
11. Dashboard: `genai-providers.html/js`, `genai-provider-detail.html/js`
12. Dashboard: `sidebar.js` — add menu item
13. Dashboard: `agent-detail.html/js` — replace/augment provider section
14. Build and test

---

## Notes

- `DeviceConfig.namespace` for providers: use `"genai/<name>"` (consistent with other device types)
- `DeviceConfig.nodeId`: use `""` (providers are cluster-global, not node-specific)
- API key masking: GraphQL returns `"***"` if key is set but non-empty (match MCP Server behavior) — **TODO: decide**
- Name validation: same `^[a-zA-Z0-9_-]+$` rule as agents
