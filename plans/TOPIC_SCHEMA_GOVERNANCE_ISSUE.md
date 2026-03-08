# Feature: Topic Schema Governance with JSON Schema

## Summary

Implement broker-level JSON Schema governance for MQTT topic filters as the first step toward industrial/IoT modeling in MonsterMQ.

This introduces explicit payload contracts for governed topics, enforced directly in the broker publish path. The goal is to ensure that later asset modeling can rely on predictable, typed MQTT payloads instead of inferring structure from arbitrary JSON.

## Background

### Current State
MonsterMQ currently supports:
- MQTT topic transport, retained messages, and archiving
- topic-tree object exposure through the I3X API
- JSON Schema validation in the JDBC logger (`JDBCLoggerBase.initializeJsonSchemaValidator()`)
- topic metadata through MCP `<config>` topics

### What Is Missing
There is no broker-native mechanism to define and enforce payload structure at the MQTT topic level.

Without that:
- topic payloads are not guaranteed to have a stable structure
- later asset/template modeling would be built on weak assumptions
- interoperability layers such as WoT or richer I3X object types would have no reliable source contract
- schema validation logic is duplicated / embedded inline in the JDBC Logger instead of being a shared, governed resource

## Product Decisions

- Canonical schema language: JSON Schema
- First payload type: JSON only
- Assignment model: schema policies bound to MQTT topic filters
- Enforcement mode in v1: hard reject on mismatch
- Storage: reuse `DeviceConfig` store (new device types `TopicSchema-Policy` and `TopicNamespace`)
- MQTT `<config>` topics remain optional metadata only, not authoritative enforcement rules
- JDBC Logger references named schema policies instead of embedding schema definitions

## Proposed Model

### TopicSchemaPolicy
Stored as `DeviceConfig` with `type = "TopicSchema-Policy"`.

Fields (in `config` JSONB):
- `topicFilter` — MQTT topic filter pattern (e.g. `sensors/#`, `machines/+/status`)
- `payloadType` — `"JSON"` (v1 only)
- `jsonSchema` — the JSON Schema object
- `enabled` — boolean
- `enforcementMode` — `"REJECT"` (v1 only)
- `contentType` — optional MIME hint
- `version` — optional schema version string
- `description`
- `examples` — optional array of example payloads

`name` in `DeviceConfig` = unique policy identifier (e.g. `sensor-reading-v1`)
`namespace` in `DeviceConfig` = topic filter prefix group for organizational display (e.g. `sensors`, `machines/line1`)

v1 constraints:
- `payloadType = JSON`
- `enforcementMode = REJECT`

### TopicNamespace
Stored as `DeviceConfig` with `type = "TopicNamespace"`.

Fields (in `config` JSONB):
- `topicPrefix` — the topic prefix this namespace covers (e.g. `sensors/`, `machines/line1/`)
- `description`
- `tags` — optional list of labels

`name` = namespace identifier (e.g. `sensor-ns`, `line1-ns`)

Topic namespaces are organizational — they let the GUI display, filter, and group policies by topic area without adding matching semantics beyond what the TopicSchemaPolicy's `topicFilter` already provides.

## New Device Type Constants

Add to `DeviceConfig.kt`:
```kotlin
const val DEVICE_TYPE_TOPIC_SCHEMA_POLICY = "TopicSchema-Policy"
const val DEVICE_TYPE_TOPIC_NAMESPACE = "TopicNamespace"
```

No new store implementations needed — the existing `IDeviceConfigStore` backed by PostgreSQL, CrateDB, MongoDB, and SQLite handles storage.

## Matching Rules

- Policies are assigned by MQTT topic filter (standard MQTT wildcard matching: `+` single-level, `#` multi-level)
- The most specific matching filter wins
- If multiple policies have equal specificity for the same topic space, reject the configuration as ambiguous
- If no policy matches, existing broker behavior remains unchanged

## Runtime Behavior

For each incoming publish:
1. Resolve matching `TopicSchemaPolicy` from the broker's in-memory policy cache
2. If no policy matches, continue current processing
3. If a policy matches:
   - require UTF-8 JSON payload
   - parse JSON
   - validate against the compiled JSON Schema (shared validator)
   - reject publish on parse failure or schema violation
4. Accepted messages continue through the existing broker pipeline
5. Rejected messages do not enter retained storage or archives

## Shared JSON Schema Validator (Extraction)

Extract the JSON Schema validation logic from `JDBCLoggerBase` into a reusable class:

**New class**: `broker/src/main/kotlin/schema/JsonSchemaValidator.kt`

Responsibilities:
- Load and compile a JSON Schema from a `JsonObject`
- Validate a JSON payload against the compiled schema
- Return a typed result (`ValidationResult`) with success flag, error category, and field/path detail
- Used by: broker publish-path enforcement, JDBC Logger, any future component

The `initializeJsonSchemaValidator()` method and the validation call in `handleIncomingMessage()` in `JDBCLoggerBase` are replaced with calls to `JsonSchemaValidator`.

**JDBC Logger updated behavior**: `JDBCLoggerConfig.jsonSchema` is replaced by `JDBCLoggerConfig.schemaPolicyName: String?`. When set, the logger looks up the named `TopicSchemaPolicy` from the cluster-cached policy registry. When not set, the logger can still have an inline schema as a fallback for backward compatibility.

The `JDBCLoggerConfig.jsonSchema` field thus becomes optional and deprecated in favor of `schemaPolicyName`.

## Cluster-Aware In-Memory Schema Cache

Every broker node maintains a local in-memory map:

```
topicFilter → CompiledSchemaPolicy
```

Built at startup from the `IDeviceConfigStore` (filtered by `type = "TopicSchema-Policy"`).

On schema policy mutation (create/update/delete/toggle) via GraphQL:
1. Persist to the `DeviceConfig` store
2. Publish a cluster-wide EventBus broadcast: `EventBusAddresses.Cluster.BROADCAST` with payload `{ "type": "schema-policy-changed" }`
3. All broker nodes receive the event and reload schema policies from the store

EventBus address for schema invalidation (add to `EventBusAddresses.kt`):
```kotlin
object SchemaPolicy {
    private const val NS = "$BASE.schema"
    const val RELOAD = "$NS.reload"
}
```

The `RELOAD` message is published cluster-wide (Hazelcast EventBus is cluster-aware). Each node listens on this address and refreshes its in-memory cache.

## Implementation Order

### Phase 1: Storage and GUI (start here)

1. Add `DEVICE_TYPE_TOPIC_SCHEMA_POLICY` and `DEVICE_TYPE_TOPIC_NAMESPACE` constants to `DeviceConfig.kt`
2. Add GraphQL types, queries, and mutations for `TopicSchemaPolicy` and `TopicNamespace`
3. Dashboard pages:
   - `topic-schema-policies.html` — list all policies, link to detail
   - `topic-schema-policy-detail.html` — create/edit form with JSON Schema editor, topic filter input, test payload validator
   - `topic-namespaces.html` — list and manage namespaces
4. Add sidebar entries for both pages

### Phase 2: Shared Validator and Logger Update

5. Extract `JsonSchemaValidator` from `JDBCLoggerBase`
6. Update `JDBCLoggerConfig` to support `schemaPolicyName` reference (in addition to inline `jsonSchema` for backward compat)
7. Update `JDBCLoggerBase` to use `JsonSchemaValidator` and look up schema by name from the in-memory cache when `schemaPolicyName` is set

### Phase 3: Broker Enforcement and Cluster Sync

8. Add `TopicSchemaPolicyCache` — loads policies from `IDeviceConfigStore`, compiles schemas, exposes `matchPolicy(topic: String): CompiledSchemaPolicy?`
9. Add EventBus broadcast on schema mutation + reload handler in all nodes
10. Integrate policy lookup into broker publish path (`MessageHandler` or `MqttClient` publish pipeline)

## GraphQL API

### Types

```graphql
type TopicSchemaPolicy {
  name: String!
  namespace: String!
  topicFilter: String!
  payloadType: String!
  jsonSchema: JSON!
  enabled: Boolean!
  enforcementMode: String!
  contentType: String
  version: String
  description: String
  createdAt: String!
  updatedAt: String!
}

type TopicNamespace {
  name: String!
  topicPrefix: String!
  description: String
  tags: [String!]
  createdAt: String!
  updatedAt: String!
}

type SchemaPolicyValidationResult {
  policyName: String!
  topicFilter: String!
  valid: Boolean!
  errorCategory: String
  errorDetail: String
}
```

### Queries

```graphql
topicSchemaPolicies: [TopicSchemaPolicy!]!
topicSchemaPolicy(name: String!): TopicSchemaPolicy
topicSchemaPolicyMatch(topic: String!): TopicSchemaPolicy
topicSchemaValidate(policyName: String!, payload: String!): SchemaPolicyValidationResult!
topicNamespaces: [TopicNamespace!]!
topicNamespace(name: String!): TopicNamespace
```

### Mutations

```graphql
createTopicSchemaPolicy(input: TopicSchemaPolicyInput!): TopicSchemaPolicy!
updateTopicSchemaPolicy(name: String!, input: TopicSchemaPolicyInput!): TopicSchemaPolicy!
deleteTopicSchemaPolicy(name: String!): Boolean!
toggleTopicSchemaPolicy(name: String!, enabled: Boolean!): TopicSchemaPolicy!
createTopicNamespace(input: TopicNamespaceInput!): TopicNamespace!
updateTopicNamespace(name: String!, input: TopicNamespaceInput!): TopicNamespace!
deleteTopicNamespace(name: String!): Boolean!
```

All mutations that create/update/delete a policy also publish the schema reload event to the cluster.

## Dashboard UI

### Pages to add

**`topic-schema-policies.html`**
- Table: name, namespace, topicFilter, payloadType, enforcementMode, enabled, actions
- Enable/disable toggle per row
- New policy button → opens detail page
- Click row → detail page

**`topic-schema-policy-detail.html`**
- Form fields: name, namespace (dropdown of existing namespaces), topicFilter, payloadType, enforcementMode, version, description, contentType
- JSON Schema editor: textarea with JSON syntax hint
- Test panel: paste example payload, click Validate → shows pass/fail with error details
- Save / Delete

**`topic-namespaces.html`**
- Table: name, topicPrefix, description, tags, actions
- New namespace button
- Inline edit or detail page

### Sidebar

Add under a new "Governance" section (or under "Configuration"):
- Topic Schema Policies
- Topic Namespaces

## Observability

Add:
- metrics per policy for validated, rejected, parse-error, and schema-error counts
- system log entries for rejected publishes (topic, policy name, error detail)
- optional MQTT system topics for validation failures (`$SYS/schema/rejected`)

## Acceptance Criteria

- A valid JSON payload to a governed topic is accepted
- An invalid JSON payload to a governed topic is rejected
- A schema-mismatched JSON payload to a governed topic is rejected
- Rejected messages do not update retained values
- Rejected messages do not enter history or archive
- Non-governed topics continue working unchanged
- Schema policy precedence works deterministically
- Ambiguous policies are rejected at configuration time
- JDBC Logger references a named schema policy instead of embedding a schema
- `JsonSchemaValidator` is used by both broker enforcement and JDBC Logger
- All broker nodes reload schema policies within one cluster broadcast round-trip
- Schema policies survive broker restart (stored in DeviceConfig store)

## Tests

- Topic filter matching and precedence
- Valid payload acceptance
- Invalid JSON rejection
- Schema violation rejection
- No side effects on retained or archive for rejected publishes
- GraphQL CRUD and policy preview (`topicSchemaPolicyMatch`, `topicSchemaValidate`)
- Invalid schema definition rejection at create/update time
- Cluster reload: update policy via GraphQL, verify other node picks up change
- Regression coverage for non-governed topics and JDBC logger behavior

## Non-Goals

- Full industrial asset modeling
- WoT or AAS export
- Binary, XML, or protobuf schema enforcement
- MQTT topic metadata as the canonical schema source
- Replacing archive groups, retained storage, or current MCP `<config>` metadata topics

## Follow-Up Work

After this feature is stable:
- introduce asset templates and asset instances
- bind asset points to governed topic filters
- enrich I3X objects beyond generic `MqttTopic`
- optionally export WoT Thing Descriptions or Thing Models
- optionally map canonical asset metadata to AAS later
