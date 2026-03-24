# feat: config-based feature flags for extension isolation

## Problem

MonsterMQ is growing fast in many directions (OPC UA, Kafka, PLC4X, WinCC, Neo4j, Flow Engine, etc.).
When deploying for a dedicated purpose — e.g. a Kafka bridge on an industrial edge device — all extensions still start, all dashboard pages are visible, and all GraphQL mutations are active, even irrelevant ones.

This creates noise, potential misconfiguration risk, and makes it harder to reason about what a given deployment actually does.

A true plugin architecture (separate JARs + classloaders) is on the long-term roadmap but is a 6–9 month effort and conflicts with the Vert.x/Hazelcast clustering model. This issue proposes a simpler, non-breaking intermediate solution.

## Proposed Solution

Add a top-level `Features` section to `config.yaml` with a boolean flag per extension (default `true` when absent). When set to `false`:

- The extension verticle is not deployed at startup
- GraphQL mutations for that extension return a clear error ("feature not enabled on this node")
- The dashboard hides or disables the corresponding section
- The feature is omitted from the node's `enabledFeatures` list reported via the `brokers` GraphQL query

This does not touch existing extension config blocks — `OpcUa:`, `Kafka:`, etc. remain unchanged. The `Features` section is purely additive and optional.

### Example config

```yaml
Features:
  OpcUa: false
  FlowEngine: false
  # anything omitted defaults to true

# Existing extension config blocks are unaffected
OpcUa:
  Host: opc.tcp://...
```

A config with no `Features` section behaves exactly as today — all extensions start.

## Clustering Behaviour

**All cluster nodes must have identical feature flags.** Heterogeneous nodes cause:
- Silent data loss: a device assigned by Hazelcast consistent hashing to a node that has the extension disabled will never connect
- Schema inconsistency: GraphQL queries hitting different nodes return different capabilities

To surface mismatches, each node publishes its `enabledFeatures` set to a shared Hazelcast `IMap<nodeId, Set<String>>` at startup. If any node's feature set differs from the others, a `WARNING` is logged and the mismatch is visible in the dashboard cluster overview.

## Changes Required

### 1. `Monster.kt` — conditional verticle deployment
Wrap each unconditional `deployVerticle` call with an `if (extensionEnabled)` check.
Also write the enabled feature set to a Hazelcast `IMap` after startup.

Currently unconditional (needs to be gated):
- `OpcUaExtension`
- `MqttClientExtension`
- `KafkaClientExtension`
- `NatsClientExtension`
- `TelegramClientExtension` *(added in v1.8.x)*
- `WinCCOaExtension`
- `WinCCUaExtension`
- `Plc4xExtension`
- `Neo4jExtension`
- `JDBCLoggerExtension`
- `SparkplugBDecoderExtension`
- `FlowEngineExtension`

Already conditional (no change needed):
- `Oa4jBridge` (default `false`)
- `OpcUaServerExtension` (null-check)

### 2. GraphQL schema — `Broker` type
Add `enabledFeatures: [String!]!` to the `Broker` type in `schema-types.graphqls`.

### 3. GraphQL resolver — `brokers` query
Return the `enabledFeatures` list per node by reading from the Hazelcast `IMap`.

### 4. GraphQL mutation resolvers — feature guards
Add a guard to each extension's mutation resolvers that returns a GraphQL error if the feature is disabled on the current node. Affected resolvers:
- `OpcUaClientConfigMutations`
- `MqttClientConfigMutations`
- `KafkaClientConfigMutations`
- `NatsClientConfigMutations`
- `TelegramClientConfigMutations` *(added in v1.8.x)*
- `WinCCOaClientConfigMutations`
- `WinCCUaClientConfigMutations`
- `Plc4xClientConfigMutations`
- `Neo4jClientConfigMutations`
- `JDBCLoggerMutations`
- `SparkplugBDecoderMutations`
- `FlowMutations`

### 5. Dashboard — cluster overview + sidebar
- Add a `Features` column to the cluster node table in `dashboard.html` / `dashboard.js`
- Highlight in orange/red if any node's feature set differs from the others
- Optionally hide sidebar menu items for disabled features (read `enabledFeatures` from the current node's `broker` query on load)

### 6. `yaml-json-schema.json`
Add a top-level `Features` object with optional boolean properties for each known extension name.

## What Does NOT Change

- The monolithic JAR — all code stays in one build, no classloader changes
- The clustering mechanism — Hazelcast and `isLocalNodeResponsible()` are untouched
- Default behaviour — omitting `Features` entirely means all extensions start, existing configs work without modification
- Extension self-idling — extensions already handle "no devices configured" gracefully; this adds the startup-time gate on top

## Developer Checklist for New Extensions

When adding a new extension, the developer must:
1. Read `Features.<ExtensionName>` (default `true`) in `Monster.kt` and wrap `deployVerticle` accordingly
2. Register the feature name in the `enabledFeatures` set written to Hazelcast
3. Add a feature-disabled guard to the extension's mutation resolver
4. Add the extension name as an optional boolean property in the `Features` object in `yaml-json-schema.json`

## Acceptance Criteria

- [ ] `Features: <Extension>: false` prevents the extension verticle from deploying
- [ ] All existing configs without a `Features` section continue working unchanged
- [ ] GraphQL mutations for a disabled extension return a clear error, not a silent no-op
- [ ] `brokers` query returns `enabledFeatures` per node
- [ ] Dashboard cluster table shows feature mismatch warning when nodes differ
- [ ] No changes to clustering behaviour or device assignment logic
- [ ] `yaml-json-schema.json` updated for all affected extensions
