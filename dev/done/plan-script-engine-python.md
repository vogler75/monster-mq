# Plan: Python Script Engine for MonsterMQ (Main Broker)

This document specifies the architecture and implementation plan for adding standalone **Python scripting** to the main JVM/Kotlin **MonsterMQ broker** (`monster-mq`), using **GraalVM Polyglot Python (`GraalPy`)**.

This implementation pairs directly with the Starlark scripting engine in **MonsterMQ Edge** (`monster-mq-edge`), sharing the exact same `DeviceConfig` JSON format, trigger types, broker APIs, and GraphQL schema.

---

## 1. Background & Goals

* **Single Script Device**: Eliminate the visual workflow graph overhead (`Flow-Class` / `Flow-Object`) in favor of a clean, standalone `"Script"` device type in `DeviceConfig`.
* **Standard OpenJDK Execution**: Use `org.graalvm.polyglot:python` (GraalPy 25.0.1) as a regular Maven dependency. No external Python runtime, C compilers, or specialized GraalVM JDK installations are required on the host system.
* **Full Parity with Edge**: Scripts written in Python/Starlark can run on either **Edge (Starlark)** or **Main (GraalPy)** with identical API bindings (`msg`, `mqtt.publish`, `archive.get_last_value`, `db.query`, `state`, `global`, `storage`, `scripts.call`).
* **Cluster & Async Ready**: Follow MonsterMQ's Vert.x **Extension + Connector** pattern. Scripts execute on dedicated Vert.x worker threads (`vertx.executeBlocking`) to ensure reactive event loops are never blocked.

---

## 2. Dependencies & Build Configuration

### `broker/pom.xml`

Add GraalPy alongside the existing GraalJS and Polyglot dependencies:

```xml
<!-- GraalVM Polyglot API (Already in pom.xml) -->
<dependency>
    <groupId>org.graalvm.polyglot</groupId>
    <artifactId>polyglot</artifactId>
    <version>25.0.1</version>
</dependency>

<!-- GraalVM JavaScript (Already in pom.xml) -->
<dependency>
    <groupId>org.graalvm.polyglot</groupId>
    <artifactId>js</artifactId>
    <version>25.0.1</version>
    <type>pom</type>
</dependency>

<!-- GraalVM Python (GraalPy) - Pure Truffle Python 3 runtime on OpenJDK -->
<dependency>
    <groupId>org.graalvm.polyglot</groupId>
    <artifactId>python</artifactId>
    <version>25.0.1</version>
    <type>pom</type>
</dependency>
```

---

## 3. Data Model & DeviceConfig

### DeviceConfig Specification
* **`name`**: Unique script identifier (e.g. `"UPSMonitor"`, `"EnergyAggregator"`).
* **`namespace`**: Logical namespace/grouping (e.g. `"default"`, `"scripts"`).
* **`nodeId`**: Target cluster node (e.g. `"node-1"`, `"*"`, or current node ID).
* **`type`**: `DeviceConfig.DEVICE_TYPE_SCRIPT = "Script"`.
* **`enabled`**: `true` / `false`.
* **`config`**: Raw JSON representing `ScriptConfig`.

### `ScriptConfig.kt` (`broker/src/main/kotlin/stores/devices/ScriptConfig.kt`)

```kotlin
data class ScriptConfig(
    val language: String = "python",         // "python", "starlark", or "javascript"
    val triggerType: String = "TOPIC",       // "TOPIC", "TIMER", "BOTH", "CALLABLE"
    val topicFilters: List<String> = emptyList(), // MQTT topic patterns (+ and # supported)
    val triggerOnChangeOnly: Boolean = false, // Skip execution if payload has not changed
    val timerIntervalMs: Int = 0,            // Periodic interval in ms (0 = disabled)
    val instanceMode: String = "SINGLETON",   // "SINGLETON" or "MULTI_INSTANCE"
    val timeoutMs: Int = 200,                // Execution timeout in ms
    val script: String = "",                 // Python / JS code
    val description: String? = null          // Human description
)
```

---

## 4. Script Execution Engine (`devices/script/ScriptEngine.kt`)

Wraps GraalVM Polyglot `Context` to support both Python and JavaScript:

```kotlin
class ScriptEngine(
    private val scriptConfig: ScriptConfig,
    private val proxies: ScriptProxies
) {
    private val context: Context = Context.newBuilder("python", "js")
        .allowAllAccess(true)
        .build()

    // Pre-compile script on creation
    private val compiledSource: Source = Source.newBuilder(
        normalizeLanguage(scriptConfig.language),
        scriptConfig.script,
        scriptConfig.description ?: "script"
    ).build()

    fun execute(msg: BrokerMessage?, customArgs: Map<String, Any?>? = null): ScriptExecutionResult
}
```

### Injected Polyglot Proxies & Modules

GraalVM Polyglot automatically converts Kotlin objects into native Python dicts/objects:

1. **`msg`**:
   - `msg.topic` or `msg["topic"]`
   - `msg.payload` or `msg["payload"]` (auto-parsed to dict/list if valid JSON, otherwise string)
   - `msg.raw_payload`
   - `msg.timestamp` (epoch millis)
   - `msg.qos`
   - `msg.retain`
   *(Passed as `None` when invoked via timer or callable)*.

2. **`mqtt` (`MqttProxy`)**:
   - `mqtt.publish(topic, payload, qos=0, retain=False)`: Publishes to MonsterMQ EventBus / MqttServer.
   - `mqtt.subscribe(filter, callback_fn)`: Registers an in-script callback for incoming topics.

3. **`archive` (`ArchiveProxy`)**:
   - `archive.get_last_value(topic, archive_group="Default")`: Queries `LastValueStore`.
   - `archive.get_last_values(pattern, limit=100, archive_group="Default")`.
   - `archive.get_history(topic, from_time=None, to_time=None, limit=100)`: Queries `IMessageArchiveExtended`.
   - `archive.get_aggregated_history(topics, interval, from_time, to_time, functions, fields)`.

4. **`db` (`DatabaseProxy`)**:
   - Accesses MonsterMQ's configured JDBC connections (`JdbcManagerHolder`):
   - `db.query(conn_name, sql, args=[])`: Returns list of row dicts `[{"col": val}, ...]`.
   - `db.execute(conn_name, sql, args=[])`: Returns `{"affected_rows": N, "success": True}`.

5. **Scoped Storage (Node-RED style)**:
   - **`state`**: Per-script instance in-memory map preserved across executions.
   - **`global`**: In-memory dictionary shared across all scripts on the node (or backed by Hazelcast `IMap` in clustered mode).
   - **`storage`**: Persistent Key-Value store backed by the broker's database (`stores.SessionStore` / `IDeviceConfigStore`) that survives script reloads and broker restarts.

6. **`scripts` (`ScriptsProxy`)**:
   - `scripts.call(script_name, args={})`: Synchronously executes another registered script by name and returns its result value.

7. **`log` / `console` (`LogProxy`)**:
   - `log.info(...)`, `log.warn(...)`, `log.error(...)`, `log.debug(...)`, and `console.log(...)`.
   - Logs to broker logger and stores recent messages in a circular in-memory buffer (`recentLogs`) for inspection via GraphQL.

---

## 5. Architecture: Vert.x Extension + Connector Pattern

Following MonsterMQ's established device bridging architecture:

```
broker/src/main/kotlin/devices/script/
├── ScriptConfig.kt          # Configuration data class
├── ScriptEngine.kt          # GraalPy / GraalJS polyglot wrapper & proxies
├── ScriptConnector.kt       # Per-script verticle (subscribes, runs timer, handles concurrency)
├── ScriptExtension.kt       # Cluster-wide coordinator managing deployment & reloads
└── ScriptStorage.kt         # Persistent KV storage implementation
```

### `ScriptExtension.kt`
- Implements `AbstractVerticle` and handles cluster events on EventBus `monstermq.devices.script.redeploy`.
- Watches `DeviceConfigStore` for devices of type `DeviceConfig.DEVICE_TYPE_SCRIPT`.
- Deploys / un-deploys `ScriptConnector` verticles assigned to this cluster node.

### `ScriptConnector.kt`
- Deployed as a Vert.x verticle for each enabled script device.
- Subscribes to local EventBus for all patterns in `topicFilters` (with MQTT `+` and `#` wildcard resolution).
- Runs `vertx.setPeriodic(timerIntervalMs)` if `timerIntervalMs > 0`.
- Manages `instanceMode`:
  - `SINGLETON`: Runs executions sequentially using a serialized worker queue.
  - `MULTI_INSTANCE`: Dispatches executions concurrently to the Vert.x worker pool.
- Wraps execution in `vertx.executeBlocking` with timeout enforcement (`timeoutMs`).

---

## 6. GraphQL Schema & Resolvers

### SDL: `broker/src/main/resources/schema-scripts.graphqls`

Identical to the Edge broker schema:

```graphql
enum ScriptTriggerType {
    TOPIC
    TIMER
    BOTH
    CALLABLE
}

enum ScriptInstanceMode {
    SINGLETON
    MULTI_INSTANCE
}

type ScriptConfig {
    language: String!
    script: String!
    triggerType: ScriptTriggerType!
    topicFilters: [String!]!
    triggerOnChangeOnly: Boolean
    timerIntervalMs: Int
    instanceMode: ScriptInstanceMode!
    timeoutMs: Int
    description: String
}

input ScriptConfigInput {
    language: String! = "starlark"
    script: String!
    triggerType: ScriptTriggerType! = TOPIC
    topicFilters: [String!]! = []
    triggerOnChangeOnly: Boolean = false
    timerIntervalMs: Int = 0
    instanceMode: ScriptInstanceMode! = SINGLETON
    timeoutMs: Int = 200
    description: String
}

input ScriptInput {
    name: String!
    namespace: String! = "script"
    nodeId: String! = "local"
    enabled: Boolean = true
    config: ScriptConfigInput!
}

type Script {
    name: String!
    namespace: String!
    nodeId: String!
    enabled: Boolean!
    config: ScriptConfig!
    createdAt: String!
    updatedAt: String!
    isOnCurrentNode: Boolean!
    executionCount: Long
    errorCount: Long
    lastExecutionTime: String
    lastExecutionStatus: String
    recentLogs: [String!]!
}

type ScriptResult {
    script: Script
    success: Boolean!
    errors: [String!]!
}

type ScriptPublishedMessage {
    topic: String!
    payload: String!
    qos: Int!
    retain: Boolean!
}

type ScriptTestResult {
    success: Boolean!
    returnValue: String
    outputMessages: [ScriptPublishedMessage!]!
    logs: [String!]!
    errors: [String!]!
    executionTimeMs: Float!
}

type ScriptMutations {
    create(input: ScriptInput!): ScriptResult!
    update(name: String!, input: ScriptInput!): ScriptResult!
    delete(name: String!): Boolean!
    toggle(name: String!, enabled: Boolean!): ScriptResult!
    start(name: String!): ScriptResult!
    stop(name: String!): ScriptResult!
    test(input: ScriptInput!, testTopic: String, testPayload: String, testArgs: String): ScriptTestResult!
}

extend type Query {
    scripts(name: String, nodeId: String): [Script!]!
    script(name: String!): Script
}

extend type Mutation {
    script: ScriptMutations!
}
```

### Resolvers (`graphql/ScriptQueries.kt` & `graphql/ScriptMutations.kt`)
- Wire queries to `IDeviceConfigStore` and `ScriptExtension`.
- The `test` mutation runs the script against mock data on a worker thread and captures publishes/logs without modifying broker state.

---

## 7. Configuration & Feature Flags

Feature flags are granular per language runtime:

1. **`Features.kt`**:
   ```kotlin
   const val PythonScripts = "PythonScripts"
   // In future: const val JavaScripts = "JavaScripts"
   ```
   Append `PythonScripts` to `Features.all`.

2. **`broker/yaml-json-schema.json`**:
   Add `PythonScripts: { type: "boolean" }` under `Features.properties`.

3. **`Monster.kt`**:
   Deploy `ScriptExtension` when `PythonScripts` (or in the future `JavaScripts`) is enabled:
   ```kotlin
   if (Monster.isFeatureEnabled(Features.PythonScripts)) {
       vertx.deployVerticle(ScriptExtension(deviceConfigStore, ...))
   }
   ```

---

## 8. Verification & Testing

1. **Unit Tests (`broker/src/test/kotlin/devices/script/ScriptEngineTest.kt`)**:
   - Test GraalPy Python execution (math, JSON parsing, conditionals).
   - Test `mqtt.publish` capture.
   - Test `archive.get_last_value` proxy.
   - Test `db.query` proxy with H2/SQLite.
   - Test memory scopes (`state`, `global`, `storage`).
   - Test `scripts.call` between two scripts.
   - Test execution timeout cancellation.
2. **Integration Tests (`tests/pytest_tests/scripts/`)**:
   - Create a Python script device via GraphQL mutation.
   - Publish to MQTT topic and verify enriched output topic.
   - Test `test()` mutation endpoint.
