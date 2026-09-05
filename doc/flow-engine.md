# Flow Engine Runtime

For creating and using flows, start with [Workflows](workflows.md). This page
covers the implemented runtime and its source locations. It replaces the old
implementation plan, which mixed proposed features, obsolete editor choices,
and malformed examples with operational documentation.

## Storage and Deployment

Flow classes are reusable templates stored as `Flow-Class` device records. Flow
instances use `Flow-Object` records, reference a class through `flowClassId`, and
provide input/output mappings and instance variables.

`FlowEngineExtension` loads and manages instances for their assigned node.
`Features.FlowEngine` gates the extension. Use `local` for a standalone broker or
a real cluster node ID when assigning an instance. Each instance runs in a
`FlowInstanceExecutor` verticle; a flow is not split across cluster nodes.

## Runtime Behavior

MQTT input mappings subscribe to topic filters. An incoming message updates the
input cache and triggers connected processing. Node outputs propagate to connected
input ports; configured output mappings publish them to MQTT.

The executor dispatches function scripts, timer nodes, and database nodes. Timer
nodes initialize when the instance starts and cancel their timers when it stops.
Database nodes use the shared JDBC manager. Query `flowNodeTypes` for the node
metadata exposed by this broker; do not assume a generic Node-RED palette.

Function scripts run through GraalVM's JavaScript context with `msg`, `inputs`,
`outputs`, `state`, `flow`, and `console` bindings. Send outputs explicitly with
`outputs.send(portName, value)`. `state` is node-local runtime state and is lost
when the instance is redeployed or the broker restarts. Instance variables are
available through `flow`; they are configuration, not a secret store.

The model includes a `language` field, but the runtime constructs a JavaScript
context. A Python language label does not mean that a working Python runtime is
bundled. JavaScript is the supported choice for the supplied broker.

## Management and Diagnostics

```graphql
query FlowStatus {
  flowInstances {
    name
    nodeId
    enabled
    status {
      running
      executionCount
      errorCount
      lastError
      subscribedTopics
    }
  }
}
```

GraphQL operations are grouped under `flow`, including class/instance creation,
updates, deletion, enable/disable, reassignment, and `testNode`. A script test does
not validate the complete MQTT deployment: verify input subscriptions, actual
message payloads, connections, output mappings, and node ownership as well.

## Source Map

| Source | Responsibility |
|---|---|
| [FlowConfig.kt](../broker/src/main/kotlin/stores/devices/FlowConfig.kt) | Stored model and serialization |
| [FlowEngineExtension.kt](../broker/src/main/kotlin/flowengine/FlowEngineExtension.kt) | Instance deployment and lifecycle |
| [FlowInstanceExecutor.kt](../broker/src/main/kotlin/flowengine/FlowInstanceExecutor.kt) | Input cache, node dispatch, routing, timers |
| [FlowScriptEngine.kt](../broker/src/main/kotlin/flowengine/FlowScriptEngine.kt) | Script bindings and execution |
| [JdbcManager.kt](../broker/src/main/kotlin/flowengine/JdbcManager.kt) | JDBC access |
| [FlowQueries.kt](../broker/src/main/kotlin/graphql/FlowQueries.kt) | Queries and node metadata |
| [FlowMutations.kt](../broker/src/main/kotlin/graphql/FlowMutations.kt) | Management and tests |
| [schema-flows.graphqls](../broker/src/main/resources/schema-flows.graphqls) | Complete API contract |

Dashboard source is maintained in the separate
[monster-mq-dashboard repository](https://github.com/vogler75/monster-mq-dashboard).
Packaged dashboard resources in the broker are generated output.
