# Flow Engine Implementation Plan

## Executive Summary

We'll build a Node-RED-like flow programming system integrated into MonsterMQ, using the existing DeviceConfig storage infrastructure. Flows will be stored as two device types: **Flow-Class** (templates) and **Flow-Object** (instances with concrete input/output mappings).

---

## 1. Architecture Overview

### Data Model Design

#### FlowClass (DeviceConfig type="Flow-Class")

Flow classes define the template/blueprint with nodes that have named inputs and outputs.

```json
{
  "name": "temperature-alarm-flow",
  "namespace": "flows/classes/temperature-alarm",
  "nodeId": "*",
  "type": "Flow-Class",
  "config": {
    "version": "1.0",
    "description": "Converts temperature and triggers alarms",
    "nodes": [
      {
        "id": "convert-1",
        "type": "function",
        "name": "Celsius to Fahrenheit",
        "config": {
          "script": "return { fahrenheit: msg.celsius * 9/5 + 32 };"
        },
        "inputs": ["celsius", "comment"],
        "outputs": ["fahrenheit", "error"],
        "position": {"x": 100, "y": 100}
      },
      {
        # Flow Engine (Current Implementation)

        This document describes the **implemented** Flow Engine inside MonsterMQ. It purposefully omits any GUI / visual editor plans and focuses strictly on the runtime, data model, deployment mechanics, scripting, and operational behavior that exist in the codebase (`flowengine/*`, `stores/devices/FlowConfig.kt`).

        Implemented scope:
        - DeviceConfig-backed storage of Flow Classes (`Flow-Class`) and Flow Instances (`Flow-Object`).
        - Per-instance Vert.x verticle execution (`FlowInstanceExecutor`).
        - Cluster-aware deployment via `FlowEngineExtension` (only flows assigned to the current node are deployed).
        - JavaScript execution using GraalVM (per node function scripts).
        - TOPIC and TEXT input mapping types (TOPIC triggers execution; TEXT provides constants).
        - Named input/output ports with explicit connections.
        - Output mapping publishes MQTT messages through internal `SessionHandler`.
        - Internal node-to-node value propagation using a synthetic internal topic namespace.

        Not yet implemented (future / out-of-scope for this doc): visual React/ReactFlow editor, advanced node library (switch/change/template/delay/etc.), metrics, persistence of node state, test harness GraphQL mutations, additional scripting languages beyond the existing JavaScript context.
        "type": "function",
        "name": "Check Alarm Threshold",
        "config": {
          "script": "if (msg.fahrenheit > 176) { return { alarm: 'Temperature critical!' }; }"
        },
        "inputs": ["fahrenheit"],
        "outputs": ["alarm"],
        "position": {"x": 300, "y": 100}
      }
    ],
    "connections": [
      {
        "fromNode": "convert-1",
        "fromOutput": "fahrenheit",
        "toNode": "alarm-1",
        "toInput": "fahrenheit"
      }
    ]
  }
}
```

**Key Points:**
- Each node declares its **inputs** and **outputs** as named ports (arrays of strings)
- Connections explicitly map: `fromNode.fromOutput` → `toNode.toInput`
- This allows multiple inputs/outputs per node without ambiguity

#### FlowInstance (DeviceConfig type="Flow-Object")

Flow instances map the template's inputs to concrete values (topics or constants) and outputs to topics.

```json
{
  "name": "building-1-temp-alarm",
  "namespace": "flows/instances/building-1",
  "nodeId": "node-1",
  "type": "Flow-Object",
  "config": {
    "flowClassId": "temperature-alarm-flow",
    "inputMappings": {
      "convert-1.celsius": {
        "type": "topic",
        "value": "building/1/sensors/temperature"
      },
      "convert-1.comment": {
        "type": "text",
        "value": "Building 1 main sensor"
      }
    },
    "variables": {
      "threshold": 80,
      "building": "Building 1"
    }
  }
}
```

**Input Mapping Types:**

1. **`type: "topic"`** - Triggered by MQTT messages
   - Subscribes to the MQTT topic
   - Flow executes when message arrives
   - Value comes from MQTT payload

2. **`type: "text"`** - Fixed constant value
   - Value is provided as string
   - Available to all node executions
   - Does not trigger flow execution

**Output Mapping:**
- Maps node output port to MQTT topic
- Published when node produces output on that port

#### Runtime State (In-Memory)

```kotlin
class FlowInstanceRuntime(
    val instance: DeviceConfig,           // Flow instance config
    val flowClass: DeviceConfig,          // Flow class template
    val topicInputs: Map<String, String>, // "nodeId.inputName" -> "mqtt/topic"
    val textInputs: Map<String, String>,  // "nodeId.inputName" -> "constant value"
    val topicValues: MutableMap<String, TopicValue>, // Latest value per subscribed topic
    val nodeStates: MutableMap<String, Any>,         // Persistent node state
    val scriptEngine: ScriptEngine
)
```

---

**Why JavaScript?**
- Industry standard for flow programming (Node-RED compatibility)
- Excellent JVM support via GraalVM
- Familiar to developers
- Fast execution
- Rich ecosystem

**Script Context API:**

```javascript
// Available in node scripts:

// msg - Current message object
msg: {
  topic: "building/1/sensors/temperature",
  value: 85,
  timestamp: 1234567890,
  qos: 1
}

// inputs - All input port values (both topic and text inputs)
inputs: {
  'celsius': { value: 85, timestamp: 1234567890, type: 'topic' },
  'comment': { value: 'Building 1 main sensor', type: 'text' }
}

// state - Node-local persistent state
state: {
  count: 0,
  lastValue: null
}

// flow - Flow-wide state (shared across all nodes in the instance)
flow: {
  threshold: 80,
  building: 'Building 1'
}

// outputs - Object to send data to output ports
outputs: {
  send(portName, payload) { ... }
}

// console - Logging
console: {
  log(msg),
  warn(msg),
  error(msg)
}

// Example function node script:
const temp = inputs['celsius'].value;
const threshold = flow.threshold || 80;
const building = inputs['comment'].value;

if (temp > threshold) {
  outputs.send('alarm', {
    level: 'high',
    temperature: temp,
    building: building,
    message: `Temperature ${temp}°C exceeds ${threshold}°C`
  });

  // Update state
    temperature: temp,
    building: building
  });
}
```

### Core Components

```
broker/src/main/kotlin/
├── devices/flowengine/
│   ├── FlowEngineExtension.kt         # Main Verticle (loads/manages flows)
│   ├── FlowInstanceExecutor.kt        # Executes individual flow instances
│   ├── FlowNodeExecutor.kt            # Executes individual nodes
│   ├── FlowNodeRegistry.kt            # Registry of node types
│   ├── FlowScriptEngine.kt            # JavaScript execution wrapper
│       ├── ChangeNode.kt              # Value transformation
│       ├── TemplateNode.kt            # String templates
│       ├── DelayNode.kt               # Rate limiting/debounce
│       └── TriggerNode.kt             # State-based triggers
├── stores/devices/
│   ├── FlowConfig.kt                  # Data classes for Flow configs
│   └── FlowNodeTypes.kt               # Node type definitions
└── graphql/
    ├── FlowQueries.kt                 # GraphQL queries
### Execution Flow

```
1. MQTT Message Arrives
   └→ Topic: "building/1/sensors/temperature" = 85°C

2. FlowEngine finds matching instances
   └→ topicValues["building/1/sensors/temperature"] = {value: 85, timestamp: ...}

4. Execute node "convert-1" with inputs:
   └→ inputs.celsius = { value: 85, type: 'topic', timestamp: ... }
   └→ inputs.comment = { value: 'Building 1 main sensor', type: 'text' }

5. Node outputs to port "fahrenheit":
   └→ { fahrenheit: 185 }
7. Node "alarm-1" outputs to port "alarm":
   └→ { alarm: 'Temperature critical!' }

8. Map output to MQTT topic
   └→ Publish to "building/1/alarms/temperature"
```

### Built-in Node Types

#### 1. **function** (JavaScript)
- Execute custom JavaScript code
- **Inputs:** Dynamic (defined in node config)
- **Outputs:** Dynamic (defined in node config)
- **Config:**
  ```

#### 2. **switch** (Conditional Routing)
- Route to different outputs based on conditions
- **Inputs:** `["value"]`
- **Outputs:** Dynamic based on rules
- **Config:**
  ```json
  {
    "property": "value",
    "rules": [
      {"condition": "> 80", "output": "high"},
      {"condition": "<= 80", "output": "normal"}
    ]
  }
  ```

#### 3. **change** (Transform)
- Modify message properties
- **Inputs:** `["in"]`
- **Outputs:** `["out"]`
    "rules": [
#### 4. **template** (String Template)
- **Inputs:** Dynamic (any inputs available in template)
- **Config:**
    "template": "Temperature in {{building}} is {{temperature}}°C",
  }
  ```

#### 5. **delay** (Timing)
- Rate limiting, throttle, or debounce
- **Inputs:** `["in"]`
- **Outputs:** `["out"]`
- **Config:**
  ```json
  {
    "type": "delay|throttle|debounce",
    "timeout": 1000,
    "rate": 10
  }
  ```

#### 6. **trigger** (State-based)
- Send messages based on state changes
- **Inputs:** `["trigger", "reset"]`
- **Outputs:** `["on", "off"]`
- **Config:**
  ```json
  {
    "duration": 5000,
    "extend": false
  }
  ```

#### 7. **filter** (Message Filtering)
- Pass or block messages based on conditions
- **Inputs:** `["in"]`
- **Outputs:** `["pass", "block"]`
- **Config:**
  ```json
  {
    "condition": "msg.value > 0",
    "mode": "pass|block"
  }
  ```

#### 8. **aggregate** (Data Aggregation)
- Combine multiple messages
- **Inputs:** Dynamic
- **Outputs:** `["result"]`
- **Config:**
  ```json
  {
    "operation": "avg|sum|min|max|count",
    "window": 60000
  }
  ```

---

## 5. GraphQL API Design

### Schema Extensions (`schema-flows.graphqls`)

```graphql
# =======================
# Flow Types
# =======================

type FlowClass {
  name: String!
  namespace: String!
  version: String!
  description: String
  nodes: [FlowNode!]!
  connections: [FlowConnection!]!
  createdAt: String!
  updatedAt: String!
}

type FlowNode {
  id: String!
  type: String!
  name: String!
  config: JSON!
  inputs: [String!]!
  outputs: [String!]!
  position: FlowPosition
}

type FlowConnection {
  fromNode: String!
  fromOutput: String!
  toNode: String!
  toInput: String!
}

type FlowPosition {
  x: Float!
  y: Float!
}

type FlowInstance {
  name: String!
  namespace: String!
  nodeId: String!
  flowClassId: String!
  inputMappings: [FlowInputMapping!]!
  outputMappings: [FlowOutputMapping!]!
  variables: JSON
  enabled: Boolean!
  status: FlowInstanceStatus
  createdAt: String!
  updatedAt: String!
  isOnCurrentNode: Boolean!
}

type FlowInputMapping {
  nodeInput: String!      # "nodeId.inputName"
  type: FlowInputType!
  value: String!
}

enum FlowInputType {
  TOPIC   # MQTT topic subscription (triggers flow)
  TEXT    # Fixed text constant
}

type FlowOutputMapping {
  nodeOutput: String!     # "nodeId.outputName"
  topic: String!
}

type FlowInstanceStatus {
  running: Boolean!
  lastExecution: String
  executionCount: Long!
  errorCount: Long!
  lastError: String
  subscribedTopics: [String!]!
}

type FlowNodeType {
  type: String!
  category: String!
  description: String!
  defaultInputs: [String!]!
  defaultOutputs: [String!]!
  configSchema: JSON!
  icon: String
}

# =======================
# Queries
# =======================

extend type Query {
  # Get all flow classes
  flowClasses(
    name: String
  ): [FlowClass!]!

  # Get a specific flow class
  flowClass(
    name: String!
  ): FlowClass

  # Get all flow instances
  flowInstances(
    flowClassId: String
    nodeId: String
    enabled: Boolean
  ): [FlowInstance!]!

  # Get a specific flow instance
  flowInstance(
    name: String!
  ): FlowInstance

  # Get available node types
  flowNodeTypes: [FlowNodeType!]!
}

# =======================
# Mutations
# =======================

extend type Mutation {
  flow: FlowMutations
}

type FlowMutations {
  # Flow Class Management
  createClass(input: FlowClassInput!): FlowClass!
  updateClass(name: String!, input: FlowClassInput!): FlowClass!
  deleteClass(name: String!): Boolean!

  # Flow Instance Management
  createInstance(input: FlowInstanceInput!): FlowInstance!
  updateInstance(name: String!, input: FlowInstanceInput!): FlowInstance!
  deleteInstance(name: String!): Boolean!

  # Instance Control
  enableInstance(name: String!): FlowInstance!
  disableInstance(name: String!): FlowInstance!
  reassignInstance(name: String!, nodeId: String!): FlowInstance!

  # Testing
  testNode(
    flowClassName: String!
    nodeId: String!
    testInputs: JSON!
  ): FlowTestResult!
}

# =======================
# Input Types
# =======================

input FlowClassInput {
  name: String!
  namespace: String!
  version: String
  description: String
  nodes: [FlowNodeInput!]!
  connections: [FlowConnectionInput!]!
}

input FlowNodeInput {
  id: String!
  type: String!
  name: String!
  config: JSON!
  inputs: [String!]!
  outputs: [String!]!
  position: FlowPositionInput
}

input FlowConnectionInput {
  fromNode: String!
  fromOutput: String!
  toNode: String!
  toInput: String!
}

input FlowPositionInput {
  x: Float!
  y: Float!
}

input FlowInstanceInput {
  name: String!
  namespace: String!
  nodeId: String!
  flowClassId: String!
  inputMappings: [FlowInputMappingInput!]!
  outputMappings: [FlowOutputMappingInput!]!
  variables: JSON
  enabled: Boolean
}

input FlowInputMappingInput {
  nodeInput: String!      # "nodeId.inputName"
  type: FlowInputType!
  value: String!
}

input FlowOutputMappingInput {
  nodeOutput: String!     # "nodeId.outputName"
  topic: String!
}

# =======================
# Test Results
# =======================

type FlowTestResult {
  success: Boolean!
  outputs: JSON
  logs: [String!]!
  errors: [String!]!
  executionTime: Int!
}
```

---

## 6. Visual Editor (Frontend)

### Technology: **ReactFlow** ✓

**Why ReactFlow?**
- Most popular React flow library (20k+ GitHub stars)
- Highly customizable
- Active development
- MIT license
- Perfect for our use case

### Editor Features

#### 1. **Flow Class Editor**
- **Canvas**
  - Drag and drop nodes from palette
  - Pan and zoom
  - Mini-map for navigation
  - Auto-layout option

- **Node Palette** (Left sidebar)
  - Categorized node types
  - Search/filter
  - Node descriptions
  - Drag to canvas

- **Node Configuration** (Right sidebar)
  - Properties panel
  - Input/Output port editor
    - Add/remove/rename ports
    - Port type indicators
  - Script editor for function nodes
    - Monaco Editor for JavaScript
    - Syntax highlighting
    - Auto-completion
  - Live validation

- **Connection Management**
  - Visual port-to-port connections
  - Connection validation (type checking)
  - Multiple connections per port
  - Delete connections

#### 2. **Flow Instance Editor**

- **Flow Overview**
  - Read-only view of flow class
  - Highlight input/output ports

- **Input Mapping Panel**
  ```
  Node: convert-1
  ├─ Input: celsius
  │  ├─ Type: [Topic ▼]
  │  └─ Value: [building/1/sensors/temperature] [Browse...]
  └─ Input: comment
     ├─ Type: [Text ▼]
     └─ Value: [Building 1 main sensor]
  ```
  - Dropdown to select input type (Topic/Text)
  - Topic browser integration for topic inputs
  - Text input field for text inputs
  - Validation (required inputs)

- **Output Mapping Panel**
  ```
  Node: alarm-1
  └─ Output: alarm → [building/1/alarms/temperature] [Browse...]
  ```
  - Map outputs to MQTT topics
  - Topic browser

- **Variables Panel**
  - Key-value editor for flow variables
  - Used in scripts as `flow.varName`

- **Deployment Panel**
  - Select cluster node
  - Enable/disable toggle
  - Deploy button
  - Status indicator

#### 3. **Testing/Debugging**

- **Test Console**
  - Inject test messages
  - Select input port
  - Provide test payload
  - View node outputs in real-time

- **Execution Trace**
  - Visual flow of execution
  - Highlight active nodes
  - Show data flowing through connections

- **Logs Panel**
  - console.log output
  - Warnings and errors
  - Execution timing

- **Debug Nodes**
  - Special node type to capture outputs
  - Display message content
  - Breakpoint functionality

---

## 7. Implementation Phases

### Phase 1: Core Infrastructure (2-3 weeks)
**Goal:** Basic flow execution engine with input types

**Tasks:**
- [ ] Add GraalVM JavaScript dependency to pom.xml
- [ ] Create `FlowConfig.kt` data classes
  - [ ] `FlowClass` with named inputs/outputs per node
  - [ ] `FlowInstance` with input type enumeration (TOPIC/TEXT)
  - [ ] `FlowConnection` with from/to node and port names
- [ ] Add device type constants:
  - [ ] `DEVICE_TYPE_FLOW_CLASS = "Flow-Class"`
  - [ ] `DEVICE_TYPE_FLOW_OBJECT = "Flow-Object"`
- [ ] Create `FlowEngineExtension.kt` Verticle
  - [ ] Load Flow-Object instances from DeviceConfigStore
  - [ ] Subscribe to MQTT topics based on input mappings (type=TOPIC)
  - [ ] Store text inputs (type=TEXT) in memory
- [ ] Implement `FlowScriptEngine.kt` (JavaScript wrapper)
  - [ ] Context with inputs object (both topic and text)
  - [ ] Outputs object with send() method
  - [ ] State and flow objects
- [ ] Implement `FlowInstanceExecutor.kt`
  - [ ] Execute nodes in connection order
  - [ ] Pass data between connected nodes
  - [ ] Handle errors gracefully
- [ ] Basic MQTT message routing to flows
- [ ] Simple in-memory state management

**Testing:**
- Create a Flow-Class manually in database
- Create a Flow-Object with mixed input types
- Publish to topic input → verify execution
- Verify text inputs are available in script

**Deliverable:** Can execute a simple flow with both topic and text inputs

### Phase 2: GraphQL API (1-2 weeks)
**Goal:** Full CRUD operations for flows

**Tasks:**
- [ ] Create `schema-flows.graphqls` with updated schema
  - [ ] `FlowInputType` enum (TOPIC, TEXT)
  - [ ] `FlowInputMapping` type
  - [ ] Named input/output connections
- [ ] Implement `FlowQueries.kt`
  - [ ] `flowClasses()`
  - [ ] `flowClass(name)`
  - [ ] `flowInstances()`
  - [ ] `flowInstance(name)`
  - [ ] `flowNodeTypes()`
- [ ] Implement `FlowMutations.kt`
  - [ ] `createClass()`
  - [ ] `updateClass()`
  - [ ] `deleteClass()`
  - [ ] `createInstance()` - validate input mappings
  - [ ] `updateInstance()`
  - [ ] `deleteInstance()`
  - [ ] `enableInstance()`
  - [ ] `disableInstance()`
  - [ ] `reassignInstance()`
  - [ ] `testNode()`
- [ ] Add resolvers to `GraphQLServer.kt`
- [ ] Testing with GraphQL Playground

**Testing:**
- Create flow classes via GraphQL
- Create instances with both TOPIC and TEXT inputs
- Test all CRUD operations
- Verify validation

**Deliverable:** Can create/edit/delete flows via GraphQL

### Phase 3: Extended Node Types (1-2 weeks)
**Goal:** Rich set of node types

**Tasks:**
- [ ] Create `FlowNodeRegistry.kt`
  - [ ] Register all node types
  - [ ] Provide schema/metadata per type
- [ ] Implement node types:
  - [ ] `FunctionNode.kt` (JavaScript)
  - [ ] `SwitchNode.kt` (conditional routing)
  - [ ] `ChangeNode.kt` (transformation)
  - [ ] `TemplateNode.kt` (string templates)
  - [ ] `DelayNode.kt` (throttle/debounce)
  - [ ] `TriggerNode.kt` (state-based)
  - [ ] `FilterNode.kt` (message filtering)
  - [ ] `AggregateNode.kt` (data aggregation)
- [ ] Node type validation
- [ ] Node configuration schemas

**Testing:**
- Create flows using each node type
- Test with various configurations
- Verify all inputs/outputs work correctly

**Deliverable:** Full-featured node library

### Phase 4: Visual Editor - Part 1: Flow Class Editor (2-3 weeks)
**Goal:** Create and edit flow classes

**Tasks:**
- [ ] Setup React + TypeScript + ReactFlow frontend
  - [ ] Project structure
  - [ ] GraphQL client setup
- [ ] **Node Palette Component**
  - [ ] Fetch node types from API
  - [ ] Categorize nodes
  - [ ] Search/filter
  - [ ] Drag to canvas
- [ ] **Flow Canvas Component**
  - [ ] ReactFlow integration
  - [ ] Custom node rendering
  - [ ] Port visualization (inputs/outputs)
  - [ ] Connection handling with port names
  - [ ] Pan/zoom controls
- [ ] **Node Configuration Panel**
  - [ ] Dynamic form based on node type
  - [ ] Input/Output port editor
    - [ ] Add/remove ports
    - [ ] Rename ports
  - [ ] Script editor (Monaco) for function nodes
  - [ ] Validation
- [ ] **Flow Class Management**
  - [ ] New/Open/Save flow class
  - [ ] Delete flow class
  - [ ] Flow metadata editor

**Deliverable:** Can visually create flow classes

### Phase 5: Visual Editor - Part 2: Flow Instance Editor (2-3 weeks)
**Goal:** Configure and deploy flow instances

**Tasks:**
- [ ] **Instance Configuration UI**
  - [ ] Select flow class
  - [ ] Instance metadata (name, namespace)
- [ ] **Input Mapping Panel**
  - [ ] List all flow inputs (from all nodes)
  - [ ] Dropdown for input type (TOPIC/TEXT)
  - [ ] Topic selector with browser for TOPIC inputs
  - [ ] Text input field for TEXT inputs
  - [ ] Validation (required inputs)
- [ ] **Output Mapping Panel**
  - [ ] List all flow outputs
  - [ ] Topic selector with browser
  - [ ] Validation
- [ ] **Variables Panel**
  - [ ] Key-value editor
  - [ ] Add/remove variables
  - [ ] Type indicators
- [ ] **Deployment Panel**
  - [ ] Cluster node selector
  - [ ] Enable/disable toggle
  - [ ] Deploy/Undeploy actions
  - [ ] Status indicator
- [ ] **Topic Browser Integration**
  - [ ] Browse existing topics
  - [ ] Search topics
  - [ ] Preview topic values

**Deliverable:** Complete flow instance configuration and deployment

### Phase 6: Testing & Debugging Features (1-2 weeks)
**Goal:** Development and debugging tools

**Tasks:**
- [ ] **Test Console**
  - [ ] Inject test messages
  - [ ] Select node and input port
  - [ ] Provide test payload (JSON editor)
  - [ ] Execute and display results
- [ ] **Execution Trace**
  - [ ] Visual flow highlight
  - [ ] Show data at each connection
  - [ ] Timing information
- [ ] **Logs Panel**
  - [ ] Display console.log output
  - [ ] Error messages
  - [ ] Warnings
  - [ ] Filter by level
- [ ] **Debug Node Type**
  - [ ] Capture and display messages
  - [ ] Pause execution
  - [ ] Inspect message content

**Deliverable:** Full debugging and testing capabilities

### Phase 7: Advanced Features (2-3 weeks)
**Goal:** Production-ready features

**Tasks:**
- [ ] **Flow Metrics & Monitoring**
  - [ ] Execution count per node
  - [ ] Error count tracking
  - [ ] Average execution time
  - [ ] Metrics GraphQL API
  - [ ] Metrics visualization in UI
- [ ] **Error Handling**
  - [ ] Try-catch around node execution
  - [ ] Error output ports
  - [ ] Retry logic configuration
  - [ ] Dead letter queue
- [ ] **Flow Templates Library**
  - [ ] Predefined flow templates
  - [ ] Template categories
  - [ ] Import from library
- [ ] **Import/Export**
  - [ ] Export flow class as JSON
  - [ ] Import flow class from JSON
  - [ ] Export instance configuration
- [ ] **Flow Versioning**
  - [ ] Version tracking
  - [ ] Version comparison
  - [ ] Rollback capability
- [ ] **Sub-flows** (Reusable Components)
  - [ ] Define sub-flow as special flow class
  - [ ] Use sub-flow as node in other flows
  - [ ] Parameter passing
- [ ] **Persistent Node State**
  - [ ] Save state to database periodically
  - [ ] Restore state on restart
  - [ ] State management API

**Deliverable:** Production-ready flow engine with advanced features

### Phase 8: Testing & Documentation (1-2 weeks)
**Goal:** Quality assurance and documentation

**Tasks:**
- [ ] **Unit Tests**
  - [ ] FlowScriptEngine tests
  - [ ] Node type tests
  - [ ] Connection routing tests
  - [ ] Input type handling tests
- [ ] **Integration Tests**
  - [ ] End-to-end flow execution
  - [ ] MQTT integration tests
  - [ ] GraphQL API tests
- [ ] **Performance Tests**
  - [ ] High-throughput scenarios
  - [ ] Memory usage profiling
  - [ ] Concurrent flow execution
- [ ] **Documentation**
  - [ ] User guide
  - [ ] Node reference documentation
  - [ ] API documentation
  - [ ] Example flows
  - [ ] Best practices
  - [ ] Troubleshooting guide

**Deliverable:** Fully tested and documented system

---

## 8. Technical Decisions Summary

| Aspect | Decision | Rationale |
|--------|----------|-----------|
| **Storage** | DeviceConfig table | Reuses existing infrastructure |
| **Connection Model** | Named input/output ports | Flexible, explicit, supports multiple connections |
| **Input Types** | TOPIC (trigger) / TEXT (constant) | Supports both dynamic and static data |
| **Scripting** | GraalVM JavaScript | Industry standard, Node-RED compatible |
| **Visual Editor** | ReactFlow | Modern, lightweight, excellent support |
| **Node Definition** | JSON in config field | Flexible, easy to extend |
| **State Management** | In-memory with periodic persistence | Performance + reliability |
| **Clustering** | Node-assigned instances | Leverages existing cluster support |

---

## 9. Dependencies to Add

**Maven Dependencies (broker/pom.xml):**
```xml
<!-- GraalVM JavaScript Engine -->
<dependency>
    <groupId>org.graalvm.polyglot</groupId>
    <artifactId>polyglot</artifactId>
    <version>24.0.0</version>
</dependency>
<dependency>
    <groupId>org.graalvm.polyglot</groupId>
    <artifactId>js</artifactId>
    <version>24.0.0</version>
    <scope>runtime</scope>
</dependency>
```

**NPM Dependencies (frontend):**
```json
{
  "dependencies": {
    "react": "^18.2.0",
    "react-dom": "^18.2.0",
    "reactflow": "^11.10.0",
    "@monaco-editor/react": "^4.6.0",
    "@apollo/client": "^3.8.0",
    "graphql": "^16.8.0"
  }
}
```

---

## 10. Data Flow Examples

### Example 1: Temperature Monitoring with Mixed Inputs

**Flow Class: "temperature-monitor"**
```json
{
  "nodes": [
    {
      "id": "convert",
      "type": "function",
      "inputs": ["celsius", "unit"],
      "outputs": ["converted", "error"],
      "config": {
        "script": `
          const temp = inputs.celsius.value;
          const unit = inputs.unit.value;
          if (unit === 'F') {
            return { converted: temp * 9/5 + 32 };
          }
          return { converted: temp };
        `
      }
    },
    {
      "id": "alarm",
      "type": "function",
      "inputs": ["temperature", "threshold"],
      "outputs": ["alarm", "normal"],
      "config": {
        "script": `
          const temp = inputs.temperature.value;
          const threshold = parseInt(inputs.threshold.value);
          if (temp > threshold) {
            outputs.send('alarm', { temp, message: 'High temperature!' });
          } else {
            outputs.send('normal', { temp });
          }
        `
      }
    }
  ],
  "connections": [
    {
      "fromNode": "convert",
      "fromOutput": "converted",
      "toNode": "alarm",
      "toInput": "temperature"
    }
  ]
}
```

**Flow Instance: "building-1-monitor"**
```json
{
  "flowClassId": "temperature-monitor",
  "inputMappings": [
    {
      "nodeInput": "convert.celsius",
      "type": "TOPIC",
      "value": "building/1/sensors/temperature"
    },
    {
      "nodeInput": "convert.unit",
      "type": "TEXT",
      "value": "F"
    },
    {
      "nodeInput": "alarm.threshold",
      "type": "TEXT",
      "value": "176"
    }
  ],
  "outputMappings": [
    {
      "nodeOutput": "alarm.alarm",
      "topic": "building/1/alarms/temperature"
    }
  ]
}
```

**Execution:**
1. MQTT message arrives: `building/1/sensors/temperature = 80`
2. Flow instance triggered (because `convert.celsius` is a TOPIC input)
3. Node "convert" executes:
   - `inputs.celsius = { value: 80, type: 'topic', timestamp: ... }`
   - `inputs.unit = { value: 'F', type: 'text' }`
   - Outputs: `{ converted: 176 }`
4. Connection routes to node "alarm"
5. Node "alarm" executes:
   - `inputs.temperature = { value: 176, type: 'topic' }`
   - `inputs.threshold = { value: '176', type: 'text' }`
   - Compares: 176 > 176? No
   - Outputs to 'normal' port: `{ temp: 176 }`
6. No output mapping for "alarm.normal", so no MQTT publish

### Example 2: Multi-Sensor Aggregation

**Flow Class: "sensor-aggregator"**
```json
{
  "nodes": [
    {
      "id": "aggregate",
      "type": "function",
      "inputs": ["sensor1", "sensor2", "sensor3", "location"],
      "outputs": ["average"],
      "config": {
        "script": `
          const values = [
            inputs.sensor1.value,
            inputs.sensor2.value,
            inputs.sensor3.value
          ];
          const avg = values.reduce((a, b) => a + b, 0) / values.length;
          const location = inputs.location.value;
          outputs.send('average', {
            location,
            average: avg,
            count: values.length
          });
        `
      }
    }
  ]
}
```

**Flow Instance: "factory-hall-1"**
```json
{
  "inputMappings": [
    { "nodeInput": "aggregate.sensor1", "type": "TOPIC", "value": "factory/hall1/sensor/1" },
    { "nodeInput": "aggregate.sensor2", "type": "TOPIC", "value": "factory/hall1/sensor/2" },
    { "nodeInput": "aggregate.sensor3", "type": "TOPIC", "value": "factory/hall1/sensor/3" },
    { "nodeInput": "aggregate.location", "type": "TEXT", "value": "Factory Hall 1" }
  ],
  "outputMappings": [
    { "nodeOutput": "aggregate.average", "topic": "factory/hall1/average" }
  ]
}
```

**Execution:**
- Any of the three sensor topics triggers the flow
- All three topic values must be available (stored in topicValues)
- Text input "location" is always available
- Output publishes average to MQTT

---

## 11. Key Benefits

✓ **Leverages Existing Infrastructure** - Uses DeviceConfig storage, cluster management, GraphQL API

✓ **Flexible Input System** - Mix dynamic (topic) and static (text) inputs per flow instance

✓ **Explicit Connections** - Named ports make data flow clear and maintainable

✓ **Familiar Programming Model** - JavaScript like Node-RED

✓ **Visual Programming** - Low-code approach for rapid development

✓ **Type-Safe** - Input types prevent configuration errors

✓ **Scalable** - Distributed across cluster nodes

✓ **Extensible** - Easy to add new node types

✓ **Integrated** - Works seamlessly with MQTT broker and all existing features

✓ **Testable** - Built-in testing and debugging tools

---

## 12. Future Enhancements (Post-MVP)

- **More Input Types:**
  - `EXPRESSION` - Evaluate JavaScript expression for dynamic values
  - `VARIABLE` - Reference flow-wide variables
  - `STATE` - Reference persistent state values

- **Advanced Node Types:**
  - HTTP Request node
  - Database query node
  - Email notification node
  - Custom protocol handlers

- **Flow Orchestration:**
  - Sequential flow execution
  - Parallel execution branches
  - Loop constructs
  - Error recovery flows

- **Enhanced Debugging:**
  - Breakpoints
  - Step-through execution
  - Variable inspection
  - Performance profiling

- **Collaboration Features:**
  - Flow sharing
  - Version control integration (Git)
  - Comments and annotations
  - Team permissions

---

This plan is now ready for implementation with the improved connection model and flexible input type system!
