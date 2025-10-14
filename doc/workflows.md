# Workflows (Flow Engine)

MonsterMQ includes a powerful flow-based programming engine that enables visual data processing and transformation pipelines. The workflow system allows you to create reusable processing logic that can be deployed and configured across multiple instances.

## Overview

The workflow system provides:
- **Visual Flow Editor** - Drag-and-drop interface for creating data processing flows
- **JavaScript Runtime** - Execute custom logic using GraalVM JavaScript engine
- **Flow Classes** - Reusable flow templates/blueprints
- **Flow Instances** - Deployable instances with custom configuration
- **Real-time Processing** - Process MQTT messages as they arrive
- **Node-based Architecture** - Connect function nodes to create complex pipelines

## Architecture

```
┌─────────────────┐
│   MQTT Broker   │
│                 │
│  ┌───────────┐  │
│  │   Topic   │  │
│  │   Input   │  │
│  └─────┬─────┘  │
│        │        │
│  ┌─────▼─────┐  │
│  │   Flow    │  │
│  │ Instance  │  │
│  │           │  │
│  │  Node 1   │  │
│  │    ↓      │  │
│  │  Node 2   │  │
│  │    ↓      │  │
│  │  Node 3   │  │
│  └─────┬─────┘  │
│        │        │
│  ┌─────▼─────┐  │
│  │   Topic   │  │
│  │  Output   │  │
│  └───────────┘  │
│                 │
└─────────────────┘
```

## Key Concepts

### Flow Classes

A **Flow Class** is a reusable template that defines:
- **Nodes**: Individual processing units with inputs, outputs, and JavaScript code
- **Connections**: Links between node outputs and inputs
- **Structure**: The blueprint for data flow

Think of a Flow Class as a function definition - it describes the logic but doesn't execute until instantiated.

**Example**: A "TemperatureConverter" flow class that converts Celsius to Fahrenheit.

### Flow Instances

A **Flow Instance** is a deployed, running instance of a Flow Class with:
- **Input Mappings**: Connect MQTT topics or constants to node inputs
- **Output Mappings**: Publish node outputs to MQTT topics
- **Variables**: Instance-specific configuration values
- **Runtime State**: Execution statistics and current status

Think of a Flow Instance as a function call - it uses the Flow Class logic with specific input/output configuration.

**Example**: "warehouse-temp-converter" instance that subscribes to "sensors/warehouse/temp" and publishes to "sensors/warehouse/temp/fahrenheit".

### Nodes

Nodes are the building blocks of flows. Each node has:
- **Unique ID**: Identifier within the flow
- **Inputs**: Named input ports that receive data
- **Outputs**: Named output ports that send data
- **Script**: JavaScript code that processes inputs and produces outputs
- **State**: Persistent data that survives across executions

### Connections

Connections link nodes together:
- Connect an output port of one node to an input port of another
- Data flows through connections automatically when nodes execute
- Multiple connections can originate from one output

### Input Mappings

Define how external data enters your flow:

**TOPIC Inputs** (Dynamic):
- Subscribe to MQTT topics
- Trigger flow execution when messages arrive
- Provide fresh data on each execution

**TEXT Inputs** (Static):
- Constant values
- Configuration strings
- Don't trigger execution (provide context only)

### Output Mappings

Define where flow results are published:
- Map node output ports to MQTT topics
- Automatically publish when nodes produce output
- Support multiple outputs per flow

### Variables

Flow-wide configuration available to all nodes:
- **Read-only** in scripts (set via UI)
- **Instance-specific** (different per instance)
- **Use cases**: API keys, thresholds, URLs, feature flags

## Quick Start

### 1. Create a Flow Class

Navigate to **Workflows** page and click **"+ New Flow Class"**.

Define your flow structure:
1. **Add nodes** with inputs and outputs
2. **Write JavaScript** for each node
3. **Connect nodes** to create data flow
4. **Save** your flow class

### 2. Create a Flow Instance

Click **"+ New Flow Instance"** to deploy your flow.

Configure the instance:
1. **Select Flow Class** to instantiate
2. **Map Inputs** to MQTT topics or constants
3. **Map Outputs** to MQTT topics
4. **Set Variables** for instance-specific config
5. **Enable** and save

### 3. Test Your Flow

Publish a message to your input topic:
```bash
mosquitto_pub -t "sensors/temperature" -m "25"
```

Monitor the output topic:
```bash
mosquitto_sub -t "sensors/temperature/converted"
```

## Visual Flow Editor

The visual editor provides a drag-and-drop interface:

### Canvas Controls
- **Pan**: Click and drag the canvas background
- **Zoom**: Mouse wheel or use the +/- buttons
- **Add Node**: Drag from the palette or double-click canvas
- **Move Node**: Click and drag nodes
- **Select**: Click nodes to select, click background to deselect

### Creating Connections
Two methods:
1. **Visual**: Click an output port, then click an input port
2. **Quick Connect**: Use the form in the left panel

### Node Editor
Select a node to edit its properties:
- **ID**: Unique node identifier
- **Name**: Display name
- **Inputs**: Comma-separated input port names
- **Outputs**: Comma-separated output port names
- **Script**: JavaScript processing logic

## Script API

Nodes execute JavaScript using GraalVM. The following globals are available in your scripts:

### Inputs Object

Access input values:
```javascript
// inputs.<portName>.value - The actual data value
// inputs.<portName>.type - "topic" or "text"
// inputs.<portName>.timestamp - When the data was received
// inputs.<portName>.topic - Source topic (for topic inputs)

let temp = inputs.temperature.value;
let threshold = inputs.threshold.value;

console.log("Temperature:", temp, "from", inputs.temperature.topic);
console.log("Threshold:", threshold);
```

### Message Object (msg)

Shorthand for the triggering input:
```javascript
// msg.value - The triggering message value
// msg.timestamp - When it was received
// msg.topic - Source topic

console.log("Received:", msg.value, "from", msg.topic);
```

### Outputs Object

Send data to output ports:
```javascript
// outputs.send(portName, value)

outputs.send("out", 42);
outputs.send("result", { status: "ok", value: temp });
outputs.send("alert", "Temperature too high!");
```

### State Object

Persistent node-level storage:
```javascript
// Initialize state on first run
if (state.count === undefined) {
    state.count = 0;
}

// Update state
state.count++;
state.lastValue = msg.value;
state.history = state.history || [];
state.history.push(msg.value);

console.log("Execution count:", state.count);
console.log("Last value:", state.lastValue);
```

**Important**: State persists across executions but is stored in memory. It will reset if the flow is redeployed.

### Flow Variables

Read-only instance configuration:
```javascript
// Access variables set in the instance configuration
let apiKey = flow.apiKey;
let threshold = flow.threshold;
let serverUrl = flow.serverUrl;

console.log("Using threshold:", threshold);

if (msg.value > threshold) {
    outputs.send("alert", {
        message: "Threshold exceeded",
        url: serverUrl
    });
}
```

### Console Object

Logging functions:
```javascript
console.log("Information message");
console.warn("Warning message");
console.error("Error message");

// Multiple arguments
console.log("Temperature:", temp, "Humidity:", humidity);
```

Logs appear in:
- Server logs (when monitoring broker)
- Flow execution logs (in dashboard)

## Complete Examples

### Example 1: Temperature Alert

**Flow Class: "TemperatureAlert"**

Node: "check_threshold"
- Inputs: `temp`, `threshold`
- Outputs: `normal`, `alert`
- Script:
```javascript
let temperature = inputs.temp.value;
let threshold = inputs.threshold.value;

if (temperature > threshold) {
    outputs.send("alert", {
        temperature: temperature,
        threshold: threshold,
        message: "Temperature exceeded threshold!"
    });
} else {
    outputs.send("normal", {
        temperature: temperature,
        status: "OK"
    });
}
```

**Flow Instance: "warehouse-temp-alert"**
- Input Mappings:
  - `check_threshold.temp` → TOPIC: `sensors/warehouse/temperature`
  - `check_threshold.threshold` → TEXT: `25`
- Output Mappings:
  - `check_threshold.alert` → `alerts/warehouse/temperature`
- Variables:
  - `location`: "Warehouse A"
  - `alertEmail`: "ops@example.com"

### Example 2: Data Aggregator

**Flow Class: "DataAggregator"**

Node: "accumulate"
- Inputs: `value`
- Outputs: `average`, `count`
- Script:
```javascript
// Initialize state
if (!state.values) {
    state.values = [];
}

// Add new value
let value = parseFloat(inputs.value.value);
state.values.push(value);

// Keep only last N values
let windowSize = parseInt(flow.windowSize || "10");
if (state.values.length > windowSize) {
    state.values.shift();
}

// Calculate average
let sum = state.values.reduce((a, b) => a + b, 0);
let average = sum / state.values.length;

outputs.send("average", average);
outputs.send("count", state.values.length);
```

**Flow Instance: "sensor-average"**
- Input Mappings:
  - `accumulate.value` → TOPIC: `sensors/raw/temperature`
- Output Mappings:
  - `accumulate.average` → `sensors/processed/temperature/avg`
- Variables:
  - `windowSize`: "20"

### Example 3: Multi-Node Pipeline

**Flow Class: "DataPipeline"**

Node 1: "parse"
- Inputs: `raw`
- Outputs: `json`
- Script:
```javascript
let raw = inputs.raw.value;
try {
    let parsed = JSON.parse(raw);
    outputs.send("json", parsed);
} catch (e) {
    console.error("Parse error:", e);
}
```

Node 2: "transform"
- Inputs: `data`
- Outputs: `transformed`
- Script:
```javascript
let data = inputs.data.value;
let result = {
    temperature: data.temp * 1.8 + 32,  // C to F
    humidity: data.hum,
    timestamp: new Date().toISOString(),
    location: flow.location
};
outputs.send("transformed", result);
```

Node 3: "filter"
- Inputs: `data`
- Outputs: `valid`, `invalid`
- Script:
```javascript
let data = inputs.data.value;
if (data.temperature > 0 && data.temperature < 150) {
    outputs.send("valid", data);
} else {
    outputs.send("invalid", data);
}
```

**Connections**:
- `parse.json` → `transform.data`
- `transform.transformed` → `filter.data`

## Best Practices

### Node Design
- **Single Responsibility**: Each node should do one thing well
- **Small Scripts**: Keep node scripts concise and focused
- **Error Handling**: Use try-catch blocks for robust execution
- **Logging**: Use console.log for debugging

### Flow Organization
- **Descriptive Names**: Use clear node IDs and names
- **Logical Flow**: Organize nodes left-to-right, top-to-bottom
- **Port Naming**: Use descriptive port names (not just "in"/"out")

### Variable Usage
- **Configuration**: Use variables for thresholds, URLs, API keys
- **Instance Reuse**: Design flows to be configurable via variables
- **Secrets**: Consider using variables for sensitive data

### State Management
- **Initialize**: Always initialize state properties before use
- **Cleanup**: Limit state size (e.g., rolling windows)
- **Testing**: Remember state persists during testing

### Performance
- **Topic Filters**: Use specific topics (not wildcards) when possible
- **Batch Processing**: Aggregate data when appropriate
- **Output Frequency**: Avoid excessive output messages

## GraphQL API

Workflows can be managed programmatically via GraphQL:

### Query Flow Classes
```graphql
query {
  flowClasses {
    name
    namespace
    version
    description
    nodes {
      id
      name
      inputs
      outputs
      language
    }
    connections {
      fromNode
      fromOutput
      toNode
      toInput
    }
  }
}
```

### Create Flow Class
```graphql
mutation {
  flow {
    createClass(input: {
      name: "MyFlow"
      namespace: "default"
      version: "1.0.0"
      description: "My custom flow"
      nodes: [{
        id: "node1"
        type: "function"
        name: "Process"
        inputs: ["in"]
        outputs: ["out"]
        language: "javascript"
        config: {
          script: "outputs.send('out', inputs.in.value * 2);"
        }
      }]
      connections: []
    }) {
      name
    }
  }
}
```

### Create Flow Instance
```graphql
mutation {
  flow {
    createInstance(input: {
      name: "MyInstance"
      namespace: "default"
      nodeId: "local"
      flowClassId: "MyFlow"
      enabled: true
      inputMappings: [{
        nodeInput: "node1.in"
        type: TOPIC
        value: "sensors/input"
      }]
      outputMappings: [{
        nodeOutput: "node1.out"
        topic: "sensors/output"
      }]
      variables: {
        threshold: "100"
        location: "Building A"
      }
    }) {
      name
    }
  }
}
```

## Troubleshooting

### Flow Not Executing
1. **Check Instance Status**: Verify instance is enabled
2. **Check Input Topics**: Ensure MQTT topics are correct
3. **Check Subscriptions**: Verify broker is receiving messages
4. **Check Logs**: Look for errors in server logs

### Script Errors
1. **Console Logging**: Add console.log statements
2. **Syntax Check**: Use the "Validate" button in script editor
3. **Error Messages**: Check flow status for error details
4. **Input Validation**: Verify inputs exist before accessing

### Missing Output
1. **Check Connections**: Verify node connections are correct
2. **Check Output Calls**: Ensure outputs.send() is called
3. **Check Mappings**: Verify output mappings are configured
4. **Check Topics**: Monitor MQTT topics with mosquitto_sub

### Performance Issues
1. **Simplify Scripts**: Break complex nodes into smaller ones
2. **Limit State**: Avoid storing large arrays in state
3. **Output Frequency**: Reduce output message rate if needed
4. **Topic Specificity**: Avoid wildcard subscriptions

## Limitations

- **JavaScript Only**: Currently only JavaScript is supported (Python planned)
- **In-Memory State**: Node state is not persisted to database
- **Single Node**: Flows run on a single node (no distributed execution)
- **No Async**: Scripts are synchronous (no async/await)

## Future Enhancements

Planned features:
- **Python Support**: Execute Python scripts using GraalPy
- **Persistent State**: Save node state to database
- **Scheduled Execution**: Trigger flows on schedules (not just topics)
- **Sub-flows**: Nest flows within flows
- **Debugging Tools**: Step-through debugger, breakpoints
- **Testing Framework**: Unit tests for nodes and flows
