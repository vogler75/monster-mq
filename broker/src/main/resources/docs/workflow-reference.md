# MonsterMQ Workflow Engine Documentation

Complete guide to the MonsterMQ workflow engine - a visual, node-based programming environment for processing MQTT messages.

---

## Overview

The MonsterMQ workflow engine provides a visual, node-based programming environment for processing MQTT messages. Create reusable data transformation pipelines that execute JavaScript code in response to MQTT messages.

### Key Concepts

- **Flow Classes** - Reusable templates that define the processing logic
- **Flow Instances** - Deployed instances with specific input/output configuration
- **Nodes** - Individual processing units that execute JavaScript
- **Connections** - Links between nodes that define data flow
- **Variables** - Instance-specific configuration values

### Conceptual Model

A **Flow Class** is like a function definition - it describes what to do.
A **Flow Instance** is like calling that function - it actually does the work with specific inputs.

---

## Flow Classes

A Flow Class is a reusable template that defines your data processing logic. It contains nodes (processing steps) and connections (data flow) but doesn't specify which MQTT topics to use.

### What a Flow Class Contains

- **Nodes** - Processing units with inputs, outputs, and JavaScript code
- **Connections** - Links between node outputs and inputs
- **Metadata** - Name, namespace, version, description

### When to Create a Flow Class

Create a Flow Class when you have processing logic that you want to:

- Reuse with different MQTT topics
- Deploy multiple times with different configuration
- Version and maintain independently
- Share across different parts of your system

### Example Use Case

A flow class that converts Celsius to Fahrenheit. You can create multiple instances of this class to convert temperatures from different sensors without duplicating the conversion logic.

---

## Flow Instances

A Flow Instance is a deployed, running instance of a Flow Class. It specifies which MQTT topics to subscribe to, where to publish results, and any instance-specific configuration.

### What a Flow Instance Contains

- **Flow Class Reference** - Which template to use
- **Input Mappings** - Connect MQTT topics or constants to node inputs
- **Output Mappings** - Publish node outputs to MQTT topics
- **Variables** - Instance-specific configuration values
- **Enable/Disable** - Control whether the instance is running

### Instance Lifecycle

1. **Create** - Select a Flow Class and configure inputs/outputs
2. **Enable** - Start the instance and begin processing messages
3. **Running** - Process MQTT messages as they arrive
4. **Disable** - Stop processing (unsubscribe from topics)
5. **Update** - Change configuration (requires restart)
6. **Delete** - Remove the instance

### Multiple Instances

You can create multiple instances of the same Flow Class with different configurations. For example, a "TemperatureAlert" class can have instances for each room with different threshold values.

---

## Nodes

Nodes are the building blocks of flows. Each node has inputs, outputs, and JavaScript code that processes data.

### Node Components

- **ID** - Unique identifier within the flow (e.g., "node1", "temperature_check")
- **Name** - Display name for the node
- **Inputs** - Named input ports that receive data
- **Outputs** - Named output ports that send data
- **Script** - JavaScript code that processes inputs and produces outputs
- **Language** - Script language (currently only "javascript")

### Input and Output Ports

Define input and output ports as comma-separated names. These become available in your script via the `inputs` object and `outputs.send()` function.

**Example Configuration:**
- **Inputs:** `temp, threshold`
- **Outputs:** `alert, normal`

In your script:
- Access inputs as `inputs.temp.value`
- Send output with `outputs.send("alert", data)`

### Node Script Context

The node script is executed every time the node receives input. Your script has access to:

- `inputs` - All input values
- `msg` - The triggering message
- `outputs` - Object to send data to output ports
- `state` - Persistent node-level storage
- `flow` - Flow instance variables
- `console` - Logging functions

---

## Connections

Connections link nodes together by connecting an output port of one node to an input port of another. When a node sends data to an output port, all connected nodes receive that data on their input port.

### Creating Connections

In the visual editor:
1. Click an output port (right side of a node)
2. Click an input port on another node (left side)
3. A connection line appears showing the data flow

Or use the "Quick Connect" form in the left panel.

### Connection Rules

- One output can connect to multiple inputs
- One input can receive from multiple outputs (values merged)
- Connections cannot create circular loops (will cause infinite execution)
- Data flows automatically when `outputs.send()` is called

### Warning: Avoid Circular Loops

Do not create connections that form a loop (e.g., Node A → Node B → Node A). This will cause infinite execution and may crash your flow.

---

## Input Mappings

Input Mappings define how external data enters your flow. They connect node inputs to either MQTT topics or constant values.

### Input Types

#### TOPIC Inputs

Subscribe to MQTT topics and trigger flow execution when messages arrive. Each message on the topic causes the node to execute with fresh data.

- **Triggers execution** - Node runs when message arrives
- **Real-time data** - Always current values
- **Example:** `sensors/warehouse/temperature`

### Static Configuration Values

To provide static configuration values or constants to nodes, use **Variables** instead of input mappings. Variables are set at the instance level and are available to all nodes via the `flow` object.

### Configuring Input Mappings

1. Select which node input to map (format: `nodeId.inputName`)
2. Enter the MQTT topic (supports wildcards: # for multi-level, + for single-level)
3. Add more mappings as needed

### Example: Temperature Alert Inputs

**Input Mapping:**
- `node1.temperature` → TOPIC: `sensors/room1/temp`
  - *Triggers execution with each temperature reading*

**Threshold Configuration:**
- Set as a Variable named `threshold` with value `25`
- Access in script via `flow.threshold`

---

## Output Mappings

Output Mappings define where flow results are published. They connect node output ports to MQTT topics.

### How Output Mappings Work

When a node calls `outputs.send("portName", value)`, the system checks if that output port is mapped to an MQTT topic. If yes, it publishes the value to that topic.

### Configuring Output Mappings

1. Select which node output to map (format: `nodeId.outputName`)
2. Enter the MQTT topic to publish to
3. Add more mappings as needed

### Output Data Format

The system automatically converts output values to MQTT payloads:

- **Strings** - Published as-is
- **Numbers** - Converted to string
- **Objects** - Converted to JSON string
- **Arrays** - Converted to JSON string

### Example: Alert Output

**Output Mapping:**
- `node1.alert` → `alerts/temperature/room1`

When the node calls:
```javascript
outputs.send("alert", {temp: 30, status: "high"})
```

The system publishes this JSON to the alert topic.

---

## Variables

Variables are key-value pairs that provide instance-specific configuration. They're accessible to all nodes in the flow via the `flow` object in your scripts.

### Why Use Variables?

- **Reusability** - Same Flow Class, different configurations
- **Centralized Config** - Change values without editing node scripts
- **Instance-specific** - Different values for each instance
- **Clean Code** - Avoid hardcoding values in scripts

### Common Use Cases

- **Thresholds** - Alert limits, min/max values
- **API Keys** - External service credentials
- **URLs** - Endpoint addresses
- **Location Info** - Building, room, floor identifiers
- **Feature Flags** - Enable/disable optional behavior

### Accessing Variables in Scripts

```javascript
// Variables set in the UI:
// { "threshold": "25", "location": "Warehouse A", "alertEmail": "ops@example.com" }

let threshold = parseFloat(flow.threshold);
let location = flow.location;
let email = flow.alertEmail;

console.log("Using threshold:", threshold, "for", location);

if (inputs.temp.value > threshold) {
    outputs.send("alert", {
        temperature: inputs.temp.value,
        threshold: threshold,
        location: location,
        notifyEmail: email
    });
}
```

### Variables vs. State

- **Variables** are read-only, set via the UI, and shared across all nodes
- **State** is read-write, persists across executions, and specific to each node

### Important: Type Conversion

Variables are stored as strings. Use `parseFloat()` or `parseInt()` to convert numeric values.

---

## Script API Reference

Your node scripts execute in a JavaScript runtime with access to the following global objects and functions.

### inputs Object

Access input values from MQTT topics or connected nodes:

- `inputs.<portName>.value` - The actual data value
- `inputs.<portName>.type` - "topic"
- `inputs.<portName>.timestamp` - When the data was received (milliseconds)
- `inputs.<portName>.topic` - Source MQTT topic (for topic inputs)

### msg Object

Shorthand for the triggering input message:

- `msg.value` - The triggering message value
- `msg.timestamp` - When it was received
- `msg.topic` - Source MQTT topic

### outputs Object

Send data to output ports:

- `outputs.send(portName, value)` - Send value to the specified output port. Connected nodes will receive this value.

### state Object

Persistent node-level storage (survives across executions):

```javascript
// Initialize on first run
if (!state.count) {
    state.count = 0;
    state.values = [];
}

// Update state
state.count++;
state.values.push(msg.value);

// Keep only last 10 values
if (state.values.length > 10) {
    state.values.shift();
}
```

### flow Object

Read-only instance variables (set via UI):

```javascript
// Access variables
let threshold = parseFloat(flow.threshold);
let apiKey = flow.apiKey;
let location = flow.location;
```

### console Object

Logging functions:

- `console.log(...args)` - Log informational messages
- `console.warn(...args)` - Log warning messages
- `console.error(...args)` - Log error messages

---

## Code Examples

### Example 1: Simple Temperature Alert

```javascript
let temperature = parseFloat(inputs.temp.value);
let threshold = parseFloat(flow.threshold);

if (temperature > threshold) {
    outputs.send("alert", {
        temperature: temperature,
        threshold: threshold,
        location: flow.location,
        message: "Temperature exceeded threshold!"
    });
    console.warn("ALERT: Temperature", temperature, "exceeds", threshold);
} else {
    outputs.send("normal", {
        temperature: temperature,
        status: "OK"
    });
}
```

### Example 2: Rolling Average

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

console.log("Average:", average, "over", state.values.length, "values");

outputs.send("average", average);
outputs.send("count", state.values.length);
```

### Example 3: Data Validation

```javascript
let data = inputs.data.value;

// Validate input
if (typeof data === 'object' && data.temp !== undefined) {
    let temp = parseFloat(data.temp);

    // Range check
    if (temp >= -50 && temp <= 150) {
        outputs.send("valid", {
            temperature: temp,
            timestamp: new Date().toISOString(),
            source: msg.topic
        });
    } else {
        outputs.send("invalid", {
            reason: "Temperature out of range",
            value: temp,
            expected: "-50 to 150"
        });
        console.error("Invalid temperature:", temp);
    }
} else {
    outputs.send("invalid", {
        reason: "Invalid data format",
        received: data
    });
    console.error("Expected object with 'temp' property");
}
```

---

## Troubleshooting

### Flow Not Executing

- Check that the instance is **enabled**
- Verify input mappings use correct MQTT topics
- Confirm the broker is receiving messages on those topics
- Check server logs for errors
- Ensure at least one input is type TOPIC (to trigger execution)

### Script Errors

- Use `console.log()` to debug values
- Click "Validate" button in script editor to check syntax
- Check flow status for error messages
- Verify all inputs exist before accessing them
- Use try-catch blocks for robust error handling

### Missing Output

- Verify connections are correct in visual editor
- Ensure `outputs.send()` is called in script
- Check output mappings are configured
- Monitor MQTT topics with mosquitto_sub
- Check server logs for publishing errors

### Variables Not Working

- Remember variables are strings - use `parseFloat()` for numbers
- Check variable names match exactly (case-sensitive)
- Restart instance after changing variables
- Use `console.log(flow)` to see all variables

### Performance Issues

- Limit state size (avoid storing large arrays)
- Reduce output message frequency if possible
- Use specific topics instead of wildcards
- Break complex scripts into multiple simpler nodes

### Getting Help

Check the server logs for detailed error messages. Enable detailed logging with the `-log FINE` command line option.

---

## Best Practices

1. **Keep nodes focused** - Each node should do one thing well
2. **Use descriptive names** - Name nodes, ports, and variables clearly
3. **Test incrementally** - Start with simple flows and add complexity
4. **Log strategically** - Use console.log for debugging, but remove excessive logging in production
5. **Handle errors** - Use try-catch blocks for robust error handling
6. **Manage state carefully** - Clear old state data to prevent memory leaks
7. **Document your flows** - Use meaningful node names and add comments in scripts
8. **Version your classes** - Use semantic versioning for flow classes

---

*MonsterMQ Workflow Engine - Version 1.0*
