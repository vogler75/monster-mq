# MonsterMQ Main JavaScript Script Reference

The Broker Script Engine on MonsterMQ Main executes standalone scripts written in **modern JavaScript (ES2022)** running on the high-performance **GraalVM Polyglot JavaScript (GraalJS)** Truffle runtime.

---

## 1. Runtime & Execution Model

- **Engine**: GraalVM Polyglot JavaScript (`GraalJS`), compliant with ECMAScript 2022.
- **Object Model**: Full standard JavaScript features including `JSON`, `Math`, `RegExp`, `Date`, `Array`, `Map`, `Set`, `Promise`, template literals, destructuring, arrow functions.
- **Triggers**:
  - `TOPIC`: Triggered when an incoming MQTT message matches configured topic filter(s). Supports MQTT wildcards (`+`, `#`). Can optionally enable *Trigger on Change Only*.
  - `TIMER`: Triggered periodically at a fixed millisecond interval (e.g. `5000` ms). On timer ticks, `msg` is `null`.
  - `BOTH`: Responds to both incoming MQTT messages and periodic timer intervals.
  - `CALLABLE`: Invoked only when explicitly called by another script via `scripts.call(name, args)` or via the test sandbox.
- **Concurrency Modes**:
  - `SINGLETON` (Default): Serial execution queue guaranteeing that in-memory `state` is thread-safe.
  - `MULTI_INSTANCE`: Parallel worker execution across the thread pool for high-throughput stateless operations.

---

## 2. Global Bindings & API Reference

### 2.1 `msg` (Incoming MQTT Message)
Available during `TOPIC` or `BOTH` triggers. `null` during timer ticks or callable invocations without a message payload.

```javascript
if (msg !== null && msg !== undefined) {
    const topic = msg.topic;          // string: e.g. "sensors/chiller/temp"
    const payload = msg.payload;      // parsed JSON object/array, or string if not JSON
    const raw = msg.raw_payload;      // raw string payload
    const qos = msg.qos;              // integer: 0, 1, or 2
    const retain = msg.retain;        // boolean: true if retained
    const ts = msg.timestamp;         // timestamp
}
```

### 2.2 `mqtt` (Broker MQTT Operations)
Publish messages into the broker or subscribe dynamically to topics.

```javascript
// Publish message
// mqtt.publish(topic, payload, qos=0, retain=false)
mqtt.publish("alerts/temperature", JSON.stringify({ alarm: true, value: 85.4 }), 1, true);

// Subscribe dynamically with a callback function
mqtt.subscribe("cmd/reset/+", (subMsg) => {
    log.info("Received command on: " + subMsg.topic);
});
```

### 2.3 `archive` (Historical & Last-Value Queries)
Query retained or historical time-series data from archive groups.

```javascript
// Get latest recorded message for a topic
const lastVal = archive.get_last_value("sensors/ambient/temperature", "Default");
if (lastVal) {
    log.info("Previous value: " + JSON.stringify(lastVal.payload));
}

// Get historical messages
const history = archive.get_history("sensors/ambient/temperature", null, null, 50);

// Get aggregated history
const agg = archive.get_aggregated_history(["sensors/temp1"], "5m", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z", ["AVG", "MAX"]);
```

### 2.4 `db` (Database Connections)
Execute SQL queries or updates on configured database connections.

```javascript
// Query returning an array of row objects
const rows = db.query("PostgresStore", "SELECT id, setpoint, status FROM equipment WHERE active = $1", [true]);
for (const row of rows) {
    log.info("Equipment setpoint: " + row.setpoint);
}

// Execute INSERT / UPDATE / DELETE statements
const res = db.execute("PostgresStore", "UPDATE equipment SET last_seen = NOW() WHERE id = $1", [42]);
```

### 2.5 Scoped Storage (`state`, `global`, `storage`)

- **`state`**: Mutable JavaScript object private to this script instance, preserved in memory across invocations.
- **`global`**: Shared in-memory key-value store accessible across all scripts on the local broker node (`global.get(key, default)`, `global.set(key, val)`).
- **`storage`**: Persistent key-value store saved on disk, surviving broker restarts and script reloads (`storage.get(key, default)`, `storage.set(key, val)`, `storage.delete(key)`).

```javascript
// Retain counter in local state
state.count = (state.count || 0) + 1;

// Node-wide shared variable
global.set("active_shift", "Shift-A");
const shift = global.get("active_shift", "Default");

// Persistent on-disk storage
const lastRun = storage.get("last_calibration_time", null);
storage.set("last_calibration_time", new Date().toISOString());
storage.delete("temporary_flag");
```

### 2.6 `scripts.call` (Inter-Script Calls)
Invoke another registered script configured with trigger type `CALLABLE`.

```javascript
const result = scripts.call("CalculateFlowRate", { pressure: 4.2, diameter: 0.05 });
log.info("Calculated flow rate: " + result);
```

### 2.7 `log` and `console` (Logging)
Emit structured log entries captured in the script's recent execution buffer and broker system logs.

```javascript
log.info("Processing started");
log.warn("Value exceeded nominal range");
log.error("Failed to parse data");

// console also maps to logger
console.log("Debug details", payload);
```

### 2.8 JSON Handling
Standard JavaScript `JSON.stringify(...)` and `JSON.parse(...)` are fully supported.

---

## 3. Practical Recipes

### 3.1 Threshold Alert & Retained Alarm
```javascript
// Trigger: TOPIC (sensors/+/temp)
if (msg && typeof msg.payload === "object") {
    const temp = msg.payload.temperature || 0;
    const parts = msg.topic.split("/");
    const devId = parts[1] || "unknown";

    if (temp > 80.0) {
        const alarm = {
            device: devId,
            temperature: temp,
            status: "ALARM_HIGH",
            time: msg.timestamp
        };
        mqtt.publish(`alarms/${devId}`, JSON.stringify(alarm), 1, true);
        log.warn(`High temperature on ${devId}: ${temp}`);
    } else if (temp < 70.0) {
        mqtt.publish(`alarms/${devId}`, JSON.stringify({ device: devId, status: "OK" }), 1, true);
    }
}
```

### 3.2 Moving Average Filter
```javascript
// Trigger: TOPIC (meters/+/power)
if (msg) {
    try {
        const val = parseFloat(msg.raw_payload);
        const window = state.window || [];
        window.push(val);
        if (window.length > 10) {
            window.shift();
        }
        state.window = window;

        const sum = window.reduce((a, b) => a + b, 0);
        const avg = sum / window.length;
        mqtt.publish(`${msg.topic}/avg`, avg.toFixed(2), 0, true);
    } catch (e) {
        log.error("Failed to calculate moving average: " + e.message);
    }
}
```

### 3.3 Database Query on Periodic Timer
```javascript
// Trigger: TIMER (Interval: 10000 ms)
const rows = db.query("PostgresStore", "SELECT tag_name, tag_value FROM plant_tags WHERE modified > NOW() - INTERVAL '10 seconds'", []);
for (const row of rows) {
    mqtt.publish(`plant/tags/${row.tag_name}`, String(row.tag_value), 0, true);
}
log.info(`Synced ${rows.length} tags from database`);
```
