# MonsterMQ Main Python Script Reference

The Broker Script Engine on MonsterMQ Main executes standalone scripts written in **Python 3** running on the high-performance **GraalVM Polyglot Python (GraalPy)** Truffle runtime. Scripts execute standalone directly inside the broker JVM process.

---

## 1. Runtime & Execution Model

- **Engine**: GraalVM Polyglot Python (`GraalPy`), compliant with Python 3.11+.
- **Standard Library**: Fast JIT compilation with standard library modules such as `math`, `json`, `re`, `datetime`, etc.
- **Triggers**:
  - `TOPIC`: Triggered when an incoming MQTT message matches configured topic filter(s). Supports MQTT wildcards (`+`, `#`). Can optionally enable *Trigger on Change Only* to skip duplicate consecutive payloads.
  - `TIMER`: Triggered periodically at a fixed millisecond interval (e.g. `5000` ms). On timer ticks, `msg` is `None`.
  - `BOTH`: Responds to both incoming MQTT messages and periodic timer intervals.
  - `CALLABLE`: Invoked only when explicitly called by another script via `scripts.call(name, args)` or via the test sandbox.
- **Concurrency Modes**:
  - `SINGLETON` (Default): Sequential execution in a dedicated FIFO queue for this script instance. Guarantees that in-memory `state` is free of race conditions.
  - `MULTI_INSTANCE`: Parallel worker execution across the broker thread pool for high-throughput, stateless workloads.

---

## 2. Global Bindings & API Reference

### 2.1 `msg` (Incoming MQTT Message)
Available during `TOPIC` or `BOTH` triggers. `None` during timer ticks or callable invocations without a message payload.

```python
if msg is not None:
    topic = msg["topic"]             # string: e.g. "sensors/chiller/temp" (also msg.topic)
    payload = msg["payload"]         # parsed JSON dict/list, or string if not JSON
    raw = msg["raw_payload"]         # raw string payload
    qos = msg["qos"]                 # integer: 0, 1, or 2
    retain = msg["retain"]           # boolean: True if retained
    ts = msg["timestamp"]            # integer or string: timestamp
```

### 2.2 `mqtt` (Broker MQTT Operations)
Publish messages directly into the broker or subscribe dynamically to topics.

```python
# Publish message
# mqtt.publish(topic, payload, qos=0, retain=False)
mqtt.publish("alerts/temperature", json.dumps({"alarm": True, "value": 85.4}), qos=1, retain=True)

# Subscribe dynamically with a callback function
def on_message(sub_msg):
    log.info("Received message on: " + sub_msg["topic"])

mqtt.subscribe("cmd/reset/+", on_message)
```

### 2.3 `archive` (Historical & Last-Value Queries)
Query retained or historical time-series data from archive groups.

```python
# Get latest recorded message for a specific topic
last_val = archive.get_last_value("sensors/ambient/temperature", archive_group="Default")
if last_val is not None:
    log.info("Previous value: " + str(last_val["payload"]))

# Get historical messages (from_time, to_time in ISO-8601 strings)
records = archive.get_history("sensors/ambient/temperature", limit=50)

# Get aggregated history
# archive.get_aggregated_history(topics, interval, from_time, to_time, functions, fields, archive_group)
agg = archive.get_aggregated_history(["sensors/temp1"], "5m", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z", ["AVG", "MAX"])
```

### 2.4 `db` (Database Connections)
Execute SQL queries or updates on configured database connections (PostgreSQL, SQLite, CrateDB, etc.).

```python
# Query returning a list of row dictionaries
rows = db.query("PostgresStore", "SELECT id, setpoint, status FROM equipment WHERE active = $1", [True])
for row in rows:
    log.info("Equipment setpoint: " + str(row["setpoint"]))

# Execute INSERT / UPDATE / DELETE statements
# Returns dict: {"affected_rows": int, "success": bool}
res = db.execute("PostgresStore", "UPDATE equipment SET last_seen = NOW() WHERE id = $1", [42])
```

### 2.5 Scoped Storage (`state`, `global`, `storage`)

- **`state`**: Mutable dictionary private to this script instance, preserved in memory across invocations.
- **`global`**: Shared in-memory key-value store accessible across all scripts on the local broker node (`global.get(key, default)`, `global.set(key, val)`).
- **`storage`**: Persistent key-value store saved on disk, surviving broker restarts and script reloads (`storage.get(key, default)`, `storage.set(key, val)`, `storage.delete(key)`).

```python
# Retain counter in local state
state["count"] = state.get("count", 0) + 1

# Node-wide shared variable
global.set("active_shift", "Shift-A")
current_shift = global.get("active_shift", "Default")

# Persistent on-disk storage
last_run = storage.get("last_calibration_time", None)
storage.set("last_calibration_time", "2026-09-19T18:00:00Z")
storage.delete("temporary_flag")
```

### 2.6 `scripts.call` (Inter-Script Calls)
Invoke another registered script configured with trigger type `CALLABLE`.

```python
result = scripts.call("CalculateFlowRate", {"pressure": 4.2, "diameter": 0.05})
log.info("Calculated flow rate: " + str(result))
```

### 2.7 `log` (System Logging)
Emit structured log entries captured in the script's recent execution buffer and broker system logs.

```python
log.info("Processing completed successfully")
log.warn("Value exceeded nominal range")
log.error("Failed to parse data: " + str(err))
log.debug("Debug payload details")
```

### 2.8 `json` (Serialization & Deserialization)
Both Python standard `import json` and predeclared helper `json.encode` / `json.decode` can be used.

```python
import json
encoded = json.dumps({"temp": 22.4, "active": True})
data = json.loads(encoded)
```

---

## 3. Practical Recipes

### 3.1 Threshold Alert & Retained Alarm
```python
# Trigger: TOPIC (sensors/+/temp)
if msg is not None and isinstance(msg.get("payload"), dict):
    temp = msg["payload"].get("temperature", 0)
    parts = msg["topic"].split("/")
    dev_id = parts[1] if len(parts) > 1 else "unknown"

    if temp > 80.0:
        alarm_msg = {
            "device": dev_id,
            "temperature": temp,
            "status": "ALARM_HIGH",
            "time": msg.get("timestamp")
        }
        mqtt.publish("alarms/" + dev_id, json.dumps(alarm_msg), qos=1, retain=True)
        log.warn("High temperature on " + dev_id + ": " + str(temp))
    elif temp < 70.0:
        mqtt.publish("alarms/" + dev_id, json.dumps({"device": dev_id, "status": "OK"}), qos=1, retain=True)
```

### 3.2 Moving Average Filter
```python
# Trigger: TOPIC (meters/+/power)
if msg is not None:
    try:
        val = float(msg.get("raw_payload", 0))
        window = state.get("window", [])
        window.append(val)
        if len(window) > 10:
            window.pop(0)
        state["window"] = window

        avg_val = sum(window) / len(window)
        mqtt.publish(msg["topic"] + "/avg", str(round(avg_val, 2)), retain=True)
    except Exception as e:
        log.error("Failed to calculate moving average: " + str(e))
```

### 3.3 Database Query on Periodic Timer
```python
# Trigger: TIMER (Interval: 10000 ms)
rows = db.query("PostgresStore", "SELECT tag_name, tag_value FROM plant_tags WHERE modified > NOW() - INTERVAL '10 seconds'", [])
for row in rows:
    topic = "plant/tags/" + row["tag_name"]
    mqtt.publish(topic, str(row["tag_value"]), qos=0, retain=True)
log.info("Synced " + str(len(rows)) + " tags from database")
```
