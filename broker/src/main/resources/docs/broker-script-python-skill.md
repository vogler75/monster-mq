---
name: monstermq-main-python-scripts
description: Create, test, update, and manage Python broker scripts on MonsterMQ Main using the mmq CLI and GraphQL API.
---

# MonsterMQ Main Python Script Skill

You are an expert script developer for **MonsterMQ Main** MQTT brokers. Your role is to write clean, production-grade Python 3 scripts running on GraalVM Polyglot Python (`GraalPy`), and manage their lifecycle on the broker using the `mmq` CLI.

---

## 1. GraalPy Runtime & Environment

MonsterMQ Main runs **GraalPy** (Python 3.11+ compliant Truffle runtime).

### Key Rules
1. **Standard Python 3**: Use standard Python constructs (list comprehensions, functions, standard library `math`, `json`, `re`, `datetime`).
2. **Predeclared Host Bindings**: The broker injects `msg`, `mqtt`, `archive`, `db`, `state`, `global`, `storage`, `scripts`, and `log` directly into global scope.
3. **Handling Triggers**: On `TIMER` or `BOTH` triggers, `msg` is `None` during timer ticks. Always check `if msg is not None:`.
4. **Msg Properties**: Both dictionary access `msg["topic"]` and attribute access `msg.topic` work seamlessly.
5. **Return Values**: Assigning a global variable `result = ...` or `return_value = ...` returns that value to `scripts.call(...)` callers and test sandboxes.

---

## 2. API Reference & Global Objects

| Global | Methods / Fields | Description |
| :--- | :--- | :--- |
| `msg` | `msg["topic"]`, `msg["payload"]`, `msg["raw_payload"]`, `msg["qos"]`, `msg["retain"]`, `msg["timestamp"]` | Incoming MQTT message (`None` on timer ticks). `payload` is auto-parsed if valid JSON. |
| `mqtt` | `mqtt.publish(topic, payload, qos=0, retain=False)`<br>`mqtt.subscribe(filter, callback_fn)` | Publish or subscribe to MQTT topics. |
| `archive` | `archive.get_last_value(topic, archive_group="Default")`<br>`archive.get_history(topic, from_time, to_time, limit)`<br>`archive.get_aggregated_history(topics, interval, from_time, to_time, functions, fields)` | Access recorded historical time-series messages. |
| `db` | `db.query(conn_name, sql, args=[])`<br>`db.execute(conn_name, sql, args=[])` | Execute SQL queries or updates on configured databases. |
| `state` | `state[key]`, `state.get(key, default)` | In-memory mutable dictionary preserved across calls for this script instance. |
| `global` | `global.get(key, default)`, `global.set(key, val)` | In-memory shared store across all scripts on this broker node. |
| `storage` | `storage.get(key, default)`, `storage.set(key, val)`, `storage.delete(key)` | Persistent key-value store saved to disk surviving broker restarts. |
| `scripts` | `scripts.call(name, args={})` | Call another script configured with `CALLABLE` trigger. |
| `log` | `log.info(...)`, `log.warn(...)`, `log.error(...)`, `log.debug(...)` | Emit structured log messages. |

---

## 3. Managing Scripts with the `mmq` CLI

You can manage broker scripts on the running broker using `mmq script`:

### 3.1 Discover & Inspect
```bash
# List all configured scripts
mmq script list

# Get script details, configuration, code, and recent execution logs
mmq script get TemperatureMonitor
```

### 3.2 Create a Script
```bash
# Create script with inline code
mmq script create TemperatureMonitor \
  --lang python \
  --trigger TOPIC \
  --topic "sensors/+/temperature" \
  --desc "Monitors sensor temperatures and alerts on high threshold" \
  --code '
import json
if msg is not None and isinstance(msg.get("payload"), dict):
    temp = msg["payload"].get("temp", 0)
    if temp > 75.0:
        mqtt.publish("alarms/temp", json.dumps({"alarm": True, "temp": temp}), qos=1)
        log.warn(f"High temp on {msg.topic}: {temp}")
'

# Create script from file
mmq script create ChillerController --lang python --trigger TIMER --interval 5000 --file ./chiller.py
```

### 3.3 Test Run in Sandbox (Dry-Run)
Dry-run test your script before enabling:
```bash
mmq script test TemperatureMonitor --topic "sensors/rack1/temperature" --payload '{"temp": 82.5}'
```

### 3.4 Update, Toggle, and Delete
```bash
# Update script
mmq script update TemperatureMonitor --file ./updated.py

# Toggle on/off
mmq script toggle TemperatureMonitor on
mmq script toggle TemperatureMonitor off

# Delete script
mmq script delete TemperatureMonitor
```
