---
name: monstermq-main-javascript-scripts
description: Create, test, update, and manage JavaScript broker scripts on MonsterMQ Main using the mmq CLI and GraphQL API.
---

# MonsterMQ Main JavaScript Script Skill

You are an expert script developer for **MonsterMQ Main** MQTT brokers. Your role is to write clean, modern JavaScript (ES2022) scripts running on GraalVM Polyglot JavaScript (`GraalJS`), and manage their lifecycle on the broker using the `mmq` CLI.

---

## 1. GraalJS Runtime & Environment

MonsterMQ Main runs **GraalJS** (ECMAScript 2022 compliant Truffle runtime).

### Key Rules
1. **Modern JavaScript**: Full ES2022 support (arrow functions, template literals, optional chaining `?.`, nullish coalescing `??`, `Array`, `Map`, `Set`, `JSON`, `Math`).
2. **Predeclared Host Bindings**: The broker injects `msg`, `mqtt`, `archive`, `db`, `state`, `global`, `storage`, `scripts`, and `log` / `console`.
3. **Handling Triggers**: On `TIMER` or `BOTH` triggers, `msg` is `null` during timer ticks. Always check `if (msg !== null && msg !== undefined)`.
4. **Msg Properties**: Use property access: `msg.topic`, `msg.payload`, `msg.raw_payload`, `msg.qos`, `msg.retain`, `msg.timestamp`.
5. **Return Values**: Assigning `result = ...` or `return_value = ...` returns that value to `scripts.call(...)` callers and test sandboxes.

---

## 2. API Reference & Global Objects

| Global | Methods / Fields | Description |
| :--- | :--- | :--- |
| `msg` | `msg.topic`, `msg.payload`, `msg.raw_payload`, `msg.qos`, `msg.retain`, `msg.timestamp` | Incoming MQTT message (`null` on timer ticks). `payload` is auto-parsed if valid JSON. |
| `mqtt` | `mqtt.publish(topic, payload, qos, retain)`<br>`mqtt.subscribe(filter, callbackFn)` | Publish or subscribe to MQTT topics. |
| `archive` | `archive.get_last_value(topic, archiveGroup)`<br>`archive.get_history(topic, fromTime, toTime, limit)`<br>`archive.get_aggregated_history(topics, interval, fromTime, toTime, functions, fields)` | Access recorded historical time-series messages. |
| `db` | `db.query(connName, sql, args)`<br>`db.execute(connName, sql, args)` | Execute SQL queries or updates on configured databases. |
| `state` | `state[key]`, `state.key` | In-memory mutable object preserved across calls for this script instance. |
| `global` | `global.get(key, default)`, `global.set(key, val)` | In-memory shared store across all scripts on this broker node. |
| `storage` | `storage.get(key, default)`, `storage.set(key, val)`, `storage.delete(key)` | Persistent key-value store saved to disk surviving broker restarts. |
| `scripts` | `scripts.call(name, args)` | Call another script configured with `CALLABLE` trigger. |
| `log` / `console` | `log.info(...)`, `log.warn(...)`, `log.error(...)`, `console.log(...)` | Emit structured log messages. |

---

## 3. Managing Scripts with the `mmq` CLI

You can manage broker scripts on the running broker using `mmq script`:

### 3.1 Discover & Inspect
```bash
# List all configured scripts
mmq script list

# Get script details, configuration, code, and recent execution logs
mmq script get ChillerLogic
```

### 3.2 Create a Script
```bash
# Create script with inline code
mmq script create ChillerLogic \
  --lang javascript \
  --trigger TOPIC \
  --topic "sensors/+/temperature" \
  --desc "Calculates cooling demand from temperature sensors" \
  --code '
if (msg && typeof msg.payload === "object") {
    const temp = msg.payload.temp || 0;
    if (temp > 75.0) {
        mqtt.publish("chiller/control", JSON.stringify({ power: "HIGH", temp }), 1, false);
        log.warn("High temp detected: " + temp);
    }
}
'

# Create script from file
mmq script create MeterSync --lang javascript --trigger TIMER --interval 5000 --file ./meter.js
```

### 3.3 Test Run in Sandbox (Dry-Run)
```bash
mmq script test ChillerLogic --topic "sensors/chiller1/temperature" --payload '{"temp": 82.5}'
```

### 3.4 Update, Toggle, and Delete
```bash
# Update script
mmq script update ChillerLogic --file ./updated.js

# Toggle on/off
mmq script toggle ChillerLogic on
mmq script toggle ChillerLogic off

# Delete script
mmq script delete ChillerLogic
```
