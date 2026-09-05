# MQTT v5.0

MonsterMQ accepts MQTT v5.0 and MQTT v3.1.1 clients on the same listeners. This page distinguishes implemented behavior from current limitations.

## Overview

MQTT v5.0 introduces significant enhancements over MQTT v3.1.1, including better error reporting, enhanced property support, improved flow control, and new features for modern IoT applications. The protocol is defined in the [MQTT v5.0 Specification](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html).

## Configuration

Both protocol versions share the MQTT listeners; no separate v5 switch is required.

```yaml
MqttTcpServer:
  NoDelay: true
  ReceiveBufferSizeKb: 512
  SendBufferSizeKb: 512
  MaxMessageSizeKb: 512
  ServerReceiveMaximum: 100
```

The former top-level `Mqtt` capability block and `UserManagement.EnhancedAuth`
examples are not supported configuration. Most CONNACK capabilities below are
currently hardcoded in `MqttClient.kt`.

## Backward Compatibility

MonsterMQ maintains full backward compatibility with MQTT v3.1.1 clients:

- MQTT v3.1.1 and v5.0 clients can coexist on the same broker
- Protocol version auto-detected from CONNECT packet
- v3.1.1 clients receive standard QoS values instead of reason codes
- v5.0 properties ignored/stripped when forwarding to v3.1.1 clients
- Retained messages with v5 properties work correctly with v3.1.1 subscribers

---

## Connection Properties

MQTT v5.0 connection properties enable clients and servers to negotiate capabilities and session parameters.

**Supported Properties:**
- **Session Expiry Interval** (17) — Control session persistence duration
- **Receive Maximum** (33) — Flow control for QoS 1/2 messages (default: 100, max: 65535)
- **Maximum Packet Size** (39) — Limit packet sizes (default: 268435455 bytes)
- **Topic Alias Maximum** (34) — Server announces support for topic aliases (default: 10)
- **Request Response Information** (25) — Logged from CONNECT; the broker does not advertise a generated Response Information value

**Server-Side CONNACK Properties:**

The server advertises capabilities and limits to clients during connection:

- **Session Expiry Interval** (17) — Echoed from client or server default
- **Receive Maximum** (33) — `MqttTcpServer.ServerReceiveMaximum` (default 100)
- **Maximum QoS** (36) — Omitted, allowing clients to use QoS 2
- **Retain Available** (37) — Retained message support (1 = yes)
- **Maximum Packet Size** (39) — Currently advertised as 268435455 bytes; the listener separately enforces `MaxMessageSizeKb`, so this advertisement is not the effective inbound limit
- **Topic Alias Maximum** (34) — Number of aliases server supports (10)
- **Wildcard Subscription Available** (40) — Wildcard support (1 = yes)
- **Subscription Identifier Available** (41) — Sub ID support (0 = no)
- **Shared Subscription Available** (42) — Shared sub support (0 = no)
- **Server Keep Alive** (19) — Echoes the endpoint keep-alive value when greater than zero

---

## Reason Codes

MQTT v5.0 introduces detailed reason codes for all acknowledgment packets, enabling better error diagnosis.

**Supported ACK Packets:**
- **CONNACK** — Connection result codes
- **PUBACK** — Publish acknowledgment (QoS 1)
- **PUBREC/PUBREL/PUBCOMP** — QoS 2 acknowledgments
- **SUBACK** — Subscription acknowledgment with per-topic reason codes
- **UNSUBACK** — Unsubscription acknowledgment

**Common Reason Codes:**
- `0x00` — SUCCESS
- `0x01` — Granted QoS 1
- `0x02` — Granted QoS 2
- `0x87` — NOT_AUTHORIZED
- `0x8F` — TOPIC_FILTER_INVALID
- `0x97` — QUOTA_EXCEEDED

---

## Message Properties

Rich metadata can be attached to published messages for routing, content negotiation, and correlation.

**Supported Properties:**
- **Payload Format Indicator** (1) — UTF-8 text (1) vs binary (0); broker validates UTF-8 when indicator = 1
- **Message Expiry Interval** (2) — Time-to-live in seconds
- **Content Type** (3) — MIME type (e.g., `application/json`)
- **Response Topic** (8) — Topic for request/response pattern
- **Correlation Data** (9) — Binary correlation identifier
- **User Properties** (38) — Custom key-value pairs (bounded by packet size)

---

## Topic Aliases

Topic aliases reduce bandwidth by replacing long topic names with 2-byte integers.

1. Server announces Topic Alias Maximum (10) in CONNACK
2. Client establishes alias: `{"topic": "building/floor3/room42/sensor/temperature", "alias": 5}`
3. Subsequent publishes use alias only: `{"alias": 5}` (saves ~40 bytes per message)
4. Aliases are session-specific and cleared on disconnect

Savings depend on topic and payload sizes. Alias state also stores the associated topic strings and is scoped to the connection.

---

## Message Expiry Interval

Prevents stale messages from being delivered to offline clients by setting a time-to-live.

- Messages stored with creation timestamp and expiry interval
- Broker automatically filters expired messages from queues
- Expiry interval decremented when forwarding (per MQTT v5 spec)
- Background cleanup task purges expired messages every 60 seconds

**Database Support:** SQLite, PostgreSQL, MongoDB for persistent session queues

---

## Flow Control (Receive Maximum)

Prevents overwhelming clients with too many in-flight QoS 1/2 messages.

- Server enforces Receive Maximum (default: 100, configurable)
- Tracks in-flight messages per client session
- Pauses publishing when limit reached
- Resumes when acknowledgments free capacity

---

## Subscription Options

Advanced subscription options for fine-grained message filtering and handling.

**No Local (NL):**
Prevents receiving messages published by the same client. Useful for preventing echo in pub/sub patterns.

**Retain Handling (RH):**
Controls retained message delivery on new subscriptions:
- `0` — Send retained messages (default)
- `1` — Send retained only if new subscription
- `2` — Never send retained messages

**Retain As Published (RAP):**
Preserves the RETAIN flag when forwarding messages. When disabled, the RETAIN flag is always cleared (default MQTT v3.1.1 behavior).

---

## Request/Response Pattern

MQTT v5.0 introduces built-in support for the request/response pattern through two properties:
- **Response Topic** (Property 8) — Where the requester expects the response
- **Correlation Data** (Property 9) — Token to correlate request with response

### How It Works

1. **Requester** sends a message with `Response Topic` and `Correlation Data`
2. **Responder** receives message, processes it, and publishes response to the `Response Topic` with the same `Correlation Data`
3. **Requester** receives response and matches `Correlation Data` to identify which request it belongs to

The application must subscribe to its response topic before publishing a request,
copy the correlation bytes into the response, and apply an application timeout.
The broker forwards these properties; it does not execute the requested service.

## Current limitations

- **Enhanced authentication:** CONNECT authentication method/data are parsed and logged, but there is no complete AUTH packet challenge-response exchange. Use the supported username/password or certificate authentication described in [Security](security.md). SCRAM is not an available end-to-end authentication mode.
- **Will delay:** delay scheduling exists, but the current CONNECT handling does not obtain the separate Will Properties correctly through the MQTT API. Do not rely on delayed wills or assume a dependency upgrade automatically completes support.
- **Subscription identifiers and shared subscriptions:** CONNACK advertises both as unavailable.
- **Capability negotiation:** the packet size advertisement differs from the configured listener limit, as described above.

These limits mean that this guide is not a claim of complete MQTT v5 conformance.
Session protocol and message properties are also exposed in parts of the dashboard
and [GraphQL API](graphql.md); UI coverage varies by feature.

## Client Examples

### Python (paho-mqtt)

```python
import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.subscribeoptions import SubscribeOptions

TOPIC = "sensor/example"

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        raise RuntimeError(str(reason_code))
    client.subscribe(TOPIC, options=SubscribeOptions(qos=1, retainAsPublished=True))

def on_subscribe(client, userdata, mid, reason_codes, properties):
    if any(code.is_failure for code in reason_codes):
        raise RuntimeError("Subscription denied")
    props = Properties(PacketTypes.PUBLISH)
    props.MessageExpiryInterval = 60
    props.ContentType = "application/json"
    props.UserProperty = [("sensor", "example")]
    client.publish(TOPIC, '{"value": 23.5}', qos=1, properties=props)

def on_message(client, userdata, message):
    print(message.topic, message.payload.decode())
    print(getattr(message.properties, "ContentType", None))

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                     client_id="mqtt5-example", protocol=mqtt.MQTTv5)
# If required: client.username_pw_set("your-user", "your-password")
client.on_connect = on_connect
client.on_subscribe = on_subscribe
client.on_message = on_message
props = Properties(PacketTypes.CONNECT)
props.SessionExpiryInterval = 300
props.ReceiveMaximum = 100
client.connect("localhost", 1883, properties=props)
try:
    client.loop_forever()  # Ctrl+C to stop after observing the example message.
except KeyboardInterrupt:
    client.disconnect()
```

### JavaScript (MQTT.js)

```javascript
const mqtt = require('mqtt');

const client = mqtt.connect('mqtt://localhost:1883', {
  protocolVersion: 5,
  properties: {
    sessionExpiryInterval: 300,
    receiveMaximum: 100
  }
});

client.on('connect', () => {
  client.subscribe('sensor/data', { qos: 1, nl: false, rap: true, rh: 0 });
});

client.on('message', (topic, payload, packet) => {
  console.log(`Topic: ${topic}, Payload: ${payload.toString()}`);
  if (packet.properties) {
    console.log(`Content Type: ${packet.properties.contentType}`);
  }
});

client.publish('sensor/temperature', '{"value": 23.5}', {
  qos: 1,
  properties: {
    messageExpiryInterval: 3600,
    contentType: 'application/json',
    userProperties: { sensor: 'DHT22', location: 'room-5' }
  }
});
```

---

## Testing

Integration tests exercise implemented MQTT v5 behavior against a running broker. Check failures and skips; their presence is not a conformance certification:

```bash
cd tests
pytest pytest_tests/mqtt5/ -v              # Run all MQTT v5 tests
pytest pytest_tests/mqtt5/ -k "connection" # Run connection tests only
```

Tests live in [`tests/pytest_tests/mqtt5/`](../tests/pytest_tests/mqtt5/). See
[the installation guide](installation.md) for dependencies and broker credentials.

## References

- [MQTT v5.0 Specification](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
- [Vert.x MQTT](https://vertx.io/docs/vertx-mqtt/java/)
- [paho-mqtt Documentation](https://eclipse.dev/paho/files/paho.mqtt.python/html/client.html)
