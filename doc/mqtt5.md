# MQTT v5.0

MonsterMQ implements comprehensive MQTT v5.0 protocol support while maintaining full backward compatibility with MQTT v3.1.1.

## Overview

MQTT v5.0 introduces significant enhancements over MQTT v3.1.1, including better error reporting, enhanced property support, improved flow control, and new features for modern IoT applications. MonsterMQ supports all major MQTT v5.0 features outlined in the [MQTT v5.0 Specification](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html).

## Configuration

MQTT v5.0 is enabled by default. Configure server-side limits in `config.yaml`:

```yaml
MqttTcpServer:
  NoDelay: true
  ReceiveBufferSizeKb: 512
  SendBufferSizeKb: 512
  MaxMessageSizeKb: 512

Mqtt:
  ServerKeepAlive: 60
  ReceiveMaximum: 100
  MaximumQoS: 2
  RetainAvailable: true
  MaximumPacketSize: 268435455
  TopicAliasMaximum: 10
  WildcardSubscriptionAvailable: true
  SharedSubscriptionAvailable: true

# Enhanced Authentication (optional)
UserManagement:
  Enabled: false
  EnhancedAuth:
    Enabled: false
    Methods:
      - SCRAM-SHA-256
```

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
- **Request/Response Information** (25) — Enable response topic patterns

**Server-Side CONNACK Properties:**

The server advertises capabilities and limits to clients during connection:

- **Session Expiry Interval** (17) — Echoed from client or server default
- **Receive Maximum** (33) — Server's flow control limit (100)
- **Maximum QoS** (36) — Highest supported QoS level (2)
- **Retain Available** (37) — Retained message support (1 = yes)
- **Maximum Packet Size** (39) — Server's packet size limit
- **Topic Alias Maximum** (34) — Number of aliases server supports (10)
- **Wildcard Subscription Available** (40) — Wildcard support (1 = yes)
- **Subscription Identifier Available** (41) — Sub ID support (0 = no)
- **Shared Subscription Available** (42) — Shared sub support (1 = yes)
- **Server Keep Alive** (19) — Server's keep-alive preference (60s)

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
- **User Properties** (38) — Custom key-value pairs (unlimited)

---

## Topic Aliases

Topic aliases reduce bandwidth by replacing long topic names with 2-byte integers.

1. Server announces Topic Alias Maximum (10) in CONNACK
2. Client establishes alias: `{"topic": "building/floor3/room42/sensor/temperature", "alias": 5}`
3. Subsequent publishes use alias only: `{"alias": 5}` (saves ~40 bytes per message)
4. Aliases are session-specific and cleared on disconnect

**Performance:** ~90% bandwidth savings for long topic names with minimal memory overhead (10 aliases x 2 bytes per session).

---

## Message Expiry Interval

Prevents stale messages from being delivered to offline clients by setting a time-to-live.

- Messages stored with creation timestamp and expiry interval
- Broker automatically filters expired messages from queues
- Expiry interval decremented when forwarding (per MQTT v5 spec)
- Background cleanup task purges expired messages every 60 seconds

**Database Support:** SQLite, PostgreSQL, CrateDB, MongoDB

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

### Example: Requester

```python
import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
import uuid

client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id="requester_client",
    protocol=mqtt.MQTTv5
)

RESPONSE_TOPIC = f"response/{client._client_id.decode()}"
pending_requests = {}

def on_connect(client, userdata, flags, reason_code, properties):
    client.subscribe(RESPONSE_TOPIC)

def on_message(client, userdata, msg):
    if hasattr(msg, 'properties') and msg.properties:
        correlation_data = msg.properties.CorrelationData
        if correlation_data:
            correlation_id = correlation_data.decode('utf-8')
            if correlation_id in pending_requests:
                print(f"Response for {correlation_id}: {msg.payload.decode()}")
                del pending_requests[correlation_id]

client.on_connect = on_connect
client.on_message = on_message
client.connect("localhost", 1883)
client.loop_start()

# Send request
correlation_id = str(uuid.uuid4())
props = Properties(PacketTypes.PUBLISH)
props.ResponseTopic = RESPONSE_TOPIC
props.CorrelationData = correlation_id.encode('utf-8')
props.ContentType = "text/plain"
client.publish("service/request", "query:Vienna", qos=1, properties=props)
pending_requests[correlation_id] = True
```

### Example: Responder

```python
def on_message(client, userdata, msg):
    if hasattr(msg, 'properties') and msg.properties:
        response_topic = msg.properties.ResponseTopic
        correlation_data = msg.properties.CorrelationData
        if response_topic:
            response_props = Properties(PacketTypes.PUBLISH)
            if correlation_data:
                response_props.CorrelationData = correlation_data
            response_props.ContentType = "text/plain"
            client.publish(response_topic, "Temperature: 22.5C", qos=1, properties=response_props)
```

### Best Practices

- **Unique response topics:** Use `response/{client_id}` to avoid receiving other clients' responses
- **Correlation data:** Use UUIDs for unique identifiers
- **Timeouts:** Implement request timeouts to handle unresponsive services
- **QoS:** Use QoS 1 minimum for both requests and responses to ensure delivery
- **Content Type:** Specify content types (`application/json`, `text/plain`) for interoperability

---

## Enhanced Authentication (SCRAM-SHA-256)

Pluggable authentication architecture supporting challenge-response mechanisms beyond basic username/password.

### Architecture

```kotlin
interface EnhancedAuthProvider {
    fun getAuthMethod(): String
    fun startAuth(clientId: String, authData: ByteArray?): Future<AuthResult>
    fun continueAuth(clientId: String, authData: ByteArray?): Future<AuthResult>
    fun cleanupAuth(clientId: String)
}
```

### SCRAM-SHA-256

MonsterMQ includes a full implementation of SCRAM-SHA-256 (RFC 7677 / RFC 5802).

**Features:**
- Mutual authentication (both client and server verify each other)
- HMAC-SHA-256 with 4096 iterations (RFC recommended minimum)
- Cryptographically secure nonce and salt generation
- Protection against eavesdropping and replay attacks

**Authentication Flow:**

```
Client                           Server
  |  CONNECT                       |
  |  - Auth Method: SCRAM-SHA-256  |
  |  - Auth Data: client-first     |
  |------------------------------->|
  |                                | startAuth()
  |  CONNACK or AUTH               |
  |  - Auth Data: server-first     |
  |<-------------------------------|
  |  AUTH                          |
  |  - Auth Data: client-final     |
  |------------------------------->|
  |                                | continueAuth()
  |  AUTH                          |
  |  - Auth Data: server-final     |
  |<-------------------------------|
  |  Connection established        |
  |<==============================>|
```

### Configuration

```yaml
UserManagement:
  Enabled: true
  PasswordAlgorithm: bcrypt
  EnhancedAuth:
    Enabled: true
    Methods:
      - SCRAM-SHA-256
```

### Current Limitation

Vert.x MQTT 5.0.7 does not yet provide APIs for sending/receiving AUTH packets during the authentication flow. The implementation currently:

1. Parses authentication method and data from CONNECT
2. Logs the enhanced authentication attempt
3. Falls back to basic username/password authentication
4. Awaits Vert.x MQTT API enhancement for AUTH packet support

When AUTH packet support is added to Vert.x MQTT, the full challenge-response flow will activate automatically.

### Security Recommendations

- Use TLS (port 8883) alongside enhanced authentication
- Minimum 12-character passwords with complexity
- Monitor and alert on failed authentication attempts
- Rotate passwords and salts periodically

---

## Dashboard Integration

The MonsterMQ web dashboard fully exposes all MQTT v5.0 features:

- **Protocol Version Badges** — Visual distinction between v5.0 (purple) and v3.1.1 (blue) clients
- **Session Details** — Display connection properties (Receive Maximum, Max Packet Size, etc.)
- **Subscription Options** — No Local, Retain Handling, RAP badges in subscription lists
- **Message Properties** — Collapsible properties panel in Topic Browser (Expiry, Content Type, Response Topic, User Properties)
- **MQTT v5 Statistics** — Dashboard metric card showing adoption percentage
- **Bridge Configuration** — Full v5 connection and message property settings with protocol version selector

---

## Pending Features

### Will Delay Interval

Implementation complete, waiting for Vert.x API support. Vert.x MQTT 5.0.7 does not expose Will Properties in the CONNECT packet. See [Vert.x MQTT Issue #161](https://github.com/vert-x3/vertx-mqtt/issues/161). MonsterMQ code is ready and will activate once the API is available.

### Subscription Identifiers

Not yet implemented. Clients would assign numeric IDs to subscriptions, and the broker would echo the ID in matching messages.

---

## Implementation Status

| Feature | Status | DB Support |
|---------|--------|------------|
| Connection Properties | Complete | N/A |
| Reason Codes | Complete | N/A |
| Message Properties | Complete | All |
| Topic Aliases | Complete | N/A |
| Message Expiry | Complete | All (validated) |
| Enhanced Auth (SCRAM) | Complete | N/A |
| Server CONNACK Properties | Complete | N/A |
| Flow Control | Complete | N/A |
| Subscription Options | Complete | All |
| Request/Response | Complete | N/A |
| Payload Format / UTF-8 | Complete | N/A |
| Dashboard Integration | Complete | N/A |
| Will Delay | Pending Vert.x | N/A |
| Subscription IDs | Not implemented | N/A |

**Overall:** 12 of 14 features implemented.

---

## Client Examples

### Python (paho-mqtt)

```python
import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        client.subscribe([("sensor/data", {"qos": 1, "nl": 0, "rap": 1, "rh": 0})])

def on_message(client, userdata, msg):
    print(f"Topic: {msg.topic}, Payload: {msg.payload.decode()}")
    if msg.properties:
        print(f"Content Type: {msg.properties.ContentType}")
        if msg.properties.UserProperty:
            for key, value in msg.properties.UserProperty:
                print(f"  {key} = {value}")

client = mqtt.Client(client_id="test-client", protocol=mqtt.MQTTv5)
client.on_connect = on_connect
client.on_message = on_message

connect_props = Properties(PacketTypes.CONNECT)
connect_props.SessionExpiryInterval = 300
connect_props.ReceiveMaximum = 100
client.connect("localhost", 1883, properties=connect_props)
client.loop_start()

pub_props = Properties(PacketTypes.PUBLISH)
pub_props.MessageExpiryInterval = 3600
pub_props.ContentType = "application/json"
pub_props.UserProperty = [("sensor", "DHT22"), ("location", "room-5")]
client.publish("sensor/temperature", '{"value": 23.5}', qos=1, properties=pub_props)
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

Test suite validates all MQTT v5.0 features:

```bash
cd tests
pytest mqtt5/ -v              # Run all MQTT v5 tests
pytest mqtt5/ -k "connection" # Run connection tests only
```

Test files:
- `test_mqtt5_connection.py` — Connection properties
- `test_mqtt5_reason_codes.py` — Reason codes in ACKs
- `test_mqtt5_properties.py` — Message properties
- `test_mqtt5_topic_alias.py` — Topic alias resolution
- `test_mqtt5_message_expiry.py` — Message expiry and TTL
- `test_mqtt5_flow_control.py` — Receive Maximum enforcement
- `test_mqtt5_no_local.py` — No Local subscription option
- `test_mqtt5_retain_handling.py` — Retain handling options
- `test_mqtt5_rap_pytest.py` — Retain As Published
- `test_mqtt5_request_response.py` — Request/response pattern

## References

- [MQTT v5.0 Specification](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
- [RFC 7677 — SCRAM-SHA-256](https://tools.ietf.org/html/rfc7677)
- [RFC 5802 — SCRAM Mechanism](https://tools.ietf.org/html/rfc5802)
- [Vert.x MQTT](https://vertx.io/docs/vertx-mqtt/java/)
- [paho-mqtt Documentation](https://eclipse.dev/paho/files/paho.mqtt.python/html/client.html)
