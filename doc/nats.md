# NATS Client Bridge

MonsterMQ provides a bidirectional NATS Client bridge that lets any number of MQTT topics flow into and out of a NATS server. Each bridge device is a first-class managed entity: it is stored in the database, can be started/stopped independently, and is created and administered through the GraphQL API or the dashboard UI.

## Overview

Each NATS Client bridge device:
- Maintains a persistent connection to one or more NATS servers (with automatic reconnection).
- Supports both **Core NATS** (fire-and-forget) and **JetStream** (at-least-once, durable consumers).
- Supports four authentication modes: Anonymous, Username/Password, Token, and TLS.
- Holds a list of **address mappings** that declare the direction and topic pairing for each data flow.
- Automatically translates MQTT topic wildcards and path separators to NATS equivalents (and back) when `autoConvert` is enabled.

---

## Address Mappings

An address mapping declares one data flow between MQTT and NATS. Each mapping has a `mode`:

| Mode | Direction | Description |
|---|---|---|
| `SUBSCRIBE` | NATS → MQTT | Bridge subscribes to a NATS subject and publishes matching messages to a local MQTT topic |
| `PUBLISH` | MQTT → NATS | Bridge subscribes to a local MQTT topic and forwards messages to a NATS subject |

### Address fields

| Field | Type | Default | Description |
|---|---|---|---|
| `mode` | `SUBSCRIBE` \| `PUBLISH` | required | Direction of data flow |
| `natsSubject` | String | required | NATS subject (with NATS wildcards if desired) |
| `mqttTopic` | String | required | Local MQTT topic (with MQTT wildcards if desired) |
| `qos` | 0 \| 1 \| 2 | `0` | QoS for local MQTT subscriptions or publications |
| `autoConvert` | Boolean | `true` | Enable automatic separator and wildcard translation |

### Separator and wildcard conversion

When `autoConvert` is `true`, the bridge transparently converts between the MQTT and NATS naming conventions so you don't have to duplicate subjects:

| MQTT | NATS | Notes |
|---|---|---|
| `/` (path separator) | `.` (token separator) | All occurrences |
| `#` (multi-level wildcard) | `>` (multi-token wildcard) | |
| `+` (single-level wildcard) | `*` (single-token wildcard) | |

**Examples:**

| MQTT topic | NATS subject |
|---|---|
| `sensors/temperature/room1` | `sensors.temperature.room1` |
| `sensors/#` | `sensors.>` |
| `sensors/+/temperature` | `sensors.*.temperature` |
| `#` | `>` |

Set `autoConvert: false` to pass topics and subjects through without any modification.

---

## Connection Configuration

### Basic fields

| Field | Type | Default | Description |
|---|---|---|---|
| `servers` | List\<String\> | `["nats://localhost:4222"]` | One or more NATS server URLs |
| `authType` | String | `ANONYMOUS` | Authentication mode (see below) |
| `connectTimeoutMs` | Long | `5000` | TCP connection timeout in milliseconds |
| `reconnectDelayMs` | Long | `5000` | Wait between reconnection attempts |
| `maxReconnectAttempts` | Int | `-1` | Maximum reconnects; `-1` = unlimited |

Server URLs must begin with `nats://`, `nats+tls://`, or `tls://`.

### Authentication modes

#### `ANONYMOUS`
No credentials. Used for open NATS servers in trusted networks.

#### `USERNAME_PASSWORD`
Standard username/password credentials.

| Field | Required | Description |
|---|---|---|
| `username` | Yes | NATS username |
| `password` | Yes | NATS password |

#### `TOKEN`
Single shared-secret token auth.

| Field | Required | Description |
|---|---|---|
| `token` | Yes | NATS authentication token |

#### `TLS`
Mutual TLS or server-only TLS verification.

| Field | Required | Description |
|---|---|---|
| `tlsCaCertPath` | No | Absolute path to a JKS trust store containing the CA certificate. When absent, the JVM default trust store is used (`opentls` mode). |
| `tlsVerify` | No (default `true`) | Set to `false` to disable certificate validation (development/self-signed only — do not use in production). |

> ⚠️ `tlsVerify: false` installs a trust-all `SSLContext`. Never use in production.

---

## JetStream

When `useJetStream` is `true`, the bridge uses the JetStream API for both producing and consuming messages instead of Core NATS. This provides at-least-once delivery and durable consumers.

| Field | Required | Description |
|---|---|---|
| `useJetStream` | — | Set to `true` to enable JetStream |
| `streamName` | Yes (when JetStream is on) | The JetStream stream name to bind inbound subjects to |
| `consumerDurableName` | No | Durable consumer name. When set, the broker resumes from the last acknowledged message after a restart. |

**JetStream must be enabled on the NATS server** before using this mode. MonsterMQ does not create or manage streams — they must be provisioned beforehand.

---

## GraphQL API

### Query — list clients

```graphql
{
  natsClients {
    name
    namespace
    nodeId
    enabled
    isOnCurrentNode
    createdAt
    updatedAt
    config {
      servers
      authType
      username
      useJetStream
      streamName
      consumerDurableName
      connectTimeoutMs
      reconnectDelayMs
      maxReconnectAttempts
      addresses {
        mode
        natsSubject
        mqttTopic
        qos
        autoConvert
      }
    }
    metrics {
      messagesIn
      messagesOut
      timestamp
    }
  }
}
```

Filter by name or node:

```graphql
{
  natsClients(name: "my-nats-bridge") { name enabled }
  natsClients(node: "node-1") { name nodeId }
}
```

### Mutations

All mutations are nested under `natsClient { ... }`.

#### Create

```graphql
mutation {
  natsClient {
    create(input: {
      name: "factory-nats"
      namespace: "factory"
      nodeId: "node-1"
      enabled: true
      config: {
        servers: ["nats://192.168.1.10:4222"]
        authType: ANONYMOUS
        reconnectDelayMs: 5000
        maxReconnectAttempts: -1
        addresses: [
          {
            mode: SUBSCRIBE
            natsSubject: "factory.sensors.>"
            mqttTopic: "factory/sensors/#"
            qos: 1
            autoConvert: false
          },
          {
            mode: PUBLISH
            natsSubject: "factory.commands.*"
            mqttTopic: "factory/commands/+"
            qos: 0
            autoConvert: false
          }
        ]
      }
    }) {
      success
      errors
      client { name nodeId enabled }
    }
  }
}
```

#### Update

```graphql
mutation {
  natsClient {
    update(name: "factory-nats", input: {
      name: "factory-nats"
      namespace: "factory"
      nodeId: "node-1"
      enabled: true
      config: {
        servers: ["nats://192.168.1.10:4222", "nats://192.168.1.11:4222"]
        authType: USERNAME_PASSWORD
        username: "mqtt-bridge"
        password: "s3cr3t"
        reconnectDelayMs: 3000
      }
    }) {
      success
      errors
    }
  }
}
```

#### Delete

```graphql
mutation {
  natsClient {
    delete(name: "factory-nats")
  }
}
```

#### Start / Stop

```graphql
mutation {
  natsClient {
    toggle(name: "factory-nats", enabled: false) { success }
  }
}
```

#### Reassign to another cluster node

```graphql
mutation {
  natsClient {
    reassign(name: "factory-nats", nodeId: "node-2") { success errors client { nodeId } }
  }
}
```

#### Add address mapping

```graphql
mutation {
  natsClient {
    addAddress(
      deviceName: "factory-nats"
      input: {
        mode: SUBSCRIBE
        natsSubject: "alarms.>"
        mqttTopic: "alarms/#"
        qos: 2
        autoConvert: false
      }
    ) { success errors }
  }
}
```

#### Update address mapping (keyed by current `natsSubject`)

```graphql
mutation {
  natsClient {
    updateAddress(
      deviceName: "factory-nats"
      natsSubject: "alarms.>"
      input: {
        mode: SUBSCRIBE
        natsSubject: "alarms.critical.>"
        mqttTopic: "alarms/critical/#"
        qos: 2
        autoConvert: false
      }
    ) { success errors }
  }
}
```

#### Delete address mapping

```graphql
mutation {
  natsClient {
    deleteAddress(deviceName: "factory-nats", natsSubject: "alarms.critical.>") {
      success
      errors
    }
  }
}
```

---

## Metrics

The bridge collects lightweight throughput metrics while running.

| Field | Description |
|---|---|
| `messagesIn` | NATS messages received per second (NATS → MQTT path) |
| `messagesOut` | MQTT messages forwarded per second (MQTT → NATS path) |
| `timestamp` | ISO-8601 UTC capture time |

Both values are floating-point rates smoothed over a sliding window. A value of `0.0` indicates idle, startup, or an error (check broker log).

---

## Loop Prevention

The bridge assigns an internal MQTT client ID when subscribing to local MQTT topics for outbound (PUBLISH mode) mappings. Any message received from that internal client ID is dropped before forwarding to NATS. This prevents a forwarded message from being echoed back into the MQTT broker when a subscriber on the same broker publishes to NATS and NATS then re-delivers to the same subject.

---

## Cluster Support

Each bridge device is assigned to a specific `nodeId`. Only the node that owns the device runs the active connection. When a device is reassigned (`reassign` mutation), the original node stops the connector and the target node starts it. Device configuration is stored in the shared database so all nodes have access, but only the owner connects to NATS.

---

## Troubleshooting

**Connection timeout at startup**
Verify the server URL is reachable and the NATS server is running. Check `connectTimeoutMs` (default 5 seconds). The connector logs `WARNING` on each failed attempt and retries after `reconnectDelayMs`.

**Messages not arriving (SUBSCRIBE mode)**
- Verify the NATS subject is correct and traffic is being published on the NATS server.
- Check that `autoConvert` is set consistently with how the subject is written (`.`-separated vs `/`-separated).
- For JetStream, ensure the stream exists and its subject filter covers the configured `natsSubject`.

**Messages not forwarded (PUBLISH mode)**
- Confirm the MQTT topic is receiving messages locally (subscribe via an MQTT client for verification).
- Verify the NATS server accepts publications to the target subject (check NATS permissions).

**JetStream — consumer not resuming after restart**
Set `consumerDurableName` to a stable, unique string. Without a durable name, a new ephemeral consumer is created on each start and no offset is remembered.

**TLS handshake failure**
- If using a self-signed cert, either add the CA to a JKS trust store and point `tlsCaCertPath` at it, or set `tlsVerify: false` for testing.
- Ensure the server URL uses `nats+tls://` or `tls://` when TLS is required.
- Verify JVM version compatibility — the bridge uses the standard JVM `SSLContext` (TLS 1.2/1.3).

**Authentication rejected**
- `USERNAME_PASSWORD`: confirm username and password are correct and the NATS user has permissions on the configured subjects.
- `TOKEN`: verify the token has not expired and matches the NATS server config.
