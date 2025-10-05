# Kafka Integration

MonsterMQ offers a minimal Kafka integration that lets you:
- replace the internal Vert.x event bus with a single Kafka topic for inter-node communication
- push archive-group traffic into Kafka topics for downstream processing

Kafka support is intentionally simple. There is no automatic topic provisioning, no SASL/SSL support, and no advanced producer or consumer configuration beyond what is hard-coded in the implementation.

## Configuration

### Global Kafka settings

Set the broker connection string once. The same value is reused by the message bus and every Kafka archive store.

```yaml
Kafka:
  Servers: "localhost:9092"
  Bus:
    Enabled: true
    Topic: "monster"   # Kafka topic used as message bus when enabled
```

`Servers` must point to a reachable Kafka bootstrap server list. When `Bus.Enabled` is `false` the broker falls back to the in-memory Vert.x bus.

### Kafka as Message Bus

When `Bus.Enabled` is `true`, MonsterMQ deploys `MessageBusKafka` (`broker/src/main/kotlin/bus/MessageBusKafka.kt`).
- Producer properties are fixed to `acks=1`, string keys, and byte-array values.
- Consumer properties use group id `Monster`, `auto.offset.reset=earliest`, and automatic commits.
- No TLS/SASL configuration hooks exist; the integration expects a PLAINTEXT Kafka endpoint.

### Kafka Archive Groups

Archive groups can target Kafka by selecting the `KAFKA` store type. The archive name becomes the Kafka topic name (e.g. an archive group called `Telemetry` produces records on `TelemetryArchive`).

```yaml
ArchiveGroups:
  - Name: Telemetry
    Filter: "sensors/+"
    Enabled: true
    Store: KAFKA
```

Internally `MessageArchiveKafka` (`broker/src/main/kotlin/stores/others/MessageArchiveKafka.kt`) writes every message with the MQTT topic as the Kafka record key and a binary payload encoded through `MqttMessageCodec`.

There is no configuration field to override the topic name, partition count, or replication factor from MonsterMQ.

## Topic Management

MonsterMQ never creates Kafka topics. Ensure the required topics exist before enabling the integration:

```bash
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic monster \
  --partitions 3 \
  --replication-factor 1

kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic TelemetryArchive \
  --partitions 3 \
  --replication-factor 1
```

Choose partition and replication settings that match your Kafka cluster—MonsterMQ neither validates nor adjusts them.

## Record Format

Both the message bus and archive producer send the same structure:
- Record key: the original MQTT topic name (`message.topicName`).
- Record value: the `MqttMessage` encoded by `MqttMessageCodec`, which contains the message UUID, MQTT message-id, QoS/flags, client id, event timestamp, and the raw MQTT payload bytes.

Downstream consumers must decode this binary format using the same codec or a compatible implementation.

## Kafka Client Bridge

MonsterMQ now exposes per-client runtime metrics via GraphQL. See the new section "Kafka Client Metrics" below for details.

MonsterMQ provides a unidirectional Kafka Client bridge device that subscribes to a single Kafka topic and republishes records as MQTT messages.

Key characteristics:
- Topic Selection: The Kafka topic name is always the device Namespace value.
- Simplicity: No per-device override of the Kafka topic name; create one device per topic.
- Consumer Group: Configurable `groupId` (defaults to `monstermq-subscriber`).
- Extra Config: Optional JSON map of raw Kafka consumer properties (string values only) merged over defaults.

Payload formats (`payloadFormat`):
- DEFAULT: Kafka record value is the binary `BrokerMessage` encoded with `BrokerMessageCodec` (record key ignored). Republished exactly.
- JSON: Kafka record value is a JSON object representing a `BrokerMessage` (see structure below). Republished.
- BINARY: Record key = MQTT topic, record value bytes = payload. Record dropped if key is null.
- TEXT: Record key = MQTT topic, record value (UTF-8) = payload bytes. Record dropped if key is null.

JSON BrokerMessage fields (bridge accepts superset):
- topicName (or legacy topic): MQTT topic (required)
- payloadBase64 or payload: One of base64 encoded payload or plain text payload
- messageUuid: optional, generated if absent
- messageId: integer (default 0)
- qosLevel: integer 0..2 (default 0)
- isRetain, isDup, isQueued: booleans (default false)
- clientId: origin id (default `kafkaclient-<device>`)
- time: ISO-8601 timestamp (default now)

Unsupported / ignored legacy fields: topic, keyTransform, keyFallbackTopic, keyTopicPrefix.

Error Handling & Metrics:
- Malformed records increment internal dropped/error counters (available via metrics endpoint; UI partly displays them — error metric UI addition pending).

Recommended usage:
- Use DEFAULT when producing from another MonsterMQ instance (lossless round-trip).
- Use JSON for language-agnostic integrations without implementing the binary codec.
- Use BINARY/TEXT for simple key->topic mirroring.

## Kafka Client Metrics

The Kafka Client bridge exposes lightweight runtime throughput metrics via GraphQL.

Schema types:

```graphql
type KafkaClientMetrics {
  messagesIn: Float!      # Kafka records consumed per second (smoothed)
  messagesOut: Float!     # MQTT messages published per second (post-transform)
  timestamp: String!      # ISO-8601 capture time
}

type KafkaClient {
  name: String!
  namespace: String!
  metrics: [KafkaClientMetrics!]!
  metricsHistory(from: String, to: String, lastMinutes: Int): [KafkaClientMetrics!]!
}
```

Query examples:

```graphql
# List all Kafka clients with their latest instantaneous rate sample
{
  kafkaClients {
    name
    namespace
    metrics { messagesIn messagesOut timestamp }
  }
}

# Retrieve the most recent 15 minutes of metrics for a single client
{
  kafkaClient(name: "MyKafkaClient") {
    name
    metricsHistory(lastMinutes: 15) { messagesIn messagesOut timestamp }
  }
}
```

Behavior & Semantics:
- messagesIn: Rate (records/sec) the consumer loop ingested from Kafka during the last sampling window.
- messagesOut: Rate (messages/sec) successfully republished to MQTT after payload transformation and topic mapping.
- Rates are floating point values rounded to two decimals in GraphQL responses (`round2` in resolver).
- Sampling cadence equals the internal metrics collection interval plus client poll granularity (pollIntervalMs). Shorter polls give more responsive rates.
- Persistence: If a metrics store (PostgreSQL / CrateDB / MongoDB / SQLite) is configured, periodic samples are persisted and exposed via `metricsHistory`.
- Live Fallback: When the metrics store is disabled or contains no sample yet for the client, the resolver queries the live Vert.x event bus address (`KafkaBridge.connectorMetrics(<clientName>)`) and returns a synthetic single-sample list. This enables immediate UI feedback after client creation.
- Zeroes: A result of 0.0 for both fields can mean idle client, startup (no data yet), or an error retrieving live metrics (check broker log at `FINE` level for hints).
- History Limits: `metricsHistory` applies optional time slicing (`from`, `to`, `lastMinutes`). When absent, all stored samples for the client are returned (may be large; paginate client-side).

Operational Notes:
- No backfill: History starts only after the client first produces metrics.
- Clock Source: Timestamps use the broker node system clock in ISO-8601 UTC.
- Aggregation: An aggregate across all Kafka clients (sum of messagesIn/messagesOut) is planned for BrokerMetrics but currently not exposed; consumers can sum client samples client-side.
- Rates vs Counts: Cumulative counts are not stored; only per-second rates. To approximate counts over a period, integrate (sum rate * interval duration) across successive samples client-side.

Troubleshooting Metrics:
1. Both rates always zero: Verify the client is enabled and consuming (Kafka topic has traffic). Increase log level to FINE and watch for consumer errors.
2. messagesIn > 0 but messagesOut = 0: Likely all consumed records are dropped (e.g., null record key for formats requiring a key, or transform errors). Inspect logs for "dropped" entries.
3. Spiky rates: Consider increasing `pollIntervalMs` to smooth or aggregate samples client-side.
4. Missing history: Ensure a supported metrics store is configured; otherwise only the live single-sample endpoint is available.

## Limitations

- PLAINTEXT Kafka only (no SASL, SCRAM, or TLS).
- Single topic for the message bus, configured via `Kafka.Bus.Topic`.
- Archive topics are derived from the archive group name and cannot be customised.
- No consumer-side lag tracking or offset management utilities are exposed.
- Producer retries are minimal (`acks=1`, `retries=3` in the archive store) and should be tuned at the Kafka cluster level if stricter guarantees are needed.

## Troubleshooting

1. **Connection failures** – verify `Kafka.Servers` addresses, firewall rules, and that the target topic already exists.
2. **Missing records** – confirm the archive group or message bus is enabled, check broker logs for `Kafka` warnings, and review Kafka broker logs for rejected writes.
3. **Incompatible payloads** – ensure downstream consumers decode messages with `MqttMessageCodec`; the payload is not JSON.
