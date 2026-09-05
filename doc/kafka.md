# Kafka Integration

MonsterMQ provides three Kafka interfaces: an optional Kafka message bus, managed
Kafka client bridges, and a Kafka-compatible server. They have separate purposes
and configuration.

## Kafka Message Bus

```yaml
Kafka:
  Servers: "localhost:9092"
  Bus:
    Enabled: true
    Topic: "monster"
  Config:
    acks: "all"
    # Native Kafka client properties, including security.protocol/SASL/SSL
```

The bus sends `BrokerMessageCodec` binary records with the MQTT topic as the Kafka
key. `Kafka.Config` is merged into producer and consumer properties by
[KafkaConfigBuilder.kt](../broker/src/main/kotlin/bus/KafkaConfigBuilder.kt).
Defaults include producer `acks=1`, `retries=3`, consumer group `Monster`,
`auto.offset.reset=earliest`, and automatic commits. Preserve serializers and
deserializers compatible with the broker's binary codec.

Provision the bus topic in Kafka with the partitions and replication required by
your deployment. MonsterMQ does not manage that topic's lifecycle. Kafka bus and
Zenoh federation are mutually exclusive. With neither enabled, the broker uses
the Vert.x message bus.

## Kafka Archive Migration

`KAFKA` is no longer an `ArchiveType`. The old `MessageArchiveKafka` implementation
has been removed. To forward selected MQTT publications to an external Kafka
topic, configure outbound mappings on a Kafka client bridge. For Kafka consumers
reading broker-managed streams, use the Kafka-compatible server below.

## Kafka Client Bridge

MonsterMQ provides a bidirectional Kafka client bridge. Inbound consumption is enabled by default; outbound publishing can be enabled independently. `Features.Kafka` enables the extension.

Key characteristics:
- Inbound topic selection: The consumer subscribes to the device namespace.
- Create separate devices for different inbound Kafka topics; outbound publishing can use `outboundKafkaTopic`.
- Consumer Group: Configurable `groupId` (defaults to `monstermq-subscriber`).
- Extra Config: `extraConsumerConfig` and `extraProducerConfig` override native Kafka client properties, including security settings.
- Inbound controls: `inboundEnabled`, `destinationTopicPrefix`, `topicKeyRegex`, and `topicKeyReplacement`.
- Outbound controls: `outboundEnabled`, `outboundTopicFilters`, `outboundKafkaTopic`, `outboundTopicKeyRegex`, and `outboundTopicKeyReplacement`. The outbound Kafka topic defaults to the device namespace.
- The bridge consumer defaults to automatic commits and `auto.offset.reset=latest`. Override through `extraConsumerConfig` if replay from earlier offsets is intended.

### Payload Formats

The `payloadFormat` setting controls how Kafka record values are interpreted and mapped to MQTT messages.

#### DEFAULT
- **Deserializer**: `ByteArrayDeserializer`
- **Topic source**: Embedded in the binary-encoded `BrokerMessage` (Kafka record key is ignored)
- **Payload**: Decoded via `BrokerMessageCodec.decodeFromWire()`
- **Metadata**: Fully preserved (QoS, retain, dup, clientId, timestamp, etc.)
- **Fallback**: If binary decoding fails, automatically attempts JSON parsing before dropping the message
- **Use case**: Consuming binary `BrokerMessage` envelopes produced by a MonsterMQ bridge

#### JSON
- **Deserializer**: `StringDeserializer`
- **Topic source**: Extracted from JSON field `topicName` (or legacy `topic`) — Kafka record key is ignored
- **Payload**: Decoded from `payloadBase64` (Base64→bytes) or `payload` (UTF-8 string→bytes) in the JSON object
- **Metadata**: Fully preserved from the JSON envelope (QoS, retain, dup, clientId, timestamp, etc.)
- **Use case**: Language-agnostic integrations where external producers write BrokerMessage-compatible JSON without implementing the binary codec

#### TEXT
- **Deserializer**: `StringDeserializer`
- **Topic source**: Kafka record **key** (message is dropped if key is null)
- **Payload**: Kafka value string converted to UTF-8 bytes — raw pass-through
- **Metadata**: Not preserved — creates a fresh BrokerMessage with QoS=0, retain=false
- **Use case**: Simple Kafka→MQTT bridge where any external producer writes plain text values with the MQTT topic as the Kafka key

#### BINARY
- **Deserializer**: `ByteArrayDeserializer`
- **Topic source**: Kafka record **key** (message is dropped if key is null)
- **Payload**: Kafka value bytes used directly as MQTT payload
- **Metadata**: Not preserved — creates a fresh BrokerMessage with QoS=0, retain=false
- **Use case**: Simple Kafka→MQTT bridge for binary payloads (images, protobuf, etc.) with the MQTT topic as the Kafka key

#### Format Comparison

| Aspect | DEFAULT | JSON | TEXT | BINARY |
|--------|---------|------|------|--------|
| Deserializer | ByteArray | String | String | ByteArray |
| MQTT topic from | BrokerMessage | JSON field | Kafka key | Kafka key |
| Kafka key required? | No | No | **Yes** | **Yes** |
| QoS/Retain preserved? | Yes | Yes | No | No |
| Intended producer | MonsterMQ | Any (JSON) | Any (text) | Any (binary) |

#### JSON BrokerMessage Fields

When using DEFAULT (JSON fallback) or JSON format, the following fields are recognized:

- `topicName` (or legacy `topic`): MQTT topic (required)
- `payloadBase64` or `payload`: Base64-encoded payload or plain text payload
- `messageUuid`: optional, generated if absent
- `messageId`: integer (default 0)
- `qosLevel`: integer 0..2 (default 0)
- `isRetain`, `isDup`, `isQueued`: booleans (default false)
- `clientId`: origin id (default `kafkaclient-<device>`)
- `time`: ISO-8601 timestamp (default now)

#### Error Handling

- DEFAULT format: If `BrokerMessageCodec` decoding fails, a JSON fallback is attempted automatically with a warning log suggesting to check the `payloadFormat` setting. If both fail, the message is dropped.
- TEXT/BINARY: Messages without a Kafka record key are silently dropped.
- All dropped/errored records increment internal counters exposed via the metrics endpoint.

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
```

```graphql
# Retrieve the most recent 15 minutes of metrics for a single client
{
  kafkaClients(name: "MyKafkaClient") {
    name
    metricsHistory(lastMinutes: 15) { messagesIn messagesOut timestamp }
  }
}
```

Behavior & Semantics:
- messagesIn: Rate (records/sec) the consumer loop ingested from Kafka during the last sampling window.
- messagesOut: Rate (messages/sec) successfully republished to MQTT after payload transformation and topic mapping.
- Rates are floating point values rounded to two decimals in GraphQL responses (`round2` in resolver).
- Sampling cadence equals the internal metrics collection interval plus Kafka consumer poll granularity.
- Persistence: If a metrics store (PostgreSQL / CrateDB / MongoDB / SQLite) is configured, periodic samples are persisted and exposed via `metricsHistory`.
- Live Fallback: When the metrics store is disabled or contains no sample yet for the client, the resolver queries the live Vert.x event bus address (`KafkaBridge.connectorMetrics(<clientName>)`) and returns a synthetic single-sample list. This enables immediate UI feedback after client creation.
- Zeroes: A result of 0.0 for both fields can mean idle client, startup (no data yet), or an error retrieving live metrics (check broker log at `FINE` level for hints).
- History Limits: `metricsHistory` applies optional time slicing (`from`, `to`, `lastMinutes`). When absent, all stored samples for the client are returned (may be large; paginate client-side).

Operational Notes:
- No backfill: History starts only after the client first produces metrics.
- Clock Source: Timestamps use the broker node system clock in ISO-8601 UTC.
- Broker metrics expose `kafkaClientIn` and `kafkaClientOut`.
- Rates vs Counts: Cumulative counts are not stored; only per-second rates. To approximate counts over a period, integrate (sum rate * interval duration) across successive samples client-side.

Troubleshooting Metrics:
1. Both rates always zero: Verify the client is enabled and consuming (Kafka topic has traffic). Increase log level to FINE and watch for consumer errors.
2. messagesIn > 0 but messagesOut = 0: Likely all consumed records are dropped (e.g., null record key for formats requiring a key, or transform errors). Inspect logs for "dropped" entries.
3. Spiky rates: Aggregate samples client-side to smooth out rate fluctuations.
4. Missing history: Ensure a supported metrics store is configured; otherwise only the live single-sample endpoint is available.

## Kafka-Compatible Server

`Features.KafkaServer` enables the managed server extension. Create a server
through `kafkaServer.add`, supplying a name, namespace, node ID, port, and stream
mappings. Each stream has a `streamName`, MQTT `topicFilter`, retention period,
optional store type, and `allowWrite` flag. Configure advertised host/port values
that Kafka clients can reach.

The API exposes `kafkaServers`, `kafkaServer(name:)`, `kafkaMessages`,
`kafkaTopicOffsets`, and `kafkaConsumerGroups`. See
[schema-kafka-servers.graphqls](../broker/src/main/resources/schema-kafka-servers.graphqls)
for inputs and returned fields. This is a compatibility server backed by broker
storage, not an external Kafka cluster installation.

## Troubleshooting

Check which interface is involved first: bus, client bridge, or server. For an
external Kafka connection, verify bootstrap addresses, credentials/security
properties, topic existence, and consumer group offsets. For a client bridge,
check `inboundEnabled`/`outboundEnabled`, mapping filters, and payload format.
TEXT/BINARY inbound records need a Kafka key; DEFAULT/JSON records need a valid
BrokerMessage envelope.

Use the device metrics and broker logs to distinguish idle clients, filtered or
dropped records, and connection failures. A bridge assigned to a failed node
requires node recovery or explicit reassignment.
