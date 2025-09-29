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
