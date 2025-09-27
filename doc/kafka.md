# Kafka Integration

MonsterMQ supports Apache Kafka as both a message bus and archive storage backend. This integration enables high-throughput message streaming, event sourcing, and seamless integration with Kafka ecosystems.

## Overview

Kafka integration provides:
- **Message Bus** - Alternative to Vert.x EventBus for inter-broker communication
- **Archive Storage** - Stream MQTT messages to Kafka topics
- **Event Sourcing** - Build event-driven architectures
- **Stream Processing** - Integrate with Kafka Streams, ksqlDB
- **External Integration** - Connect to existing Kafka infrastructure

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   MQTT      │────▶│  MonsterMQ  │────▶│   Kafka     │
│  Clients    │     │   Broker    │     │   Cluster   │
└─────────────┘     └──────┬──────┘     └──────┬──────┘
                           │                    │
                    ┌──────┴──────┐      ┌──────┴──────┐
                    │  Kafka Bus  │      │   Topics:   │
                    │  Handler    │      │ • mqtt.raw  │
                    └─────────────┘      │ • mqtt.archive│
                                        │ • mqtt.events│
                                        └─────────────┘
```

## Quick Start

### Basic Configuration

```yaml
# Enable Kafka message bus
Kafka:
  Servers: "localhost:9092"
  Bus:
    Enabled: true
    Topic: "monster"  # Default topic name
```

**Note:** MonsterMQ's Kafka integration uses hardcoded settings:
- Producer acks: `1`
- Consumer group ID: `Monster`
- Auto offset reset: `earliest`
- Auto commit: `true`
- No advanced producer/consumer configuration options are available

### Kafka as Archive

```yaml
ArchiveGroups:
  - Name: Default
    Filter: "#"  # All topics
    Enabled: true
    Store: KAFKA

  - Name: Sensors
    Filter: "sensors/+"
    Enabled: true
    Store: KAFKA
    KafkaTopic: "mqtt.sensors"  # Custom Kafka topic
```

## Kafka Setup

### Local Development

```bash
# Using Docker Compose
docker-compose -f kafka-compose.yml up -d

# kafka-compose.yml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: true
```

### Production Setup

```yaml
Kafka:
  BootstrapServers: "kafka1:9092,kafka2:9092,kafka3:9092"

  # Security
  Security:
    Protocol: SASL_SSL
    SaslMechanism: PLAIN
    SaslUsername: "${KAFKA_USERNAME}"
    SaslPassword: "${KAFKA_PASSWORD}"

  # SSL Configuration
  SSL:
    TruststoreLocation: "/path/to/truststore.jks"
    TruststorePassword: "${TRUSTSTORE_PASSWORD}"
    KeystoreLocation: "/path/to/keystore.jks"
    KeystorePassword: "${KEYSTORE_PASSWORD}"
```

## Topic Management

### Auto-created Topics

MonsterMQ automatically creates these Kafka topics:

| Topic | Purpose | Partitions | Retention |
|-------|---------|------------|-----------|
| `mqtt.messages` | Raw MQTT messages | 12 | 7 days |
| `mqtt.archive` | Archived messages | 12 | 30 days |
| `mqtt.events` | Broker events | 3 | 7 days |
| `mqtt.metrics` | Performance metrics | 3 | 1 day |
| `mqtt.dlq` | Dead letter queue | 3 | 30 days |

### Custom Topic Configuration

```yaml
Kafka:
  Topics:
    Messages:
      Name: "mqtt.messages"
      Partitions: 12
      ReplicationFactor: 3
      Config:
        retention.ms: 604800000  # 7 days
        compression.type: snappy
        segment.ms: 3600000  # 1 hour

    Archive:
      Name: "mqtt.archive"
      Partitions: 24
      ReplicationFactor: 3
      Config:
        retention.ms: 2592000000  # 30 days
        compression.type: gzip
```

### Manual Topic Creation

```bash
# Create topics manually
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic mqtt.messages \
  --partitions 12 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config compression.type=snappy

# List topics
kafka-topics.sh --list --bootstrap-server localhost:9092

# Describe topic
kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --topic mqtt.messages
```

## Message Format

### Kafka Message Schema

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "topic": "sensors/temperature",
  "payload": "23.5",
  "qos": 1,
  "retained": false,
  "clientId": "sensor-001",
  "brokerId": "broker-1",
  "headers": {
    "contentType": "text/plain",
    "correlationId": "abc123"
  }
}
```

### Avro Schema (Optional)

```avsc
{
  "type": "record",
  "name": "MqttMessage",
  "namespace": "at.rocworks.kafka",
  "fields": [
    {"name": "timestamp", "type": "long"},
    {"name": "topic", "type": "string"},
    {"name": "payload", "type": "bytes"},
    {"name": "qos", "type": "int"},
    {"name": "retained", "type": "boolean"},
    {"name": "clientId", "type": ["null", "string"], "default": null}
  ]
}
```

## Configuration Limitations

MonsterMQ's current Kafka integration has a simple implementation with fixed settings. Advanced producer and consumer configuration options are not available through the configuration file.

The implementation uses basic Kafka client settings suitable for most MQTT messaging scenarios.

### Manual Offset Management

```kotlin
// Example consumer with manual commits
class KafkaArchiveConsumer {
    fun consumeMessages() {
        consumer.subscribe(listOf("mqtt.archive"))

        while (running) {
            val records = consumer.poll(Duration.ofMillis(100))

            for (record in records) {
                processMessage(record)

                // Commit after processing
                val offsets = mapOf(
                    TopicPartition(record.topic(), record.partition()) to
                    OffsetAndMetadata(record.offset() + 1)
                )
                consumer.commitSync(offsets)
            }
        }
    }
}
```

## Stream Processing

### Kafka Streams Integration

```kotlin
// Stream processing example
class MqttStreamProcessor {
    fun buildTopology(): Topology {
        val builder = StreamsBuilder()

        // Read from MQTT messages
        val messages = builder.stream<String, MqttMessage>("mqtt.messages")

        // Filter and transform
        messages
            .filter { _, msg -> msg.topic.startsWith("sensors/") }
            .mapValues { msg ->
                SensorReading(msg.topic, msg.payload, msg.timestamp)
            }
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
            .aggregate(
                { SensorAggregate() },
                { _, reading, agg -> agg.add(reading) }
            )
            .toStream()
            .to("mqtt.aggregates")

        return builder.build()
    }
}
```

### ksqlDB Queries

```sql
-- Create stream from MQTT messages
CREATE STREAM mqtt_messages (
    timestamp BIGINT,
    topic VARCHAR,
    payload VARCHAR,
    qos INT,
    clientId VARCHAR
) WITH (
    KAFKA_TOPIC='mqtt.messages',
    VALUE_FORMAT='JSON'
);

-- Aggregate sensor data
CREATE TABLE sensor_stats AS
SELECT
    topic,
    COUNT(*) as message_count,
    AVG(CAST(payload AS DOUBLE)) as avg_value,
    WINDOWSTART as window_start
FROM mqtt_messages
WHERE topic LIKE 'sensors/%'
WINDOW TUMBLING (SIZE 5 MINUTES)
GROUP BY topic;

-- Detect anomalies
CREATE TABLE anomalies AS
SELECT
    topic,
    payload,
    timestamp
FROM mqtt_messages
WHERE CAST(payload AS DOUBLE) > 100
    OR CAST(payload AS DOUBLE) < 0;
```

## Monitoring

### Kafka Metrics

```yaml
Kafka:
  Metrics:
    Enabled: true
    JmxEnabled: true
    Reporters:
      - class: io.confluent.metrics.reporter.ConfluentMetricsReporter
        config:
          bootstrap.servers: localhost:9092
```

### Key Metrics to Monitor

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `records-lag` | Consumer lag | > 10000 |
| `records-per-request-avg` | Fetch efficiency | < 100 |
| `produce-throttle-time` | Producer throttling | > 0 |
| `failed-fetch-rate` | Consumer failures | > 0.01 |
| `network-io-rate` | Network throughput | > 100MB/s |

### Monitoring Commands

```bash
# Check consumer lag
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group monstermq-cluster \
  --describe

# Monitor topic throughput
kafka-run-class.sh kafka.tools.JmxTool \
  --object-name kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec \
  --attributes OneMinuteRate

# Check partition distribution
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --under-replicated-partitions
```

## Performance Tuning

### Performance Considerations

MonsterMQ's Kafka integration uses fixed performance settings optimized for typical MQTT workloads. For advanced performance tuning, you would need to modify the underlying Kafka cluster configuration rather than MonsterMQ settings.

### JVM Tuning

```bash
# Kafka JVM options
export KAFKA_HEAP_OPTS="-Xmx4G -Xms4G"
export KAFKA_JVM_PERFORMANCE_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=20"
```

### OS Tuning

```bash
# Linux kernel parameters
echo 'net.core.wmem_default=131072' >> /etc/sysctl.conf
echo 'net.core.rmem_default=131072' >> /etc/sysctl.conf
echo 'net.ipv4.tcp_wmem=4096 65536 2048000' >> /etc/sysctl.conf
echo 'net.ipv4.tcp_rmem=4096 65536 2048000' >> /etc/sysctl.conf
sysctl -p
```

## Troubleshooting

### Common Issues

1. **Connection Failures**
   - Verify Kafka broker is accessible at configured server address
   - Check network connectivity and firewall rules
   - Ensure Kafka cluster is running and healthy

2. **Consumer Lag**
   ```bash
   # Reset consumer group offset
   kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 \
     --group monstermq-cluster \
     --reset-offsets \
     --to-earliest \
     --execute \
     --topic mqtt.messages
   ```

3. **Producer Timeouts**
   - Producer timeout settings are hardcoded in MonsterMQ
   - Check Kafka broker performance and network latency
   - Monitor Kafka logs for any broker-side issues

4. **Partition Imbalance**
   ```bash
   # Reassign partitions
   kafka-reassign-partitions.sh \
     --bootstrap-server localhost:9092 \
     --reassignment-json-file reassignment.json \
     --execute
   ```

### Debug Logging

```yaml
# Enable Kafka debug logging
Logging:
  Loggers:
    org.apache.kafka: DEBUG
    at.rocworks.kafka: DEBUG
```

## Integration Examples

### Spring Boot Integration

```kotlin
@Configuration
class KafkaConfig {
    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, MqttMessage> {
        val props = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to JsonSerializer::class.java
        )
        val factory = DefaultKafkaProducerFactory<String, MqttMessage>(props)
        return KafkaTemplate(factory)
    }
}
```

### Python Client

```python
from kafka import KafkaConsumer, KafkaProducer
import json

# Consumer
consumer = KafkaConsumer(
    'mqtt.messages',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='python-consumer'
)

for message in consumer:
    print(f"Topic: {message.value['topic']}")
    print(f"Payload: {message.value['payload']}")

# Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer.send('mqtt.messages', {
    'topic': 'test/topic',
    'payload': 'test message',
    'timestamp': '2024-01-15T10:00:00Z'
})
```

## Best Practices

1. **Partition Strategy** - Use consistent hashing on MQTT topic for partition assignment
2. **Compression** - Use LZ4 for real-time, GZIP for archival
3. **Replication** - Set replication factor to min(3, broker_count)
4. **Monitoring** - Set up alerts for consumer lag and broker health
5. **Retention** - Configure based on storage capacity and compliance
6. **Security** - Always use SSL/SASL in production
7. **Backup** - Regular snapshots of Kafka data directories
8. **Capacity Planning** - Plan for 2x expected throughput