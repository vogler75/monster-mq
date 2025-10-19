# MonsterMQ GraphQL Test Scripts

This directory contains Python test scripts for validating MonsterMQ GraphQL functionality, including real-time subscriptions to MQTT topic updates and system logs.

## Quick Start

### Prerequisites

```bash
pip install websockets requests
```

### Basic Usage

#### 1. Test Topic Subscriptions (Multiple Filters)

Subscribe to multiple MQTT topic filters simultaneously:

```bash
# Monitor device metrics from multiple topics
python test_graphql_topic_subscriptions.py \
  --filters "device/+/metrics" "device/+/status" "device/+/error"

# Monitor all sensors
python test_graphql_topic_subscriptions.py --filters "sensor/#"

# Monitor specific topics
python test_graphql_topic_subscriptions.py --filters "temperature" "humidity" "pressure"
```

#### 2. Publish Test Messages

Use the publisher to send test messages:

```bash
# Publish a single message
python test_graphql_publisher.py \
  --topic "test/message" \
  --payload '{"value": 42}'

# Publish repeatedly with delay
python test_graphql_publisher.py \
  --topic "sensor/temp" \
  --payload "25.5" \
  --repeat 5 \
  --delay 1

# Continuous monitoring mode (publishes test messages every 2 seconds)
python test_graphql_publisher.py --monitor --interval 2
```

## Test Scripts

### test_graphql_topic_subscriptions.py

Subscribe to real-time MQTT message updates from **multiple topic filters simultaneously**.

**Features:**
- Monitor multiple independent topic hierarchies in one subscription
- Efficient bandwidth usage (single WebSocket connection)
- Flexible topic filtering with MQTT wildcards (`+` and `#`)
- Support for JSON and BINARY payload formats
- Pretty-printed output with timestamps and metadata
- Automatic payload formatting (JSON pretty-print, binary display)

**GraphQL Subscription Used:**
```graphql
subscription MultiTopicUpdates($filters: [String!]!, $format: DataFormat) {
    multiTopicUpdates(topicFilters: $filters, format: $format) {
        topic
        payload
        format
        timestamp
        qos
        retained
        clientId
    }
}
```

**Usage Examples:**

```bash
# Monitor device metrics (multiple independent topics)
python test_graphql_topic_subscriptions.py \
  --filters "device/1/metrics" "device/2/metrics" "device/3/metrics"

# Monitor a single specific topic
python test_graphql_topic_subscriptions.py --filters "sensor/temperature"

# Monitor system topics (using wildcards)
python test_graphql_topic_subscriptions.py --filters "\$SYS/#"

# Monitor with binary data format (e.g., images)
python test_graphql_topic_subscriptions.py \
  --format BINARY \
  --filters "camera/snapshot"

# Connect to custom broker URL
python test_graphql_topic_subscriptions.py \
  --url "ws://broker.example.com:4000/graphqlws" \
  --filters "sensor/#"
```

**Command-Line Options:**

```
--url URL              GraphQL WebSocket URL (default: ws://localhost:4000/graphqlws)
--filters FILTERS      MQTT topic filters (supports + and # wildcards). Default: #
--format FORMAT        Data format: JSON or BINARY (default: JSON)
```

### test_graphql_publisher.py

Publish test messages to MQTT topics via GraphQL mutations.

**Features:**
- Publish single or multiple messages
- Control QoS level and retention flag
- Repeat mode with configurable delays
- Monitor mode for continuous test message publishing
- Support for JSON and BINARY payloads
- Batch publishing to multiple topics

**GraphQL Mutation Used:**
```graphql
mutation PublishMessage($input: PublishInput!) {
    publish(input: $input) {
        success
        topic
        timestamp
        error
    }
}
```

**Usage Examples:**

```bash
# Publish a simple message
python test_graphql_publisher.py \
  --topic "test/message" \
  --payload "Hello MQTT"

# Publish JSON data
python test_graphql_publisher.py \
  --topic "sensor/temp" \
  --payload '{"temperature": 23.5, "unit": "celsius"}'

# Publish with QoS and retention
python test_graphql_publisher.py \
  --topic "device/status" \
  --payload "online" \
  --qos 1 \
  --retained

# Publish multiple times
python test_graphql_publisher.py \
  --topic "heartbeat" \
  --payload "ping" \
  --repeat 10 \
  --delay 1  # 1 second between publishes

# Publish to multiple topics (same payload)
python test_graphql_publisher.py \
  --topics "device/1/temp" "device/2/temp" "device/3/temp" \
  --payload "25"

# Monitor mode: publish test messages every 2 seconds
python test_graphql_publisher.py --monitor --interval 2

# Publish binary data
python test_graphql_publisher.py \
  --topic "camera/snapshot" \
  --payload "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJ..." \
  --format BINARY
```

**Command-Line Options:**

```
--url URL       GraphQL HTTP URL (default: http://localhost:4000/graphql)
--topic TOPIC   Single topic to publish to
--topics TOPICS Multiple topics to publish to (same payload)
--payload DATA  Message payload (default: empty JSON object)
--qos QOS       Quality of Service: 0, 1, or 2 (default: 0)
--retained      Flag to retain message on broker
--format FORMAT Data format: JSON or BINARY (default: JSON)
--repeat COUNT  Number of times to publish (default: 1)
--delay SECS    Delay between publishes in seconds (default: 0)
--monitor       Monitor mode: continuously publish test messages
--interval SECS Interval for monitor mode in seconds (default: 2)
```

### test_graphql_system_logs.py

Subscribe to real-time system logs with advanced filtering.

**Features:**
- Filter logs by level (INFO, WARNING, SEVERE)
- Filter by logger name (regex support)
- Filter by source class/method (regex support)
- Filter by message content (regex support)
- Pretty-printed output with colors
- Exception stack trace display

**Usage Examples:**

```bash
# Monitor all logs
python test_graphql_system_logs.py

# Monitor only errors
python test_graphql_system_logs.py --level SEVERE

# Monitor specific component
python test_graphql_system_logs.py --logger ".*GraphQL.*"

# Monitor connection events
python test_graphql_system_logs.py --message ".*connect.*"

# Combine filters
python test_graphql_system_logs.py --level WARNING --logger ".*Session.*"
```

## End-to-End Testing Workflow

### Setup

1. Start MonsterMQ with GraphQL enabled:

```yaml
GraphQL:
  Enabled: true
  Port: 4000
  Path: /graphql
```

### Test Scenario 1: Multi-Topic Subscription

**Terminal 1 - Subscribe to multiple topics:**
```bash
python test_graphql_topic_subscriptions.py \
  --filters "test/+/message" "status/#"
```

**Terminal 2 - Publish messages:**
```bash
# Publish to matching topics
python test_graphql_publisher.py --topic "test/device1/message" --payload "data1"
python test_graphql_publisher.py --topic "test/device2/message" --payload "data2"
python test_graphql_publisher.py --topic "status/online" --payload "ok"
python test_graphql_publisher.py --topic "status/cpu" --payload "42"
```

**Expected Result:** All messages appear in Terminal 1's subscription.

### Test Scenario 2: Continuous Monitoring

**Terminal 1 - Subscribe to all topics:**
```bash
python test_graphql_topic_subscriptions.py --filters "#"
```

**Terminal 2 - Publish test messages continuously:**
```bash
python test_graphql_publisher.py --monitor --interval 2
```

**Expected Result:** Terminal 1 continuously receives test messages with timestamps.

### Test Scenario 3: System Logs Monitoring

**Terminal 1 - Monitor system logs:**
```bash
python test_graphql_system_logs.py --level SEVERE
```

**Terminal 2 - Generate activity (publish or query):**
```bash
python test_graphql_publisher.py --topic "test/topic" --payload "test"
```

**Expected Result:** System events appear in Terminal 1 if any errors occur.

## Configuration

### MonsterMQ GraphQL Configuration

Ensure your `config.yaml` includes:

```yaml
GraphQL:
  Enabled: true
  Port: 4000          # GraphQL HTTP port
  Path: /graphql      # GraphQL HTTP path
```

The WebSocket endpoint is automatically available at:
- `ws://<host>:<port><path>ws` (e.g., `ws://localhost:4000/graphqlws`)

### Custom Broker URLs

If MonsterMQ is running on a different host/port, pass the URL:

```bash
python test_graphql_topic_subscriptions.py \
  --url "ws://192.168.1.100:5000/graphqlws" \
  --filters "sensor/#"
```

## Troubleshooting

### Connection Refused

**Error:** `WebSocket error: Connection refused`

**Solution:** Ensure MonsterMQ is running with GraphQL enabled:
```bash
java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt
```

### Module Not Found

**Error:** `ModuleNotFoundError: No module named 'websockets'`

**Solution:** Install dependencies:
```bash
pip install websockets requests
```

### No Messages Received

**Check:**
1. Is MonsterMQ connected and receiving messages?
2. Do the topic filters match published topics?
3. Try monitoring all topics: `--filters "#"`

### WebSocket Protocol Mismatch

**Error:** `WebSocket error: invalid subprotocol`

**Note:** These scripts use the `graphql-transport-ws` subprotocol. Ensure your broker supports this protocol.

## Performance Notes

- **multiTopicUpdates** is more efficient than multiple single-topic subscriptions
- WebSocket connections have low latency and are suitable for real-time monitoring
- Payload formatting (JSON pretty-print) may add minor overhead for large messages
- Monitor mode is useful for stress testing and continuous validation

## Protocol Details

### WebSocket Connection Flow

1. Client connects to `ws://localhost:4000/graphqlws` with subprotocol `graphql-transport-ws`
2. Client sends `connection_init` message
3. Server responds with `connection_ack`
4. Client sends `subscribe` message with GraphQL query
5. Server sends `next` messages for each update
6. Client sends `complete` to unsubscribe

### Message Types

- `connection_init` - Initialize connection
- `connection_ack` - Server acknowledges
- `subscribe` - Start subscription
- `next` - Data payload
- `error` - Subscription error
- `complete` - End subscription

## References

- [GraphQL Documentation](../doc/graphql.md)
- [GraphQL System Logs Documentation](../doc/graphql-system-logs.md)
- [GraphQL WebSocket Protocol](https://github.com/enisdenjo/graphql-ws)
- [MQTT Specification](http://mqtt.org/)
