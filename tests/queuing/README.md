# MQTT Persistent Session / Queuing Tests

Tests and scripts for verifying persistent session message delivery in MonsterMQ.

## Prerequisites

```bash
pip install paho-mqtt PyYAML pytest pytest-timeout
```

## Automated Test (pytest)

`test_queuing.py` verifies that QoS 1/2 messages published while a subscriber is offline are delivered without gaps when the subscriber reconnects.

### What it does

1. Subscriber connects with `clean_session=False` and subscribes
2. Publisher starts and waits until the subscriber confirms receipt of the first message
3. Publisher keeps publishing while the subscriber cycles through disconnect/reconnect every 25% of messages
4. After all cycles, verifies the full sequence has no missing values

### Running

```bash
# All rates (1, 10, 100 msg/s) with QoS 1
pytest queuing/test_queuing.py -v -s

# Single rate
pytest queuing/test_queuing.py -v -s --rate 10

# QoS 2
pytest queuing/test_queuing.py -v -s --qos 2

# Custom disconnect duration
pytest queuing/test_queuing.py -v -s --disconnect-seconds 10

# Clustering: different broker for publisher and subscriber
pytest queuing/test_queuing.py -v -s --pub-host broker1 --sub-host broker2

# Different ports
pytest queuing/test_queuing.py -v -s --pub-host broker1 --pub-port 1883 --sub-host broker2 --sub-port 1884
```

### CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--pub-host` | `localhost` / `$MQTT_BROKER` | Publisher broker host |
| `--pub-port` | `1883` / `$MQTT_PORT` | Publisher broker port |
| `--sub-host` | same as `--pub-host` | Subscriber broker host |
| `--sub-port` | same as `--pub-port` | Subscriber broker port |
| `--qos` | `1` | QoS level (1 or 2) |
| `--rate` | all (1, 10, 100) | Publish rate in msg/s |
| `--disconnect-seconds` | `5` | Duration of the publish phase |
| `--username` | `Test` / `$MQTT_USERNAME` | MQTT username |
| `--password` | `Test` / `$MQTT_PASSWORD` | MQTT password |

---

## Manual Test Scripts

Standalone scripts for interactive/manual testing of persistent session behavior. Configured via `config.yaml`.

### producer.py

Publishes messages with increasing sequence numbers.

```bash
python producer.py                          # defaults from config.yaml
python producer.py --qos 2                  # override QoS
python producer.py --burst-count 10 --burst-delay 0.01  # high frequency
```

### consumer.py

Subscribes with `clean_session=False` and detects gaps in the sequence.

```bash
python consumer.py                          # start/resume
python consumer.py --reset                  # reset session and state
python consumer.py --start-from 100         # start from specific sequence
```

### Manual Test Procedure

```bash
# Terminal 1: start consumer
python consumer.py

# Terminal 2: start producer
python producer.py

# Terminal 1: Ctrl+C to stop consumer (producer keeps running)
# Wait a few seconds for messages to queue up
# Restart consumer
python consumer.py

# Check output for gaps
```

### Configuration (config.yaml)

| Section | Key | Default | Description |
|---------|-----|---------|-------------|
| `broker` | `host` | `localhost` | Broker hostname |
| `broker` | `port` | `1883` | Broker port |
| `broker` | `keepalive` | `60` | Keep-alive interval (seconds) |
| | `topic` | `test/sequence` | MQTT topic |
| | `qos` | `1` | QoS level (0, 1, 2) |
| `producer` | `client_id` | `producer_test` | Producer client ID |
| `producer` | `burst_count` | `5` | Messages per burst |
| `producer` | `burst_delay` | `0.2` | Delay between bursts (seconds) |
| `consumer` | `client_id` | `consumer_test_persistent` | Consumer client ID |

---

## Troubleshooting

If messages are lost:
1. Check the `queuedmessagesclients` table for stuck messages
2. Check broker logs for errors during delivery
3. Verify the subscriber's persistent session exists (connected with `clean_session=False`)

```sql
-- Check queued messages for a client
SELECT COUNT(*) FROM queuedmessagesclients WHERE client_id = 'consumer_test_persistent';

-- Check message status distribution
SELECT status, COUNT(*) FROM queuedmessagesclients GROUP BY status;
-- status: 0=pending, 1=in-flight, 2=delivered
```
