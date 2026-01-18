# MQTT Persistent Session Test Scripts

These scripts help test persistent session handling and detect message loss in MonsterMQ.

## Prerequisites

```bash
pip install paho-mqtt PyYAML
```

## Configuration

The scripts use a `config.yaml` file for configuration. The file includes comprehensive documentation for all available options:

### Broker Settings
- `broker.host` - MQTT broker hostname or IP (default: localhost)
- `broker.port` - MQTT broker port (default: 1883)
- `broker.keepalive` - Keep-alive interval in seconds (default: 60)

### Topic and QoS
- `topic` - MQTT topic for message exchange (default: test/sequence)
- `qos` - Quality of Service level: 0, 1, or 2 (default: 1)

### Producer Settings
- `producer.client_id` - Unique client identifier (default: producer_test)
- `producer.burst_count` - Messages per burst (default: 1)
- `producer.burst_delay` - Delay between bursts in seconds (default: 0.1)
- `producer.state_file` - File for persisting sequence state (default: .producer_state)

### Consumer Settings
- `consumer.client_id` - Unique client identifier for persistent sessions (default: consumer_test_persistent)
- `consumer.state_file` - File for persisting sequence state (default: .consumer_state)

All settings have detailed comments in the config file explaining their purpose and providing examples. Command-line arguments can override any config file setting.

## Scripts

### producer.py
Publishes messages with increasing sequence numbers to test message delivery.
- Sends messages in configurable bursts (default: 1 message per burst)
- Waits 100ms between bursts (configurable)
- Uses QoS 1 for guaranteed delivery (configurable)
- Command-line arguments:
  - `--qos {0,1,2}` - Override QoS level from config
  - `--burst-count N` - Override number of messages per burst
  - `--burst-delay SECONDS` - Override delay between bursts

### consumer.py
Subscribes with a persistent session and detects gaps in message sequences.
- Uses `clean_session=False` for persistent sessions
- Tracks expected sequence numbers
- Logs warnings if gaps are detected (indicating message loss)
- Persists across disconnections
- **Saves last received sequence to `.consumer_state` file** - allows resuming from correct position after script restarts
- Command-line arguments:
  - `--qos {0,1,2}` - Override QoS level from config
  - `--reset` - Reset session and delete state file
  - `--start-from N` - Override expected starting sequence number

## Testing Persistent Session Handling

### Test 1: Basic Message Delivery
```bash
# Terminal 1: Start consumer
python test/consumer.py

# Terminal 2: Start producer
python test/producer.py

# Let it run for a few seconds, then stop both with Ctrl+C
```

Expected: No gaps detected, all messages received in order.

### Test 2: Disconnect/Reconnect During High Load
```bash
# Terminal 1: Start consumer
python test/consumer.py

# Terminal 2: Start producer
python test/producer.py

# Terminal 1: Stop consumer with Ctrl+C (while producer is still running)
# Wait 2-3 seconds for messages to queue up
# Restart consumer
python test/consumer.py

# Check for gaps in the sequence
```

Expected: No gaps detected. All messages queued during disconnection should be delivered when the persistent session reconnects.

### Test 3: Multiple Disconnect/Reconnect Cycles
Repeat Test 2 multiple times to stress test the queuing mechanism.

### Test 4: State Persistence Across Restarts
```bash
# Terminal 1: Start consumer
python test/consumer.py

# Terminal 2: Start producer
python test/producer.py

# Terminal 1: Stop consumer with Ctrl+C (note the last received sequence)
# The state is saved to test/.consumer_state

# Terminal 1: Restart consumer - it will resume from where it left off
python test/consumer.py
```

Expected: Consumer loads state from disk and resumes checking from the last received sequence. No gaps detected.

### Test 5: Reset Session
```bash
# Reset the persistent session, delete state file, and start fresh
python test/consumer.py --reset
```

### Test 6: Manual Sequence Override
```bash
# Start from a specific sequence number (useful for debugging)
python test/consumer.py --start-from 100
```

### Test 7: Check Database State
While testing, you can check the `queuedmessagesclients` table to see if messages are being properly cleaned up after delivery:

```sql
-- Check queued messages
SELECT * FROM queuedmessagesclients WHERE clientid = 'consumer_test_persistent';

-- Count queued messages
SELECT COUNT(*) FROM queuedmessagesclients WHERE clientid = 'consumer_test_persistent';
```

## Interpreting Results

### Good (No Issues)
```
[Consumer] ✓ Received: 100 (expected 100)
[Consumer] ✓ Received: 101 (expected 101)
...
Statistics:
  Total messages received: 150
  Next expected sequence: 150
  Gaps detected: 0
  ✓ No gaps detected - all messages received in order
```

### Bad (Message Loss)
```
[Consumer] ✓ Received: 100 (expected 100)
[Consumer] ⚠ WARNING: Gap detected! Received 115, expected 101
[Consumer] ⚠ Missing 14 message(s): 101 to 114
...
Statistics:
  Total messages received: 136
  Next expected sequence: 150
  Gaps detected: 1
  ⚠ WARNING: 1 gap(s) detected - possible message loss!
```

### Troubleshooting

If gaps are detected:
1. Check if messages remain in `queuedmessagesclients` table
2. Check broker logs for errors during message delivery
3. Verify the reconnection buffer is being properly flushed
4. Check if messages are being sent but not deleted from the database

If no gaps but database still has entries:
- Messages are being delivered but not cleaned up
- Check the deletion logic in `SessionHandler.deleteMessage()`
