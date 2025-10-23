# Monitoring and Metrics

MonsterMQ publishes real-time metrics to MQTT $SYS topics that show the performance of bulk messaging and bulk processing features.

## Bulk Messaging Metrics

**Topic:** `$SYS/brokers/<node>/messaging`

Metrics show the batching and delivery of messages to clients and remote nodes.

### Example Metric:
```json
{
  "clientAddedPerSec": 150000,
  "clientFlushedPerSec": 145000,
  "nodeAddedPerSec": 50000,
  "nodeFlushedPerSec": 48000,
  "clientBuffers": 5,
  "nodeBuffers": 2,
  "bufferedClientMessages": 234,
  "bufferedNodeMessages": 567
}
```

### Client Messages

**`clientAddedPerSec`** - Messages/second being added to per-client buffers
- Count of messages entering client delivery buffers
- Only counted when subscribers exist for a topic
- Reflects actual incoming publish rate that has subscribers

**`clientFlushedPerSec`** - Messages/second being sent to clients via eventBus
- Count of messages actually delivered from buffers
- Flush triggers when:
  - Buffer reaches `BulkSize` messages (default 1000), OR
  - `TimeoutMS` milliseconds pass (default 100ms)
- This is the actual client delivery rate

**`clientBuffers`** - Number of active per-client buffers
- One buffer per client that has buffered messages
- Example: 100 clients subscribe but only 5 get messages = 5 buffers

**`bufferedClientMessages`** - Total messages currently in all client buffers
- Messages waiting to be flushed
- Shows current backlog at this moment
- High value indicates messages accumulating faster than flushing

### Node Messages (Cluster Mode Only)

**`nodeAddedPerSec`** - Messages/second being added to per-node buffers
- Only in cluster mode when messages need to go to remote nodes

**`nodeFlushedPerSec`** - Messages/second being sent to remote nodes via eventBus

**`nodeBuffers`** - Number of remote nodes with buffered messages

**`bufferedNodeMessages`** - Total messages waiting for remote nodes

## Bulk Processing Metrics

**Topic:** `$SYS/brokers/<node>/processing`

Metrics show the batching and parallel processing of published messages.

### Example Metric:
```json
{
  "workersCount": 4,
  "batchesPerSec": 150,
  "messagesPerSec": 150000,
  "workers": [
    {"id": 0, "messagesPerSec": 37500, "queueSize": 0},
    {"id": 1, "messagesPerSec": 38000, "queueSize": 2},
    {"id": 2, "messagesPerSec": 37200, "queueSize": 0},
    {"id": 3, "messagesPerSec": 37300, "queueSize": 1}
  ]
}
```

### Pool-Level Metrics

**`workersCount`** - Number of worker threads in the pool
- Configured via `BulkProcessing.WorkerThreads` (default 4)
- Typically tune to ~1 thread per 75k msg/s

**`batchesPerSec`** - Batches processed per second across all workers
- Batches are collections of messages grouped by topic
- Higher is better (more grouping = better efficiency)

**`messagesPerSec`** - Total messages processed per second across all workers
- Sum of all messages in all batches
- Overall throughput metric

### Per-Worker Metrics

**`workers[].messagesPerSec`** - Messages processed per second by this worker
- Per-second delta rate (not lifetime)
- Should be roughly balanced across workers
- Imbalance indicates uneven load distribution

**`workers[].queueSize`** - Messages currently waiting in this worker's queue
- Shows backlog for this specific worker
- High value indicates that worker is slow or overloaded

## Interpreting Metrics

### Healthy Bulk Messaging State
```json
{
  "clientAddedPerSec": 100000,
  "clientFlushedPerSec": 100000,    // ← Same as added
  "bufferedClientMessages": 50       // ← Small buffer
}
```
- Messages flowing smoothly through buffers
- No accumulation (batching working efficiently)

### Problem: Messages Accumulating
```json
{
  "clientAddedPerSec": 100000,
  "clientFlushedPerSec": 50000,     // ← Half the incoming rate
  "bufferedClientMessages": 50000    // ← Growing backlog
}
```
- Messages arriving faster than delivery
- Clients are slow or network is congested
- Consider:
  - Increasing `BulkSize` (batch more messages)
  - Increasing `TimeoutMS` (wait longer before sending)
  - Checking client performance

### Healthy Bulk Processing State
```json
{
  "batchesPerSec": 150,
  "messagesPerSec": 150000,
  "workers": [
    {"id": 0, "messagesPerSec": 37500, "queueSize": 0},
    {"id": 1, "messagesPerSec": 37500, "queueSize": 0},
    {"id": 2, "messagesPerSec": 37500, "queueSize": 0},
    {"id": 3, "messagesPerSec": 37500, "queueSize": 0}
  ]
}
```
- Load evenly distributed across workers
- No queue backlog
- Good throughput

### Problem: Uneven Worker Load
```json
{
  "workers": [
    {"id": 0, "messagesPerSec": 80000, "queueSize": 50},
    {"id": 1, "messagesPerSec": 10000, "queueSize": 0},
    {"id": 2, "messagesPerSec": 5000, "queueSize": 0},
    {"id": 3, "messagesPerSec": 5000, "queueSize": 0}
  ]
}
```
- Worker 0 is overloaded (high queue size)
- Workers 1-3 are idle
- May indicate specific topics with many subscribers
- Consider:
  - Increasing `WorkerThreads`
  - Checking if topic subscriptions are skewed
  - Reviewing client performance

## Configuration for Monitoring

To enable these metrics, ensure bulk processing/messaging are enabled:

```yaml
BulkMessaging:
  Enabled: true
  TimeoutMS: 100
  BulkSize: 1000

BulkProcessing:
  Enabled: true
  TimeoutMS: 50
  BulkSize: 10000
  WorkerThreads: 4
```

Metrics are published every 1 second (hardcoded in code) when these features are enabled.

## Consuming Metrics

Subscribe to the $SYS topics to consume metrics:

```bash
# Bulk messaging metrics
mosquitto_sub -h localhost -t '$SYS/brokers/+/messaging'

# Bulk processing metrics
mosquitto_sub -h localhost -t '$SYS/brokers/+/processing'
```

Or use any MQTT client/library to subscribe and forward to your monitoring system (Prometheus, InfluxDB, Grafana, etc.)
