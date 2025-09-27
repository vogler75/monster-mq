# Performance & Monitoring

This guide covers performance optimization, monitoring, and tuning for MonsterMQ deployments. Learn how to maximize throughput, minimize latency, and monitor system health.

## Performance Benchmarks

### Expected Performance

| Metric | Single Node | 3-Node Cluster | 5-Node Cluster |
|--------|-------------|----------------|----------------|
| **Messages/sec** | 50,000 | 150,000 | 250,000 |
| **Concurrent Clients** | 10,000 | 30,000 | 50,000 |
| **Subscriptions** | 100,000 | 300,000 | 500,000 |
| **Message Latency** | < 5ms | < 10ms | < 15ms |
| **Memory Usage** | 2-4 GB | 2-4 GB/node | 2-4 GB/node |

## JVM Tuning

### Memory Configuration

```bash
# Recommended JVM settings
java -Xms4g -Xmx4g \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=200 \
  -XX:+ParallelRefProcEnabled \
  -XX:+UnlockExperimentalVMOptions \
  -XX:+DisableExplicitGC \
  -XX:+AlwaysPreTouch \
  -XX:G1NewSizePercent=30 \
  -XX:G1MaxNewSizePercent=40 \
  -XX:G1HeapRegionSize=8M \
  -XX:G1ReservePercent=15 \
  -XX:G1HeapWastePercent=5 \
  -XX:G1MixedGCCountTarget=5 \
  -XX:InitiatingHeapOccupancyPercent=45 \
  -XX:G1MixedGCLiveThresholdPercent=90 \
  -XX:G1RSetUpdatingPauseTimePercent=5 \
  -XX:SurvivorRatio=8 \
  -XX:+PerfDisableSharedMem \
  -XX:MaxTenuringThreshold=1 \
  -jar monstermq.jar
```

### GC Tuning by Workload

#### High Throughput
```bash
# Optimize for throughput
-XX:+UseParallelGC \
-XX:ParallelGCThreads=8 \
-XX:MaxGCPauseMillis=500
```

#### Low Latency
```bash
# Optimize for low latency
-XX:+UseZGC \
-XX:ZCollectionInterval=30 \
-XX:ZUncommitDelay=300
```

#### Balanced
```bash
# Balanced performance (recommended)
-XX:+UseG1GC \
-XX:MaxGCPauseMillis=200 \
-XX:G1HeapRegionSize=16M
```

## Vert.x Tuning

### Event Loop Configuration

```yaml
Vertx:
  EventLoopPoolSize: 8  # 2 * CPU cores
  WorkerPoolSize: 20
  InternalBlockingPoolSize: 20
  BlockedThreadCheckInterval: 1000
  MaxEventLoopExecuteTime: 2000000000  # 2 seconds
  MaxWorkerExecuteTime: 60000000000    # 60 seconds
  WarningExceptionTime: 5000000000     # 5 seconds
```

### Network Tuning

```yaml
Network:
  # TCP settings
  TCP:
    SendBufferSize: 131072  # 128 KB
    ReceiveBufferSize: 131072
    TCPNoDelay: true
    TCPKeepAlive: true
    SoLinger: -1
    AcceptBacklog: 1024

  # HTTP settings (for GraphQL/Dashboard)
  HTTP:
    MaxWebSocketFrameSize: 65536
    MaxWebSocketMessageSize: 262144
    CompressionSupported: true
    CompressionLevel: 6
```

## Database Optimization

### PostgreSQL Performance

```sql
-- Connection pool tuning
ALTER SYSTEM SET max_connections = 500;
ALTER SYSTEM SET shared_buffers = '1GB';
ALTER SYSTEM SET effective_cache_size = '3GB';
ALTER SYSTEM SET work_mem = '8MB';

-- Write performance
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET default_statistics_target = 100;
ALTER SYSTEM SET random_page_cost = 1.1;

-- Vacuum settings
ALTER SYSTEM SET autovacuum = on;
ALTER SYSTEM SET autovacuum_max_workers = 4;
ALTER SYSTEM SET autovacuum_naptime = '30s';

-- Apply changes
SELECT pg_reload_conf();
```

### Index Optimization

```sql
-- Essential indexes for MonsterMQ
CREATE INDEX CONCURRENTLY idx_messages_topic_timestamp
  ON messages(topic, timestamp DESC);

CREATE INDEX CONCURRENTLY idx_messages_client_timestamp
  ON messages(client_id, timestamp DESC);

CREATE INDEX CONCURRENTLY idx_sessions_client_active
  ON sessions(client_id) WHERE active = true;

CREATE INDEX CONCURRENTLY idx_retained_topic
  ON retained_messages(topic);

-- Analyze tables after indexing
ANALYZE messages;
ANALYZE sessions;
ANALYZE retained_messages;
```

### Connection Pooling

```yaml
Database:
  ConnectionPool:
    MinSize: 5
    MaxSize: 50
    AcquireTimeout: 30000
    IdleTimeout: 600000
    MaxLifetime: 1800000
    ConnectionTestQuery: "SELECT 1"
    TestOnBorrow: true
```

## Message Processing

### Batch Processing

```yaml
MessageProcessing:
  Batching:
    Enabled: true
    BatchSize: 1000
    BatchTimeout: 100  # ms
    MaxBatchMemory: 10485760  # 10 MB

  # Parallel processing
  Parallelism:
    PublishThreads: 4
    SubscribeThreads: 4
    ArchiveThreads: 2
```

### Queue Configuration

```yaml
Queues:
  # In-memory queue sizes
  PublishQueueSize: 10000
  SubscribeQueueSize: 10000

  # Overflow handling
  OverflowStrategy: DROP_OLDEST  # DROP_OLDEST, DROP_NEWEST, BLOCK

  # Priority queuing
  PriorityQueue:
    Enabled: true
    Levels: 3  # HIGH, NORMAL, LOW
```

## Monitoring Setup

### Prometheus Integration

```yaml
Monitoring:
  Prometheus:
    Enabled: true
    Port: 9090
    Path: /metrics
    IncludeDefaultMetrics: true

  # Custom metrics
  CustomMetrics:
    - messages_published_total
    - messages_delivered_total
    - clients_connected
    - subscriptions_active
    - message_processing_duration
```

### Grafana Dashboard

```json
{
  "dashboard": {
    "title": "MonsterMQ Performance",
    "panels": [
      {
        "title": "Message Throughput",
        "targets": [
          {
            "expr": "rate(messages_published_total[5m])"
          }
        ]
      },
      {
        "title": "Client Connections",
        "targets": [
          {
            "expr": "clients_connected"
          }
        ]
      },
      {
        "title": "Message Latency",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, message_processing_duration)"
          }
        ]
      }
    ]
  }
}
```

### Health Checks

```yaml
HealthCheck:
  Enabled: true
  Port: 8080
  Path: /health

  Checks:
    - Name: database
      Timeout: 5000
      Critical: true

    - Name: memory
      Threshold: 90  # percentage
      Critical: true

    - Name: diskspace
      Threshold: 90  # percentage
      Critical: false

    - Name: cluster
      MinNodes: 2
      Critical: true
```

## Performance Testing

### Load Testing with MQTT-Bench

```bash
# Install mqtt-bench
go get github.com/takanorig/mqtt-bench

# Basic load test
mqtt-bench -broker tcp://localhost:1883 \
  -clients 1000 \
  -count 100 \
  -size 256 \
  -topic "test/bench" \
  -username admin \
  -password password

# Sustained load test
mqtt-bench -broker tcp://localhost:1883 \
  -clients 5000 \
  -count 0 \
  -interval 100 \
  -size 1024 \
  -topic "load/test/+"
```

### JMeter Test Plan

```xml
<TestPlan>
  <ThreadGroup>
    <numThreads>1000</numThreads>
    <rampUp>60</rampUp>
    <duration>300</duration>
  </ThreadGroup>

  <MQTTPublisher>
    <server>localhost</server>
    <port>1883</port>
    <topic>test/${__threadNum}</topic>
    <qos>1</qos>
    <message>${__RandomString(256)}</message>
  </MQTTPublisher>

  <ResponseAssertion>
    <testField>RESPONSE_CODE</testField>
    <pattern>200</pattern>
  </ResponseAssertion>
</TestPlan>
```

## Optimization Strategies

### Topic Hierarchy Optimization

```yaml
# Efficient topic design
Good:
  - sensors/building1/floor1/temp
  - sensors/building1/floor2/temp

Bad:
  - sensors/temp/building1/floor1
  - sensors/temp/building1/floor2

# Why: Subscribers often filter by location, not sensor type
```

### Subscription Optimization

```yaml
Subscriptions:
  # Limit wildcard depth
  MaxWildcardDepth: 3

  # Cache compiled patterns
  PatternCache:
    Enabled: true
    Size: 10000
    TTL: 3600

  # Optimize matching algorithm
  MatchingAlgorithm: TRIE  # TRIE, HASHMAP, LINEAR
```

### Message Compression

```yaml
Compression:
  Enabled: true
  Algorithm: LZ4  # LZ4, GZIP, SNAPPY
  MinSize: 1024  # Only compress messages > 1KB
  Level: 3  # 1-9, higher = better compression, more CPU

  # Per-topic compression
  Topics:
    - Pattern: "data/+"
      Algorithm: GZIP
      Level: 6
```

## Resource Monitoring

### CPU Monitoring

```bash
# Monitor CPU usage
top -p $(pgrep -f monstermq) -d 1

# Thread dump for high CPU
jstack $(pgrep -f monstermq) > thread-dump.txt

# CPU profiling
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
  -jar monstermq.jar
```

### Memory Monitoring

```bash
# Heap dump on OutOfMemory
-XX:+HeapDumpOnOutOfMemoryError \
-XX:HeapDumpPath=/var/log/monstermq/

# Monitor heap usage
jmap -heap $(pgrep -f monstermq)

# Memory histogram
jmap -histo:live $(pgrep -f monstermq)

# GC logging
-Xlog:gc*:file=/var/log/monstermq/gc.log:time,uptime,level,tags:filecount=10,filesize=10M
```

### Network Monitoring

```bash
# Monitor network connections
netstat -anp | grep $(pgrep -f monstermq)

# Network throughput
iftop -i eth0 -f "port 1883"

# TCP statistics
ss -s
```

## Scaling Guidelines

### Vertical Scaling

```yaml
# Resource recommendations by load
SmallDeployment:  # < 1000 clients
  CPU: 2 cores
  Memory: 2 GB
  Disk: 10 GB SSD

MediumDeployment:  # < 10000 clients
  CPU: 4 cores
  Memory: 8 GB
  Disk: 50 GB SSD

LargeDeployment:  # < 50000 clients
  CPU: 8 cores
  Memory: 16 GB
  Disk: 200 GB SSD

EnterpriseDeployment:  # > 50000 clients
  CPU: 16+ cores
  Memory: 32+ GB
  Disk: 500+ GB NVMe
```

### Horizontal Scaling

```yaml
# Scaling triggers
AutoScaling:
  Metrics:
    - Type: CPU
      Threshold: 70%
      ScaleUp: 1
      CoolDown: 300

    - Type: Memory
      Threshold: 80%
      ScaleUp: 1
      CoolDown: 300

    - Type: Connections
      Threshold: 10000
      ScaleUp: 1
      CoolDown: 600

    - Type: MessageRate
      Threshold: 50000  # msg/sec
      ScaleUp: 2
      CoolDown: 300
```

## Performance Troubleshooting

### Common Issues

1. **High Memory Usage**
   ```bash
   # Check for memory leaks
   jmap -histo:live $(pgrep -f monstermq) | head -20

   # Tune heap size
   -Xmx8g -XX:MaxMetaspaceSize=512m
   ```

2. **Slow Message Delivery**
   ```yaml
   # Check queue sizes
   Monitoring:
     LogQueueStats: true
     QueueStatsInterval: 10000

   # Increase processing threads
   MessageProcessing:
     PublishThreads: 8
     DeliveryThreads: 8
   ```

3. **Database Bottlenecks**
   ```sql
   -- Check slow queries
   SELECT query, calls, mean_exec_time
   FROM pg_stat_statements
   WHERE mean_exec_time > 100
   ORDER BY mean_exec_time DESC;
   ```

### Performance Checklist

- [ ] JVM heap sized appropriately
- [ ] GC tuned for workload
- [ ] Database indexes created
- [ ] Connection pools sized correctly
- [ ] Network buffers optimized
- [ ] Monitoring enabled
- [ ] Alerts configured
- [ ] Load testing completed
- [ ] Capacity planning done
- [ ] Backup/recovery tested

## Best Practices

1. **Monitor Everything** - Set up comprehensive monitoring from day one
2. **Test at Scale** - Load test with realistic workloads
3. **Plan Capacity** - Plan for 2x expected peak load
4. **Optimize Database** - Keep database well-maintained
5. **Use Caching** - Cache frequently accessed data
6. **Batch Operations** - Batch messages when possible
7. **Async Processing** - Use async patterns throughout
8. **Regular Maintenance** - Schedule regular maintenance windows