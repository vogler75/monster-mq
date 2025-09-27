# Clustering

MonsterMQ uses Hazelcast for clustering, providing high availability, load balancing, and horizontal scalability. This guide covers cluster setup, configuration, and management.

## Overview

MonsterMQ clustering provides:
- **High Availability** - Automatic failover when nodes fail
- **Load Balancing** - Distribute client connections across nodes
- **Shared State** - Session and subscription synchronization
- **Scalability** - Add/remove nodes dynamically

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ MonsterMQ   │────│ MonsterMQ   │────│ MonsterMQ   │
│   Node 1    │    │   Node 2    │    │   Node 3    │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       └──────────┬───────┴──────────────────┘
                  │
           ┌──────┴──────┐
           │  Hazelcast  │
           │   Cluster   │
           └──────┬──────┘
                  │
       ┌──────────┴──────────┐
       │                     │
┌──────┴──────┐     ┌────────┴────┐
│ PostgreSQL  │     │ Shared State │
│  (Primary)  │     │  (In-Memory) │
└─────────────┘     └──────────────┘
```

## Quick Start

### Basic Cluster Setup

1. **Start first node:**
```bash
java -jar monstermq.jar -cluster -config config-node1.yaml
```

2. **Start additional nodes:**
```bash
# Node 2
java -jar monstermq.jar -cluster -config config-node2.yaml

# Node 3
java -jar monstermq.jar -cluster -config config-node3.yaml
```

### Docker Compose Cluster

```yaml
version: '3.8'
services:
  monstermq-node1:
    image: rocworks/monstermq:latest
    command: ["-cluster"]
    environment:
      - HAZELCAST_MEMBER_NAME=node1
    ports:
      - "1883:1883"
    networks:
      - monster-cluster
    volumes:
      - ./config-cluster.yaml:/app/config.yaml

  monstermq-node2:
    image: rocworks/monstermq:latest
    command: ["-cluster"]
    environment:
      - HAZELCAST_MEMBER_NAME=node2
    ports:
      - "1884:1883"
    networks:
      - monster-cluster
    volumes:
      - ./config-cluster.yaml:/app/config.yaml

  monstermq-node3:
    image: rocworks/monstermq:latest
    command: ["-cluster"]
    environment:
      - HAZELCAST_MEMBER_NAME=node3
    ports:
      - "1885:1883"
    networks:
      - monster-cluster
    volumes:
      - ./config-cluster.yaml:/app/config.yaml

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: monster
      POSTGRES_USER: system
      POSTGRES_PASSWORD: manager
    networks:
      - monster-cluster

networks:
  monster-cluster:
    driver: bridge
```

## Hazelcast Configuration

### Basic Configuration

```yaml
# config-cluster.yaml
Cluster:
  Enabled: true

Hazelcast:
  ClusterName: monster-prod
  NetworkConfig:
    Port: 5701
    PortAutoIncrement: true
    PortCount: 100

  # Multicast discovery (default)
  Multicast:
    Enabled: true
    MulticastGroup: 224.2.2.3
    MulticastPort: 54327

  # TCP/IP discovery (recommended for production)
  TcpIp:
    Enabled: false
    Members:
      - 192.168.1.100
      - 192.168.1.101
      - 192.168.1.102
```

### Advanced Configuration

```yaml
Hazelcast:
  ClusterName: monster-prod

  # Member configuration
  MemberConfig:
    Attributes:
      datacenter: us-east-1
      rack: rack-1

  # Network configuration
  NetworkConfig:
    Port: 5701
    PortAutoIncrement: true
    OutboundPorts:
      - 33000-35000
    RestApiEnabled: true

    # SSL configuration
    SSL:
      Enabled: true
      FactoryClassName: com.hazelcast.nio.ssl.BasicSSLContextFactory
      Properties:
        keyStore: /path/to/keystore.jks
        keyStorePassword: password
        trustStore: /path/to/truststore.jks
        trustStorePassword: password

  # Partition group configuration
  PartitionGroup:
    Enabled: true
    GroupType: CUSTOM
    MemberGroups:
      - [node1, node2]
      - [node3, node4]

  # Management center
  ManagementCenter:
    Enabled: true
    Url: http://localhost:8080/hazelcast-mancenter
    UpdateInterval: 3
```

## Discovery Mechanisms

### 1. Multicast (Default)

Best for: Development, small networks

```yaml
Hazelcast:
  Multicast:
    Enabled: true
    MulticastGroup: 224.2.2.3
    MulticastPort: 54327
    MulticastTimeToLive: 32
    MulticastTimeoutSeconds: 2
```

### 2. TCP/IP

Best for: Production, known member addresses

```yaml
Hazelcast:
  TcpIp:
    Enabled: true
    RequiredMember: 192.168.1.100  # Master node
    Members:
      - 192.168.1.100-110  # Range notation
      - 192.168.1.200
    ConnectionTimeoutSeconds: 5
```

### 3. AWS Discovery

Best for: AWS deployments

```yaml
Hazelcast:
  Aws:
    Enabled: true
    AccessKey: your-access-key
    SecretKey: your-secret-key
    Region: us-east-1
    SecurityGroupName: monster-cluster
    TagKey: cluster
    TagValue: monster-prod
```

### 4. Kubernetes Discovery

Best for: Kubernetes deployments

```yaml
Hazelcast:
  Kubernetes:
    Enabled: true
    Namespace: default
    ServiceName: monstermq-cluster
    ServicePort: 5701
    KubernetesMaster: https://kubernetes.default.svc
```

## Load Balancing

### HAProxy Configuration

```conf
global
    maxconn 4096
    daemon

defaults
    mode tcp
    timeout connect 5000ms
    timeout client 50000ms
    timeout server 50000ms

frontend mqtt_frontend
    bind *:1883
    default_backend mqtt_backend

backend mqtt_backend
    balance roundrobin
    option tcp-check
    server node1 192.168.1.100:1883 check
    server node2 192.168.1.101:1883 check
    server node3 192.168.1.102:1883 check
```

### NGINX Configuration

```nginx
stream {
    upstream mqtt_cluster {
        least_conn;
        server 192.168.1.100:1883 max_fails=3 fail_timeout=30s;
        server 192.168.1.101:1883 max_fails=3 fail_timeout=30s;
        server 192.168.1.102:1883 max_fails=3 fail_timeout=30s;
    }

    server {
        listen 1883;
        proxy_pass mqtt_cluster;
        proxy_connect_timeout 5s;
        proxy_timeout 1h;  # Long timeout for MQTT
        proxy_buffer_size 4k;
    }
}
```

## Session Management

### Session Persistence

```yaml
# Ensure sessions persist across cluster
SessionStore:
  Type: POSTGRES  # Or CRATEDB, MONGODB
  CleanSession: false
  SessionExpiry: 3600  # Seconds

# Hazelcast session cache
Hazelcast:
  Maps:
    Sessions:
      BackupCount: 2
      AsyncBackupCount: 1
      TimeToLiveSeconds: 3600
      MaxIdleSeconds: 600
```

### Session Migration

When a client reconnects to a different node:
1. Node checks distributed session cache
2. Retrieves session from database if not cached
3. Restores subscriptions and pending messages
4. Resumes message delivery

## State Synchronization

### Distributed Data Structures

```yaml
Hazelcast:
  Maps:
    # Retained messages
    RetainedMessages:
      BackupCount: 2
      InMemoryFormat: OBJECT
      EvictionPolicy: LRU
      MaxSizePolicy: USED_HEAP_PERCENTAGE
      MaxSize: 10

    # Topic subscriptions
    Subscriptions:
      BackupCount: 2
      ReadBackupData: true

    # Client connections
    ClientConnections:
      BackupCount: 1
      TimeToLiveSeconds: 0
```

## Monitoring

### Hazelcast Management Center

1. **Download Management Center:**
```bash
wget https://repo1.maven.org/maven2/com/hazelcast/hazelcast-management-center/5.3.0/hazelcast-management-center-5.3.0.tar.gz
tar -xzf hazelcast-management-center-5.3.0.tar.gz
```

2. **Start Management Center:**
```bash
cd hazelcast-management-center-5.3.0
./bin/start.sh
# Access at http://localhost:8080
```

3. **Configure MonsterMQ:**
```yaml
Hazelcast:
  ManagementCenter:
    Enabled: true
    Url: http://localhost:8080
    UpdateInterval: 3
```

### Metrics to Monitor

- **Cluster Size** - Number of active nodes
- **Partition Distribution** - Even distribution across nodes
- **Memory Usage** - Heap usage per node
- **Network Traffic** - Inter-node communication
- **Client Connections** - Distribution across nodes
- **Message Throughput** - Messages/second per node

### Health Checks

```bash
# Check cluster health via REST API
curl http://node1:5701/hazelcast/health

# Response
{
  "nodeState": "ACTIVE",
  "clusterState": "ACTIVE",
  "clusterSafe": true,
  "nodeCount": 3,
  "clusterSize": 3
}
```

## Scaling

### Adding Nodes

1. **Prepare new node:**
```bash
# Same configuration as existing nodes
cp config-cluster.yaml config-node4.yaml
```

2. **Start new node:**
```bash
java -jar monstermq.jar -cluster -config config-node4.yaml
```

3. **Verify cluster membership:**
```bash
# Check logs for cluster join
tail -f logs/monstermq.log | grep "Members"
```

### Removing Nodes

1. **Graceful shutdown:**
```bash
# Send SIGTERM to process
kill -15 <pid>

# Or use admin API
curl -X POST http://node3:4000/admin/shutdown
```

2. **Monitor redistribution:**
- Partitions redistribute automatically
- Client connections migrate to other nodes
- Sessions persist in database

### Auto-scaling

#### Kubernetes HPA

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: monstermq-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: monstermq
  minReplicas: 3
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

## Failure Scenarios

### Node Failure

1. **Detection:** Hazelcast detects via heartbeat timeout
2. **Partition Migration:** Data partitions redistribute
3. **Client Reconnection:** Clients reconnect to available nodes
4. **Session Recovery:** Sessions restored from database

### Network Partition (Split-Brain)

```yaml
Hazelcast:
  # Split-brain protection
  SplitBrainProtection:
    MinimumClusterSize: 3

  # Merge policy
  MergePolicy:
    BatchSize: 100
    Type: PUT_IF_ABSENT  # Or LATEST_UPDATE, HIGHER_HITS
```

### Recovery Procedures

1. **Single Node Recovery:**
```bash
# Restart failed node
java -jar monstermq.jar -cluster -config config-node1.yaml

# Verify cluster join
grep "Members" logs/monstermq.log
```

2. **Full Cluster Recovery:**
```bash
# Start nodes in sequence
for i in 1 2 3; do
  java -jar monstermq.jar -cluster -config config-node$i.yaml &
  sleep 10
done
```

## Performance Tuning

### JVM Settings

```bash
# Recommended JVM flags
java -Xms2g -Xmx2g \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=200 \
  -XX:+ParallelRefProcEnabled \
  -Dhazelcast.operation.thread.count=4 \
  -Dhazelcast.io.thread.count=4 \
  -jar monstermq.jar -cluster
```

### Network Optimization

```yaml
Hazelcast:
  NetworkConfig:
    # Increase for better throughput
    SocketSendBufferSizeKb: 128
    SocketReceiveBufferSizeKb: 128

    # Connection settings
    ConnectionTimeout: 30000
    SocketLingerSeconds: 0
    SocketKeepAlive: true
    SocketTcpNoDelay: true
```

### Partition Configuration

```yaml
Hazelcast:
  # Optimize partition count
  PartitionCount: 271  # Prime number for better distribution

  # Backup configuration
  BackupCount: 1  # Synchronous backups
  AsyncBackupCount: 1  # Asynchronous backups
```

## Best Practices

1. **Use odd number of nodes** - Prevents split-brain in network partitions
2. **Configure proper discovery** - Use TCP/IP or cloud discovery in production
3. **Monitor cluster health** - Set up alerting for node failures
4. **Plan capacity** - Each node should handle 150% of average load
5. **Regular backups** - Backup shared database regularly
6. **Test failover** - Regularly test failure scenarios
7. **Network security** - Use SSL/TLS for inter-node communication
8. **Resource allocation** - Ensure adequate CPU/memory for Hazelcast

## Troubleshooting

### Common Issues

1. **Nodes not joining cluster:**
   - Check network connectivity
   - Verify cluster name matches
   - Check firewall rules for port 5701

2. **High memory usage:**
   - Tune eviction policies
   - Increase heap size
   - Check for memory leaks

3. **Poor performance:**
   - Check network latency
   - Optimize partition count
   - Review backup configuration

4. **Split-brain scenario:**
   - Configure split-brain protection
   - Use odd number of nodes
   - Implement proper merge policies

### Debug Logging

```yaml
# Enable Hazelcast debug logging
Logging:
  Level: DEBUG
  Loggers:
    com.hazelcast: DEBUG
    at.rocworks.cluster: DEBUG
```