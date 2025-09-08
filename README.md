# MonsterMQ

MonsterMQ is a high-performance, scalable MQTT broker built on Vert.X and Hazelcast with persistent data storage through PostgreSQL, CrateDB, MongoDB, or SQLite. It features built-in clustering, unlimited message storage, and AI integration through a Model Context Protocol (MCP) server.

![Logo](Logo.png)

## 📑 Table of Contents

- [🚀 Key Features](#-key-features)
  - [Enterprise-Grade MQTT Broker](#enterprise-grade-mqtt-broker)
  - [Horizontal Scaling & High Availability](#horizontal-scaling--high-availability)
  - [Multi-Database Backend Support](#multi-database-backend-support)
  - [AI Integration (MCP Server)](#ai-integration-mcp-server)
  - [SparkplugB Extension](#sparkplugb-extension)
- [🏗️ Architecture](#️-architecture)
- [📦 Quick Start](#-quick-start)
  - [Docker Compose (Recommended)](#docker-compose-recommended)
  - [SQLite (Lightweight Setup)](#sqlite-lightweight-setup)
  - [Build from Source](#build-from-source)
  - [Example Configurations](#example-configurations)
- [⚙️ Configuration Reference](#️-configuration-reference)
  - [YAML Schema Support](#yaml-schema-support)
  - [Network Ports](#network-ports)
  - [Storage Configuration](#storage-configuration)
  - [Archive Groups](#archive-groups)
  - [Database Connections](#database-connections)
  - [Clustering](#clustering)
  - [Kafka Integration](#kafka-integration)
    - [Archive Streaming](#1-archive-streaming-selective-export)
    - [Kafka as Message Bus](#2-kafka-as-message-bus-complete-data-stream)
  - [Extensions](#extensions)
- [🔧 Advanced Features](#-advanced-features)
  - [Clustering Setup](#clustering-setup)
  - [MCP Server Integration](#mcp-server-integration)
- [🌐 Hazelcast Clustering](#-hazelcast-clustering)
  - [Cluster Configuration](#cluster-configuration)
  - [Cluster Operations](#cluster-operations)
  - [Monitoring Cluster Health](#monitoring-cluster-health)
  - [Production Deployment Best Practices](#production-deployment-best-practices)
- [🚨 Limitations](#-limitations)
- [📊 Monitoring](#-monitoring)
  - [Health Endpoints](#health-endpoints)
  - [Logging Configuration](#logging-configuration)
  - [Custom Logging](#custom-logging)
- [🔐 Security](#-security)
  - [TLS Configuration](#tls-configuration)
  - [Database Security](#database-security)

---

## 🚀 Key Features

### **Enterprise-Grade MQTT Broker**
- **MQTT 3.1.1 Protocol Support** - Full compliance with MQTT 3.1.1 specification
- **Unlimited Message Storage** - Store unlimited retained messages and persistent sessions
- **QoS 0, 1, 2 Support** - Complete Quality of Service level implementation
- **Persistent Sessions** - Offline message queuing for `cleanSession=false` clients
- **WebSocket Support** - MQTT over WebSockets (WS/WSS) for web applications
- **TLS/SSL Security** - Secure connections with certificate-based authentication

### **Horizontal Scaling & High Availability**
- **Hazelcast Clustering** - Native support for multi-node clusters
- **Hierarchical Architecture** - Build distributed systems with multiple broker levels
- **Cross-Cluster Messaging** - Clients can subscribe to topics across different cluster nodes
- **Load Distribution** - Automatic load balancing across cluster members
- **High Availability** - Automatic failover and redundancy

### **Multi-Database Backend Support**
MonsterMQ supports multiple database backends for different storage needs:

| Database | Session Store | Retained Store | Message Archive | Message Store | Cluster Support | Use Case |
|----------|:-------------:|:--------------:|:---------------:|:-------------:|:---------------:|:---------|
| **PostgreSQL** | ✅ | ✅ | ✅ | ✅ | ✅ | Production, full SQL features |
| **SQLite** | ✅ | ✅ | ✅ | ✅ | ❌ | Development, single-instance only |
| **CrateDB** | ✅ | ✅ | ✅ | ✅ | ✅ | Time-series, analytics |
| **MongoDB** | ✅ | ✅ | ✅ | ✅ | ✅ | Document-based, NoSQL |
| **Memory** | ❌ | ✅ | ❌ | ✅ | ✅ | High-speed, volatile |
| **Hazelcast** | ❌ | ✅ | ❌ | ✅ | ✅ | Distributed cache, clustering |
| **Kafka** | ❌ | ❌ | ✅ | ❌ | ✅ | Streaming, event sourcing |

**Important Clustering Note:**
- **SQLite cannot be used in cluster mode** - it's file-based and not shareable between nodes
- Clustering requires a central database: PostgreSQL, CrateDB, or MongoDB
- All cluster nodes must connect to the same database instance

**Advanced Features** (PostgreSQL & SQLite only):
- Historical message queries with time filtering
- Advanced topic search and discovery  
- Raw SQL query execution for analytics
- Configuration-based topic filtering

### **AI Integration (MCP Server)**
- **Model Context Protocol Support** - Built-in MCP server for AI model integration
- **Real-time Data Access** - AI models can query current and historical MQTT data
- **Topic Analytics** - Advanced search and filtering capabilities for AI applications
- **RESTful API** - HTTP interface for external integrations

### **SparkplugB Extension**
- **Automatic Message Expansion** - Converts SparkplugB messages from `spBv1.0` to `spBv1.0e` topics
- **Industrial IoT Support** - Native support for Sparkplug specification
- **Metric Extraction** - Automatic parsing and expansion of Sparkplug payloads

## 🏗️ Architecture

MonsterMQ follows a modular architecture with pluggable storage backends:

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   MQTT Clients  │───▶│   MonsterMQ  │───▶│   Databases     │
│                 │    │   Broker     │    │                 │
│ • TCP/TLS       │    │              │    │ • PostgreSQL    │
│ • WebSocket     │    │ ┌──────────┐ │    │ • SQLite        │
│ • QoS 0,1,2     │    │ │Clustering│ │    │ • CrateDB       │
└─────────────────┘    │ │Hazelcast │ │    │ • MongoDB       │
                       │ └──────────┘ │    └─────────────────┘
┌─────────────────┐    │              │    
│   MCP Server    │◀───┤ ┌──────────┐ │    ┌─────────────────┐
│                 │    │ │ Message  │ │───▶│   Kafka         │
│ • AI Models     │    │ │ Archive  │ │    │   (Archive)     │
│ • Analytics     │    │ └──────────┘ │    └─────────────────┘
│ • Port 3000     │    └──────────────┘    
└─────────────────┘
```

## 📦 Quick Start

### Docker Compose (Recommended)

Create a `docker-compose.yml`:

```yaml
services:
  postgres:
    image: timescale/timescaledb:latest-pg16
    container_name: postgres
    restart: unless-stopped
    ports:
      - 5432:5432
    volumes:
      - ./db:/var/lib/postgresql/data
    environment:
      POSTGRES_USER: system
      POSTGRES_PASSWORD: manager
      POSTGRES_DB: monster

  monstermq:
    image: rocworks/monstermq:latest
    container_name: monstermq
    restart: unless-stopped
    ports:
      - 1883:1883    # MQTT TCP
      - 8883:8883    # MQTT TLS
      - 9000:9000    # WebSocket
      - 9001:9001    # WebSocket TLS
      - 3000:3000    # MCP Server
    volumes:
      - ./config.yaml:/app/config.yaml
    command: ["-config", "config.yaml", "-log", "INFO"]
    depends_on:
      - postgres
```

Create a `config.yaml`:

```yaml
TCP: 1883
WS: 9000
TCPS: 8883
WSS: 9001
MaxMessageSizeKb: 512

SessionStoreType: POSTGRES
RetainedStoreType: POSTGRES

QueuedMessagesEnabled: true

ArchiveGroups:
  - Name: MCP
    Enabled: true
    TopicFilter: [ "#" ]
    RetainedOnly: false
    LastValType: POSTGRES
    ArchiveType: POSTGRES

Postgres:
  Url: jdbc:postgresql://postgres:5432/monster
  User: system
  Pass: manager

MCP:
  Enabled: true
  Port: 3000
```

Start the services:

```bash
docker-compose up -d
```

### SQLite (Lightweight Setup)

For development or single-instance deployments, use SQLite:

```yaml
TCP: 1883
WS: 8080
MaxMessageSizeKb: 8

SessionStoreType: SQLITE
RetainedStoreType: SQLITE
QueuedMessagesEnabled: true

ArchiveGroups:
  - Name: MCP
    Enabled: true
    TopicFilter: [ "#" ]
    RetainedOnly: false
    LastValType: SQLITE
    ArchiveType: SQLITE

SQLite:
  Path: "monstermq.db"

MCP:
  Enabled: true
  Port: 3000
```

### Build from Source

```bash
cd broker
mvn clean package

# Show help for available options
java -classpath target/classes:target/dependencies/* at.rocworks.MainKt -help

# Start with configuration
java -classpath target/classes:target/dependencies/* at.rocworks.MainKt -config config.yaml

# Or use the convenience script
./run.sh -help
./run.sh -config config.yaml
```

### Example Configurations

Several pre-configured examples are available:

- **`config.yaml`** - Default configuration with PostgreSQL
- **`config-postgres.yaml`** - PostgreSQL configuration example
- **`config-sqlite.yaml`** - SQLite configuration (single-instance only)
- **`config-memory.yaml`** - Memory-only configuration (no persistence)
- **`config-hazelcast.yaml`** - Hazelcast clustering with PostgreSQL
- **`config-kafka.yaml`** - Kafka streaming integration (selective topics)
- **`config-kafka-bus.yaml`** - Kafka as message bus (ALL messages)
- **`docker-compose-kafka.yaml`** - Complete Kafka stack with MonsterMQ

```bash
# Development with SQLite (single instance only)
./run.sh -config config-sqlite.yaml

# Production cluster with Hazelcast
./run.sh -cluster -config config-hazelcast.yaml

# Testing with memory-only storage
./run.sh -config config-memory.yaml

# Stream to Kafka
./run.sh -config config-kafka.yaml

# Full Kafka stack with Docker
docker-compose -f docker-compose-kafka.yaml up -d
```

## ⚙️ Configuration Reference

### YAML Schema Support

MonsterMQ includes a comprehensive JSON schema for configuration validation and auto-completion:

**VS Code Setup:**
1. Install the [YAML extension](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml) by Red Hat
2. Add this to your VS Code `settings.json`:
   ```json
   {
     "yaml.schemas": {
       "./broker/yaml-json-schema.json": "config*.yaml"
     }
   }
   ```
3. Open any `config*.yaml` file to get:
   - ✅ Auto-completion for all configuration options
   - ✅ Inline documentation and descriptions
   - ✅ Real-time validation and error highlighting
   - ✅ Type checking for values

**Other Editors:**
- The schema file is located at `broker/yaml-json-schema.json`
- Most modern editors with YAML/JSON schema support can use this file
- Provides complete documentation of all available configuration options

### Network Ports
```yaml
TCP: 1883           # MQTT TCP port (0=disabled)
TCPS: 8883          # MQTT TLS port (0=disabled)
WS: 9000            # MQTT WebSocket port (0=disabled)
WSS: 9001           # MQTT WebSocket TLS port (0=disabled)
MaxMessageSizeKb: 512
```

### Storage Configuration
```yaml
SessionStoreType: POSTGRES     # POSTGRES, CRATEDB, MONGODB, SQLITE
RetainedStoreType: POSTGRES    # MEMORY, HAZELCAST, POSTGRES, CRATEDB, SQLITE
QueuedMessagesEnabled: true    # Enable QoS>0 message queuing
```

### Archive Groups
Archive groups define how messages are stored for historical access and analytics:

```yaml
ArchiveGroups:
  - Name: "production"
    Enabled: true
    TopicFilter: [ "sensors/#", "devices/#" ]
    RetainedOnly: false
    LastValType: POSTGRES      # Current value storage
    ArchiveType: POSTGRES      # Historical message storage

  - Name: MCP                  # Required for MCP server
    Enabled: true
    TopicFilter: [ "#" ]
    RetainedOnly: false
    LastValType: POSTGRES      # Required for MCP
    ArchiveType: POSTGRES      # Optional for MCP
```

### Database Connections

**PostgreSQL:**
```yaml
Postgres:
  Url: jdbc:postgresql://localhost:5432/monster
  User: system
  Pass: manager
```

**SQLite:**
```yaml
SQLite:
  Path: "monstermq.db"  # File path (created automatically)
```

**CrateDB:**
```yaml
CrateDB:
  Url: jdbc:postgresql://cratedb:5432/monster
  User: crate
  Pass: ""
```

**MongoDB:**
```yaml
MongoDB:
  Url: mongodb://system:manager@mongodb:27017
  Database: monster
```

### Clustering
```yaml
# Run with: java ... -cluster
# Automatic Hazelcast clustering
# Nodes discover each other automatically
```

### Kafka Integration

MonsterMQ offers two powerful ways to integrate with Apache Kafka: **Archive Streaming** and **Kafka as Message Bus**.

#### 1. Archive Streaming (Selective Export)

Stream specific MQTT topics to Kafka for downstream processing:

```yaml
Kafka:
  Servers: kafka:9092          # Kafka broker addresses

# Stream specific topics to different Kafka topics
ArchiveGroups:
  - Name: sensors
    TopicFilter: [ "sensors/#" ]
    ArchiveType: KAFKA          # Stream to Kafka topic "sensors"
    
  - Name: events
    TopicFilter: [ "events/#", "alerts/#" ]
    ArchiveType: KAFKA          # Stream to Kafka topic "events"
```

#### 2. Kafka as Message Bus (Complete Data Stream)

Replace Vert.x EventBus with Kafka for **ALL** internal message distribution:

```yaml
Kafka:
  Servers: kafka:9092
  Bus:
    Enabled: true               # 🔴 KAFKA AS MESSAGE BUS
    Topic: monster-bus          # All MQTT messages flow through this topic
```

**Architecture Comparison:**

```
Standard Mode (Vert.x EventBus):
Client → MonsterMQ → [Vert.x EventBus] → Subscribers
                  ↓
            Archive to Kafka (selective)

Kafka Bus Mode:
Client → MonsterMQ → [Kafka Topic] → MonsterMQ → Subscribers
                          ↓
                  ALL messages available in Kafka
```

**Why Use Kafka as Message Bus?**

✅ **Complete Data Stream**
- Every single MQTT message flows through Kafka
- No need for separate archive configuration
- Guaranteed capture of all broker traffic

✅ **Universal Access**
- Any Kafka client can tap into the message stream
- Real-time analytics on ALL broker traffic
- Debug and monitor everything happening in the broker

✅ **Advanced Capabilities**
- Kafka's built-in replay functionality
- Time-travel debugging (replay from any point)
- Automatic data retention and compaction
- Stream processing with Kafka Streams/ksqlDB

✅ **Integration Benefits**
- Direct integration with data platforms (Spark, Flink)
- Native CDC (Change Data Capture) capabilities
- Easy data lake ingestion
- Microservices can consume the raw stream

⚠️ **Trade-offs:**
- **Higher Latency**: Additional network hop through Kafka (typically 5-20ms)
- **Dependency**: Kafka becomes critical infrastructure
- **Throughput**: Limited by Kafka cluster capacity
- **Complexity**: More components to manage

**Configuration Example:**

```yaml
# config-kafka-bus.yaml
TCP: 1883
WS: 8080

# Use Kafka as the internal message bus
Kafka:
  Servers: kafka1:9092,kafka2:9092,kafka3:9092
  Bus:
    Enabled: true               # Enable Kafka bus mode
    Topic: monstermq-stream     # All messages flow here

# Storage configuration
SessionStoreType: POSTGRES
RetainedStoreType: POSTGRES

# No need for ArchiveGroups - everything is in Kafka!
# But you can still add them for additional filtering if needed
```

**Monitoring the Complete Stream:**

```bash
# View ALL MQTT messages flowing through the broker
kafka-console-consumer \
  --topic monstermq-stream \
  --from-beginning \
  --bootstrap-server localhost:9092 | jq '.'

# Count messages per topic
kafka-console-consumer \
  --topic monstermq-stream \
  --from-beginning \
  --bootstrap-server localhost:9092 | \
  jq -r '.topic' | sort | uniq -c

# Stream to analytics platform
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  analyze-mqtt-stream.py
```

**Use Cases for Kafka Bus Mode:**

1. **Compliance & Auditing**: Capture every message for regulatory requirements
2. **Analytics Platform**: Feed all data into a data lake or analytics system
3. **Development/Testing**: Record and replay entire test scenarios
4. **Debugging**: Time-travel debugging of production issues
5. **Multi-System Integration**: Multiple systems consuming the same stream

**Performance Considerations:**

```yaml
# Optimize for throughput (higher latency acceptable)
Kafka:
  Bus:
    Enabled: true
    Topic: monstermq-stream
    ProducerConfig:           # Optional Kafka producer tuning
      batch.size: 65536      # Larger batches
      linger.ms: 10          # Wait up to 10ms to batch
      compression.type: lz4   # Fast compression

# Optimize for latency (lower throughput)
Kafka:
  Bus:
    Enabled: true  
    Topic: monstermq-stream
    ProducerConfig:
      batch.size: 0          # No batching
      linger.ms: 0           # Send immediately
      acks: 1                # Don't wait for all replicas
```

**Message Format:**
```json
{
  "topic": "sensors/temperature/room1",
  "payload": "23.5",
  "timestamp": "2024-01-15T10:30:00Z",
  "qos": 1,
  "retained": false,
  "clientId": "sensor-001"
}
```

**Quick Start with Docker:**
```bash
# Start complete stack with Kafka bus
docker-compose -f docker-compose-kafka.yaml up -d

# Monitor the complete message stream
docker exec -it kafka kafka-console-consumer \
  --topic monster-bus --from-beginning \
  --bootstrap-server localhost:9092

# View Kafka UI
open http://localhost:8090
```

### Extensions
```yaml
SparkplugMetricExpansion:
  Enabled: true          # Expand SparkplugB messages

MCP:
  Enabled: true
  Port: 3000            # Model Context Protocol server
```

## 🔧 Advanced Features

### Clustering Setup

1. **Start first node:**
   ```bash
   java -classpath target/classes:target/dependencies/* at.rocworks.MainKt -cluster -config config.yaml
   ```

2. **Start additional nodes:**
   ```bash
   # On different machines, same config
   java -classpath target/classes:target/dependencies/* at.rocworks.MainKt -cluster -config config.yaml
   ```

3. **Hierarchical clusters:**
   - Configure different archive groups per level
   - Use topic filters to route messages between levels
   - Clients can subscribe across cluster boundaries

## 🌐 Hazelcast Clustering

MonsterMQ uses **Hazelcast** as its clustering engine to provide high availability, horizontal scaling, and distributed caching across multiple broker instances. When running in cluster mode with the `-cluster` flag, MonsterMQ automatically forms a distributed cluster.

**Important Requirements:**
- **Central Database Required**: Clustering requires PostgreSQL, CrateDB, or MongoDB for session storage
- **SQLite Cannot Be Used**: SQLite is file-based and cannot be shared between cluster nodes
- **All Nodes Must Share**: All cluster nodes must connect to the same database instance

**Cluster Capabilities:**
- **MQTT clients** can connect to any cluster node
- **Messages are shared** across all cluster members in real-time  
- **Retained messages** are distributed using Hazelcast distributed maps
- **Session state** is synchronized via the shared database
- **Automatic failover** ensures high availability

### Cluster Configuration

#### 1. Docker Setup with Hazelcast

The `docker` directory contains a complete Hazelcast clustering setup. Here's how to use it:

**File structure:**
```
docker/
├── docker-compose.yml     # Container orchestration  
├── hazelcast.xml         # Cluster network configuration
├── entrypoint.sh         # Startup script with Hazelcast support
├── config.yaml           # MonsterMQ broker configuration
└── Dockerfile            # Container build instructions
```

**Starting a cluster with Docker:**

```bash
cd docker

# Start first cluster node
docker-compose up -d

# Scale to multiple nodes
docker-compose up -d --scale mqtt=3
```

The `docker-compose.yml` automatically configures clustering:

```yaml
services:
  mqtt:
    image: rocworks/monstermq:latest
    restart: always
    network_mode: host
    environment:
      - HAZELCAST_CONFIG=hazelcast.xml        # 👈 Key configuration
      - PUBLIC_ADDRESS=192.168.1.10          # Optional: external IP
    volumes:
      - ./log:/app/log
      - ./config.yaml:/app/config.yaml
      - ./hazelcast.xml:/app/hazelcast.xml    # 👈 Mount cluster config
    command: ["-cluster"]                     # 👈 Enable clustering
```

#### 2. Hazelcast Network Configuration

The `docker/hazelcast.xml` file defines how cluster nodes discover each other:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<hazelcast xmlns="http://www.hazelcast.com/schema/config">
    <network>
        <port auto-increment="true" port-count="100">5701</port>
        
        <join>
            <!-- Disable multicast for production -->
            <multicast enabled="false"/>
            
            <!-- Manual TCP/IP discovery -->
            <tcp-ip enabled="true">
                <member>192.168.1.4:5701</member>   <!-- Node 1 -->
                <member>192.168.1.31:5701</member>  <!-- Node 2 -->
                <member>192.168.1.32:5701</member>  <!-- Node 3 -->
            </tcp-ip>
        </join>
        
        <!-- Restrict to specific network -->
        <interfaces enabled="true">
            <interface>192.168.1.*</interface>
        </interfaces>
    </network>
</hazelcast>
```

**Key configuration options:**
- **Port**: Hazelcast inter-node communication (default: 5701)
- **TCP/IP Discovery**: Manually specify cluster member IPs
- **Network Interfaces**: Restrict cluster traffic to specific networks
- **Auto-increment**: Allow multiple nodes on same machine (testing only)

#### 3. Environment Variables

The `docker/entrypoint.sh` script supports these environment variables:

| Variable | Description | Example |
|----------|-------------|---------|
| `HAZELCAST_CONFIG` | Path to hazelcast.xml config file | `hazelcast.xml` |
| `PUBLIC_ADDRESS` | External IP for cluster communication | `192.168.1.10:5701` |

**Setting environment variables:**

```bash
# Docker Compose
environment:
  - HAZELCAST_CONFIG=hazelcast.xml
  - PUBLIC_ADDRESS=192.168.1.10

# Docker Run  
docker run -e HAZELCAST_CONFIG=hazelcast.xml \
           -e PUBLIC_ADDRESS=192.168.1.10 \
           rocworks/monstermq -cluster

# Standalone Java
java -Dvertx.hazelcast.config=hazelcast.xml \
     -Dhazelcast.local.publicAddress=192.168.1.10 \
     -classpath ... at.rocworks.MainKt -cluster
```

### Cluster Operations

#### Distributed Message Stores

When clustering is enabled, MonsterMQ requires both database and Hazelcast configuration:

```yaml
# Cluster-aware configuration (config-hazelcast.yaml)
SessionStoreType: POSTGRES      # Required: Central database for sessions
RetainedStoreType: HAZELCAST   # Distributed retained messages

# Database configuration (required for clustering)
Postgres:
  Url: jdbc:postgresql://shared-db-server:5432/monster
  User: system
  Pass: manager
```

**How it works:**
- **Retained messages** are stored in Hazelcast distributed maps
- **All cluster nodes** have access to the same retained message state
- **Automatic replication** ensures no data loss on node failures
- **Memory efficient** - messages are partitioned across nodes

#### Client Connection Balancing

Clients can connect to any cluster node:

```bash
# Clients can connect to any node
mqtt_client.connect("192.168.1.4:1883")   # Node 1
mqtt_client.connect("192.168.1.31:1883")  # Node 2  
mqtt_client.connect("192.168.1.32:1883")  # Node 3

# All receive the same messages regardless of connection point
```

#### Cross-Node Message Routing

Messages published to one node are automatically distributed:

```
Client A → Node 1 → [Message] → Hazelcast Cluster → Node 2 → Client B
                              → Node 3 → Client C
```

### Monitoring Cluster Health

#### Log Monitoring
Cluster status appears in MonsterMQ logs:

```bash
# Successful cluster formation
INFO: Hazelcast cluster formed with 3 members
INFO: Local member: Node[192.168.1.4]:5701
INFO: Cluster members: [192.168.1.4:5701, 192.168.1.31:5701, 192.168.1.32:5701]

# Node joining cluster  
INFO: Member added to cluster: Node[192.168.1.32]:5701
```

### Production Deployment Best Practices

#### 1. Network Topology
```bash
# Production setup - separate Hazelcast network
# App Traffic: 1883 (MQTT), 9000 (WebSocket), 3000 (MCP)
# Cluster Traffic: 5701 (Hazelcast)

# Firewall rules
iptables -A INPUT -p tcp --dport 5701 -s 192.168.1.0/24 -j ACCEPT
iptables -A INPUT -p tcp --dport 1883 -s 0.0.0.0/0 -j ACCEPT
```

#### 2. Resource Planning
```bash
# Memory: Plan for distributed maps + local data
# Network: Hazelcast uses significant bandwidth for replication  
# Disk: Only for database backends (PostgreSQL, SQLite)

# Example 3-node cluster
Node 1: 4GB RAM, 2 CPUs
Node 2: 4GB RAM, 2 CPUs  
Node 3: 4GB RAM, 2 CPUs
Database: 8GB RAM, 4 CPUs (separate server)
```

#### 3. Split-Brain Prevention
```xml
<!-- Add to hazelcast.xml for production -->
<network>
    <join>
        <tcp-ip enabled="true">
            <member-list>
                <member>192.168.1.4:5701</member>
                <member>192.168.1.31:5701</member>
                <member>192.168.1.32:5701</member>
            </member-list>
        </tcp-ip>
    </join>
</network>

<!-- Minimum cluster size -->
<cluster-member-count>2</cluster-member-count>
```

### MCP Server Integration

The MCP server provides tools with access to MQTT data:
> pip install mcp

```python
# Example: Connect AI model to MonsterMQ MCP server

import mcp_client

client = mcp_client.connect("http://localhost:3000")

# Query current values
current_data = client.query_topics("sensors/#")

# Get historical data
history = client.get_history("sensors/temperature", 
                           start_time="2024-01-01T00:00:00Z",
                           limit=1000)

# Execute custom queries (PostgreSQL/SQLite only)
results = client.execute_query("""
    SELECT topic, AVG(payload::float) as avg_value 
    FROM mcparchive 
    WHERE topic LIKE 'sensors/%' 
    AND time > NOW() - INTERVAL '1 hour'
    GROUP BY topic
""")
```

## 🚨 Limitations

- **MQTT Version:** Only MQTT 3.1.1 supported (MQTT 5.0 features planned)
- **Authentication:** No built-in ACL system (planned for future release)
- **Performance:** Limited by database backend performance and network latency
- **Advanced Features:** Historical queries and topic search only available with PostgreSQL and SQLite

## 📊 Monitoring

### Health Endpoints
- Basic health monitoring through log files
- Database connection status in logs
- Client connection metrics in `$SYS` topics

### Logging Configuration
```bash
# Available log levels
java ... -log FINEST    # Detailed debugging
java ... -log FINE      # Debug information  
java ... -log INFO      # General information
java ... -log WARNING   # Warnings only
java ... -log SEVERE    # Errors only
```

### Custom Logging
```bash
# Use custom logging configuration
-Djava.util.logging.config.file=logging.properties
```

## 🔐 Security

### TLS Configuration
1. **Generate keystore:**
   ```bash
   keytool -genkeypair -alias monstermq -keyalg RSA -keysize 2048 -validity 365 \
           -keystore server-keystore.jks -storepass password
   ```

2. **Configure TLS ports:**
   ```yaml
   TCPS: 8883     # MQTT over TLS
   WSS: 9001      # MQTT over WebSocket TLS
   ```

### Database Security
- Use encrypted connections to databases
- Configure appropriate user permissions
- Regular security updates for database systems


---

**MonsterMQ** - Powering the next generation of IoT and real-time messaging applications with enterprise-grade reliability and AI integration.