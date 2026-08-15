---
name: monstermq-broker-config
description: >
  Guide for configuring, deploying, and operating the MonsterMQ broker. Use this skill whenever
  the user needs help with YAML configuration (config.yaml), Docker deployment, clustering,
  database setup, storage backend selection, queue store types, feature flags, TLS/SSL certificates,
  Kafka/NATS integration, or any operational/DevOps task related to MonsterMQ. Also trigger when
  the user asks about config.yaml structure, environment variables, command-line arguments,
  or troubleshooting broker startup issues.
  Trigger on "config.yaml", "Docker", "deploy", "cluster", "PostgreSQL setup", "MongoDB setup",
  "Kafka", "TLS", "certificate", "archive group", "storage backend", "feature flags", "queue store",
  or "how to configure".
---

# MonsterMQ Broker Configuration & Operations Skill

This skill provides instructions for configuring, deploying, operating, and troubleshooting the MonsterMQ MQTT broker process via YAML (`config.yaml`), environment variables, Docker containers, and CLI flags.

> [!NOTE]
> This skill covers **static/startup configuration** (`config.yaml`, Docker, JVM args, clustering, storage backends).
> For **runtime management via GraphQL API** (publishing, mutating sessions, creating devices at runtime), use the [`graphql-config`](../graphql-config/SKILL.md) skill instead.

---

## Configuration File (`config.yaml`)

The broker is configured via `config.yaml`. The single source of truth for all YAML settings, property types, and default values is the JSON Schema file at [`broker/yaml-json-schema.json`](file:///home/vogler/Workspace/monster-mq/broker/yaml-json-schema.json).

### Core Structure Example

```yaml
# Network listener ports (0 = disabled)
Port:
  TCP: 1883       # Plain MQTT
  TCPS: 8883      # MQTT over TLS
  WS: 1884        # WebSocket
  WSS: 8884       # Secure WebSocket
  HTTP: 4000      # GraphQL API + Dashboard + REST API
  NATS: 4222      # Native NATS listener

# Server Ports (Protocol & Service Servers)
MCP:
  Enabled: true
  Port: 3000
Prometheus:
  Enabled: false
  Port: 3001
I3x:
  Enabled: false
  Port: 3002
RedisServer:
  Enabled: false
  Port: 6379
KafkaServer:
  Enabled: false
  Port: 9092

# Storage backend defaults
DefaultStoreType: POSTGRES | MONGODB | SQLITE # Global default for store types if not overridden
SessionStoreType: POSTGRES | MONGODB | SQLITE | MEMORY
RetainedStoreType: MEMORY | HAZELCAST | POSTGRES | CRATEDB | MONGODB | SQLITE
LastValueStoreType: MEMORY | HAZELCAST | POSTGRES | CRATEDB
QueueStoreType: POSTGRES | MONGODB | SQLITE    # Defaults to DefaultStoreType or persistent SessionStoreType. V2 single-table design.

# Zenoh Broker Federation (Optional)
Zenoh:
  Enabled: false
  Mode: peer | client
  Connect: []
  RemotePrefix: "monstermq/mqtt"
  LocalPrefix: ""

# High-load performance batching
BulkProcessing:
  Enabled: false
  TimeoutMS: 50
  BulkSize: 1000
  WorkerThreads: 4

BulkMessaging:
  Enabled: true
  TimeoutMS: 100
  BulkSize: 1000

# Database connection pools
Postgres:
  Url: "jdbc:postgresql://localhost:5432/monster"
  User: "system"
  Password: "manager"
  Schema: "public"            # Optional PostgreSQL schema name

CrateDB:
  Url: "jdbc:postgresql://localhost:5433/monster"
  User: "crate"
  Password: ""

MongoDB:
  ConnectionString: "mongodb://system:manager@localhost:27017"
  DatabaseName: "monster"

SQLite:
  Path: "sqlite"
  EnableWAL: true

# Message Archiving Groups
ArchiveGroups:
  - Name: "Default"           # Required for MCP Server & general topics
    TopicFilter: ["#"]
    StoreType: Postgres | CrateDB | MongoDB | Kafka
    RetainedStoreType: Postgres | CrateDB
    LastValueStoreType: Postgres | CrateDB

# Top-level Feature Flags (Gating Verticles & GraphQL Mutations)
Features:
  OpcUa: true                 # OPC UA Client bridge
  OpcUaServer: true           # Embedded OPC UA Server
  MqttClient: true            # Remote MQTT broker bridge
  Kafka: true                 # Kafka client bridge
  KafkaServer: true           # Kafka-compatible server
  Nats: true                  # NATS bridge
  Redis: true                 # Redis Pub/Sub & KV bridge
  RedisServer: true           # Redis-compatible protocol server
  Telegram: true              # Telegram bot bridge
  WinCCOa: true               # WinCC OA connector
  WinCCUa: true               # WinCC Unified connector
  Plc4x: true                 # PLC4X connector (S7, Modbus, AB)
  Neo4j: true                 # Neo4j graph database logger
  JdbcLogger: true            # JDBC SQL logger
  InfluxDBLogger: true        # InfluxDB logger device
  TimeBaseLogger: true        # TimeBase logger device
  SparkplugB: true            # SparkplugB decoder
  FlowEngine: true            # Visual flow engine
  Agents: true                # AI agent framework
  GenAi: true                 # GenAI provider management
  Mcp: true                   # MCP server management
  SchemaPolicy: true          # Topic schema policy management
  TopicNamespace: true        # Topic namespace management
  DeviceImportExport: true    # Device configuration import/export
  Zenoh: true                 # Zenoh broker federation transport
  I3xClient: true             # i3X client bridge

# User Management & Auth
Auth:
  UserStoreType: Postgres | MONGODB | SQLITE
  JwtSecret: "your-secret-key"
  TokenExpiry: 3600            # seconds
  PasswordAlgorithm: bcrypt

# GenAI Providers
GenAI:
  Enabled: true
  Providers:
    Gemini:
      ApiKey: "${GENAI_GEMINI_API_KEY}"
    Claude:
      ApiKey: "${GENAI_CLAUDE_API_KEY}"
    OpenAI:
      ApiKey: "${GENAI_OPENAI_API_KEY}"
    Ollama:
      BaseUrl: "http://localhost:11434"
```

---

## Command-Line Arguments

```bash
cd broker
./run.sh [options]

# Run options:
-cluster              Enable Hazelcast clustering mode
-log LEVEL            Set logging verbosity: INFO | FINE | FINER | FINEST | ALL
-config FILE          Path to custom YAML config file (default: config.yaml)
-build                Rebuild Vite dashboard + package JAR before running
```

---

## Docker Deployment

### Build Image
```bash
cd docker
./build
```

### Run Container
```bash
docker run -d \
  -p 1883:1883 \
  -p 4222:4222 \
  -p 4000:4000 \
  -v ./config.yaml:/app/config.yaml \
  -v ./log:/app/log \
  rocworks/monstermq:latest
```

### Docker Compose Example (PostgreSQL Backend)
```yaml
services:
  monstermq:
    image: rocworks/monstermq:latest
    ports:
      - "1883:1883"
      - "4000:4000"
      - "4222:4222"
    volumes:
      - ./config.yaml:/app/config.yaml
    depends_on:
      - postgres

  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: monster
      POSTGRES_USER: system
      POSTGRES_PASSWORD: manager
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

volumes:
  pgdata:
```

---

## Storage Backend Selection Guide

| Use Case | Session Store | Queue Store | Retained / Last Value | Archive Store |
|----------|---------------|-------------|-----------------------|---------------|
| Single Node (Default) | PostgreSQL | PostgreSQL (V2) | PostgreSQL | PostgreSQL |
| High-Volume Analytics | PostgreSQL | PostgreSQL | CrateDB | CrateDB |
| Document-Oriented | MongoDB | MongoDB (V2) | MongoDB | MongoDB |
| Lightweight / Embedded | SQLite | SQLite (V2) | Memory / SQLite | SQLite |
| High-Throughput Stream | PostgreSQL | PostgreSQL | Memory | Kafka |
| Multi-Node Cluster | PostgreSQL / MongoDB | PostgreSQL / MongoDB | Hazelcast | PostgreSQL / CrateDB |

- **Queue Store V2**: Uses a PGMQ-inspired single-table design with visibility timeout for high throughput.
- **CrateDB**: Best for time-series message archives, but not supported for Session/Queue stores due to eventual consistency.
- **SQLite**: Not supported in multi-node clustered setups.

---

## Clustering & High Availability

- Enable clustering using `./run.sh -cluster`.
- Uses Hazelcast for distributed coordination and message routing.
- Sessions and Retained Messages live in the shared database store.
- **HA Scope**: MQTT client connections can reconnect to any surviving node. Device connectors (OPC UA, PLC4X, bridges) are pinned to their assigned `nodeId` and recover when their node restarts or when reassigned via GraphQL.

---

## Troubleshooting Startup Issues

### 1. Broker fails to start
- Check YAML syntax against `broker/yaml-json-schema.json`.
- Verify database connection strings and credentials.
- Ensure ports (`1883`, `4000`, `4222`, `3000`, `3001`, `3002`, `6379`, `9092`) are not occupied by other processes.

### 2. Feature Verticles not loading
- Check the `Features` block in `config.yaml`. Verticles for disabled features (e.g. `OpcUa: false`) are skipped at startup.

### 3. Messages not archiving
- Verify an `ArchiveGroup` exists with a matching `TopicFilter`.
- Confirm database connection status in logs or via `archiveGroups` GraphQL query.
