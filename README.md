# MonsterMQ

MonsterMQ is a high-performance MQTT broker for industrial IoT and real-time messaging, built on Vert.x. It combines MQTT messaging with storage, protocol bridges, workflows, a web dashboard, and management APIs.

## Highlights

- MQTT 3.1.1 with broad MQTT 5 support ([details](doc/mqtt5.md))
- Native NATS protocol support and MQTT <-> NATS bridging ([details](doc/nats.md))
- WebSocket, TLS, retained messages, clustering, and MQTT-based system logging ([details](doc/mqtt-logging.md))
- Storage backends for PostgreSQL, CrateDB, MongoDB, SQLite, and more via JDBC ([database docs](doc/databases.md))
- Archive groups, last-value storage, retention policies, and schema-based JDBC logging ([archiving](doc/archiving.md), [Snowflake](doc/snowflake.md))
- Device integrations for OPC UA, PLC4X, WinCC OA, WinCC Unified, Neo4j, Kafka, Redis, Telegram, and others
- Flow engine for visual, JavaScript-based message processing ([details](doc/workflows.md))
- AI agents with multi-provider LLM support (Gemini, Claude, OpenAI, Ollama), MQTT/cron triggers, and agent-to-agent orchestration
- GraphQL API, REST API (with InfluxDB Line Protocol ingestion), MCP server, web dashboard, and CESMII I3X API support
- **Config-based feature flags** to enable or disable individual extensions per node

## Quick Start

### Docker

Run the published image:

```bash
docker run \
  -p 1883:1883 \
  -p 4222:4222 \
  -p 4000:4000 \
  -p 3000:3000 \
  -p 3002:3002 \
  -v ./config.yaml:/app/config.yaml \
  rocworks/monstermq:latest
```

### Build From Source

```bash
cd broker

# Build everything (dashboard + broker) and run
./run.sh -build

# Or run with a custom config
./run.sh -build -config configs/config-postgres.yaml

# Or run in cluster mode
./run.sh -build -cluster -config configs/config-hazelcast.yaml
```

The `./run.sh -build` flag builds the dashboard (`npm install && npm run build` in `dashboard/`), copies the output to `broker/src/main/resources/dashboard/`, then runs `mvn package` to compile the broker with the dashboard embedded.

For dashboard development, use `npm run dev` in the `dashboard/` directory to start a Vite dev server with hot reload on port 5173.

### Configuration

```yaml
TCP: 1883
TCPS: 8883
WSS: 8083
NATS: 4222

DefaultStoreType: POSTGRES
#DefaultStoreType: MONGODB
#DefaultStoreType: SQLITE

BulkProcessing:
  Enabled: false
  TimeoutMS: 50
  BulkSize: 1000
  WorkerThreads: 4

BulkMessaging:
  Enabled: true
  TimeoutMS: 100
  BulkSize: 1000

# MQTT TCP Server configuration (socket-level settings)
MqttTcpServer:
  NoDelay: true                  # TCP_NODELAY - disable Nagle's algorithm for immediate packet transmission
  ReceiveBufferSizeKb: 512       # Socket receive buffer size (default: 512KB, recommended: 512KB-2MB for high load)
  SendBufferSizeKb: 512          # Socket send buffer size (default: 512KB, recommended: 512KB-2MB for high load)
  MaxMessageSizeKb: 512          # Maximum MQTT message size (default: 512KB)

QueuedMessagesEnabled: true
AllowRootWildcardSubscription: true

# Rate limiting for MQTT clients (0 = unlimited)
#MaxPublishRate: 10000    # Max messages per second a client can publish
#MaxSubscribeRate: 10000  # Max messages per second a client can receive

# Queue size configuration for high-load scenarios
# Increase these values if experiencing NetworkTimeout errors during heavy subscription load
Queues:
  SubscriptionQueueSize: 50000  # Queue size for subscription add/delete operations (default: 10,000)
  MessageQueueSize: 50000       # Queue size for message add/delete operations (default: 10,000)

# User Management configuration
UserManagement:
  Enabled: true  # Set to true to enable user authentication and ACL
  PasswordAlgorithm: bcrypt
  CacheRefreshInterval: 60  # seconds
  DisconnectOnUnauthorized: true

Metrics:
  Enabled: true
  RetentionHours: 1   # Default 1 hour
  CollectionInterval: 10  # Collection interval in seconds (default: 1)

Logging:
  Mqtt:
    Enabled: true          # Enable MQTT log publishing to $SYS/syslogs/<node>/<level> topics
  Memory:
    Enabled: true         # Enable in-memory log storage (circular buffer)
    Entries: 1000          # Maximum number of log entries to store in memory

# SQLite configuration
SQLite:
  Path: "sqlite"
  EnableWAL: true  # Set to true for better performance, false for easier multi-client access

# PostgreSQL configuration
Postgres:
  Url: jdbc:postgresql://localhost:5432/monster
  User: system
  Pass: manager

# MongoDB configuration
MongoDB:
  Url: mongodb://system:manager@localhost:27017
  Database: monster

# CrateDB configuration
CrateDB:
  Url: jdbc:postgresql://localhost:5433/monster
  User: crate
  Pass: ""

GraphQL:
  Enabled: true
  Port: 4000
  Path: /graphql

Dashboard:
  Enabled: true

# MCP Server configuration
MCP:
  Enabled: true
  Port: 3000

Prometheus:
  Enabled: true
  Port: 3001
  RawQueryLimit: 10000

I3x:
  Enabled: true
  Port: 3002
  DefaultArchiveGroup: SCADA

# GenAI Configuration
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
      BaseUrl: http://localhost:11434
  Assistant:
    Provider: "gemini"
    Model: "gemini-2.5-flash-lite"
```

For more examples, see `broker/configs/`.

## Feature Flags

All optional extensions are **enabled by default**. Use the top-level `Features` section in `config.yaml` to disable extensions on a node.

```yaml
Features:
  OpcUa: false        # OPC UA client bridge (disabled)
  OpcUaServer: true
  MqttClient: true
  Kafka: true
  Nats: true
  Redis: true
  Telegram: true
  WinCCOa: true
  WinCCUa: true
  Plc4x: true
  Neo4j: true
  JdbcLogger: true
  SparkplugB: true
  FlowEngine: true
  Agents: true
```

Omitting a key defaults to `true`. GraphQL mutations for a disabled feature return an error immediately; the extension's verticle is not deployed at startup.

**Cluster behaviour:** device-to-node assignment is explicit — each device stores the `nodeId` it belongs to and extensions only load devices assigned to their own node. Heterogeneous cluster roles are fully supported: create-device mutations check the *target* node's feature set and reject assignments to nodes where that feature is disabled. Reassign mutations apply the same check. A feature set mismatch between nodes is logged as a `WARNING` and highlighted in the dashboard cluster table.

**HA scope:** MQTT client connections are HA — clients can reconnect to any surviving node because sessions and retained messages live in the shared database. Device connectors (OPC UA, Kafka bridges, etc.) are **not** HA: each is pinned to its assigned node and goes offline if that node fails until the node recovers or an operator reassigns the device.

See [configuration.md](doc/configuration.md) for details.

## Performance Tuning

### BulkProcessing and BulkMessaging

MonsterMQ supports two bulk operation modes that batch multiple operations together for higher throughput at the cost of added latency.

```yaml
BulkProcessing:
  Enabled: false       # Batch subscription matching and message routing
  TimeoutMS: 50        # Max wait time before flushing a partial batch
  BulkSize: 1000       # Max messages per batch
  WorkerThreads: 4     # Parallel processing threads

BulkMessaging:
  Enabled: true        # Batch message delivery to subscribers
  TimeoutMS: 100       # Max wait time before flushing a partial batch
  BulkSize: 1000       # Max messages per batch
```

- **High throughput**: Enable both options. Messages are collected into batches and processed together, reducing per-message overhead. Best for telemetry, data collection, and high-volume IoT workloads.
- **Low latency**: Disable both options. Messages are processed individually as they arrive, minimizing delivery delay. Best for real-time control, command/response patterns, and interactive applications.

## Core Capabilities

### Messaging and Protocols

- MQTT over TCP, TLS, and WebSocket
- Native NATS server for standard NATS clients
- MQTT client bridge for remote brokers
- MQTT logging for publishing broker logs to topics

### Storage and Archiving

- Session, retained, archive, last-value, and queue storage
- Queue store with V2 (single-table PGMQ-inspired design, default) and V1 (two-table design) for PostgreSQL, MongoDB, and SQLite
- Archive groups with retention and performance metrics
- JDBC logger with JSON schema validation, field mapping, and bulk writes
- Optional PostgreSQL schema support for multi-tenant setups

### Device and System Integration

- OPC UA client and server ([docs](doc/opcua.md), [server](doc/opcua-server.md))
- PLC4X for direct PLC access
- WinCC OA and WinCC Unified connectors ([WinCC OA](doc/winccoa.md), [WinCC Unified](doc/winccua.md))
- Neo4j integration for topic hierarchy analysis ([details](doc/neo4j.md))
- Kafka integration for event streaming ([details](doc/kafka.md))
- Redis bridge for Pub/Sub channel bridging and key-value sync

### Processing and APIs

- Flow engine with reusable flow classes and deployed instances
- JavaScript execution with GraalVM
- GraphQL queries, mutations, and subscriptions ([details](doc/graphql.md))
- REST API for publish, read, subscribe over HTTP, and high-throughput InfluxDB Line Protocol ingestion ([details](doc/rest-api.md))
- MCP server for AI-oriented access ([details](doc/mcp.md))

### AI Agents

MonsterMQ includes a built-in AI agent framework powered by LangChain4j. Agents are configured and managed through the dashboard or GraphQL API and run as Vert.x verticles inside the broker.

- **Multi-provider support**: Gemini, Claude, OpenAI, and Ollama (local models)
- **MQTT-triggered agents**: Subscribe to topics and react to incoming messages with LLM-driven logic
- **Cron and manual triggers**: Schedule agents on cron expressions or invoke them on demand
- **Broker tools**: Agents can publish/subscribe, query archives, read retained messages, and access last values
- **MCP integration**: Connect agents to external MCP servers for additional tool capabilities
- **Agent-to-agent communication (A2A)**: Agents can delegate tasks to other agents via MQTT-based orchestration
- **Conversation memory**: Configurable sliding-window chat memory with optional persistent state
- **Context injection**: Automatically provide agents with last-value data, retained messages, or historical queries as context

### Operations and UI

- Web dashboard for monitoring, configuration, users, topics, devices, and workflows
- Hazelcast-based clustering; MQTT client HA with shared session/retained store ([details](doc/clustering.md))
- TLS, certificates, ACLs, and user management ([security](doc/security.md), [users](doc/users.md), [ACL](doc/acl.md))

## Database Support

| Database | Session Store | Queue Store | Retained / Last Value | Message Archive | Config Store |
|----------|:---:|:---:|:---:|:---:|:---:|
| PostgreSQL | Yes | Yes (V2 default, V1) | Yes | Yes | Yes |
| MongoDB | Yes | Yes (V2 default, V1) | Yes | Yes | Yes |
| SQLite | Yes | Yes (V2 default, V1) | Yes | Yes | Yes |
| CrateDB | - | - | Yes | Yes | - |
| Memory | - | - | Yes | - | - |
| Hazelcast | - | - | Yes | - | - |
| Kafka | - | - | - | Yes | - |

- **Session Store**, **Queue Store**, and **Config Store** require strict read-after-write consistency. CrateDB is not supported for these due to its eventual consistency model.
- **Queue Store V2** is the default for all databases. It uses a PGMQ-inspired single-table design with visibility timeout for higher throughput. Use `POSTGRES_V1`, `MONGODB_V1`, or `SQLITE_V1` to select the legacy two-table design.
- SQLite is not suitable for cluster mode; use PostgreSQL or MongoDB instead.

## Default Endpoints

| Service | Port |
|---------|:----:|
| MQTT TCP | 1883 |
| MQTT TLS | 8883 |
| WebSocket | 9000 |
| WebSocket TLS | 9001 |
| NATS | 4222 |
| OPC UA Server | 4840 |
| GraphQL API, REST API, and Dashboard | 4000 |
| MCP Server | 3000 |
| I3X API | 3002 |

## Example Usage

### MQTT

```bash
mosquitto_pub -h localhost -p 1883 -t "sensors/temp1" -m "23.5"
mosquitto_sub -h localhost -p 1883 -t "sensors/#"
```

### NATS

```bash
nats sub "sensors.>" --server nats://localhost:4222
nats pub "sensors.temp1" "23.5" --server nats://localhost:4222
```

### GraphQL

```bash
curl -X POST http://localhost:4000/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "query { currentValue(topic: \"sensors/temp1\") { payload timestamp } }"}'
```

### HTTP REST (InfluxDB Line Protocol)

```bash
curl -u Admin:Admin -X POST \
  "http://localhost:4000/api/v1/write/influx?base=factory" \
  -H "Content-Type: text/plain" \
  -d 'motor,location=basement,unit=pump1 temp=45.2,vibration=0.8,running=true 1640995200000000000'
```

## Documentation

See `doc/` for full documentation:

- [installation.md](doc/installation.md) - installation and setup
- [configuration.md](doc/configuration.md) - configuration reference
- [databases.md](doc/databases.md) - database backends and tuning
- [archiving.md](doc/archiving.md) - archive groups and retention
- [graphql.md](doc/graphql.md) - GraphQL API
- [rest-api.md](doc/rest-api.md) - REST API
- [workflows.md](doc/workflows.md) - flow engine
- [nats.md](doc/nats.md) - native NATS and bridging
- [opcua.md](doc/opcua.md) - OPC UA client integration
- [opcua-server.md](doc/opcua-server.md) - OPC UA server
- [winccoa.md](doc/winccoa.md) - WinCC OA integration
- [winccua.md](doc/winccua.md) - WinCC Unified integration
- [neo4j.md](doc/neo4j.md) - Neo4j integration
- [mcp.md](doc/mcp.md) - MCP server
- [kafka.md](doc/kafka.md) - Kafka integration
- [security.md](doc/security.md) - TLS and security
- [users.md](doc/users.md) - user management
- [acl.md](doc/acl.md) - access control lists
- [ai-agents.md](doc/ai-agents.md) - AI agents

See also `dev/` for developer and AI coding documentation (iX guidelines, device integration guide, implementation plans). Start with [dev/INDEX.md](dev/INDEX.md).

## Requirements

- Java 21+
- Maven 3.6+ for builds
- PostgreSQL, MongoDB, CrateDB, SQLite, or another supported backend

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting a pull request. All contributions require a signed Copyright Assignment Agreement.

## License

GNU General Public License v3.0.
