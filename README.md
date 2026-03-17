# MonsterMQ

MonsterMQ is a high-performance MQTT broker for industrial IoT and real-time messaging, built on Vert.x. It combines MQTT messaging with storage, protocol bridges, workflows, a web dashboard, and management APIs.

## Highlights

- MQTT 3.1.1 with broad MQTT 5 support ([details](doc/mqtt5.md))
- Native NATS protocol support and MQTT <-> NATS bridging ([details](doc/nats.md))
- WebSocket, TLS, retained messages, clustering, and MQTT-based system logging ([details](doc/mqtt-logging.md))
- Storage backends for PostgreSQL, CrateDB, MongoDB, SQLite, and more via JDBC ([database docs](doc/databases.md))
- Archive groups, last-value storage, retention policies, and schema-based JDBC logging ([archiving](doc/archiving.md), [Snowflake](doc/snowflake.md))
- Device integrations for OPC UA, PLC4X, WinCC OA, WinCC Unified, Neo4j, Kafka, and others
- Flow engine for visual, JavaScript-based message processing ([details](doc/workflows.md))
- GraphQL API, MCP server, web dashboard, and CESMII I3X API support

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
  MqttApi: false         # Enable/disable MQTT API service (JSON-RPC 2.0)

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
  Provider: "gemini"
  # Set your API key in environment: export GENAI_API_KEY="your-key-here"
  ApiKey: "${GENAI_API_KEY}"
  Model: "gemini-2.5-flash"
  MaxTokens: 0
  Temperature: 0.7
  DocsPath: "docs"  
```

For more examples, see `broker/configs/`.

## Core Capabilities

### Messaging and Protocols

- MQTT over TCP, TLS, and WebSocket
- Native NATS server for standard NATS clients
- MQTT client bridge for remote brokers
- MQTT logging for publishing broker logs to topics

### Storage and Archiving

- Session, retained, archive, and last-value storage
- Archive groups with retention and performance metrics
- JDBC logger with JSON schema validation, field mapping, and bulk writes
- Optional PostgreSQL schema support for multi-tenant setups

### Device and System Integration

- OPC UA client and server ([docs](doc/opcua.md), [server](doc/opcua-server.md))
- PLC4X for direct PLC access
- WinCC OA and WinCC Unified connectors ([WinCC OA](doc/winccoa.md), [WinCC Unified](doc/winccua.md))
- Neo4j integration for topic hierarchy analysis ([details](doc/neo4j.md))
- Kafka integration for event streaming ([details](doc/kafka.md))

### Processing and APIs

- Flow engine with reusable flow classes and deployed instances
- JavaScript execution with GraalVM
- GraphQL queries, mutations, and subscriptions ([details](doc/graphql.md))
- MQTT JSON-RPC API for GraphQL over MQTT ([details](doc/mqtt-api.md))
- MCP server for AI-oriented access ([details](doc/mcp.md))

### Operations and UI

- Web dashboard for monitoring, configuration, users, topics, devices, and workflows
- Hazelcast-based clustering and failover ([details](doc/clustering.md))
- TLS, certificates, ACLs, and user management ([security](doc/security.md), [users](doc/users.md), [ACL](doc/acl.md))

## Database Support

| Database | Session Store | Retained Store | Message Archive | Clustering |
|----------|:-------------:|:--------------:|:---------------:|:----------:|
| PostgreSQL | Yes | Yes | Yes | Yes |
| CrateDB | Yes | Yes | Yes | Yes |
| MongoDB | Yes | Yes | Yes | Yes |
| SQLite | Yes | Yes | Yes | No |
| Memory | No | Yes | No | Yes |
| Hazelcast | No | Yes | No | Yes |

SQLite is not suitable for cluster mode; use PostgreSQL, CrateDB, or MongoDB instead.

## Default Endpoints

| Service | Port |
|---------|:----:|
| MQTT TCP | 1883 |
| MQTT TLS | 8883 |
| WebSocket | 9000 |
| WebSocket TLS | 9001 |
| NATS | 4222 |
| OPC UA Server | 4840 |
| GraphQL API and Dashboard | 4000 |
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

## Documentation

See `doc/` for full documentation:

- `doc/installation.md` - installation and setup
- `doc/configuration.md` - configuration reference
- `doc/databases.md` - database backends and tuning
- `doc/archiving.md` - archive groups and retention
- `doc/graphql.md` - GraphQL API
- `doc/workflows.md` - flow engine
- `doc/nats.md` - native NATS and bridging
- `doc/opcua.md` - OPC UA integration
- `doc/winccoa.md` - WinCC OA integration
- `doc/winccua.md` - WinCC Unified integration
- `doc/neo4j.md` - Neo4j integration
- `doc/mcp.md` - MCP server
- `doc/kafka.md` - Kafka integration
- `doc/security.md` - TLS and security
- `doc/development.md` - build, test, and development notes

## Requirements

- Java 21+
- Maven 3.6+ for builds
- PostgreSQL, MongoDB, CrateDB, SQLite, or another supported backend

## License

GNU General Public License v3.0.
