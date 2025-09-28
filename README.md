# MonsterMQ

A MQTT broker built with Kotlin on Vert.X and Hazelcast with persistent data storage through PostgreSQL, CrateDB, MongoDB, or SQLite.

## 🚀 Key Features

- **MQTT 3.1.1 Protocol** - Complete QoS 0,1,2 support with persistent sessions
- **Multi-Database Support** - PostgreSQL, CrateDB, MongoDB, SQLite backends
- **Hazelcast Clustering** - Multi-node scalability with automatic failover
- **Message Archiving** - Persistent storage with configurable retention policies
- **OPC UA Server** - Industrial protocol server with MQTT bridge and real-time subscriptions
- **GraphQL API** - Real-time data access and management interface
- **MCP Server** - AI model integration through Model Context Protocol
- **User Authentication** - BCrypt-secured user management with ACL rules
- **SparkplugB Support** - Industrial IoT message expansion
- **WebSocket Support** - MQTT over WebSockets (WS/WSS)
- **TLS/SSL Security** - Certificate-based authentication

## 🏃 Quick Start

### Docker (Recommended)

```bash
# Pull from Docker Hub
docker run -p 1883:1883 -p 4840:4840 -p 3000:3000 -p 4000:4000 -v ./config.yaml:/app/config.yaml rocworks/monstermq:latest

# Or with PostgreSQL
docker-compose up -d
```

### Build from Source

```bash
cd broker
mvn clean package

# Run with SQLite (development)
java -classpath "target/classes:target/dependencies/*" at.rocworks.MonsterKt -config example-config.yaml

# Run with clustering
java -classpath "target/classes:target/dependencies/*" at.rocworks.MonsterKt -cluster -config config-hazelcast.yaml

# Or use convenience script
./run.sh -config config.yaml
```

### Configuration Examples

```yaml
# Minimal configuration (config-sqlite.yaml)
TCP: 1883
DefaultStoreType: SQLITE
SQLite:
  Path: "monstermq.db"

# Production configuration (config.yaml)
TCP: 1883
TCPS: 8883
WS: 9000
WSS: 9001
DefaultStoreType: POSTGRES
Postgres:
  Url: jdbc:postgresql://localhost:5432/monster
  User: system
  Pass: manager
GraphQL:
  Enabled: true
  Port: 4000
MCP:
  Enabled: true
  Port: 3000
```

## 🗄️ Database Support

| Database | Session Store | Retained Store | Message Archive | Clustering |
|----------|:-------------:|:--------------:|:---------------:|:----------:|
| **PostgreSQL** | ✅ | ✅ | ✅ | ✅ |
| **CrateDB** | ✅ | ✅ | ✅ | ✅ |
| **MongoDB** | ✅ | ✅ | ✅ | ✅ |
| **SQLite** | ✅ | ✅ | ✅ | ❌ |
| **Memory** | ❌ | ✅ | ❌ | ✅ |
| **Hazelcast** | ❌ | ✅ | ❌ | ✅ |

**Note:** SQLite cannot be used in cluster mode - use PostgreSQL, CrateDB, or MongoDB for clustering.

## 🔧 Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐
│   MQTT Clients  │───▶│   MonsterMQ  │───▶│   Databases     │
│ • TCP/TLS       │    │   Broker     │    │ • PostgreSQL    │
│ • WebSocket     │    │              │    │ • CrateDB       │
│ • QoS 0,1,2     │    │ ┌──────────┐ │    │ • MongoDB       │
└─────────────────┘    │ │Clustering│ │    │ • SQLite        │
                       │ │Hazelcast │ │    └─────────────────┘
┌─────────────────┐    │ └──────────┘ │
│ OPC UA Devices  │◀───┤              │    ┌─────────────────┐
│ • Certificates  │    │ ┌──────────┐ │───▶│   Kafka         │
│ • Secure Conn.  │    │ │ Archive  │ │    │   (Optional)    │
└─────────────────┘    │ │ Manager  │ │    └─────────────────┘
                       │ └──────────┘ │
┌─────────────────┐    │              │    ┌─────────────────┐
│   AI Models     │◀───┤ ┌──────────┐ │    │   GraphQL API   │
│ • MCP Server    │    │ │ GraphQL  │ │───▶│ • Real-time     │
│ • Analytics     │    │ │   API    │ │    │ • Management    │
└─────────────────┘    │ └──────────┘ │    └─────────────────┘
                       └──────────────┘
```

## 📚 Documentation

For detailed documentation, see the [`doc/`](doc/) directory:

- **[Installation & Setup](doc/installation.md)** - Complete setup guide with examples
- **[Configuration Reference](doc/configuration.md)** - All configuration options and schema
- **[Database Setup](doc/databases.md)** - Database-specific configuration and optimization
- **[Clustering](doc/clustering.md)** - Hazelcast clustering and high availability
- **[Message Archiving](doc/archiving.md)** - Archive groups, retention policies, and storage
- **[User Management & ACL](doc/user-management.md)** - Authentication and authorization system
- **[OPC UA Integration](doc/opcua.md)** - Industrial protocol support and certificates
- **[GraphQL API](doc/graphql.md)** - Real-time data access and management
- **[MCP Server](doc/mcp.md)** - AI model integration and analytics
- **[Kafka Integration](doc/kafka.md)** - Stream processing and event sourcing
- **[Security](doc/security.md)** - TLS, certificates, and best practices
- **[Performance & Monitoring](doc/performance.md)** - Optimization and monitoring
- **[Development](doc/development.md)** - Building, testing, and contributing

## 🐳 Docker Hub

Available at: **[rocworks/monstermq:latest](https://hub.docker.com/r/rocworks/monstermq)**

```bash
# Latest stable release
docker pull rocworks/monstermq:latest

# Run with custom configuration
docker run -p 1883:1883 -p 4840:4840 -p 3000:3000 -p 4000:4000 -v ./config.yaml:/app/config.yaml rocworks/monstermq:latest

# Docker Compose with PostgreSQL

# Or create your own docker-compose.yml:
version: '3.8'
services:
  monstermq:
    image: rocworks/monstermq:latest
    restart: unless-stopped
    ports:
      - "1883:1883"    # MQTT TCP
      - "8883:8883"    # MQTT TLS
      - "9000:9000"    # WebSocket
      - "9001:9001"    # WebSocket TLS
      - "4840:4840"    # OPC UA Server
      - "4000:4000"    # GraphQL API
      - "3000:3000"    # MCP Server
    volumes:
      - ./log:/app/log
      - ./config.yaml:/app/config.yaml
      - ./security:/app/security
    depends_on:
      - postgres
    environment:
      - JAVA_OPTS=-Xmx512m

  postgres:
    image: postgres:16-alpine
    restart: unless-stopped
    environment:
      POSTGRES_DB: monster
      POSTGRES_USER: system
      POSTGRES_PASSWORD: manager
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

volumes:
  postgres_data:

# Example config.yaml for PostgreSQL setup:
TCP: 1883
TCPS: 8883
WS: 9000
WSS: 9001

DefaultStoreType: POSTGRES

Postgres:
  Url: jdbc:postgresql://postgres:5432/monster
  User: system
  Pass: manager

GraphQL:
  Enabled: true
  Port: 4000

MCP:
  Enabled: true
  Port: 3000
```

> docker-compose up -d


## 🌐 Endpoints

| Service | Default Port | Description |
|---------|:------------:|-------------|
| MQTT TCP | 1883 | Standard MQTT protocol |
| MQTT TLS | 8883 | MQTT over TLS/SSL |
| WebSocket | 9000 | MQTT over WebSocket |
| WebSocket TLS | 9001 | MQTT over secure WebSocket |
| **OPC UA Server** | **4840** | **Industrial protocol with MQTT bridge** |
| GraphQL API | 4000 | Management and real-time data |
| MCP Server | 3000 | AI model integration |

## 🏭 OPC UA Server

MonsterMQ includes a built-in **OPC UA Server** that bridges MQTT and OPC UA protocols, enabling seamless integration between modern IoT systems and industrial automation.

### Key Features

- **MQTT-to-OPC UA Bridge** - Automatically creates OPC UA nodes from MQTT topics
- **Real-time Subscriptions** - OPC UA clients receive live updates when MQTT messages arrive
- **Multiple Data Types** - Support for TEXT, NUMERIC, BOOLEAN, BINARY, and JSON data types
- **Hierarchical Structure** - MQTT topic paths become OPC UA folder hierarchies
- **Eclipse Milo Foundation** - Built on industry-standard Eclipse Milo OPC UA SDK
- **Web-based Configuration** - Configure address mappings through the GraphQL dashboard

### Configuration

Configure OPC UA servers through the GraphQL API or dashboard:

```yaml
# Example configuration (via GraphQL API)
mutation {
  createOpcUaServer(input: {
    name: "test"
    enabled: true
    port: 4840
    namespaceUri: "urn:MonsterMQ:OpcUaServer"
    addressMappings: [
      {
        topicPattern: "sensors/temperature/#"
        browseName: "temperature"
        dataType: NUMERIC
        accessLevel: READ_ONLY
      }
      {
        topicPattern: "devices/status/#"
        browseName: "status"
        dataType: TEXT
        accessLevel: READ_WRITE
      }
    ]
  }) {
    success
  }
}
```

### Data Type Mapping

| MQTT Payload | OPC UA Data Type | Example |
|-------------|------------------|---------|
| `"23.5"` | Double (NUMERIC) | Temperature sensor |
| `"true"` | Boolean (BOOLEAN) | Status flags |
| `"Hello World"` | String (TEXT) | Status messages |
| `"base64data"` | ByteString (BINARY) | File transfers |
| `{"temp":23.5}` | String (JSON) | Complex data |

### Address Space Structure

MQTT topics are automatically mapped to OPC UA nodes:

```
MQTT Topic: sensors/temperature/room1
OPC UA Path: Objects/MonsterMQ/sensors/temperature/room1

MQTT Topic: factory/line1/status
OPC UA Path: Objects/MonsterMQ/factory/line1/status
```

### Real-time Updates

When MQTT messages are published, OPC UA subscribers receive immediate notifications:

1. **MQTT Message Published** → `sensors/temp1` with payload `"24.5"`
2. **OPC UA Node Updated** → `MonsterMQ/sensors/temp1` value becomes `24.5`
3. **Subscriptions Notified** → All OPC UA clients subscribed to the node receive the update

## 🧪 Example Usage

### MQTT Operations
```bash
# Publish message
mosquitto_pub -h localhost -p 1883 -t "sensors/temp1" -m "23.5"

# Subscribe to topics
mosquitto_sub -h localhost -p 1883 -t "sensors/#"
```

### OPC UA Operations
```bash
# Connect with OPC UA client
opc.tcp://localhost:4840/server

# Browse namespace
Objects/MonsterMQ/sensors/temp1

# Subscribe to data changes
# (Temperature updates will be received in real-time)
```

### GraphQL Queries
```bash
# Query current values
curl -X POST http://localhost:4000/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "query { currentValue(topic: \"sensors/temp1\") { payload timestamp } }"}'

# Publish via GraphQL
curl -X POST http://localhost:4000/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "mutation { publish(input: {topic: \"sensors/temp1\", payload: \"25.0\", qos: 0}) { success } }"}'
```

## 📋 Requirements

- **Java 21+**
- **Maven 3.6+** (for building)
- **Database** (PostgreSQL, MongoDB, CrateDB, or SQLite)
- **Kafka** (optional, for streaming)

## 📄 License

This project is licensed under the GNU GENERAL PUBLIC Version 3 License.

---

**MonsterMQ** - Factory MQTT Broker for industrial IoT and real-time messaging applications.