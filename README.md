# MonsterMQ

A MQTT broker built with Kotlin on Vert.X and Hazelcast with persistent data storage through PostgreSQL, CrateDB, MongoDB, or SQLite.

## 🚀 Key Features

- **MQTT 3.1.1 Protocol** - Complete QoS 0,1,2 support with persistent sessions
- **Multi-Database Support** - PostgreSQL, CrateDB, MongoDB, SQLite backends
- **Hazelcast Clustering** - Multi-node scalability with automatic failover
- **Message Archiving** - Persistent storage with configurable retention policies
- **Rate Limiting** - Configurable publish/subscribe rate protection against client overload
- **OPC UA Server** - Industrial protocol server with MQTT bridge and real-time subscriptions
- **WinCC OA Client** - High-performance bulk message transfer from Siemens WinCC Open Architecture SCADA systems
- **WinCC Unified Client** - GraphQL/WebSocket integration for Siemens WinCC Unified tag values and alarms
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
│ WinCC OA SCADA  │───▶│ ┌──────────┐ │    │   GraphQL API   │
│ • SQL Queries   │    │ │ GraphQL  │ │───▶│ • Real-time     │
│ • Bulk Transfer │    │ │   API    │ │    │ • Management    │
└─────────────────┘    │ └──────────┘ │    └─────────────────┘
                       │              │
┌─────────────────┐    │              │
│   AI Models     │◀───┤              │
│ • MCP Server    │    │              │
│ • Analytics     │    │              │
└─────────────────┘    └──────────────┘
```

## 📚 Documentation

For detailed documentation, see the [`doc/`](doc/) directory:

- **[Installation & Setup](doc/installation.md)** - Complete setup guide with examples
- **[Configuration Reference](doc/configuration.md)** - All configuration options and schema
- **[Database Setup](doc/databases.md)** - Database-specific configuration and optimization
- **[Clustering](doc/clustering.md)** - Hazelcast clustering and high availability
- **[Message Archiving](doc/archiving.md)** - Archive groups, retention policies, and storage
- **[User Management & ACL](doc/users.md)** - Authentication and authorization system
- **[Access Control Lists (ACL)](doc/acl.md)** - Comprehensive ACL documentation and examples
- **[OPC UA Integration](doc/opcua.md)** - Industrial protocol support and certificates
- **[WinCC OA Integration](doc/winccoa.md)** - Siemens WinCC OA SCADA system integration
- **[GraphQL API](doc/graphql.md)** - Real-time data access and management
- **[MCP Server](doc/mcp.md)** - AI model integration and analytics
- **[Kafka Integration](doc/kafka.md)** - Stream processing and event sourcing
- **[Security](doc/security.md)** - TLS, certificates, and best practices
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

## 🧪 Example Usage

### MQTT Operations
```bash
# Publish message
mosquitto_pub -h localhost -p 1883 -t "sensors/temp1" -m "23.5"

# Subscribe to topics
mosquitto_sub -h localhost -p 1883 -t "sensors/#"
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

## 🏭 WinCC OA Integration

**High-Performance SCADA Data Transfer**

MonsterMQ includes a native WinCC OA client that efficiently transfers data from Siemens WinCC OA SCADA systems to MQTT.

### 🤨 Why not use WinCC OA's built-in MQTT capabilities?

Because the MQTT protocol doesn't support bulk messages, it's not efficient for transferring a large number of topic value changes. MonsterMQ's WinCC OA client leverages WinCC OA's powerful **continuous SQL queries** (`dpQueryConnectSingle`), making it possible to subscribe to **5 million tags with just a single SQL query**.

### Key Benefits

- **Massive Scale** - Subscribe to millions of datapoints with minimal configuration
- **Efficient Bulk Transfer** - Process high-volume tag changes without MQTT per-message overhead
- **Real-time Streaming** - GraphQL subscription-based updates via WebSocket
- **Flexible Topic Mapping** - Transform WinCC OA datapoint names to MQTT topic hierarchies
- **Multiple Formats** - Publish as JSON (ISO/epoch) or raw values

See the [WinCC OA Integration documentation](doc/winccoa.md) for detailed configuration and setup instructions.

## 🏭 WinCC Unified Integration

**Modern SCADA Integration via GraphQL**

MonsterMQ includes a WinCC Unified client that connects to Siemens WinCC Unified systems using their GraphQL API over WebSocket for real-time tag values and alarm notifications.

### Key Features

- **GraphQL Subscriptions** - Real-time tag value updates via WebSocket
- **Active Alarms** - Subscribe to alarm notifications with full alarm details
- **Quality Information** - Optional OPC UA quality data in tag values
- **Flexible Filtering** - Name filters with wildcards (e.g., `HMI_*`, `TANK_*`) for tag subscriptions
- **Topic Transformation** - Convert tag names to MQTT topic hierarchies with regex support
- **Multiple Formats** - JSON with ISO timestamps, milliseconds, or raw values

### Example Configuration

```yaml
WinCCUa:
  Clients:
    - Name: "winccua-plant1"
      Namespace: "winccua/plant1"
      GraphqlEndpoint: "http://winccua-server:4000/graphql"
      Username: "admin"
      Password: "password"
      MessageFormat: "JSON_ISO"
      Addresses:
        - Type: TAG_VALUES
          Topic: "tags"
          NameFilters: ["HMI_*", "TANK_*"]
          IncludeQuality: true
        - Type: ACTIVE_ALARMS
          Topic: "alarms"
          SystemNames: ["System1"]
```

See the device integration documentation for detailed configuration and setup instructions.

## 📋 Requirements

- **Java 21+**
- **Maven 3.6+** (for building)
- **Database** (PostgreSQL, MongoDB, CrateDB, or SQLite)
- **Kafka** (optional, for streaming)

## 📄 License

This project is licensed under the GNU GENERAL PUBLIC Version 3 License.

---

**MonsterMQ** - Factory MQTT Broker for industrial IoT and real-time messaging applications.