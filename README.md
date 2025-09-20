# MonsterMQ

A MQTT broker built with Kotlin on Vert.X and Hazelcast with persistent data storage through PostgreSQL, CrateDB, MongoDB, or SQLite.

## 🚀 Key Features

- **MQTT 3.1.1 Protocol** - Complete QoS 0,1,2 support with persistent sessions
- **Multi-Database Support** - PostgreSQL, CrateDB, MongoDB, SQLite backends
- **Hazelcast Clustering** - Multi-node scalability with automatic failover
- **Message Archiving** - Persistent storage with configurable retention policies
- **OPC UA Integration** - Native industrial protocol support with certificate security
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
docker run -p 1883:1883 -p 3000:3000 -p 4000:4000 -v ./config.yaml:/app/config.yaml rocworks/monstermq:latest

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
docker run -p 1883:1883 -p 3000:3000 -p 4000:4000 -v ./config.yaml:/app/config.yaml rocworks/monstermq:latest

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
| GraphQL API | 4000 | Management and real-time data |
| MCP Server | 3000 | AI model integration |

## 🧪 Example Usage

```bash
# Publish message
mosquitto_pub -h localhost -p 1883 -t "sensors/temp1" -m "23.5"

# Subscribe to topics
mosquitto_sub -h localhost -p 1883 -t "sensors/#"

# GraphQL query (current values)
curl -X POST http://localhost:4000/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "query { currentValue(topic: \"sensors/temp1\") { payload timestamp } }"}'

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