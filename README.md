# MonsterMQ

A MQTT broker built with Kotlin on Vert.X and Hazelcast with persistent data storage through PostgreSQL, CrateDB, MongoDB, or SQLite.

## 🚀 Key Features

- **MQTT 3.1.1 Protocol** - Complete QoS 0,1,2 support with persistent sessions
- **Multi-Database Support** - PostgreSQL, CrateDB, MongoDB, SQLite backends
- **Hazelcast Clustering** - Multi-node scalability with automatic failover
- **Message Archiving** - Persistent storage with configurable retention policies
- **Workflows (Flow Engine)** - Visual flow-based programming with JavaScript runtime for data processing pipelines
- **Rate Limiting** - Configurable publish/subscribe rate protection against client overload
- **OPC UA Server** - Industrial protocol server with MQTT bridge and real-time subscriptions
- **WinCC OA Client** - High-performance bulk message transfer from Siemens WinCC Open Architecture SCADA systems
- **WinCC Unified Client** - GraphQL/WebSocket integration for Siemens WinCC Unified tag values and alarms
- **Neo4j Client** - Graph database integration for topic hierarchy analysis and path-based data modeling
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
└─────────────────┘    │              │    ┌─────────────────┐
                       │              │───▶│   Neo4j Graph   │
┌─────────────────┐    │              │    │ • Topic Trees   │
│ Graph Analytics │◀───┤              │    │ • Relationships │
│ • Neo4j Client  │    │              │    └─────────────────┘
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
- **[WinCC Unified Integration](doc/winccua.md)** - Siemens WinCC Unified GraphQL/WebSocket integration
- **[Neo4j Integration](doc/neo4j.md)** - Graph database for MQTT topic hierarchies
- **[GraphQL API](doc/graphql.md)** - Real-time data access and management
- **[MCP Server](doc/mcp.md)** - AI model integration and analytics
- **[Workflows (Flow Engine)](doc/workflows.md)** - Visual flow-based programming and data processing
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

### Key Benefits

- **GraphQL Subscriptions** - Real-time tag value updates via WebSocket
- **Active Alarms** - Subscribe to alarm notifications with full alarm details
- **Flexible Filtering** - Name filters with wildcards for tag subscriptions
- **Topic Transformation** - Convert tag names to MQTT topic hierarchies
- **Multiple Formats** - JSON with ISO timestamps, milliseconds, or raw values

See the [WinCC Unified Integration documentation](doc/winccua.md) for detailed configuration and setup instructions.

## 🕸️ Neo4j Graph Database Integration

**MQTT Topic Hierarchy as Graph Database**

MonsterMQ includes a native Neo4j client that automatically converts MQTT topic hierarchies into graph database structures, enabling powerful path-based queries and relationship analysis.

### Why Graph Database?

MQTT topics naturally form hierarchical structures. Neo4j excels at:
- **Path Queries** - Find all sensors under a building or floor
- **Relationship Analysis** - Discover connections between devices
- **Hierarchical Visualization** - Graph-based UI for topic exploration
- **Message Rate Limiting** - Prevent database overload with configurable suppression

See the [Neo4j Integration documentation](doc/neo4j.md) for detailed configuration, query examples, and best practices.

## 🔀 Workflows (Flow Engine)

**Visual Flow-Based Programming for MQTT Data Processing**

MonsterMQ includes a powerful workflow engine that enables visual data processing and transformation pipelines using a node-based programming model.

### Key Features

- **Visual Flow Editor** - Drag-and-drop interface for creating data processing flows
- **JavaScript Runtime** - Execute custom logic using GraalVM JavaScript engine
- **Flow Classes** - Reusable flow templates/blueprints
- **Flow Instances** - Deployable instances with custom configuration
- **Real-time Processing** - Process MQTT messages as they arrive
- **Instance Variables** - Configure flows with instance-specific values

### Quick Example

Create a temperature alert flow:

```javascript
// Node script: check_threshold
let temperature = inputs.temp.value;
let threshold = inputs.threshold.value;

if (temperature > threshold) {
    outputs.send("alert", {
        temperature: temperature,
        threshold: threshold,
        message: "Temperature exceeded!"
    });
}
```

Connect MQTT topics and deploy:
- **Input**: Subscribe to `sensors/warehouse/temperature`
- **Output**: Publish alerts to `alerts/warehouse/temperature`
- **Variables**: `location: "Warehouse A"`, `alertEmail: "ops@example.com"`

### Script API

Workflows provide the following globals in node scripts:

```javascript
// Input values from connected nodes or MQTT topics
inputs.temperature.value
inputs.threshold.value

// Message shorthand for triggering input
msg.value
msg.topic
msg.timestamp

// Send data to output ports
outputs.send("out", result);

// Persistent node-level state
state.count = (state.count || 0) + 1;

// Read-only instance configuration
let apiKey = flow.apiKey;
let serverUrl = flow.serverUrl;

// Logging
console.log("Processing:", msg.value);
```

See the [Workflows documentation](doc/workflows.md) for complete guide, examples, and API reference.

## 📋 Requirements

- **Java 21+**
- **Maven 3.6+** (for building)
- **Database** (PostgreSQL, MongoDB, CrateDB, or SQLite)
- **Kafka** (optional, for streaming)

## 📄 License

This project is licensed under the GNU GENERAL PUBLIC Version 3 License.

---

**MonsterMQ** - Factory MQTT Broker for industrial IoT and real-time messaging applications.