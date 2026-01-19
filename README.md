# MonsterMQ

A high-performance, enterprise-grade MQTT broker with advanced data processing capabilities, built on Vert.x and designed for industrial IoT and real-time messaging scenarios.

## 🚀 Key Features

### Core MQTT Broker
- **Full MQTT 3.1.1 Support** - Complete protocol implementation with QoS 0, 1, 2
- **High Performance** - Built on Vert.x for maximum throughput and low latency  
- **SSL/TLS Security** - End-to-end encryption with certificate management
- **WebSocket Support** - MQTT over WebSocket for web applications
- **Clustering** - Multi-node deployment with Hazelcast clustering and automatic failover
- **Retained Messages** - Persistent message storage for new subscribers
- **MQTT Logging** - Publish all system logs to MQTT topics in real-time ([details](doc/mqtt-logging.md))

### Data Integration & Storage

#### Multi-Database Support 📊
Advanced database integration for real-time data archiving and analytics:

- **PostgreSQL, QuestDB, TimescaleDB** - Time-series and relational databases
- **SQLite** - Lightweight embedded deployments
- **CrateDB** - Distributed analytics and large-scale IoT data
- **MongoDB** - Document-based storage with flexible schema
- **MySQL** - Widely-used relational database
- **Snowflake** - Cloud data warehouse with private key authentication
- **Custom JDBC** - Support for any JDBC-compatible database

#### Archive Groups 🗄️
Flexible message storage with configurable retention and archiving:

- **Last Value Store** - Keep latest values for instant access
- **Message Archive** - Long-term storage with compression
- **Retention Policies** - Automatic cleanup and data lifecycle management
- **Performance Monitoring** - Real-time throughput and storage metrics
- **Smart Data Processing** - JSON Schema validation and transformation
- **Connection Pooling** - Enterprise-grade database connection management

**Note:** Two archive groups are automatically created:
- **Default** - In-memory store for process images (disabled by default, must be enabled in configuration)
- **Syslogs** - Archive for broker system logs (disabled by default, requires `ArchiveType` to be defined before enabling)

#### JDBC Logger 📝
Schema-based MQTT message logging to databases with JSON validation:

- **Schema Validation** - Validate MQTT messages against JSON Schema before writing
- **Bulk Writes** - Configurable batch size and timeout for optimal performance
- **Dynamic Tables** - Extract table name from message payload using JSONPath
- **Queue Buffering** - Memory or disk-based message queuing with overflow protection
- **Auto Table Creation** - Automatically create tables based on JSON schema
- **Field Mapping** - Map JSON fields to database columns with JSONPath expressions
- **Supported Databases**:
  - **QuestDB** - High-performance time-series database
  - **PostgreSQL** - Enterprise-grade relational database
  - **TimescaleDB** - PostgreSQL extension for time-series data
  - **MySQL** - Popular open-source database
  - **Snowflake** - Cloud data warehouse ([configuration guide](broker/SNOWFLAKE.md))

**Snowflake Configuration Example:**
```yaml
JDBC URL: jdbc:snowflake://account.snowflakecomputing.com
Account: MYORG-MYACCOUNT
Username: mqtt_logger_user
Private Key File: /etc/snowflake/keys/rsa_key.p8
Warehouse: COMPUTE_WH
Database: IOT_DATA
Schema: SENSORS
```

For detailed Snowflake setup, see [SNOWFLAKE.md](broker/SNOWFLAKE.md).

### Device Integration 🔌

#### OPC UA Integration
- **Client & Server** - Connect to PLCs and industrial systems
- **Security** - Certificate management and authentication
- **Real-time Data** - Subscribe to OPC UA variables with automatic mapping to MQTT

#### MQTT Client Bridge
- **Remote Broker Connection** - Bridge multiple MQTT brokers
- **Topic Mapping** - Flexible topic transformation and routing
- **Failover Support** - Automatic reconnection and buffering

#### PLC4X Integration
- **Multi-Protocol Support** - Connect to Siemens, Allen-Bradley, Modbus, and more
- **Direct PLC Access** - Read/write PLC variables without gateways
- **Industrial Standards** - Support for major industrial protocols

#### WinCC OA & Unified Integration
- **High-Performance SCADA** - Bulk message transfer from Siemens systems
- **GraphQL/WebSocket** - Real-time tag values and alarm notifications
- **Massive Scale** - Subscribe to millions of datapoints efficiently

#### Neo4j Graph Database
- **Topic Hierarchy Analysis** - MQTT topics as graph relationships
- **Path-based Queries** - Discover device connections and hierarchies
- **Graph Analytics** - Powerful relationship analysis for IoT data

### Workflow Engine 🔄
Visual flow-based data processing inspired by Node-RED:

#### Flow Classes (Templates)
- **Reusable Templates** - Define processing logic once, deploy everywhere
- **Node Types** - Function, filter, transform, and custom JavaScript nodes
- **Visual Design** - Drag-and-drop workflow creation with zoom/pan editor

#### Flow Instances (Deployments)  
- **Input Mapping** - Connect MQTT topics to workflow inputs
- **Output Mapping** - Publish results to configured topics
- **JavaScript Processing** - GraalVM-powered JavaScript execution
- **Real-time Execution** - Event-driven processing with low latency

#### Workflow Features
- **Cluster Deployment** - Distribute workflows across nodes
- **State Management** - Persistent variables and context
- **Error Handling** - Robust error capture and reporting
- **Performance Monitoring** - Execution metrics and debugging

### Web Dashboard 🖥️
Modern, responsive web interface for complete system management:

- **Real-time Monitoring** - Live metrics and system status
- **Configuration Management** - Visual configuration of all components
- **User Management** - Role-based access control with ACL
- **Topic Browser** - Interactive MQTT topic exploration with drag-and-drop
- **Workflow Editor** - Visual workflow design, debugging, and deployment
- **Device Management** - Configure and monitor all connected devices

## 🏃 Quick Start

### Docker (Recommended)

```bash
# Pull from Docker Hub
docker run -p 1883:1883 -p 4840:4840 -p 3000:3000 -p 4000:4000 -v ./config.yaml:/app/config.yaml rocworks/monstermq:latest

# Or with PostgreSQL and full docker-compose
docker-compose up -d
```

### Build from Source

```bash
cd broker
mvn compile

# Run with SQLite (development)
./run.sh

# Run with PostgreSQL (production)  
./run.sh -config config-postgres.yaml

# Run with clustering
./run.sh -cluster -config config-hazelcast.yaml
```

### Configuration Examples

```yaml
# Production configuration (config.yaml)
TCP: 1883
TCPS: 8883
WS: 9000
WSS: 9001

DefaultStoreType: POSTGRES

# PostgreSQL configuration
Postgres:
  Url: jdbc:postgresql://localhost:5432/monster
  User: system
  Pass: manager
  # Schema: mqtt_broker    # Optional: custom PostgreSQL schema (defaults to 'public')

# Enable MQTT logging
Logging:
  MqttEnabled: true    # Publish logs to MQTT topics
  MqttLevel: INFO      # Minimum log level: INFO, WARNING, or SEVERE

# Service endpoints
GraphQL:
  Enabled: true
  Port: 4000
```

#### PostgreSQL Multi-Schema Setup

For multi-tenant or multi-environment deployments, use different PostgreSQL schemas:

```yaml
# Environment-specific configuration
Postgres:
  Url: jdbc:postgresql://db.example.com:5432/monstermq
  User: system
  Pass: manager
  Schema: prod_mqtt_broker    # Production environment
```

See [Database Configuration](doc/databases.md#postgresql-schema-support-optional) for detailed documentation on schema support.

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

MonsterMQ follows a modular, event-driven architecture built on Eclipse Vert.x, providing high-performance message routing with enterprise-grade reliability and clustering capabilities.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                Client Layer                                     │
│                                                                                 │
│  MQTT Clients    OPC UA Devices    WinCC Systems    PLC4X    AI/Analytics       │
│  IoT Devices     Industrial PLCs   SCADA Systems    Modbus   MCP Clients        │
└────────────────────────────────┬────────────────────────────────────────────────┘
                                 │
┌────────────────────────────────▼────────────────────────────────────────────────┐
│                             Protocol Layer                                      │
│                                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌──────────────────────┐.   │
│  │ MQTT 3.1.1/5│  │  OPC UA     │  │  GraphQL    │  │  Device Integration  │    │
│  │ TCP / TLS   │  │  Server     │  │  WebSocket  │  │  WinCC / PLC4X       │    │
│  │ WebSocket   │  │  Client     │  │  Queries    │  │  Neo4j / Kafka       │    │
│  └─────────────┘  └─────────────┘  └─────────────┘  └──────────────────────┘    │
└────────────────────────────────┬────────────────────────────────────────────────┘
                                 │
┌────────────────────────────────▼────────────────────────────────────────────────┐
│                        Core Processing Layer                                    │
│                          (Vert.x Event Bus)                                     │
│                                                                                 │
│  ┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐.  │
│  │   Message Router     │  │  Workflow Engine     │  │  Archive Manager     │.  │
│  │  • Topic Matching    │  │  • Flow Classes      │  │  • Archive Groups    │   │
│  │  • Pub/Sub Logic     │  │  • Flow Instances    │  │  • Default (memory)  │.  │
│  │  • QoS 0/1/2         │  │  • JavaScript Engine │  │  • Syslogs           │   │
│  │  • Subscriptions     │  │  • Real-time Exec    │  │  • Last Value Store  │   │
│  └──────────────────────┘  └──────────────────────┘  └──────────────────────┘   │
│                                                                                 │
│  ┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐   │
│  │  Auth & ACL          │  │  Session Manager     │  │  Retained Store      │.  │
│  │  • User Management   │  │  • Client State      │  │  • Last Messages     │   │
│  │  • Topic Permissions │  │  • Subscriptions     │  │  • QoS Persistence   │.  │
│  │  • Password Hashing  │  │  • Will Messages     │  │  • Multi-Backend     │   │
│  └──────────────────────┘  └──────────────────────┘  └──────────────────────┘.  │
└────────────────────────────────┬────────────────────────────────────────────────┘
                                 │
┌────────────────────────────────▼────────────────────────────────────────────────┐
│                           Storage Layer                                         │
│                                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │ PostgreSQL  │  │  CrateDB    │  │  MongoDB    │  │   SQLite    │             │
│  │ QuestDB     │  │ TimescaleDB │  │   Kafka     │  │   MySQL     │             │
│  │ Snowflake   │  │   Custom    │  │             │  │             │             │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘             │
│                                                                                 │
│  Sessions • Retained • Archives • Last Values • Logs • JDBC Logger (Schema)     │
└────────────────────────────────┬────────────────────────────────────────────────┘
                                 │
┌────────────────────────────────▼────────────────────────────────────────────────┐
│                       Clustering Layer (Optional)                               │
│                                                                                 │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │                         Hazelcast Grid                                   │   │
│  │  • Distributed Maps  • Event Sourcing  • Node Discovery  • State Sync    │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Management & Monitoring                                 │
│                                                                                 │
│  ┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐   │
│  │   GraphQL API        │  │   Web Dashboard      │  │   MCP Server         │   │
│  │  • Queries           │  │  • Configuration     │  │  • AI Integration    │   │
│  │  • Mutations         │  │  • Topic Browser     │  │  • GraphQL API       │   │
│  │  • Subscriptions     │  │  • Flow Editor       │  │  • SQL Queries       │   │
│  │  • Port 4000         │  │  • User Management   │  │                      │   │ 
│  └──────────────────────┘  └──────────────────────┘  └──────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Component Layers

#### 1. **Protocol & Transport Layer**
- **MQTT 3.1.1** - Full protocol compliance with QoS 0, 1, 2
- **Multiple Transports** - TCP, TLS, WebSocket, WebSocket Secure
- **Authentication** - Username/password, certificates, token-based
- **Access Control** - Topic-based ACL with pattern matching

#### 2. **Message Processing Core**
- **Topic Router** - High-performance topic matching and subscription management
- **QoS Manager** - Message persistence, acknowledgments, and retry logic
- **Session Manager** - Client state, subscriptions, and will messages
- **Retained Store** - Last message storage with configurable persistence

#### 3. **Workflow Engine**
- **Visual Editor** - Drag-and-drop flow creation with zoom/pan interface
- **Flow Classes** - Reusable templates with configurable parameters
- **Flow Instances** - Deployed workflows with input/output topic mapping
- **JavaScript Runtime** - GraalVM-powered execution with state management

#### 4. **Archive & Storage**
- **Archive Groups** - Configurable message persistence with retention policies
- **JDBC Logger** - Schema-based message logging with validation (PostgreSQL, QuestDB, TimescaleDB, MySQL, Snowflake)
- **Multi-Database** - Support for PostgreSQL, QuestDB, TimescaleDB, MongoDB, CrateDB, SQLite, MySQL, Snowflake
- **JSON Schema** - Message validation and transformation
- **Connection Pooling** - Enterprise-grade database connection management

#### 5. **Integration & APIs**
- **GraphQL API** - Real-time queries, mutations, and subscriptions
- **Web Dashboard** - Modern React-based management interface  
- **OPC UA Server** - Industrial protocol bridge with certificate management
- **Device Clients** - WinCC OA, PLC4X, Neo4j, Kafka integration

#### 6. **Clustering & High Availability**
- **Hazelcast Grid** - Distributed state management and coordination
- **Node Discovery** - Automatic cluster formation and health monitoring
- **Load Balancing** - Client connection distribution across nodes
- **Failover** - Automatic client migration and message routing

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
- **[MQTT JSON-RPC 2.0 API](doc/mqtt-api.md)** - Execute GraphQL queries/mutations over MQTT
- **[MCP Server](doc/mcp.md)** - AI model integration with GraphQL API and SQL queries
- **[Workflows (Flow Engine)](doc/workflows.md)** - Visual flow-based programming and data processing
- **[Kafka Integration](doc/kafka.md)** - Stream processing and event sourcing
- **[MQTT Logging](doc/mqtt-logging.md)** - Real-time system logging via MQTT topics
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

This project is licensed under the GNU General Public License v3.0.

---

**MonsterMQ** - Factory MQTT Broker for industrial IoT and real-time messaging applications.