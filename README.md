# MonsterMQ

MonsterMQ is a high-performance, scalable MQTT broker built on Vert.X and Hazelcast with persistent data storage through PostgreSQL, CrateDB, MongoDB, or SQLite. It features built-in clustering, unlimited message storage, comprehensive user authentication with ACL support, automatic message retention, and AI integration through a Model Context Protocol (MCP) server. 

## 📑 Table of Contents

- [🚀 Key Features](#-key-features)
  - [Enterprise-Grade MQTT Broker](#enterprise-grade-mqtt-broker)
  - [Horizontal Scaling & High Availability](#horizontal-scaling--high-availability)
  - [Multi-Database Backend Support](#multi-database-backend-support)
  - [AI Integration (MCP Server)](#ai-integration-mcp-server)
  - [SparkplugB Extension](#sparkplugb-extension)
  - [User Authentication & ACL System](#user-authentication--acl-system)
  - [Message Retention & Purging](#message-retention--purging)
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
  - [GraphQL API Integration](#graphql-api-integration)
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

### **OPC UA Integration**
- **Unified Message Archiving** - OPC UA values use the same central publishing system as MQTT clients, ensuring complete archival
- **Cluster-Aware Device Management** - Dynamic deployment of OPC UA connectors across cluster nodes
- **Flexible Address Configuration** - Support for both NodeId and BrowsePath subscriptions with configurable topic mapping
- **Real-Time Configuration** - Hot reconfiguration of devices and addresses without broker restart

### **SparkplugB Extension**
- **Automatic Message Expansion** - Converts SparkplugB messages from `spBv1.0` to `spBv1.0e` topics
- **Industrial IoT Support** - Native support for Sparkplug specification
- **Metric Extraction** - Automatic parsing and expansion of Sparkplug payloads

### **User Authentication & ACL System**
- **Multi-Database User Storage** - PostgreSQL, SQLite, CrateDB, MongoDB support for user data
- **BCrypt Password Security** - Industry-standard password hashing with configurable work factor
- **Fine-Grained ACL Rules** - Topic-level permissions with MQTT wildcard support (`+`, `#`)
- **Anonymous User Support** - Configurable anonymous access for unauthenticated clients
- **GraphQL User Management API** - Complete CRUD operations for users and ACL rules

### **Archive Configuration Management**
- **Dynamic Archive Groups** - Create, modify, and delete archive groups via GraphQL API without restarts
- **Database-Stored Configuration** - Archive group configurations persisted in database instead of YAML files
- **Real-Time Configuration Updates** - Enable/disable archive groups instantly with automatic message routing
- **Storage Lifecycle Management** - Automatic creation and cleanup of database tables/collections
- **Web-Based Management** - Full-featured dashboard for archive group administration
- **Multi-Database Support** - Configuration storage across PostgreSQL, SQLite, CrateDB, MongoDB

### **Message Retention & Purging**
- **Automatic Message Cleanup** - Configurable retention policies per ArchiveGroup
- **Flexible Time Periods** - Support for seconds, minutes, hours, days, weeks, months, years
- **Cluster-Aware Purging** - Distributed locks prevent duplicate cleanup operations
- **Performance Optimized** - Backend-specific implementations for maximum efficiency
- **Dual Retention Policies** - Separate retention for current values and historical archives
- **Real-Time Monitoring** - Detailed logging of purge operations with performance metrics

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
      - 4000:4000    # GraphQL Server
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
ConfigStoreType: POSTGRES

QueuedMessagesEnabled: true

Postgres:
  Url: jdbc:postgresql://postgres:5432/monster
  User: system
  Pass: manager

MCP:
  Enabled: true
  Port: 3000

GraphQL:
  Enabled: true
  Port: 4000
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
ConfigStoreType: SQLITE
QueuedMessagesEnabled: true

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
java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt -help

# Start with configuration
java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt -config config.yaml

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

## 🔐 User Management & ACL System

MonsterMQ includes a comprehensive user authentication and authorization system that provides fine-grained control over who can connect to the broker and what topics they can access.

### Quick Configuration

Enable user management in your `config.yaml`:

```yaml
UserManagement:
  Enabled: true
  AuthStoreType: SQLITE           # SQLITE, POSTGRES, CRATEDB, MONGODB
  PasswordAlgorithm: bcrypt       # Industry-standard password hashing
  CacheRefreshInterval: 60        # Seconds between automatic cache refreshes
  DisconnectOnUnauthorized: true  # Disconnect clients on unauthorized actions

# Database configuration (example with SQLite)
SQLite:
  Path: "./users.db"             # User database (created automatically)

# Optional: GraphQL API for user management
GraphQL:
  Enabled: true
  Port: 8080                     # GraphQL API endpoint
```

### Default Admin User

When user management is enabled, MonsterMQ automatically creates a default admin user on first startup:

- **Username:** `Admin`
- **Password:** `Admin`

⚠️ **Important:** Change the default password immediately after first login for security reasons.

### Key Features

#### **Multi-Database User Storage**
Store users and ACL rules in your choice of database:

| Database | Authentication | ACL Rules | Performance | Use Case |
|----------|:-------------:|:---------:|:-----------:|:---------|
| **SQLite** | ✅ | ✅ | Fast | Development, single instance |
| **PostgreSQL** | ✅ | ✅ | Excellent | Production, clustering |
| **CrateDB** | ✅ | ✅ | Excellent | Time-series, analytics |
| **MongoDB** | ✅ | ✅ | Excellent | NoSQL, document-based |

#### **Permission Model**

MonsterMQ uses a hierarchical permission system:

1. **Admin Override**: Admin users bypass all ACL checks
2. **Global Permissions**: User-level `canSubscribe`/`canPublish` settings
3. **ACL Rules**: Topic-specific permissions with wildcard support
4. **Anonymous Access**: Configurable anonymous user for unauthenticated clients

#### **MQTT Topic Wildcards**

ACL rules support full MQTT wildcard syntax:

```yaml
# Examples of ACL topic patterns:
- "sensors/+/temperature"      # Single-level wildcard (+)
- "sensors/#"                  # Multi-level wildcard (#)  
- "building/+/sensor/#"        # Combined wildcards
- "user/alice/devices/+"       # User-specific namespace
```

### GraphQL User Management API

Complete user and ACL management via GraphQL:

**API Endpoint**: `http://localhost:8080/graphql`

#### Quick Examples

**Create a user:**
```graphql
mutation {
  createUser(input: {
    username: "sensor_device"
    password: "secure_password_123"
    canSubscribe: false
    canPublish: true
  }) {
    success
    message
    user { username }
  }
}
```

**Create ACL rule:**
```graphql
mutation {
  createAclRule(input: {
    username: "sensor_device"
    topicPattern: "sensors/+/data"
    canPublish: true
    canSubscribe: false
    priority: 1
  }) {
    success
    message
  }
}
```

**Get all users:**
```graphql
query {
  getAllUsers {
    username
    enabled
    canSubscribe
    canPublish
    isAdmin
  }
}
```

### Real-World Examples

#### **IoT Sensor Network**
```graphql
# Sensor devices (publish-only)
mutation {
  createUser(input: {
    username: "sensor_network"
    password: "sensors_secret_key"
    canSubscribe: false
    canPublish: false
  }) { success }
}

mutation {
  createAclRule(input: {
    username: "sensor_network"
    topicPattern: "sensors/+/data"
    canPublish: true
    priority: 1
  }) { success }
}

# Dashboard (subscribe-only)  
mutation {
  createUser(input: {
    username: "dashboard"
    password: "dashboard_secret"
    canSubscribe: false
    canPublish: false
  }) { success }
}

mutation {
  createAclRule(input: {
    username: "dashboard"
    topicPattern: "sensors/#"
    canSubscribe: true
    priority: 1
  }) { success }
}
```

#### **Multi-Tenant System**
```graphql
# Tenant A - isolated namespace
mutation {
  createUser(input: {
    username: "tenant_a_user"
    password: "tenant_a_secret"
  }) { success }
}

mutation {
  createAclRule(input: {
    username: "tenant_a_user"
    topicPattern: "tenant_a/#"
    canSubscribe: true
    canPublish: true
    priority: 1
  }) { success }
}
```

### Security Features

- **BCrypt Hashing**: Industry-standard password security with configurable work factor
- **In-Memory Caching**: High-performance ACL lookup with automatic cache refresh
- **Topic Tree Optimization**: Efficient wildcard matching for MQTT topic patterns
- **Admin User Support**: Admin users bypass all ACL restrictions
- **Anonymous Users**: Configurable access for unauthenticated connections

### Complete Documentation

For comprehensive documentation including:
- Database schema details
- Advanced ACL configuration
- Permission resolution algorithm
- Command-line tools
- Performance optimization
- Security best practices
- Troubleshooting guide

See: **[📖 Complete ACL Documentation](broker/README_ACL.md)**

For comprehensive OPC UA integration documentation including configuration examples, device management, and architectural details, see: **[📖 OPC UA Extension Documentation](README_OPCUA.md)**

## 🗂️ Archive Configuration Management

MonsterMQ now supports dynamic archive group configuration through a comprehensive management system that stores configurations in the database and provides real-time updates without requiring broker restarts.

### Key Features

- **Database-Stored Configuration**: Archive groups are now stored in the database instead of YAML files
- **GraphQL API Management**: Complete CRUD operations for archive groups via GraphQL
- **Real-Time Updates**: Enable/disable archive groups instantly with automatic message routing updates
- **Storage Lifecycle Management**: Automatic database table/collection creation and cleanup
- **Web Dashboard**: Full-featured browser-based management interface
- **Multi-Database Support**: Configuration storage works with PostgreSQL, SQLite, CrateDB, MongoDB

### Configuration Migration

Archive groups can now be managed dynamically. The YAML configuration is still supported for initial setup:

```yaml
# Archive groups are now managed via GraphQL API and dashboard
# No YAML configuration needed for archive groups

# Required: Enable configuration storage
ConfigStoreType: POSTGRES  # POSTGRES, SQLITE, CRATEDB, MONGODB

# Database connection
Postgres:
  Url: jdbc:postgresql://localhost:5432/monster
  User: system
  Pass: manager

# Other required settings
SessionStoreType: POSTGRES
RetainedStoreType: POSTGRES
TCP: 1883

GraphQL:
  Enabled: true
  Port: 4000
```

### Bulk Import with -archiveConfig

For initial setup or bulk configuration updates, you can use the `-archiveConfig` parameter to import archive groups from a separate YAML file:

```bash
# Import archive groups from YAML file into database
java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt -archiveConfig archive-setup.yaml
```

**Example archive-setup.yaml:**
```yaml
ArchiveGroups:
  - Name: "ProductionSensors"
    Enabled: true
    TopicFilter: ["production/sensors/+/data", "production/alerts/#"]
    RetainedOnly: false
    LastValType: POSTGRES
    ArchiveType: POSTGRES
    LastValRetention: "7d"
    ArchiveRetention: "30d"
    PurgeInterval: "6h"

  - Name: "DebugData"
    Enabled: false
    TopicFilter: ["debug/+/+", "test/+/data"]
    RetainedOnly: false
    LastValType: MEMORY
    ArchiveType: POSTGRES
    LastValRetention: "1h"
    ArchiveRetention: "24h"
    PurgeInterval: "1h"

  - Name: "CriticalAlarms"
    Enabled: true
    TopicFilter: ["alarms/critical/+", "system/errors/#"]
    RetainedOnly: true
    LastValType: HAZELCAST
    ArchiveType: POSTGRES
    LastValRetention: "90d"
    ArchiveRetention: "1y"
    PurgeInterval: "12h"
```

**How -archiveConfig Works:**

1. **With ConfigStoreType configured**: Archive groups are imported into the database and can be managed via GraphQL/dashboard
2. **Without ConfigStoreType**: Archive groups are loaded into memory (legacy YAML mode)
3. **Automatic Detection**: System automatically detects if database storage is available
4. **Migration Tool**: Perfect for migrating from YAML-based configurations to database storage

**Import Process:**
```bash
# Set ConfigStoreType in your main config.yaml
ConfigStoreType: POSTGRES

# Import archive groups into database
./run.sh -archiveConfig my-archive-config.yaml

# After import, manage via GraphQL API or dashboard
# The YAML file is no longer needed for runtime
```

### GraphQL Archive Management API

Complete archive group management through GraphQL:

**API Endpoint**: `http://localhost:4000/graphql`

#### Create Archive Group
```graphql
mutation {
  createArchiveGroup(input: {
    name: "ProductionSensors"
    enabled: true
    topicFilter: ["sensors/#", "devices/#"]
    retainedOnly: false
    lastValType: POSTGRES
    archiveType: POSTGRES
    lastValRetention: "7d"
    archiveRetention: "30d"
    purgeInterval: "1h"
  }) {
    success
    message
    archiveGroup {
      name
      enabled
      deployed
    }
  }
}
```

#### Update Archive Group
```graphql
mutation {
  updateArchiveGroup(
    name: "ProductionSensors"
    input: {
      topicFilter: ["sensors/#", "devices/#", "alerts/#"]
      archiveRetention: "60d"
    }
  ) {
    success
    message
    archiveGroup {
      name
      archiveRetention
    }
  }
}
```

#### Enable/Disable Archive Group
```graphql
mutation {
  enableArchiveGroup(name: "ProductionSensors") {
    success
    message
    archiveGroup {
      name
      enabled
      deployed
    }
  }
}

mutation {
  disableArchiveGroup(name: "ProductionSensors") {
    success
    message
  }
}
```

#### Delete Archive Group
```graphql
mutation {
  deleteArchiveGroup(name: "ProductionSensors") {
    success
    message
  }
}
```

#### Query Archive Groups
```graphql
query {
  archiveGroups {
    name
    enabled
    deployed
    topicFilter
    retainedOnly
    lastValType
    archiveType
    lastValRetention
    archiveRetention
    purgeInterval
    createdAt
    updatedAt
  }
}

query {
  archiveGroup(name: "ProductionSensors") {
    name
    enabled
    deployed
    topicFilter
  }
}
```

### Web Dashboard Management

Access the archive management dashboard at: `http://localhost:4000/archives`

**Dashboard Features:**
- **Visual Archive Group Overview** - Table view with status indicators
- **Create/Edit Archive Groups** - Modal forms with validation
- **Enable/Disable Toggle** - One-click archive group activation
- **Real-Time Status Updates** - Live deployment status monitoring
- **Bulk Operations** - Manage multiple archive groups efficiently
- **Search and Filtering** - Find archive groups quickly
- **Storage Cleanup** - Automatic table/collection cleanup on deletion

### Storage Lifecycle Management

The system automatically manages database storage:

#### Automatic Table/Collection Creation
When an archive group is enabled, MonsterMQ automatically creates the required database tables/collections:

```bash
# PostgreSQL
CREATE TABLE productionsensorslastval (topic VARCHAR, time TIMESTAMPTZ, payload_b64 VARCHAR, ...);
CREATE TABLE productionsensorsarchive (topic VARCHAR, time TIMESTAMPTZ, payload_b64 VARCHAR, ...);

# MongoDB
db.createCollection("productionsensorslastval");
db.createCollection("productionsensorsarchive");

# CrateDB
CREATE TABLE productionsensorslastval (topic VARCHAR, time TIMESTAMPTZ, payload_b64 VARCHAR, ...);
```

#### Automatic Storage Cleanup
When an archive group is deleted, the system automatically drops associated storage:

```bash
# PostgreSQL
DROP TABLE IF EXISTS productionsensorslastval CASCADE;
DROP TABLE IF EXISTS productionsensorsarchive CASCADE;

# MongoDB
db.productionsensorslastval.drop();
db.productionsensorsarchive.drop();

# CrateDB
DROP TABLE IF EXISTS productionsensorslastval;
DROP TABLE IF EXISTS productionsensorsarchive;
```

### Configuration Database Schema

Archive group configurations are stored in a dedicated table:

```sql
CREATE TABLE archive_groups (
    name VARCHAR(255) PRIMARY KEY,
    enabled BOOLEAN NOT NULL DEFAULT true,
    topic_filter TEXT[] NOT NULL,
    retained_only BOOLEAN NOT NULL DEFAULT false,
    last_val_type VARCHAR(50) NOT NULL,
    archive_type VARCHAR(50) NOT NULL,
    last_val_retention VARCHAR(50),
    archive_retention VARCHAR(50),
    purge_interval VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Real-Time Message Routing

The system includes dynamic message routing that automatically updates when archive groups are enabled/disabled:

```kotlin
// Automatic registration of new archive groups
archiveHandler.registerArchiveGroup(archiveGroup)

// Dynamic message routing updates
messageHandler.registerArchiveGroup(archiveGroup)
messageHandler.unregisterArchiveGroup(archiveGroupName)
```

**Benefits:**
- **No Broker Restart Required** - Changes take effect immediately
- **Zero Downtime** - Existing connections remain active during configuration changes
- **Instant Message Routing** - New archive groups start receiving messages immediately
- **Automatic Cleanup** - Disabled archive groups stop processing messages instantly

### Migration from YAML Configuration

Existing YAML-based archive groups can be migrated to database storage:

1. **Keep YAML Configuration** - Initial archive groups are loaded from YAML on startup
2. **Use GraphQL API** - Create equivalent database entries via GraphQL mutations
3. **Remove YAML Entries** - Once migrated, YAML entries can be removed
4. **Dynamic Management** - All future changes handled via GraphQL/Dashboard

### Best Practices

#### Configuration Management
- **Use Descriptive Names** - Archive group names should reflect their purpose
- **Plan Topic Filters** - Use specific patterns to avoid message duplication
- **Monitor Storage Growth** - Set appropriate retention policies to manage disk usage
- **Test Before Production** - Use development archive groups for testing configurations

#### Performance Optimization
- **Separate Storage Types** - Use MEMORY/HAZELCAST for current values, POSTGRES/MONGODB for archives
- **Appropriate Retention** - Balance storage costs with data requirements
- **Purge Interval Tuning** - Frequent purging reduces batch sizes but increases overhead
- **Database Indexing** - Ensure proper indexes on topic and time columns

#### Security Considerations
- **Database Access Control** - Secure configuration database with appropriate permissions
- **GraphQL Authentication** - Implement authentication for GraphQL management endpoints
- **Audit Trail** - Monitor configuration changes through database logs
- **Backup Strategy** - Include configuration database in backup procedures

## 🗄️ Message Retention & Purging

MonsterMQ provides automatic message cleanup with configurable retention policies, ensuring optimal storage usage while maintaining data availability for the required time periods.

### Configuration

Configure retention per ArchiveGroup via GraphQL API or dashboard:

```graphql
# Create archive group with retention settings
mutation {
  createArchiveGroup(input: {
    name: "Production"
    enabled: true
    topicFilter: ["sensors/#", "devices/#"]
    lastValRetention: "7d"      # Keep current values for 7 days
    archiveRetention: "30d"     # Keep historical messages for 30 days
    purgeInterval: "1h"         # Check for old messages every hour
    lastValType: POSTGRES       # Storage backends
    archiveType: POSTGRES
  }) {
    success
    message
  }
}
```

### Retention Period Format

Supports flexible time units:

| Unit | Description | Example |
|------|-------------|---------|
| `s` | Seconds | `30s` = 30 seconds |
| `m` | Minutes | `15m` = 15 minutes |
| `h` | Hours | `2h` = 2 hours |
| `d` | Days | `7d` = 7 days |
| `w` | Weeks | `2w` = 2 weeks |
| `M` | Months | `3M` = 3 months |
| `y` | Years | `1y` = 1 year |

**Examples:**
```yaml
LastValRetention: "5m"         # 5 minutes
ArchiveRetention: "12h"        # 12 hours  
PurgeInterval: "30d"           # 30 days
LastValRetention: "6M"         # 6 months
ArchiveRetention: "1y"         # 1 year
```

### Storage Backend Support

All storage backends include optimized purging implementations:

#### **Database Stores**
- **PostgreSQL**: SQL DELETE with proper transaction handling and batch optimization
- **CrateDB**: Time-series optimized with efficient batch deletions
- **SQLite**: Async operations via event bus to prevent blocking
- **MongoDB**: Native `deleteMany()` with time-based filters and index utilization

#### **Memory Stores**
- **Memory**: Direct HashMap operations with progress logging
- **Hazelcast**: Distributed predicates for cluster-wide efficient purging

### Cluster-Aware Purging

In clustered deployments, MonsterMQ automatically coordinates purging operations:

#### **Distributed Locking**
- Each ArchiveGroup uses cluster-wide distributed locks
- Lock format: `purge-lock-{ArchiveGroupName}-{StoreType}`
- Only one node in cluster can acquire lock and perform purging
- 30-second lock timeout prevents deadlocks
- Other nodes skip with informational logging

#### **Example Log Output**
```bash
# Node 1 (acquired lock)
[FINE] Acquired purge lock for LastVal store [Production] - starting purge
[FINE] Purge completed for [ProductionLastval]: deleted 1,247 messages in 234ms

# Node 2 (lock unavailable)  
[FINE] Could not acquire purge lock for Archive store [Production] - skipping purge (likely another cluster node is purging)
```

### Purge Process

1. **Scheduled Execution**: Based on `PurgeInterval` setting per ArchiveGroup
2. **Cluster Coordination**: Distributed locks ensure only one node purges per store
3. **LastVal Purging**: Removes current values older than `LastValRetention`
4. **Archive Purging**: Removes historical messages older than `ArchiveRetention`
5. **Performance Logging**: Tracks deletion counts and execution time
6. **Error Handling**: Graceful handling of database connectivity issues

### Monitoring

Monitor purge operations through detailed logging:

```bash
# Successful purge operation
[FINE] Starting purge for [ProductionArchive] - removing messages older than 2024-08-01T00:00:00Z
[FINE] Purge completed for [ProductionArchive]: deleted 5,432 messages in 1,247ms

# Cluster coordination
[FINE] Acquired purge lock for Archive store [Production] - starting purge
[FINE] Could not acquire purge lock for LastVal store [Development] - skipping (another node purging)

# Error handling
[WARNING] Failed to acquire purge lock for store [Production] - will retry next interval
[SEVERE] Error during purge operation: Database connection timeout - skipping this cycle
```

### Best Practices

#### **Retention Planning**
- **Balance storage costs** with data requirements
- **Consider query patterns** - frequently accessed data needs longer retention
- **Plan for compliance** - regulatory requirements may mandate minimum retention
- **Monitor growth rates** - adjust retention based on data volume trends

#### **Performance Optimization**
- **Frequent purging** reduces batch sizes but increases overhead
- **Database choice matters**:
  - Use Memory/Hazelcast for high-performance current values
  - Use PostgreSQL/CrateDB for reliable long-term archiving  
  - Use MongoDB for flexible schema and time-series optimization
- **Monitor purge logs** to ensure proper operation and performance

#### **Cluster Considerations**
- **Central database required** for clustered deployments
- **Network bandwidth** - purging generates database traffic
- **Timing coordination** - ensure all nodes use synchronized time (NTP)

### Configuration Examples

#### **High-Volume IoT System**
```graphql
mutation {
  createArchiveGroup(input: {
    name: "HighVolume"
    topicFilter: ["sensors/#"]
    lastValRetention: "24h"     # Keep current readings for 1 day
    archiveRetention: "7d"      # Keep history for 1 week
    purgeInterval: "1h"         # Clean up hourly
    lastValType: HAZELCAST      # Fast access
    archiveType: POSTGRES       # Reliable storage
  }) { success }
}
```

#### **Long-Term Analytics**
```graphql
mutation {
  createArchiveGroup(input: {
    name: "Analytics"
    topicFilter: ["metrics/#", "events/#"]
    lastValRetention: "30d"     # Keep current state for 30 days
    archiveRetention: "2y"      # Keep history for 2 years
    purgeInterval: "24h"        # Clean up daily
    lastValType: POSTGRES
    archiveType: POSTGRES
  }) { success }
}
```

#### **Development Environment**
```graphql
mutation {
  createArchiveGroup(input: {
    name: "Testing"
    topicFilter: ["test/#"]
    lastValRetention: "1h"      # Keep current for 1 hour
    archiveRetention: "6h"      # Keep history for 6 hours
    purgeInterval: "15m"        # Clean up every 15 minutes
    lastValType: MEMORY         # Fast, volatile
    archiveType: MEMORY         # No persistence needed
  }) { success }
}
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
Archive groups are now managed dynamically via GraphQL API and web dashboard:

```graphql
# Create archive groups via GraphQL API
mutation {
  createArchiveGroup(input: {
    name: "production"
    enabled: true
    topicFilter: ["sensors/#", "devices/#"]
    retainedOnly: false
    lastValType: POSTGRES      # Current value storage
    archiveType: POSTGRES      # Historical message storage
  }) { success }
}

# Default archive group for MCP server
mutation {
  createArchiveGroup(input: {
    name: "Default"
    enabled: true
    topicFilter: ["#"]
    retainedOnly: false
    lastValType: POSTGRES      # Required for MCP server
    archiveType: POSTGRES      # Required for MCP historical queries
  }) { success }
}
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

# Create archive groups for Kafka streaming via GraphQL
# Stream specific topics to different Kafka topics
mutation {
  createArchiveGroup(input: {
    name: "sensors"
    topicFilter: ["sensors/#"]
    archiveType: KAFKA          # Stream to Kafka topic "sensors"
  }) { success }
}

mutation {
  createArchiveGroup(input: {
    name: "events"
    topicFilter: ["events/#", "alerts/#"]
    archiveType: KAFKA          # Stream to Kafka topic "events"
  }) { success }
}
```

**Kafka Data Format:**

When streaming to Kafka, MonsterMQ uses a custom binary serialization format:

- **Kafka Topic**: Archive group name (e.g., "sensors", "events")
- **Kafka Key**: MQTT topic name (for partitioning)
- **Kafka Value**: Custom binary encoded `MqttMessage` with:
  - messageUuid (length-prefixed UTF-8 string)
  - messageId (4-byte integer)
  - Flags byte (QoS, isDup, isRetain, isQueued)
  - topicName (length-prefixed UTF-8 string)
  - clientId (length-prefixed UTF-8 string)
  - timestamp (8-byte epoch milliseconds)
  - payload (raw MQTT message bytes)

This format is optimized for space efficiency and requires the same `MqttMessageCodec` for deserialization.

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

# Archive groups can still be created via GraphQL for additional filtering if needed
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

GraphQL:
  Enabled: true
  Port: 8080            # GraphQL server with subscriptions
```

## 🔧 Advanced Features

### Clustering Setup

1. **Start first node:**
   ```bash
   java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt -cluster -config config.yaml
   ```

2. **Start additional nodes:**
   ```bash
   # On different machines, same config
   java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt -cluster -config config.yaml
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
     -classpath ... at.rocworks.MonsterKt -cluster
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

### GraphQL API Integration

MonsterMQ includes a powerful GraphQL API for querying and subscribing to MQTT data in real-time. The GraphQL server provides a modern, flexible interface for web applications, analytics platforms, and third-party integrations.

**Features:**
- **Real-time Subscriptions** - Live MQTT message streams via GraphQL subscriptions
- **Historical Queries** - Query archived messages with time filtering
- **Current Values** - Get latest values from topics and retained messages  
- **Multiple Archive Groups** - Query different storage backends
- **WebSocket Support** - Real-time updates over WebSocket connections
- **JSON & Binary Data** - Automatic format detection and Base64 encoding

#### Configuration

Enable GraphQL server in your config:

```yaml
# Basic GraphQL configuration
GraphQL:
  Enabled: true
  Port: 8080

# Create required archive groups via GraphQL API
# At least one archive group needed for historical data
mutation {
  createArchiveGroup(input: {
    name: "Default"          # Default archive group
    enabled: true
    topicFilter: ["#"]
    lastValType: POSTGRES     # Required for current values
    archiveType: POSTGRES     # Required for historical queries
  }) { success }
}

mutation {
  createArchiveGroup(input: {
    name: "Sensors"          # Custom archive group
    enabled: true
    topicFilter: ["sensors/#", "devices/#"]
    lastValType: POSTGRES
    archiveType: POSTGRES
  }) { success }
}

RetainedStoreType: POSTGRES   # Required for retained message queries
```

#### API Endpoints

- **GraphQL Playground**: `http://localhost:8080/graphql` (web interface)
- **GraphQL API**: `http://localhost:8080/graphql` (POST requests)
- **WebSocket Subscriptions**: `ws://localhost:8080/graphql` (subscription endpoint)

#### Query Examples

**1. Get Current Topic Value**
```graphql
query {
  currentValue(topic: "sensors/temperature/room1", archiveGroup: "Default") {
    topic
    payload
    timestamp
    qos
  }
}
```

**2. Query Multiple Current Values**
```graphql
query {
  currentValues(topicFilter: "sensors/#", limit: 10, archiveGroup: "Sensors") {
    topic
    payload
    format
    timestamp
    qos
  }
}
```

**3. Get Historical Messages**
```graphql
query {
  archivedMessages(
    topicFilter: "sensors/temperature/room1"
    startTime: "2024-01-15T00:00:00Z"
    endTime: "2024-01-15T23:59:59Z"
    format: JSON
    limit: 100
    archiveGroup: "Default"
  ) {
    topic
    payload
    timestamp
    qos
    clientId
  }
}
```

**4. Get Retained Messages**
```graphql
query {
  retainedMessages(topicFilter: "config/#", format: JSON) {
    topic
    payload
    timestamp
    qos
  }
}
```

**5. Real-time Message Subscription**
```graphql
subscription {
  topicUpdates(topicFilter: "sensors/#", format: JSON) {
    topic
    payload
    timestamp
    qos
    retained
    clientId
  }
}
```

**6. Publish Messages**
```graphql
mutation {
  publish(input: {
    topic: "commands/device1/restart"
    payload: "{\"action\":\"restart\",\"delay\":5}"
    format: JSON
    qos: 1
    retained: false
  }) {
    success
    topic
    timestamp
    error
  }
}
```

#### Data Format Handling

The GraphQL API supports both JSON and Binary data formats:

```graphql
# Automatic format detection
query {
  currentValue(topic: "sensors/data") {
    payload    # Returns JSON string or Base64 for binary
    format     # AUTO-DETECTED: JSON or BINARY  
  }
}

# Force specific format
query {
  currentValue(topic: "sensors/data", format: JSON) {
    payload    # Always returns as JSON string
    format     # Returns: JSON
  }
  
  currentValue(topic: "image/camera1", format: BINARY) {
    payload    # Returns Base64 encoded binary data
    format     # Returns: BINARY
  }
}
```

#### Archive Group Selection

Specify which archive group to query for different data sources:

```graphql
# Query default archive group
query {
  archivedMessages(topicFilter: "data/#", archiveGroup: "Default") {
    topic
    payload
  }
}

# Query sensors archive group  
query {
  archivedMessages(topicFilter: "sensors/#", archiveGroup: "Sensors") {
    topic
    payload
  }
}

# Query production archive group
query {
  currentValues(topicFilter: "production/#", archiveGroup: "Production") {
    topic
    payload
  }
}
```

#### JavaScript Client Example

```javascript
// Using Apollo Client for GraphQL subscriptions
import { ApolloClient, InMemoryCache, split, HttpLink } from '@apollo/client';
import { GraphQLWsLink } from '@apollo/client/link/subscriptions';
import { createClient } from 'graphql-ws';
import { getMainDefinition } from '@apollo/client/utilities';

// HTTP connection for queries and mutations
const httpLink = new HttpLink({
  uri: 'http://localhost:8080/graphql'
});

// WebSocket connection for subscriptions  
const wsLink = new GraphQLWsLink(createClient({
  url: 'ws://localhost:8080/graphql'
}));

// Split based on operation type
const splitLink = split(
  ({ query }) => {
    const definition = getMainDefinition(query);
    return (
      definition.kind === 'OperationDefinition' &&
      definition.operation === 'subscription'
    );
  },
  wsLink,
  httpLink
);

const client = new ApolloClient({
  link: splitLink,
  cache: new InMemoryCache()
});

// Subscribe to real-time sensor data
const SENSOR_SUBSCRIPTION = gql`
  subscription {
    topicUpdates(topicFilter: "sensors/#") {
      topic
      payload
      timestamp
    }
  }
`;

const { data } = useSubscription(SENSOR_SUBSCRIPTION);
```

#### cURL Examples

**Query current value:**
```bash
curl -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query { currentValue(topic: \"sensors/temp1\") { topic payload timestamp } }"
  }'
```

**Publish message:**
```bash
curl -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { publish(input: {topic: \"test/topic\", payload: \"hello\", qos: 1}) { success timestamp } }"
  }'
```

#### WebSocket Subscription (Node.js)

```javascript
const WebSocket = require('ws');

const ws = new WebSocket('ws://localhost:8080/graphql', 'graphql-ws');

ws.on('open', () => {
  // Connection init
  ws.send(JSON.stringify({ type: 'connection_init' }));
  
  // Start subscription
  ws.send(JSON.stringify({
    id: '1',
    type: 'start',
    payload: {
      query: `
        subscription {
          topicUpdates(topicFilter: "sensors/#") {
            topic
            payload
            timestamp
          }
        }
      `
    }
  }));
});

ws.on('message', (data) => {
  const message = JSON.parse(data);
  if (message.type === 'data') {
    console.log('New sensor data:', message.payload.data.topicUpdates);
  }
});
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