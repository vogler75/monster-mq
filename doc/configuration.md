# Configuration Reference

Complete reference for MonsterMQ configuration options with examples and schema validation.

## Configuration File

MonsterMQ uses YAML configuration files with JSON schema validation for auto-completion and error checking.

### Schema Support

**VS Code Setup:**
1. Install [YAML extension](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml)
2. Add to VS Code `settings.json`:
```json
{
  "yaml.schemas": {
    "./broker/yaml-json-schema.json": "config*.yaml"
  }
}
```

## Network Configuration

### Ports

```yaml
TCP: 1883           # MQTT TCP port (0 to disable)
TCPS: 8883          # MQTT TLS port (0 to disable)
WS: 9000            # MQTT WebSocket port (0 to disable)
WSS: 9001           # MQTT WebSocket TLS port (0 to disable)
MaxMessageSizeKb: 512   # Maximum message size in KB
```

**Default Values:**
- TCP: 1883
- TCPS: 0 (disabled)
- WS: 0 (disabled)
- WSS: 0 (disabled)
- MaxMessageSizeKb: 8

## Storage Configuration

### Store Types

```yaml
SessionStoreType: POSTGRES      # Client session persistence
RetainedStoreType: POSTGRES     # Retained message storage
ConfigStoreType: POSTGRES       # Archive group configuration
QueuedMessagesEnabled: true     # Enable QoS>0 message queuing
```

**Available Store Types:**
- **SessionStoreType:** POSTGRES, CRATEDB, MONGODB, SQLITE
- **RetainedStoreType:** MEMORY, HAZELCAST, POSTGRES, CRATEDB, SQLITE
- **ConfigStoreType:** POSTGRES, CRATEDB, MONGODB, SQLITE

## Database Connections

### PostgreSQL

```yaml
Postgres:
  Url: jdbc:postgresql://localhost:5432/monster
  User: system
  Pass: manager
```

**Connection String Format:**
- Local: `jdbc:postgresql://localhost:5432/database`
- Remote: `jdbc:postgresql://host:port/database`
- SSL: `jdbc:postgresql://host:port/database?ssl=true`

### CrateDB

```yaml
CrateDB:
  Url: jdbc:postgresql://localhost:5432/monster
  User: crate
  Pass: ""
```

**Notes:**
- Uses PostgreSQL protocol
- Default user is usually `crate` with empty password
- Supports clustering and sharding

### MongoDB

```yaml
MongoDB:
  Url: mongodb://system:manager@localhost:27017
  Database: monster
```

**Connection String Formats:**
- Simple: `mongodb://localhost:27017`
- Authenticated: `mongodb://user:pass@localhost:27017`
- Replica Set: `mongodb://host1:27017,host2:27017/database?replicaSet=rs0`

### SQLite

```yaml
SQLite:
  Path: "monstermq.db"
```

**Notes:**
- File path relative to working directory
- Database file created automatically
- Cannot be used in cluster mode

## Extensions

### GraphQL API

```yaml
GraphQL:
  Enabled: true
  Port: 4000
```

**Endpoints:**
- API: `http://localhost:4000/graphql`
- Playground: `http://localhost:4000/graphql` (browser)
- WebSocket: `ws://localhost:4000/graphql`

### MCP Server

```yaml
MCP:
  Enabled: true
  Port: 3000
```

**Requirements:**
- Requires archive group named "Default" with `lastValType` configured
- Used for AI model integration

### SparkplugB Extension

```yaml
SparkplugMetricExpansion:
  Enabled: true
```

**Functionality:**
- Expands SparkplugB messages from `spBv1.0` to `spBv1.0e` topics
- Automatic metric extraction and topic creation

## User Management

### Basic Configuration

```yaml
UserManagement:
  Enabled: true
  StoreType: POSTGRES
  PasswordAlgorithm: bcrypt
  CacheRefreshInterval: 60
  DisconnectOnUnauthorized: true
```

**Parameters:**
- **StoreType:** POSTGRES, CRATEDB, MONGODB, SQLITE
- **PasswordAlgorithm:** bcrypt (only supported algorithm)
- **CacheRefreshInterval:** Seconds between cache refreshes
- **DisconnectOnUnauthorized:** Disconnect clients on ACL violations

### Default Admin User

When user management is enabled, default admin user is created:
- **Username:** Admin
- **Password:** Admin

⚠️ **Change default password immediately for security**

## Kafka Integration

### Archive Streaming

Stream specific topics to Kafka:

```yaml
Kafka:
  Servers: kafka1:9092,kafka2:9092,kafka3:9092
```

Create archive groups with `archiveType: KAFKA`:

```graphql
mutation {
  createArchiveGroup(input: {
    name: "sensors"
    topicFilter: ["sensors/#"]
    archiveType: KAFKA
  }) { success }
}
```

### Kafka as Message Bus

Replace Vert.x EventBus with Kafka:

```yaml
Kafka:
  Servers: kafka:9092
  Bus:
    Enabled: true
    Topic: monster-bus
    ProducerConfig:
      batch.size: 65536
      linger.ms: 10
      compression.type: lz4
```

**Trade-offs:**
- ✅ Complete message stream capture
- ✅ Kafka's replay and analytics features
- ⚠️ Higher latency (5-20ms additional)
- ⚠️ Kafka becomes critical dependency

## OPC UA Configuration

OPC UA devices are managed via GraphQL API, not YAML configuration.

**Example device creation:**
```graphql
mutation {
  createOpcUaDevice(input: {
    name: "plc01"
    namespace: "opcua/factory"
    nodeId: "node1"
    config: {
      endpointUrl: "opc.tcp://192.168.1.100:4840"
      securityPolicy: "Basic256Sha256"
      addresses: [
        {
          address: "NodeId://ns=2;i=1001"
          topic: "temperature"
        }
      ]
      certificateConfig: {
        securityDir: "security"
        createSelfSigned: true
      }
    }
  }) { success }
}
```

## Complete Configuration Examples

### Development (SQLite)

```yaml
# config-sqlite.yaml
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

GraphQL:
  Enabled: true
  Port: 4000

UserManagement:
  Enabled: true
  StoreType: SQLITE

SparkplugMetricExpansion:
  Enabled: true
```

### Production (PostgreSQL)

```yaml
# config-postgres.yaml
TCP: 1883
TCPS: 8883
WS: 9000
WSS: 9001
MaxMessageSizeKb: 512

SessionStoreType: POSTGRES
RetainedStoreType: POSTGRES
ConfigStoreType: POSTGRES
QueuedMessagesEnabled: true

Postgres:
  Url: jdbc:postgresql://localhost:5432/monster
  User: system
  Pass: manager

MCP:
  Enabled: true
  Port: 3000

GraphQL:
  Enabled: true
  Port: 4000

UserManagement:
  Enabled: true
  StoreType: POSTGRES
  PasswordAlgorithm: bcrypt

SparkplugMetricExpansion:
  Enabled: true
```

### Clustering (Hazelcast)

```yaml
# config-hazelcast.yaml
TCP: 1883
WS: 9000
MaxMessageSizeKb: 512

# Clustering requires central database
SessionStoreType: POSTGRES
RetainedStoreType: HAZELCAST     # Distributed across cluster
ConfigStoreType: POSTGRES

Postgres:
  Url: jdbc:postgresql://shared-db-server:5432/monster
  User: system
  Pass: manager

MCP:
  Enabled: true
  Port: 3000

GraphQL:
  Enabled: true
  Port: 4000

UserManagement:
  Enabled: true
  StoreType: POSTGRES
```

**Run with clustering:**
```bash
java -classpath "target/classes:target/dependencies/*" at.rocworks.MonsterKt -cluster -config config-hazelcast.yaml
```

### Memory-Only (Testing)

```yaml
# config-memory.yaml
TCP: 1883
WS: 8080
MaxMessageSizeKb: 8

SessionStoreType: SQLITE        # Minimal persistence for sessions
RetainedStoreType: MEMORY       # Volatile retained messages
QueuedMessagesEnabled: false    # No message queuing

SQLite:
  Path: ":memory:"              # In-memory SQLite

MCP:
  Enabled: false
GraphQL:
  Enabled: false
UserManagement:
  Enabled: false
```

### Kafka Streaming

```yaml
# config-kafka.yaml
TCP: 1883
WS: 9000
MaxMessageSizeKb: 512

SessionStoreType: POSTGRES
RetainedStoreType: POSTGRES
ConfigStoreType: POSTGRES

Postgres:
  Url: jdbc:postgresql://localhost:5432/monster
  User: system
  Pass: manager

Kafka:
  Servers: kafka:9092

GraphQL:
  Enabled: true
  Port: 4000

# Create archive groups via GraphQL for Kafka streaming
```

### Kafka as Message Bus

```yaml
# config-kafka-bus.yaml
TCP: 1883
WS: 8080

SessionStoreType: POSTGRES
RetainedStoreType: POSTGRES

Postgres:
  Url: jdbc:postgresql://localhost:5432/monster
  User: system
  Pass: manager

Kafka:
  Servers: kafka1:9092,kafka2:9092,kafka3:9092
  Bus:
    Enabled: true
    Topic: monster-bus
    ProducerConfig:
      batch.size: 65536
      linger.ms: 10
      compression.type: lz4
      acks: 1
```

## Command Line Options

### Basic Usage

```bash
# Show help
java -classpath "target/classes:target/dependencies/*" at.rocworks.MonsterKt -help

# Run with configuration file
java -classpath "target/classes:target/dependencies/*" at.rocworks.MonsterKt -config config.yaml

# Enable clustering
java -classpath "target/classes:target/dependencies/*" at.rocworks.MonsterKt -cluster -config config.yaml

# Set log level
java -classpath "target/classes:target/dependencies/*" at.rocworks.MonsterKt -log FINE -config config.yaml
```

### Convenience Script

```bash
# Show help
./run.sh -help

# Run with configuration
./run.sh -config config.yaml

# Clustering
./run.sh -cluster -config config-hazelcast.yaml

# Debug logging
./run.sh -log FINE -config config.yaml
```

### Archive Configuration Import

```bash
# Import archive groups from YAML into database
./run.sh -archiveConfig archive-setup.yaml

# Combined with regular config
./run.sh -config config.yaml -archiveConfig archive-setup.yaml
```

### Log Levels

| Level | Description | Usage |
|-------|-------------|--------|
| SEVERE | Errors only | Production |
| WARNING | Warnings and errors | Production |
| INFO | General information | Production |
| FINE | Debug information | Development |
| FINER | Detailed debugging | Troubleshooting |
| FINEST | Maximum detail | Development |

## Environment Variables

### Docker Environment

```bash
# Hazelcast configuration
HAZELCAST_CONFIG=hazelcast.xml
PUBLIC_ADDRESS=192.168.1.10

# Java options
JAVA_OPTS="-Xms1g -Xmx2g"
```

### Java System Properties

```bash
# Hazelcast configuration
-Dvertx.hazelcast.config=hazelcast.xml
-Dhazelcast.local.publicAddress=192.168.1.10

# Logging configuration
-Djava.util.logging.config.file=logging.properties

# Memory settings
-Xms1g -Xmx2g -XX:+UseG1GC
```

## Validation and Troubleshooting

### Configuration Validation

1. **Use JSON schema** for auto-completion and validation
2. **Check syntax** with YAML linter
3. **Verify database connections** before starting
4. **Test with minimal configuration** first

### Common Issues

**1. Invalid YAML Syntax**
```bash
# Validate YAML syntax
python -c "import yaml; yaml.safe_load(open('config.yaml'))"
```

**2. Database Connection Errors**
```bash
# Test PostgreSQL connection
psql -h localhost -U system -d monster

# Test MongoDB connection
mongo mongodb://system:manager@localhost:27017/monster
```

**3. Port Conflicts**
```bash
# Check port usage
sudo netstat -tulpn | grep 1883
sudo lsof -i :1883
```

**4. Memory Issues**
```bash
# Increase heap size
export JAVA_OPTS="-Xms2g -Xmx4g"
./run.sh -config config.yaml
```

## Performance Tuning

### Database Optimization

**PostgreSQL:**
```sql
-- Create indexes for better performance
CREATE INDEX idx_archive_topic_time ON archive_table(topic, time);
CREATE INDEX idx_archive_time ON archive_table(time);
```

**Memory Settings:**
```yaml
# Increase message size for larger payloads
MaxMessageSizeKb: 1024

# Adjust database connection pools (implementation specific)
```

### JVM Tuning

```bash
# Production JVM settings
export JAVA_OPTS="-Xms2g -Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
```

## Related Documentation

- **[Installation & Setup](installation.md)** - Getting started guide
- **[Database Setup](databases.md)** - Database-specific configuration
- **[User Management](user-management.md)** - Authentication and ACL
- **[OPC UA Integration](opcua.md)** - Industrial protocol configuration
- **[Security](security.md)** - TLS and security configuration