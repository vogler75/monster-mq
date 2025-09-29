# Installation & Setup

This guide covers complete installation and setup of MonsterMQ for different environments.

## Quick Start

### Docker (Recommended)

The fastest way to get MonsterMQ running:

```bash
# Run with default configuration (SQLite)
docker run -p 1883:1883 -p 3000:3000 rocworks/monstermq:latest

# Test MQTT connection
mosquitto_pub -h localhost -p 1883 -t "test/topic" -m "Hello MonsterMQ"
mosquitto_sub -h localhost -p 1883 -t "test/#"
```

### Docker Compose with PostgreSQL

For production-like setup:

1. **Create docker-compose.yml:**
```yaml
services:
  postgres:
    image: timescale/timescaledb:latest-pg16
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

2. **Create config.yaml:**
```yaml
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

3. **Start services:**
```bash
docker-compose up -d
```

## Build from Source

### Prerequisites

- **Java 21+** (OpenJDK or Oracle JDK)
- **Maven 3.6+**
- **Git**

### Download and Build

```bash
# Clone repository
git clone https://github.com/rocworks/monstermq.git
cd monstermq/broker

# Build with Maven
mvn clean package

# Verify build
ls target/classes target/dependencies/
```

### Run from Source

```bash
# Show available options
java -classpath "target/classes:target/dependencies/*" at.rocworks.MonsterKt -help

# Run with SQLite (development)
java -classpath "target/classes:target/dependencies/*" at.rocworks.MonsterKt -config config-sqlite.yaml

# Run with clustering
java -classpath "target/classes:target/dependencies/*" at.rocworks.MonsterKt -cluster -config config-hazelcast.yaml

# Or use the convenience script
./run.sh -config config.yaml
./run.sh -cluster -config config-hazelcast.yaml
```

## Configuration Examples

### SQLite (Development)

Perfect for development and testing:

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
```

### PostgreSQL (Production)

Production-ready configuration:

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

UserManagement:
  Enabled: true
  StoreType: POSTGRES

MCP:
  Enabled: true
  Port: 3000

GraphQL:
  Enabled: true
  Port: 4000

SparkplugMetricExpansion:
  Enabled: true
```

### Clustering with Hazelcast

Multi-node clustering setup:

```yaml
# config-hazelcast.yaml
TCP: 1883
WS: 9000
MaxMessageSizeKb: 512

# Clustering requires central database
SessionStoreType: POSTGRES
RetainedStoreType: HAZELCAST  # Distributed across cluster
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
```

Run with clustering:
```bash
# Start cluster nodes
java -classpath "target/classes:target/dependencies/*" at.rocworks.MonsterKt -cluster -config config-hazelcast.yaml
```

## Troubleshooting

### Common Issues

**1. Port Already in Use**
```bash
# Check what's using the port
sudo netstat -tulpn | grep 1883
sudo lsof -i :1883

# Kill process if needed
sudo kill -9 <PID>
```

**2. Database Connection Failed**
```bash
# Check database is running
systemctl status postgresql
docker ps | grep postgres

# Test database connection
psql -h localhost -U system -d monster
```

**3. Java Version Issues**
```bash
# Check Java version
java -version

# Should be Java 21+
# Install OpenJDK 21 if needed
apt-get install openjdk-21-jdk
```

**4. Build Failures**
```bash
# Clean and rebuild
mvn clean
mvn package

# Check Maven version
mvn -version
```

### Log Analysis

```bash
# Start with debug logging
./run.sh -log FINE

# Monitor log files
tail -f log/monstermq.log

# Check for specific errors
grep ERROR log/monstermq.log
grep "Failed to" log/monstermq.log
```
