# Installation & Setup

This guide covers complete installation and setup of MonsterMQ for different environments.

## Quick Start

### 1-Line Setup (Linux & macOS)

Downloads the native setup executable, verifies Java 21+, and launches the interactive configuration wizard in your browser:

```bash
curl -fsSL https://raw.githubusercontent.com/vogler75/monster-mq/main/setup.sh | bash
```

### 1-Line Setup (Windows PowerShell)

```powershell
irm https://raw.githubusercontent.com/vogler75/monster-mq/main/setup.ps1 | iex
```

### Docker (Recommended)

The fastest way to get MonsterMQ running:

```bash
# Run with default configuration (SQLite)
docker run -p 1883:1883 -p 4000:4000 -v monstermq-data:/cfg-data rocworks/monstermq:latest

# Test MQTT connection
# Terminal 1: start the subscriber first
mosquitto_sub -h localhost -p 1883 -t "test/#"
# Terminal 2: publish
mosquitto_pub -h localhost -p 1883 -t "test/topic" -m "Hello MonsterMQ"
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

DefaultStoreType: POSTGRES
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
docker compose up -d
```

## Build from Source

### Prerequisites

- **Java 21+** (OpenJDK or Oracle JDK)
- **Maven 3.6+**
- **Git**

### Download and Build

```bash
# Clone repository
git clone https://github.com/vogler75/monster-mq.git
cd monster-mq/broker

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
./run.sh -- -config config.yaml
./run.sh -- -cluster -config config-hazelcast.yaml
```

The configuration filenames below are examples to create; they are not all shipped in the broker working directory. Start from [example-config.yaml](../broker/example-config.yaml). TLS listeners require configured certificates; see [Security](security.md). The source starter uses WebSocket port 1884 and MCP port 3000; the Docker defaults currently use WebSocket port 9000 and MCP port 4001.

## Configuration Examples

### SQLite (Development)

Perfect for development and testing:

```yaml
# config-sqlite.yaml
TCP: 1883
WS: 8080

SessionStoreType: SQLITE
RetainedStoreType: SQLITE
ConfigStoreType: SQLITE
QueuedMessagesEnabled: true

SQLite:
  Path: "./sqlite"  # Directory; created automatically if needed

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

DefaultStoreType: POSTGRES
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

Features:
  SparkplugB: true  # Configure decoder devices through GraphQL/dashboard
```

### Clustering with Hazelcast

Multi-node clustering setup:

```yaml
# config-hazelcast.yaml
TCP: 1883
WS: 9000

# Clustering requires central database
DefaultStoreType: POSTGRES
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

# Stop the owning service gracefully or choose a different broker port.
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

The default logging configuration writes to the console. For a local diagnostic
run, capture console output explicitly:

```bash
./run.sh -- -log FINE > broker-debug.log 2>&1
```

In another terminal:

```bash
tail -f broker-debug.log
rg 'SEVERE|Failed to' broker-debug.log
```

For Docker, use `docker logs <container-name>`. The dashboard log viewer requires
[the in-memory log buffer](graphql-system-logs.md); it is separate from console capture.
