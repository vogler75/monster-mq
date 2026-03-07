---
name: broker-config
description: >
  Guide for configuring, deploying, and operating the MonsterMQ broker. Use this skill whenever
  the user needs help with YAML configuration, Docker deployment, clustering, database setup,
  storage backend selection, TLS/SSL certificates, Kafka integration, or any operational/DevOps
  task related to MonsterMQ. Also trigger when the user asks about config.yaml structure,
  environment variables, command-line arguments, or troubleshooting broker startup issues.
  Trigger on "config.yaml", "Docker", "deploy", "cluster", "PostgreSQL setup", "MongoDB setup",
  "Kafka", "TLS", "certificate", "archive group", "storage backend", or "how to configure".
---

# MonsterMQ Broker Configuration & Operations Skill

You are helping configure, deploy, or troubleshoot the MonsterMQ MQTT broker.

## Configuration File

The broker is configured via `config.yaml`. The JSON schema is at `broker/yaml-json-schema.json` —
read it to understand all available options and their types.

### Core Structure
```yaml
# Network ports
Port:
  TCP: 1883       # Plain MQTT
  TCPS: 8883      # MQTT over TLS
  WS: 1884        # WebSocket
  WSS: 8884       # Secure WebSocket
  HTTP: 4000      # GraphQL API + Dashboard

# Storage backends
SessionStoreType: Memory | Postgres | CrateDB | MongoDB
RetainedStoreType: Memory | Hazelcast | Postgres | CrateDB
LastValueStoreType: Memory | Hazelcast | Postgres | CrateDB

# Database connections
Postgres:
  Url: "jdbc:postgresql://localhost:5432/monstermq"
  User: "postgres"
  Password: "password"

CrateDB:
  Url: "jdbc:postgresql://localhost:5433/doc"

MongoDB:
  ConnectionString: "mongodb://localhost:27017"
  DatabaseName: "monstermq"

# Message archiving
ArchiveGroups:
  - Name: "Default"           # Required for MCP Server
    TopicFilter: ["#"]
    StoreType: Postgres | CrateDB | MongoDB | Kafka
    RetainedStoreType: Postgres | CrateDB  # Optional
    LastValueStoreType: Postgres | CrateDB  # Optional

# Kafka (optional)
Kafka:
  Servers: "localhost:9092"
  TopicPrefix: "monstermq"

# User management
Auth:
  UserStoreType: Postgres | CrateDB | MongoDB
  JwtSecret: "your-secret-key"
  TokenExpiry: 3600            # seconds
```

## Command-Line Arguments

```bash
./run.sh [options]

-cluster              Enable Hazelcast clustering
-log LEVEL            Set log level: INFO|FINE|FINER|FINEST|ALL
-config FILE          Custom config file path (default: config.yaml)
-dashboardPath PATH   Serve dashboard from filesystem (dev mode)
```

## Docker Deployment

### Build
```bash
cd docker
./build
```

### Run
```bash
docker run -d \
  -p 1883:1883 \
  -p 4000:4000 \
  -v ./config.yaml:/app/config.yaml \
  -v ./log:/app/log \
  rocworks/monstermq
```

### Docker Compose (typical setup with PostgreSQL)
```yaml
services:
  monstermq:
    image: rocworks/monstermq
    ports:
      - "1883:1883"
      - "4000:4000"
    volumes:
      - ./config.yaml:/app/config.yaml
    depends_on:
      - postgres

  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: monstermq
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: password
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

volumes:
  pgdata:
```

## Clustering

- Enable with `-cluster` flag
- Uses Hazelcast for cluster coordination
- Configure `hazelcast.xml` or `hazelcast.yaml` in classpath
- Devices are distributed across nodes via `clusterManager.isLocalNodeResponsible()`
- Message bus distributes messages across all nodes

## TLS/SSL Configuration

```yaml
Port:
  TCPS: 8883
  WSS: 8884

SSL:
  KeyStore: "keystore.jks"           # or .p12
  KeyStorePassword: "password"
  TrustStore: "truststore.jks"       # optional, for client certs
  TrustStorePassword: "password"
```

Supports JKS and PKCS12/PFX certificate formats.

## Storage Backend Selection Guide

| Use Case | Recommended Backend |
|----------|-------------------|
| Development/testing | Memory (no setup needed) |
| Single node, persistent | PostgreSQL |
| Time-series data | CrateDB |
| Document-oriented | MongoDB |
| High-throughput archiving | Kafka + one of above |
| Multi-node clustering | Hazelcast (retained/last-value) + DB (sessions/archive) |

## MCP Server

The MCP Server runs on the HTTP port (default 4000) and requires:
- An ArchiveGroup named "Default" in the configuration
- This enables AI tools to query MQTT message history

## Troubleshooting

### Broker won't start
- Check config.yaml syntax (validate against JSON schema)
- Ensure database is reachable if using DB backends
- Check port conflicts

### Messages not persisting
- Verify ArchiveGroup has matching TopicFilter
- Check database connection and table creation
- Ensure StoreType is set correctly

### Cluster issues
- Verify Hazelcast can discover other nodes (multicast or TCP-IP)
- Check firewall rules for Hazelcast ports (5701+)
- Review logs at FINE level: `-log FINE`

### Dashboard not loading
- Verify HTTP port is configured and not blocked
- Check browser console for JavaScript errors
- Verify GraphQL endpoint is accessible at `/graphql`
