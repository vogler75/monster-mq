# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MonsterMQ is a MQTT broker built with Kotlin on Vert.X and Hazelcast with data persistence through PostgreSQL, CrateDB, or MongoDB. It includes a MCP (Model Context Protocol) Server for AI integration running on port 3000.

## Build and Run Commands

### Building the Project
```bash
cd broker
mvn clean package
```

### Running Locally
```bash
cd broker
# Run the broker
java -classpath target/classes:target/dependencies/* at.rocworks.MonsterKt [-cluster] [-log INFO|FINE|FINER|FINEST|ALL]
# Or use the convenience script
./run.sh [-cluster] [-log INFO|FINE|FINER|FINEST|ALL]
```

### Running Tests
```bash
cd broker
mvn test
```

### Docker Commands
```bash
# Build Docker image
cd docker
./build

# Run with Docker
docker run -v ./log:/app/log -v ./config.yaml:/app/config.yaml rocworks/monstermq [-cluster] [-log INFO|FINE|FINER|FINEST|ALL]
```

## Code Architecture

### Core Components

- **Main Entry Point**: `broker/src/main/kotlin/Main.kt` → `Monster.kt`
- **MQTT Server**: `MqttServer.kt` - Handles MQTT protocol implementation
- **MQTT Client Handler**: `MqttClient.kt` - Manages individual client connections and session state
- **Message Bus**: `bus/` directory - Abstraction for message distribution (Vertx EventBus or Kafka)
- **Storage Layer**: `stores/` directory - Modular storage implementations for sessions, retained messages, and archives

### Storage Architecture

The system uses different store types for different purposes:
- **SessionStore**: Persistent client sessions (PostgreSQL, CrateDB, MongoDB)
- **RetainedStore**: Retained MQTT messages (Memory, Hazelcast, PostgreSQL, CrateDB)
- **MessageArchive**: Historical message storage (PostgreSQL, CrateDB, MongoDB, Kafka)
- **LastValueStore**: Current value cache for topics (Memory, Hazelcast, PostgreSQL, CrateDB)

### Key Directories

- `broker/src/main/kotlin/bus/` - Message bus implementations (Vertx, Kafka)
- `broker/src/main/kotlin/stores/` - Storage layer implementations
  - `postgres/` - PostgreSQL storage implementations
  - `cratedb/` - CrateDB storage implementations
  - `mongodb/` - MongoDB storage implementations
- `broker/src/main/kotlin/handlers/` - Message and subscription handlers
- `broker/src/main/kotlin/extensions/` - Extensions (MCP Server, Sparkplug)
- `broker/src/main/kotlin/data/` - Data models and codecs

### Configuration

Configuration is done via YAML file (`config.yaml`). The schema is defined in `broker/yaml-json-schema.json`.

Key configuration sections:
- Network ports (TCP, TCPS, WS, WSS)
- Storage backends (SessionStoreType, RetainedStoreType)
- Archive groups with topic filters
- Database connections (PostgreSQL, CrateDB, MongoDB)
- Kafka configuration
- MCP Server requires an ArchiveGroup named "Default"

### Extension Points

1. **MCP Server** (`extensions/McpServer.kt`, `extensions/McpHandler.kt`): Model Context Protocol integration for AI models
2. **Sparkplug Extension** (`extensions/SparkplugExtension.kt`): Expands SparkplugB messages

### Technology Stack

- **Language**: Kotlin (JVM 21)
- **Framework**: Vert.X 4.5.14 (async/reactive)
- **Clustering**: Hazelcast
- **Databases**: PostgreSQL, CrateDB, MongoDB
- **Message Bus**: Kafka (optional)
- **Build Tool**: Maven
- **Protocol**: MQTT 3.1.1 (MQTT5 not yet supported)

## Git and Commit Guidelines

⚠️ **CRITICAL: NEVER AUTO-COMMIT** ⚠️

- **MUST NEVER commit unless explicitly told to do so** - This is non-negotiable. Wait for the user to explicitly say "commit" or "merge to main"
- **ALWAYS ask the user to review changes first** before committing
- **Do NOT commit as Claude**: Do not include "Generated with Claude Code" or "Co-Authored-By: Claude" in commits
- **Manual commits only**: Only commit when the user explicitly instructs with phrases like "please commit", "merge to main", "create a commit", etc.
- Create branches for work, but changes should remain staged/unstaged until instructed otherwise
- **If you auto-commit, you have made a mistake** - Always err on the side of caution and let the user decide when to commit

## Development Notes

- The project uses Vert.X's asynchronous programming model extensively
- Storage operations are abstracted through interfaces (IMessageStore, ISessionStore)
- Clustering is optional and controlled via `-cluster` command line argument
- Logging level can be configured via command line or properties files in `src/main/resources/`
- The MCP Server integration uses the official MCP SDK (io.modelcontextprotocol.sdk)