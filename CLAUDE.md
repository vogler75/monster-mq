# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MonsterMQ is a MQTT broker built with Kotlin on Vert.X and Hazelcast with data persistence through PostgreSQL, CrateDB, MongoDB, or SQLite. It features a web dashboard, GraphQL API, MCP (Model Context Protocol) Server for AI integration, I3X API for manufacturing data, a flow engine for automation workflows, and device bridging for OPC UA, PLC4X, Kafka, NATS, WinCC OA/Unified, Neo4j, and SparkplugB.

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

#### Dashboard Development Mode
When making changes to HTML/CSS/JavaScript dashboard files, you can run the broker with the `-dashboardPath` option to avoid rebuilding:
```bash
cd broker
./run.sh -dashboardPath src/main/resources/dashboard [-cluster] [-log INFO|FINE|FINER|FINEST|ALL]
```

This allows you to:
- Edit `.html` files in `src/main/resources/dashboard/pages/`
- Edit `.css` files in `src/main/resources/dashboard/assets/`
- Edit `.js` files in `src/main/resources/dashboard/js/`
- Refresh the browser to see changes immediately without rebuilding the project

This significantly speeds up dashboard development iterations.

### Running Tests

#### Kotlin Unit Tests
```bash
cd broker
mvn test
```

#### Python Integration Tests
```bash
cd tests
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run all tests
pytest

# Run by category
pytest mqtt3/     # MQTT v3.1.1 tests
pytest mqtt5/     # MQTT v5 tests
pytest graphql/   # GraphQL API tests
pytest opcua/     # OPC UA tests
pytest database/  # Database backend tests

# Run single test
pytest mqtt3/test_basic_pubsub.py::test_basic_pubsub_qos0 -v
```

Test configuration via environment variables:
```
MQTT_BROKER=localhost  MQTT_PORT=1883  MQTT_USERNAME=Test  MQTT_PASSWORD=Test
GRAPHQL_URL=http://localhost:4000/graphql
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

- **Main Entry Point**: `broker/src/main/kotlin/Main.kt` -> `Monster.kt`
- **MQTT Server**: `MqttServer.kt` - Handles MQTT protocol implementation (v3.1.1 and v5)
- **MQTT Client Handler**: `MqttClient.kt` - Manages individual client connections and session state
- **Message Bus**: `bus/` directory - Abstraction for message distribution (Vertx EventBus or Kafka)
- **Storage Layer**: `stores/` directory - Modular storage implementations for sessions, retained messages, and archives
- **GraphQL Server**: `graphql/GraphQLServer.kt` - GraphQL API for dashboard and external integrations
- **Flow Engine**: `flowengine/` directory - Automation workflow engine with script execution
- **Auth System**: `auth/` directory - User management, password encoding, ACL caching

### Storage Architecture

The system uses different store types for different purposes:
- **SessionStore**: Persistent client sessions (PostgreSQL, CrateDB, MongoDB, SQLite)
- **RetainedStore**: Retained MQTT messages (Memory, Hazelcast, PostgreSQL, CrateDB)
- **MessageArchive**: Historical message storage (PostgreSQL, CrateDB, MongoDB, SQLite, Kafka)
- **LastValueStore**: Current value cache for topics (Memory, Hazelcast, PostgreSQL, CrateDB)
- **MetricsStore**: Broker metrics history (PostgreSQL, CrateDB, MongoDB, SQLite)

### Device Bridging Architecture

Devices follow a pattern of Extension (cluster-aware coordinator) + Connector (per-device verticle):

- `devices/mqttclient/` - MQTT client bridge
- `devices/kafkaclient/` - Kafka client bridge
- `devices/natsclient/` - NATS client bridge
- `devices/opcua/` - OPC UA client
- `devices/opcuaserver/` - OPC UA server
- `devices/plc4x/` - PLC4X protocol (industrial PLCs)
- `devices/winccoa/` - WinCC OA integration
- `devices/winccua/` - WinCC Unified integration
- `devices/neo4j/` - Neo4j graph database bridge
- `devices/sparkplugb/` - SparkplugB decoder

For device integration guidance, see `plans/DEVICE_INTEGRATION.md`.

### Key Directories

- `broker/src/main/kotlin/bus/` - Message bus implementations (Vertx, Kafka)
- `broker/src/main/kotlin/stores/` - Storage layer implementations
  - `dbs/postgres/` - PostgreSQL storage implementations
  - `dbs/cratedb/` - CrateDB storage implementations
  - `dbs/mongodb/` - MongoDB storage implementations
  - `dbs/sqlite/` - SQLite storage implementations
  - `devices/` - Device configuration data classes
- `broker/src/main/kotlin/devices/` - Device connector/extension implementations
- `broker/src/main/kotlin/handlers/` - Message and subscription handlers
- `broker/src/main/kotlin/extensions/` - Extensions (MCP Server, I3X API, Sparkplug, API Service)
- `broker/src/main/kotlin/graphql/` - GraphQL resolvers, queries, and mutations
- `broker/src/main/kotlin/flowengine/` - Flow engine for automation workflows
- `broker/src/main/kotlin/auth/` - Authentication and authorization
- `broker/src/main/kotlin/data/` - Data models and codecs
- `broker/src/main/kotlin/genai/` - GenAI provider abstraction
- `broker/src/main/kotlin/logging/` - Syslog and in-memory log handling

### Web Dashboard

The dashboard is a vanilla HTML/CSS/JS web interface (no frameworks):

- `broker/src/main/resources/dashboard/` - Dashboard root
  - `assets/monster-theme.css` - Global dark theme (CSS variables)
  - `js/storage.js` - localStorage wrapper
  - `js/graphql-client.js` - `GraphQLDashboardClient` class
  - `js/sidebar.js` - `SidebarManager` with menu config (add new pages here)
  - `js/log-viewer.js` - Global log viewer component
  - `pages/` - HTML pages (one per view)
  - `js/` - Page-specific JavaScript

### HTTP Services and Ports

| Service | Default Port | Description |
|---------|-------------|-------------|
| MQTT TCP | 1883 | Plain MQTT connections |
| MQTT TLS | 8883 | MQTT over TLS |
| WebSocket | 1884 | MQTT over WebSocket |
| WebSocket TLS | 8884 | MQTT over Secure WebSocket |
| GraphQL + Dashboard | 4000 | HTTP API and web UI |
| MCP Server | 3000 | Model Context Protocol for AI |
| Grafana | 3001 | Grafana integration |
| I3X API | 3002 | CESMII I3X manufacturing API |

### Configuration

Configuration is done via YAML file (`config.yaml`). The schema is defined in `broker/yaml-json-schema.json`.

Key configuration sections:
- Network ports (TCP, TCPS, WS, WSS, HTTP)
- Storage backends (SessionStoreType, RetainedStoreType, LastValueStoreType)
- Archive groups with topic filters
- Database connections (PostgreSQL, CrateDB, MongoDB)
- Kafka configuration
- Auth/User management (UserStoreType, JwtSecret)
- MCP Server requires an ArchiveGroup named "Default"

### Extension Points

1. **MCP Server** (`extensions/McpServer.kt`, `extensions/McpHandler.kt`): Model Context Protocol integration for AI models
2. **I3X API** (`extensions/I3xServer.kt`): CESMII I3X standard API for manufacturing data
3. **API Service** (`extensions/ApiService.kt`): Generic REST API service
4. **Sparkplug Extension** (`extensions/SparkplugExtension.kt`): Expands SparkplugB messages

### Technology Stack

- **Language**: Kotlin (JVM 21)
- **Framework**: Vert.X 5.0.7 (async/reactive)
- **Clustering**: Hazelcast
- **Databases**: PostgreSQL, CrateDB, MongoDB, SQLite
- **Message Bus**: Kafka (optional)
- **Build Tool**: Maven
- **Protocol**: MQTT 3.1.1 and MQTT 5.0
- **API**: GraphQL (graphql-java)
- **Testing**: pytest (Python integration tests), JUnit (Kotlin unit tests)

## Git and Commit Guidelines

**CRITICAL: NEVER AUTO-COMMIT UNDER ANY CIRCUMSTANCES**

- **MUST NEVER automatically commit changes** - This is non-negotiable and absolute. Claude Code must NEVER create commits without explicit user authorization
- **Wait for explicit user instruction** - Only commit when the user explicitly says "commit", "merge to main", "create a commit", or similar clear instruction
- **ALWAYS present changes for review first** - Show the user what was changed (via git diff or git status) and ask if they want you to proceed with committing
- **Do NOT commit as Claude** - Do not include "Generated with Claude Code" or "Co-Authored-By: Claude" in commits
- **Create branches for work** - Create feature/fix branches as needed, but changes should remain staged/unstaged until explicitly instructed to commit
- **If you auto-commit, you have made a critical mistake** - Always err on the side of caution and let the user decide when and how to commit their changes
- **No assumptions about commit intent** - Even if it seems obvious that changes should be committed, always wait for explicit user instruction

## Development Notes

- The project uses Vert.X's asynchronous programming model extensively
- Storage operations are abstracted through interfaces (IMessageStore, ISessionStore, IDeviceConfigStore)
- Clustering is optional and controlled via `-cluster` command line argument
- Logging level can be configured via command line or properties files in `src/main/resources/`
- The MCP Server integration uses the official MCP SDK (io.modelcontextprotocol.sdk)
- Device integrations follow the Extension + Connector pattern (see `plans/DEVICE_INTEGRATION.md`)
- The dashboard uses vanilla JS with `GraphQLDashboardClient` — no build tools or frameworks
- GraphQL schema is split across `broker/src/main/resources/schema-*.graphqls` files
- Planning documents are in `plans/` (MQTT5 implementation, device integration, GraalVM analysis)
