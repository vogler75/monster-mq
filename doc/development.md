# Development

This guide covers setting up a development environment, building MonsterMQ from source, contributing code, and extending functionality.

## Development Environment

### Prerequisites

- **Java 21+** (OpenJDK or Oracle JDK)
- **Maven 3.6+**
- **Git**
- **Docker** (optional, for testing)
- **IDE** (IntelliJ IDEA recommended for Kotlin)

### Setup

```bash
# Clone repository
git clone https://github.com/rocworks/monstermq.git
cd monstermq

# Build project
cd broker
mvn clean install

# Run tests
mvn test

# Run with development config
java -cp "target/classes:target/dependencies/*" \
  at.rocworks.MonsterKt \
  -config ../example-config.yaml \
  -log FINE
```

## Building from Source

### Standard Build

```bash
cd broker

# Clean build
mvn clean package

# Skip tests for faster build
mvn clean package -DskipTests

# Build with specific profile
mvn clean package -Pproduction
```

## MQTT Bridging Terminology

The UI refers to MQTT "Bridges" when configuring outbound connections to remote brokers. Internally (GraphQL schema names, REST/JS identifiers, existing config fields) the historical term "mqttClient" / "client" is retained to avoid breaking existing integrations. Only user-facing labels, headings, buttons, tooltips, and notifications have been updated to say "Bridge" or "Bridges".

Retained internal identifiers (examples):
- GraphQL query names: `mqttClients`, mutations: `createMqttClient`, `toggleMqttClient`, etc.
- URL/query params: `?client=<name>`
- Element IDs / variable names: `client-*`

When extending functionality prefer continuing this pattern until a versioned API change can deprecate the old names.

