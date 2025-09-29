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

