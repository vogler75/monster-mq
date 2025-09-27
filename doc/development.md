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

## Project Structure

```
monstermq/
├── broker/                    # Main broker implementation
│   ├── src/
│   │   ├── main/
│   │   │   ├── kotlin/       # Kotlin source code
│   │   │   │   ├── Main.kt   # Entry point
│   │   │   │   ├── Monster.kt # Core broker
│   │   │   │   ├── MqttServer.kt
│   │   │   │   ├── MqttClient.kt
│   │   │   │   ├── bus/      # Message bus implementations
│   │   │   │   ├── stores/   # Storage implementations
│   │   │   │   ├── handlers/ # Protocol handlers
│   │   │   │   └── extensions/ # Extensions (MCP, Sparkplug)
│   │   │   └── resources/    # Config, dashboard, etc.
│   │   └── test/             # Unit tests
│   ├── pom.xml               # Maven configuration
│   └── yaml-json-schema.json # Config schema
├── docker/                   # Docker build files
├── doc/                      # Documentation
└── README.md
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

### Build Profiles

```xml
<!-- pom.xml profiles -->
<profiles>
  <!-- Development profile (default) -->
  <profile>
    <id>development</id>
    <properties>
      <build.environment>development</build.environment>
      <skip.tests>false</skip.tests>
    </properties>
  </profile>

  <!-- Production profile -->
  <profile>
    <id>production</id>
    <properties>
      <build.environment>production</build.environment>
      <maven.test.skip>true</maven.test.skip>
    </properties>
  </profile>
</profiles>
```

### Creating Executable JAR

```bash
# Build fat JAR with dependencies
mvn clean compile assembly:single

# Run fat JAR
java -jar target/monstermq-1.0-jar-with-dependencies.jar
```

## IDE Setup

### IntelliJ IDEA

1. **Import Project:**
   - File → Open → Select `broker` folder
   - Import as Maven project

2. **Configure Kotlin:**
   - Ensure Kotlin plugin is installed
   - Set language level to 1.9

3. **Run Configuration:**
   ```
   Main class: at.rocworks.MonsterKt
   Program arguments: -config example-config.yaml -log FINE
   Working directory: $PROJECT_DIR$/broker
   ```

### Visual Studio Code

1. **Install Extensions:**
   - Kotlin Language
   - Maven for Java
   - Debugger for Java

2. **Configure launch.json:**
   ```json
   {
     "version": "0.2.0",
     "configurations": [
       {
         "type": "java",
         "name": "Launch MonsterMQ",
         "request": "launch",
         "mainClass": "at.rocworks.MonsterKt",
         "args": ["-config", "example-config.yaml", "-log", "FINE"],
         "projectName": "broker"
       }
     ]
   }
   ```

## Testing

### Unit Tests

```kotlin
// Example unit test
class MqttServerTest {
    @Test
    fun `test client connection`() {
        val server = MqttServer(config)
        val client = TestMqttClient("test-client")

        client.connect("localhost", 1883)
        assertTrue(client.isConnected)

        client.disconnect()
        assertFalse(client.isConnected)
    }
}
```

### Integration Tests

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=MqttServerTest

# Run with coverage
mvn jacoco:prepare-agent test jacoco:report
```

### Load Testing

```kotlin
// Load test example
class LoadTest {
    @Test
    fun `test high throughput`() = runBlocking {
        val server = startServer()
        val clients = (1..1000).map {
            TestMqttClient("client-$it")
        }

        // Connect all clients
        clients.parMap { it.connect() }

        // Publish messages
        val startTime = System.currentTimeMillis()
        clients.parMap { client ->
            repeat(100) {
                client.publish("test/topic", "message-$it")
            }
        }
        val duration = System.currentTimeMillis() - startTime

        val messagesPerSecond = (1000 * 100) / (duration / 1000.0)
        println("Throughput: $messagesPerSecond msg/sec")

        assertTrue(messagesPerSecond > 10000)
    }
}
```

## Code Style

### Kotlin Style Guide

```kotlin
// Package structure
package at.rocworks.feature

// Imports (alphabetical)
import kotlinx.coroutines.*
import java.util.concurrent.*

// Class definition
class ExampleService(
    private val config: Config,
    private val repository: Repository
) : Service {

    // Constants
    companion object {
        private const val DEFAULT_TIMEOUT = 5000L
        private val logger = LoggerFactory.getLogger(ExampleService::class.java)
    }

    // Properties
    private val cache = ConcurrentHashMap<String, Any>()

    // Initialization
    init {
        initialize()
    }

    // Public methods
    override suspend fun process(message: Message): Result {
        return try {
            // Implementation
            Result.Success(processInternal(message))
        } catch (e: Exception) {
            logger.error("Processing failed", e)
            Result.Failure(e)
        }
    }

    // Private methods
    private fun processInternal(message: Message): Any {
        // Implementation
    }
}
```

### Code Formatting

```bash
# Format code with ktlint
mvn ktlint:format

# Check code style
mvn ktlint:check
```

## Extending MonsterMQ

### Creating a Custom Store

```kotlin
// Implement the storage interface
class CustomStore : IMessageStore {
    override suspend fun init(config: JsonObject) {
        // Initialize your store
    }

    override suspend fun store(message: Message): Boolean {
        // Store the message
        return true
    }

    override suspend fun retrieve(topic: String): List<Message> {
        // Retrieve messages
        return emptyList()
    }

    override suspend fun close() {
        // Cleanup resources
    }
}

// Register the store
StoreFactory.register("CUSTOM", CustomStore::class)
```

### Creating an Extension

```kotlin
// Create an extension
class MyExtension : Extension {
    override fun getName(): String = "MyExtension"

    override suspend fun init(broker: Monster, config: JsonObject) {
        // Initialize extension
        broker.eventBus.consumer<Message>("mqtt.messages") { msg ->
            processMessage(msg.body())
        }
    }

    private suspend fun processMessage(message: Message) {
        // Process messages
    }
}

// Register in configuration
Extensions:
  - Name: MyExtension
    Class: at.rocworks.extensions.MyExtension
    Config:
      param1: value1
```

### Custom Authentication Provider

```kotlin
class LdapAuthProvider : AuthenticationProvider {
    override suspend fun authenticate(
        username: String,
        password: String
    ): AuthResult {
        return try {
            val user = ldapClient.authenticate(username, password)
            AuthResult.Success(User(username, user.roles))
        } catch (e: Exception) {
            AuthResult.Failure("Authentication failed")
        }
    }
}
```

## Debugging

### Remote Debugging

```bash
# Start with debug port
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005 \
  -jar monstermq.jar

# Connect IDE debugger to localhost:5005
```

### Logging Configuration

```properties
# src/main/resources/logging.properties
.level=INFO
at.rocworks.level=FINE

# Console handler
java.util.logging.ConsoleHandler.level=FINE
java.util.logging.ConsoleHandler.formatter=java.util.logging.SimpleFormatter

# File handler
java.util.logging.FileHandler.level=FINE
java.util.logging.FileHandler.pattern=logs/monstermq-%u.log
java.util.logging.FileHandler.limit=10485760
java.util.logging.FileHandler.count=10
java.util.logging.FileHandler.formatter=java.util.logging.SimpleFormatter
```

### Debug Output

```kotlin
// Add debug logging
class MqttClient {
    private val logger = Logger.getLogger(javaClass.name)

    fun handleMessage(message: Message) {
        logger.fine { "Received message: topic=${message.topic}, payload=${message.payload}" }

        if (logger.isLoggable(Level.FINEST)) {
            logger.finest { "Message details: $message" }
        }
    }
}
```

## Contributing

### Development Workflow

1. **Fork the repository**
2. **Create feature branch:**
   ```bash
   git checkout -b feature/my-feature
   ```

3. **Make changes and test:**
   ```bash
   mvn clean test
   ```

4. **Commit with descriptive message:**
   ```bash
   git commit -m "Add feature: description of changes"
   ```

5. **Push and create PR:**
   ```bash
   git push origin feature/my-feature
   ```

### Commit Message Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `style`: Code style
- `refactor`: Refactoring
- `perf`: Performance
- `test`: Testing
- `chore`: Maintenance

Example:
```
feat(mqtt): add MQTT 5.0 support

- Implement MQTT 5.0 protocol features
- Add property support
- Update client handling

Closes #123
```

### Code Review Checklist

- [ ] Tests pass
- [ ] Code follows style guide
- [ ] Documentation updated
- [ ] No security issues
- [ ] Performance impact considered
- [ ] Backward compatibility maintained

## Release Process

### Version Numbering

```xml
<!-- pom.xml -->
<version>1.0.0-SNAPSHOT</version>

<!-- Release version format: MAJOR.MINOR.PATCH -->
<!-- 1.0.0 - Major release -->
<!-- 1.1.0 - Minor release -->
<!-- 1.1.1 - Patch release -->
```

### Release Steps

```bash
# 1. Update version
mvn versions:set -DnewVersion=1.0.0

# 2. Run full test suite
mvn clean verify

# 3. Build release artifacts
mvn clean package -Pproduction

# 4. Tag release
git tag -a v1.0.0 -m "Release version 1.0.0"
git push origin v1.0.0

# 5. Build Docker image
cd docker
./build.sh 1.0.0

# 6. Push to Docker Hub
docker push rocworks/monstermq:1.0.0
docker push rocworks/monstermq:latest
```

## Performance Profiling

### CPU Profiling

```bash
# Using async-profiler
java -agentpath:/path/to/async-profiler.so=start,event=cpu,file=profile.html \
  -jar monstermq.jar

# Using JFR (Java Flight Recorder)
java -XX:StartFlightRecording=duration=60s,filename=recording.jfr \
  -jar monstermq.jar
```

### Memory Profiling

```bash
# Heap dump
jmap -dump:format=b,file=heap.bin $(pgrep -f monstermq)

# Analyze with Eclipse MAT or VisualVM
```

## Troubleshooting Development Issues

### Common Build Issues

1. **Maven dependency issues:**
   ```bash
   mvn dependency:purge-local-repository
   mvn clean install -U
   ```

2. **Kotlin compilation errors:**
   ```bash
   mvn clean compile -X  # Verbose output
   ```

3. **Test failures:**
   ```bash
   mvn test -Dtest=FailingTest -X
   ```

### Development Tools

- **VisualVM** - JVM monitoring
- **WireMock** - API mocking
- **Testcontainers** - Integration testing
- **JMH** - Microbenchmarking

## Resources

### Documentation
- [Kotlin Documentation](https://kotlinlang.org/docs)
- [Vert.x Documentation](https://vertx.io/docs)
- [MQTT Specification](https://mqtt.org/mqtt-specification/)

### Community
- GitHub Issues: Report bugs and request features
- Discussions: Ask questions and share ideas
- Wiki: Additional documentation and guides

### Learning Resources
- Kotlin Koans: Learn Kotlin syntax
- Vert.x Examples: Example applications
- MQTT Essentials: MQTT protocol guide