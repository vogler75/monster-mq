# MonsterMQ Build Commands Reference

Quick reference for building MonsterMQ locally or in Docker.

---

## Local Development Build

### Build with Maven in Docker (No local Maven needed)
```powershell
# From project root - builds the broker subdirectory
docker run -it --rm -v ${PWD}:/app -w /app/broker maven:3.9-eclipse-temurin-21 mvn clean package -DskipTests
```

**Then run locally with the built JAR:**
```powershell
cd broker
.\run.bat -config C:\Projects\monster-mq\local\config.yaml
```

### Maven Build (Windows)
```powershell
cd C:\Projects\monster-mq\broker
mvn clean package -DskipTests
```

### Run with Custom Config
```powershell
cd C:\Projects\monster-mq\broker
.\run.bat -config C:\Projects\monster-mq\local\config.yaml
```

---

## Docker Build Commands

### Build Docker Image (Linux/Mac)
```bash
cd docker
./build
```

Options:
- `-d` - Skip Maven build (Docker only, uses existing JAR)
- `-n` - Don't publish to Docker Hub
- `-y` - Auto-publish to Docker Hub without prompt
- `--testing` or `-t` - Build testing image

### Build Docker Image (Windows PowerShell)

**Full build with Maven:**
```powershell
cd docker
docker run --rm -v ${PWD}/..:/workspace -w /workspace/broker maven:3.9-eclipse-temurin-21 mvn clean package -DskipTests
```

**Then build Docker image:**
```powershell
# Copy artifacts to docker/target
Remove-Item -Recurse -Force target -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path target
Copy-Item ../broker/target/broker-1.0-SNAPSHOT.jar target/
Copy-Item -Recurse ../broker/target/dependencies target/

# Get version
$version = (Get-Content ../version.txt).Split('+')[0]

# Build image
docker build -t rocworks/monstermq:$version -t rocworks/monstermq:latest .

# Optional: Push to Docker Hub
docker push rocworks/monstermq:latest
docker push rocworks/monstermq:$version
```

---

## Docker Run Commands

### Run with Local Config
```powershell
docker run -p 1883:1883 -p 4000:4000 -p 3000:3000 `
  -v C:\Projects\monster-mq\local:/config `
  rocworks/monstermq:latest -config /config/config.yaml
```

### Run with Docker Compose
```powershell
cd docker
docker-compose -f docker-compose-local.yaml up
```

---

## Testing Changes

### Frontend Changes Only (No Rebuild Needed)
- HTML/CSS/JavaScript changes in `broker/src/main/resources/dashboard/`
- Just refresh browser with `Ctrl + F5`
- MonsterMQ serves these files directly

### Backend Changes (Rebuild Required)
- Kotlin code changes in `broker/src/main/kotlin/`
- GraphQL schema changes in `broker/src/main/resources/*.graphqls`
- Configuration changes in `broker/src/main/resources/`

**Rebuild steps:**
```powershell
cd C:\Projects\monster-mq\broker
mvn clean package -DskipTests
.\run.bat -config C:\Projects\monster-mq\local\config.yaml
```

---

## Testing Commands

### Run Tests in Docker (Recommended)
```powershell
# From project root - compile and run Kotlin tests directly in Docker
docker run --rm -v ${PWD}:/workspace -v maven-cache:/root/.m2 -w /workspace/broker maven:3.9-eclipse-temurin-22 bash -c "mvn clean compile dependency:copy-dependencies -DskipTests && java -cp 'target/classes:target/dependency/*' at.rocworks.tests.TestUserManagementKt"
```

### Run Tests Locally
```powershell
cd C:\Projects\monster-mq\broker
mvn test
```

### Run Kotlin Tests Directly (Without JUnit)
```powershell
# Build first, then run as Kotlin main function
cd C:\Projects\monster-mq\broker
mvn clean compile -DskipTests
mvn dependency:copy-dependencies -DskipTests
java -cp "target/classes;target/dependencies/*" at.rocworks.tests.TestUserManagementKt
```

**Note:** Docker command uses Java 22 (matching pom.xml) and creates a named volume `maven-cache` to persist downloaded dependencies between runs.

---

## Quick Commands Cheat Sheet

| Task | Command |
|------|---------|
| **Maven build** | `mvn clean package -DskipTests` |
| **Run local** | `.\run.bat -config ..\local\config.yaml` |
| **Run tests (Docker)** | `docker run --rm -v ${PWD}:/workspace -v maven-cache:/root/.m2 -w /workspace/broker maven:3.9-eclipse-temurin-22 bash -c "mvn clean compile dependency:copy-dependencies -DskipTests && java -cp 'target/classes:target/dependency/*' at.rocworks.tests.TestUserManagementKt"` |
| **Docker Maven build** | `docker run --rm -v ${PWD}/..:/workspace -w /workspace/broker maven:3.9-eclipse-temurin-21 mvn clean package -DskipTests` |
| **Docker build image** | `docker build -t rocworks/monstermq:latest .` |
| **Hard refresh browser** | `Ctrl + F5` or `Ctrl + Shift + R` |

---

## Allen-Bradley PLC4X Connection Strings

For EtherNet/IP troubleshooting:

```
# EtherNet/IP (may have byte order issues)
eip://10.1.25.87:44818

# AB-Ethernet (requires backplane/slot)
ab-eth://10.1.25.87:44818/0/0

# Recommended: Use Node-RED bridge
# See: doc/plc4x-ethernet-ip-troubleshooting.md
```

---

## GenAI Configuration

MonsterMQ supports AI-assisted JavaScript coding in the Flow Engine.

### Supported Providers

| Provider | Status | Config File |
|----------|--------|-------------|
| **Google Gemini** | ✅ Implemented | `genai-config-example.yaml` |
| **Azure OpenAI** | ✅ Implemented | `genai-azure-openai-example.yaml` |
| Claude | ⚠️ Planned | - |
| OpenAI | ⚠️ Planned | - |

### Quick Setup (Azure OpenAI)

1. **Set environment variables:**
   ```powershell
   $env:AZURE_OPENAI_API_KEY="your-api-key"
   $env:AZURE_OPENAI_ENDPOINT="https://your-resource.openai.azure.com/"
   ```

2. **Add to config.yaml:**
   ```yaml
   GenAI:
     Enabled: true
     Provider: "azure-openai"
     ApiKey: "${AZURE_OPENAI_API_KEY}"
     Endpoint: "${AZURE_OPENAI_ENDPOINT}"
     Deployment: "gpt-4o"  # Your Azure deployment name
     Model: "gpt-4o"
   ```

3. **Rebuild and run:**
   ```powershell
   mvn clean package -DskipTests
   .\run.bat -config ..\local\config.yaml
   ```

See `genai-azure-openai-example.yaml` for detailed configuration.

---

*Last Updated: November 27, 2025*
