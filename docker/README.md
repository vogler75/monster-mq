# MonsterMQ Main Broker Docker Images

Build and publication scripts for `rocworks/monstermq` Docker images.

---

## Quick Start

### 1. Build Local Image (Native Platform)

Builds the local image for the current host architecture without pushing to Docker Hub:

```bash
cd docker
./build.sh -n
```

If the broker bundle is already built in `dist/` or `broker/target/`, pass `-d` to skip rebuilding Maven:

```bash
./build.sh -d -n
```

### 2. Multi-Arch Build & Publish to Docker Hub

Builds multi-architecture images (`linux/amd64` and `linux/arm64`) using `docker-buildx` and pushes directly to Docker Hub:

```bash
cd docker
./build.sh -d -y
```

This publishes:
- `rocworks/monstermq:<version>` & `:latest`
- `rocworks/monstermq:<version>-jdk21` & `:latest-jdk21`

---

## Command-Line Options

```text
Usage: ./build.sh [options]

  -d                 Docker only (skip Maven/dashboard build, uses existing target or dist zip)
  -c, --container    Build Maven inside Docker container (GraalVM 21 builder)
  -n                 Do not publish to Docker Hub (local build only)
  -y                 Publish to Docker Hub without asking
  --testing, -t      Build testing image (rocworks/monstermq:testing)
  --clean            Clean output directories before build
```

---

## Helper Scripts

- **`./build-testing.sh`**: Convenience wrapper running `./build.sh -c -t -n` (builds `rocworks/monstermq:testing` locally in container).
- **`./build-version.sh`**: Convenience wrapper running `./build.sh -c -y --clean` (clean, multi-arch build and publish to Docker Hub).

