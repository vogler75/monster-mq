# MonsterMQ Core Docker Images

Build and publication commands for `rocworks/monstermq` Docker images. All commands run from the
repository root with Python 3.9+ (Windows, macOS and Linux).

---

## Quick Start

### 1. Build Local Image (Native Platform)

Builds the local image for the current host architecture without pushing to Docker Hub:

```bash
python make.py docker -n
```

If the broker bundle is already built in `dist/` or `broker/target/`, pass `-d` to skip rebuilding Maven:

```bash
python make.py docker -d -n
```

### 2. Multi-Arch Build & Publish to Docker Hub

Builds multi-architecture images (`linux/amd64` and `linux/arm64`) using `docker buildx` and pushes directly to Docker Hub:

```bash
python make.py docker -d -y
```

This publishes:
- `rocworks/monstermq:<version>` & `:latest`
- `rocworks/monstermq:<version>-jdk21` & `:latest-jdk21`

For releases prefer `python make.py publish --docker`: it does the same, but first checks that the
release tag is on HEAD and that the broker was built from that commit.

---

## Command-Line Options

```text
Usage: python make.py docker [options]

  -d                 Docker only (skip Maven/dashboard build, uses existing target or dist zip)
  -c, --container    Build Maven inside Docker container (GraalVM 21 builder)
  -n                 Do not publish to Docker Hub (local build only)
  -y                 Publish to Docker Hub without asking
  --testing, -t      Build testing image (rocworks/monstermq:testing)
  --clean            Clean output directories before build
```

---

## Former Helper Scripts

- `build-testing.sh` is now `python make.py docker -c -t -n` (builds `rocworks/monstermq:testing` locally in a container).
- `build-version.sh` is now `python make.py docker -c -y --clean` (clean, multi-arch build and publish to Docker Hub).
