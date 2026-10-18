<#
.SYNOPSIS
    Build script for MonsterMQ Core, Setup Executables, and Docker Images on Windows / PowerShell.

.DESCRIPTION
    Builds the standalone Java broker bundle (zip), cross-platform Go setup executables,
    and local Docker images. Converted from build.sh for Windows PowerShell and PowerShell Core.

.PARAMETER All
    Build all artifacts locally (broker zip, setup executables, docker image).

.PARAMETER Broker
    Build standalone Java broker bundle (zip).

.PARAMETER Setup
    Build cross-platform Go setup executables (setup.exe, setup-mac, setup-linux).

.PARAMETER Docker
    Build local Docker image (native platform).

.PARAMETER Testing
    Build local Docker image tagged as rocworks/monstermq:testing.

.PARAMETER Container
    Build dashboard (and broker) inside Docker containers (Node.js LTS / GraalVM 21).

.PARAMETER Clean
    Clean output build directories.

.PARAMETER Help
    Show this help message.

.EXAMPLE
    .\build.ps1 -All
    Build all artifacts locally.

.EXAMPLE
    .\build.ps1 -Broker
    Build Java broker zip bundle only.

.EXAMPLE
    .\build.ps1 -Setup
    Build cross-platform Go setup executables only.

.EXAMPLE
    .\build.ps1 -Docker
    Build local Docker image only.

.EXAMPLE
    .\build.ps1 -Clean
    Clean output build directories.
#>

[CmdletBinding()]
param(
    [Alias("a")]
    [switch]$All,

    [Alias("b")]
    [switch]$Broker,

    [Alias("s", "installer")]
    [switch]$Setup,

    [Alias("d")]
    [switch]$Docker,

    [Alias("t")]
    [switch]$Testing,

    [Alias("c")]
    [switch]$Container,

    [switch]$Clean,

    [Alias("h", "?")]
    [switch]$Help
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

function Show-Usage {
    Write-Host "Usage: .\build.ps1 [options]"
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -All, --all            Build all artifacts (broker zip, setup executables, docker image)"
    Write-Host "  -Broker, --broker      Build standalone Java broker bundle (zip)"
    Write-Host "  -Setup, --setup        Build cross-platform Go setup executables (setup.exe, setup-mac, setup-linux)"
    Write-Host "  -Docker, --docker      Build local Docker image (native platform)"
    Write-Host "  -Testing, -t           Build local Docker image tagged as rocworks/monstermq:testing"
    Write-Host "  -Container, -c         Build dashboard (and broker) inside Docker containers (Node.js LTS / GraalVM 21)"
    Write-Host "  -Clean, --clean        Clean output build directories"
    Write-Host "  -Help, -h, --help, -?  Show this help message"
    Write-Host ""
}

if ($Help -or ($PSBoundParameters.Count -eq 0 -and $args.Count -eq 0)) {
    Show-Usage
    exit 0
}

$versionFile = Join-Path $PSScriptRoot "version.txt"
if (-not (Test-Path $versionFile)) {
    Write-Host "Error: version.txt not found" -ForegroundColor Red
    exit 1
}

$rawVersion = (Get-Content $versionFile -TotalCount 1).Trim()
$version = ($rawVersion -split '\+')[0].Trim()

$buildBroker = $false
$buildSetup = $false
$buildDocker = $false
$dockerTesting = $false
$containerBuild = $Container.IsPresent
$cleanMode = $Clean.IsPresent
$explicitTarget = $false

if ($All) {
    $buildBroker = $true
    $buildSetup = $true
    $buildDocker = $true
    $explicitTarget = $true
}

if ($Broker) {
    $buildBroker = $true
    $explicitTarget = $true
}

if ($Setup) {
    $buildSetup = $true
    $explicitTarget = $true
}

if ($Docker) {
    $buildDocker = $true
    $explicitTarget = $true
}

if ($Testing) {
    $buildDocker = $true
    $dockerTesting = $true
    $explicitTarget = $true
}

# Default to building all if no target or clean specified
if (-not $explicitTarget -and -not $cleanMode) {
    $buildBroker = $true
    $buildSetup = $true
    $buildDocker = $true
}

$DashboardVolume = "monstermq-dashboard-build"
$BrokerVolume = "monstermq-broker-build"
$M2Volume = "monstermq-m2-cache"

Write-Host "=== MonsterMQ Main Build Pipeline (v$version) ===" -ForegroundColor Green

# Clean output build directories if requested
if ($cleanMode) {
    Write-Host "Cleaning output build directories..." -ForegroundColor Yellow

    $dirsToClean = @(
        "broker/target",
        "dist",
        "docker/target"
    )
    foreach ($d in $dirsToClean) {
        $p = Join-Path $PSScriptRoot $d
        if (Test-Path $p) {
            Remove-Item -Recurse -Force $p -ErrorAction SilentlyContinue
        }
    }

    # Clean dashboard/dist if found
    $candidateDashboards = @("dashboard", "../dashboard", "../../dashboard", "../monster-mq-dashboard", "../../monster-mq-dashboard")
    foreach ($cand in $candidateDashboards) {
        $candDist = Join-Path $PSScriptRoot "$cand/dist"
        if (Test-Path $candDist) {
            Remove-Item -Recurse -Force $candDist -ErrorAction SilentlyContinue
            break
        }
    }

    if (Get-Command docker -ErrorAction SilentlyContinue) {
        foreach ($vol in @($DashboardVolume, $BrokerVolume)) {
            $existing = (& docker volume ls -q --filter "name=^$vol$" 2>&1)
            if ($existing -eq $vol) {
                & docker volume rm $vol 2>&1 | Out-Null
            }
        }
    }

    Write-Host "[OK] Clean complete" -ForegroundColor Green
}

if (-not $buildBroker -and -not $buildSetup -and -not $buildDocker) {
    Write-Host "No build targets specified. Clean operation finished." -ForegroundColor Green
    exit 0
}

# 1. Build Broker Zip Bundle
if ($buildBroker) {
    Write-Host "[1/3] Building Web Dashboard and Java Broker..." -ForegroundColor Green

    # Resolve dashboard directory
    $dashboardDir = $null
    $candidates = @("dashboard", "../dashboard", "../../dashboard", "../monster-mq-dashboard", "../../monster-mq-dashboard")
    foreach ($candidate in $candidates) {
        $pkgPath = Join-Path $PSScriptRoot "$candidate/package.json"
        if (Test-Path $pkgPath) {
            $dashboardDir = (Resolve-Path (Join-Path $PSScriptRoot $candidate)).Path
            break
        }
    }

    if (-not $dashboardDir) {
        Write-Host "Error: Dashboard checkout not found!" -ForegroundColor Red
        Write-Host "Please create a symlink or clone https://github.com/vogler75/monster-mq-dashboard into a sibling folder:" -ForegroundColor Red
        Write-Host "  cd `"$PSScriptRoot`"" -ForegroundColor Yellow
        Write-Host "  # Or clone: git clone https://github.com/vogler75/monster-mq-dashboard.git ../dashboard" -ForegroundColor Yellow
        exit 1
    }

    if ($containerBuild) {
        Write-Host "Building web dashboard frontend in Docker container (Node.js LTS) from $dashboardDir..." -ForegroundColor Yellow
        $nodeModules = Join-Path $dashboardDir "node_modules"
        if (-not (Test-Path $nodeModules)) {
            New-Item -ItemType Directory -Path $nodeModules -Force | Out-Null
        }

        & docker run --rm `
            -v "${dashboardDir}:/app" `
            -v "${DashboardVolume}:/app/node_modules" `
            -w /app `
            node:24-slim `
            bash -c "set -e && npm ci && npm run build"
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Container dashboard build failed with exit code $LASTEXITCODE"
            exit $LASTEXITCODE
        }
    } else {
        Write-Host "Building web dashboard frontend from $dashboardDir..." -ForegroundColor Yellow
        Push-Location $dashboardDir
        try {
            & npm ci
            if ($LASTEXITCODE -ne 0) {
                Write-Host "npm ci failed, falling back to npm install..." -ForegroundColor Yellow
                & npm install
            }
            if ($LASTEXITCODE -ne 0) {
                Write-Error "npm install failed with exit code $LASTEXITCODE"
                exit $LASTEXITCODE
            }
            & npm run build
            if ($LASTEXITCODE -ne 0) {
                Write-Error "npm run build failed with exit code $LASTEXITCODE"
                exit $LASTEXITCODE
            }
        } finally {
            Pop-Location
        }
    }

    Write-Host "Copying web dashboard to broker resources..." -ForegroundColor Yellow
    $destDashboard = Join-Path $PSScriptRoot "broker/src/main/resources/dashboard"
    if (Test-Path $destDashboard) {
        Remove-Item -Recurse -Force $destDashboard
    }
    New-Item -ItemType Directory -Path $destDashboard -Force | Out-Null

    $srcDist = Join-Path $dashboardDir "dist"
    if (-not (Test-Path $srcDist)) {
        Write-Error "Error: $srcDist not found after dashboard build."
        exit 1
    }
    Copy-Item -Path "$srcDist\*" -Destination $destDashboard -Recurse -Force

    $brokersJson = Join-Path $destDashboard "config/brokers.json"
    if (Test-Path $brokersJson) {
        Remove-Item -Force $brokersJson
    }

    Write-Host "Syncing version.txt to broker resources..." -ForegroundColor Yellow
    $destResources = Join-Path $PSScriptRoot "broker/src/main/resources"
    if (-not (Test-Path $destResources)) {
        New-Item -ItemType Directory -Path $destResources -Force | Out-Null
    }
    Copy-Item -Path $versionFile -Destination (Join-Path $destResources "version.txt") -Force

    if ($containerBuild) {
        Write-Host "Compiling Java broker with Maven in Docker container (GraalVM 21)..." -ForegroundColor Yellow
        $mavenBuilderImage = "monstermq-maven-builder:graalvm21"

        $inspectOut = (& docker image inspect $mavenBuilderImage 2>&1)
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Building Maven builder image (first time only)..." -ForegroundColor Yellow
            & docker build -t $mavenBuilderImage -f (Join-Path $PSScriptRoot "docker/Dockerfile.maven-builder") (Join-Path $PSScriptRoot "docker")
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        }

        $brokerDir = (Resolve-Path (Join-Path $PSScriptRoot "broker")).Path
        $targetDir = Join-Path $brokerDir "target"
        if (-not (Test-Path $targetDir)) {
            New-Item -ItemType Directory -Path $targetDir -Force | Out-Null
        }

        & docker run --rm `
            -v "${brokerDir}:/build" `
            -v "${BrokerVolume}:/build/target" `
            -v "${M2Volume}:/root/.m2" `
            -e "MAVEN_OPTS=-XX:MaxRAMPercentage=75.0 -Duser.home=/root" `
            -w /build `
            $mavenBuilderImage `
            mvn package -DskipTests
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Container Maven build failed with exit code $LASTEXITCODE"
            exit $LASTEXITCODE
        }

        $tempCid = (& docker create -v "${BrokerVolume}:/build/target" $mavenBuilderImage sh).Trim()
        if (Test-Path $targetDir) {
            Remove-Item -Recurse -Force $targetDir
        }
        New-Item -ItemType Directory -Path $targetDir -Force | Out-Null
        & docker cp "${tempCid}:/build/target/broker-1.0-SNAPSHOT.jar" "$targetDir/"
        & docker cp "${tempCid}:/build/target/dependencies" "$targetDir/"
        & docker rm $tempCid 2>$null | Out-Null
    } else {
        Write-Host "Compiling Java broker with Maven..." -ForegroundColor Yellow
        Push-Location (Join-Path $PSScriptRoot "broker")
        try {
            $mvnCmd = if (Get-Command "mvn.cmd" -ErrorAction SilentlyContinue) { "mvn.cmd" } else { "mvn" }
            & $mvnCmd package -DskipTests
            if ($LASTEXITCODE -ne 0) {
                Write-Error "Maven compilation failed with exit code $LASTEXITCODE"
                exit $LASTEXITCODE
            }
        } finally {
            Pop-Location
        }
    }

    Write-Host "Packaging broker zip bundle..." -ForegroundColor Yellow
    $distDir = Join-Path $PSScriptRoot "dist"
    if (-not (Test-Path $distDir)) {
        New-Item -ItemType Directory -Path $distDir -Force | Out-Null
    }
    $bundleName = "monstermq-broker-$version"
    $stageDir = Join-Path $distDir $bundleName
    if (Test-Path $stageDir) {
        Remove-Item -Recurse -Force $stageDir
    }
    New-Item -ItemType Directory -Path $stageDir -Force | Out-Null

    Copy-Item -Path (Join-Path $PSScriptRoot "broker/target/broker-1.0-SNAPSHOT.jar") -Destination (Join-Path $stageDir "monstermq-broker-$version.jar") -Force
    Copy-Item -Path (Join-Path $PSScriptRoot "broker/target/dependencies") -Destination (Join-Path $stageDir "dependencies") -Recurse -Force

    if (Test-Path (Join-Path $PSScriptRoot "broker/example-config.yaml")) {
        Copy-Item -Path (Join-Path $PSScriptRoot "broker/example-config.yaml") -Destination (Join-Path $stageDir "config.yaml") -Force
    } elseif (Test-Path (Join-Path $PSScriptRoot "broker/example-config-sqlite.yaml")) {
        Copy-Item -Path (Join-Path $PSScriptRoot "broker/example-config-sqlite.yaml") -Destination (Join-Path $stageDir "config.yaml") -Force
    }

    New-Item -ItemType Directory -Path (Join-Path $stageDir "sqlite") -Force | Out-Null
    New-Item -ItemType Directory -Path (Join-Path $stageDir "log") -Force | Out-Null

    if (Test-Path (Join-Path $PSScriptRoot "broker/yaml-json-schema.json")) {
        Copy-Item -Path (Join-Path $PSScriptRoot "broker/yaml-json-schema.json") -Destination (Join-Path $stageDir "yaml-json-schema.json") -Force
    }

    if (Test-Path (Join-Path $PSScriptRoot "broker/logging.properties")) {
        Copy-Item -Path (Join-Path $PSScriptRoot "broker/logging.properties") -Destination (Join-Path $stageDir "logging.properties") -Force
    }

    # Generate run.sh
    $runShContent = @'
#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
JAR_FILE=$(ls monstermq-broker-*.jar 2>/dev/null | head -n 1)
exec java -classpath "${JAR_FILE}:dependencies/*" at.rocworks.MonsterKt "$@"
'@
    [System.IO.File]::WriteAllText((Join-Path $stageDir "run.sh"), $runShContent.Replace("`r`n", "`n"), [System.Text.UTF8Encoding]::new($false))

    # Generate run.bat
    $runBatContent = @'
@echo off
set SCRIPT_DIR=%~dp0
cd /d "%SCRIPT_DIR%"
set "JAR_FILE="
for %%f in ("monstermq-broker-*.jar") do set "JAR_FILE=%%f"
if not defined JAR_FILE (
    echo Error: monstermq-broker-*.jar not found.
    exit /b 1
)
java -classpath "%JAR_FILE%;dependencies/*" at.rocworks.MonsterKt %*
'@
    [System.IO.File]::WriteAllText((Join-Path $stageDir "run.bat"), $runBatContent.Replace("`r`n", "`r`n"), [System.Text.Encoding]::ASCII)

    # Generate run.ps1
    $runPs1Content = @'
$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot
$jarFile = (Get-ChildItem -Filter "monstermq-broker-*.jar" | Select-Object -First 1).Name
if (-not $jarFile) {
    Write-Error "Error: monstermq-broker-*.jar not found."
    exit 1
}
& java -classpath "$jarFile;dependencies/*" at.rocworks.MonsterKt @args
exit $LASTEXITCODE
'@
    [System.IO.File]::WriteAllText((Join-Path $stageDir "run.ps1"), $runPs1Content, [System.Text.UTF8Encoding]::new($false))

    # Generate README.md
    $readmeContent = @"
# MonsterMQ Broker v$version

## How to Run

Requirements: Java 21+

Linux / macOS:
  ./run.sh

Windows:
  .\run.ps1
  # or: run.bat

Web Dashboard & GraphQL:
  http://localhost:4000/

MQTT Broker:
  mqtt://localhost:1883
"@
    [System.IO.File]::WriteAllText((Join-Path $stageDir "README.md"), $readmeContent, [System.Text.UTF8Encoding]::new($false))

    # Generate AGENTS.md
    $agentsContent = @'
# MonsterMQ Agent Guide

This file provides guidance to AI coding and DevOps agents working with this MonsterMQ broker instance.

## What MonsterMQ Is

MonsterMQ is a high-performance, industrial-grade MQTT broker (MQTT 3.1.1 and MQTT 5.0) built on Vert.x with multi-database persistence, embedded web dashboard, GraphQL API, Model Context Protocol (MCP) server for AI tools, and industrial protocol bridging (OPC UA, PLC4X, SparkplugB, WinCC OA/Unified, Kafka, NATS, Redis).

## Quick Start & Running

Requirements: **Java 21+** (`java -version`)

- **Linux / macOS**: `./run.sh` (or `./run.sh -config config.yaml`)
- **Windows**: `.\run.ps1` (or `run.bat`, or `.\run.ps1 -config config.yaml`)

## Endpoints

| Service | Port / Path | Description |
| :--- | :--- | :--- |
| **MQTT TCP** | `tcp://localhost:1883` | Standard plain MQTT protocol |
| **MQTT WebSocket** | `ws://localhost:1884` | MQTT over WebSocket for web apps |
| **GraphQL & Dashboard** | `http://localhost:4000/` | Web Dashboard & `/graphql` API endpoint |
| **MCP Server** | `http://localhost:3000/` | Model Context Protocol for AI tool integration |

## Configuration & Schema (`yaml-json-schema.json`)

The broker configuration is defined in `config.yaml`.
The draft-07 JSON schema is provided in `yaml-json-schema.json` in this directory.

### Key Configuration Concepts:
- **`DefaultStoreType`**: Default database backend (`SQLITE`, `POSTGRES`, `MONGODB`, `CRATEDB`, `MEMORY`). Defaults to `SQLITE`.
- **`SQLite.Path`**: Directory for SQLite database files (defaults to `"sqlite"`).
- **`Features`**: Map of feature flags (`OpcUa`, `MqttClient`, `FlowEngine`, `Agents`, `GenAi`, `Mcp`, `Plc4x`, `SparkplugB`, etc.).
- **`UserManagement`**: Authentication and ACL rules (`Enabled: false` by default for quickstart).
- **`GraphQL`**: Controls web dashboard and GraphQL API (`Port: 4000`, `Path: /graphql`).
- **`MCP`**: Controls Model Context Protocol server (`Port: 3000`).

## Database Backends

- **SQLite (Default)**: Zero external setup. Database files are stored locally under `./sqlite/`.
- **PostgreSQL**: Set `DefaultStoreType: POSTGRES` and configure connection details under `Postgres: { Url, User, Pass }`.
- **MongoDB**: Set `DefaultStoreType: MONGODB` and configure `MongoDB: { Url, Database }`.

## CLI Arguments

- `-config <file>`: Specify custom configuration YAML (defaults to `config.yaml` or `GATEWAY_CONFIG` env var).
- `-cluster`: Enable Hazelcast cluster mode.
- `-log <level>`: Set log level (`INFO`, `FINE`, `FINER`, `FINEST`, `ALL`).
- `-workerPoolSize <num>`: Vert.x worker thread pool size.
'@
    [System.IO.File]::WriteAllText((Join-Path $stageDir "AGENTS.md"), $agentsContent, [System.Text.UTF8Encoding]::new($false))

    $zipPath = Join-Path $distDir "$bundleName.zip"
    if (Test-Path $zipPath) {
        Remove-Item -Force $zipPath
    }

    # Use tar.exe if available for standard forward-slash zip entries, otherwise Compress-Archive
    if (Get-Command "tar.exe" -ErrorAction SilentlyContinue) {
        Push-Location $distDir
        try {
            & tar.exe -a -c -f "$bundleName.zip" "$bundleName"
            if ($LASTEXITCODE -ne 0) {
                Write-Host "tar.exe failed, falling back to Compress-Archive..." -ForegroundColor Yellow
                Compress-Archive -Path $stageDir -DestinationPath $zipPath -Force
            }
        } finally {
            Pop-Location
        }
    } else {
        Compress-Archive -Path $stageDir -DestinationPath $zipPath -Force
    }

    Remove-Item -Recurse -Force $stageDir
    Write-Host "[OK] Broker bundle created: $zipPath" -ForegroundColor Green
}

# 2. Build Setup Executables (setup.exe, setup-mac, setup-linux)
if ($buildSetup) {
    Write-Host "[2/3] Building Cross-Platform Setup Executables (Go)..." -ForegroundColor Green
    $setupBuildPs1 = Join-Path $PSScriptRoot "setup/build.ps1"
    if (Test-Path $setupBuildPs1) {
        & $setupBuildPs1
    } else {
        Push-Location (Join-Path $PSScriptRoot "setup")
        try {
            & bash ./build.sh
        } finally {
            Pop-Location
        }
    }
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Setup build failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }

    $distDir = Join-Path $PSScriptRoot "dist"
    if (-not (Test-Path $distDir)) {
        New-Item -ItemType Directory -Path $distDir -Force | Out-Null
    }

    $setupBin = Join-Path $PSScriptRoot "setup/bin"
    $filesToCopy = @(
        "setup.exe",
        "setup-win-arm64.exe",
        "setup-mac-arm64",
        "setup-mac-x64",
        "setup-linux-amd64",
        "setup-linux-arm64"
    )
    foreach ($f in $filesToCopy) {
        $src = Join-Path $setupBin $f
        if (Test-Path $src) {
            Copy-Item -Path $src -Destination (Join-Path $distDir $f) -Force
        }
    }
    Write-Host "[OK] Setup executables copied to dist/" -ForegroundColor Green
}

# 3. Build Docker Image (Local)
if ($buildDocker) {
    if ($dockerTesting) {
        Write-Host "[3/3] Building Local Docker Image (testing)..." -ForegroundColor Green
    } else {
        Write-Host "[3/3] Building Local Docker Image..." -ForegroundColor Green
    }

    $dockerBuildPs1 = Join-Path $PSScriptRoot "docker/build.ps1"
    if (Test-Path $dockerBuildPs1) {
        $dockerParams = @{ NoPublish = $true }
        if ($buildBroker) {
            $dockerParams["DockerOnly"] = $true
        } elseif ($containerBuild) {
            $dockerParams["Container"] = $true
        }
        if ($dockerTesting) {
            $dockerParams["Testing"] = $true
        }
        & $dockerBuildPs1 @dockerParams
    } else {
        $dockerBuildFlags = @("-n")
        if ($buildBroker) {
            $dockerBuildFlags += "-d"
        } elseif ($containerBuild) {
            $dockerBuildFlags += "-c"
        }
        if ($dockerTesting) {
            $dockerBuildFlags += "--testing"
        }

        Push-Location (Join-Path $PSScriptRoot "docker")
        try {
            & bash ./build.sh @dockerBuildFlags
        } finally {
            Pop-Location
        }
    }
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Docker build failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }

    Write-Host "[OK] Local Docker image built" -ForegroundColor Green
}

Write-Host "=== Build Complete ===" -ForegroundColor Green
