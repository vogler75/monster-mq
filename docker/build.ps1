<#
.SYNOPSIS
    Build script for MonsterMQ Docker Images on Windows / PowerShell.

.DESCRIPTION
    Builds local Docker images or multi-arch images with optional push to Docker Hub.
    Converted from docker/build.sh.
#>

[CmdletBinding()]
param(
    [Alias("t")]
    [switch]$Testing,

    [Alias("d")]
    [switch]$DockerOnly,

    [Alias("c")]
    [switch]$Container,

    [Alias("n")]
    [switch]$NoPublish,

    [Alias("y")]
    [switch]$Yes,

    [switch]$Clean,

    [Alias("h", "?")]
    [switch]$Help
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

if ($Help) {
    Write-Host "Usage: .\build.ps1 [options]"
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -Testing, -t      Build testing image (rocworks/monstermq:testing)"
    Write-Host "  -DockerOnly, -d   Docker only (skip Maven build)"
    Write-Host "  -Container, -c    Build Maven inside Docker container (GraalVM 21)"
    Write-Host "  -NoPublish, -n    Do not publish to Docker Hub (local image only)"
    Write-Host "  -Yes, -y          Publish to Docker Hub without asking"
    Write-Host "  -Clean            Clean output directories before build"
    Write-Host "  -Help, -h, -?     Show this help message"
    exit 0
}

$DashboardVolume = "monstermq-dashboard-build"
$BrokerVolume = "monstermq-broker-build"
$M2Volume = "monstermq-m2-cache"

$versionFile = Join-Path $PSScriptRoot "..\version.txt"
if (-not (Test-Path $versionFile)) {
    Write-Host "Error: version.txt not found at $versionFile" -ForegroundColor Red
    exit 1
}

$rawVersion = (Get-Content $versionFile -TotalCount 1).Trim()
$version = ($rawVersion -split '\+')[0].Trim()

if ($Testing) {
    Write-Host "Building Docker image for MonsterMQ (testing)..." -ForegroundColor Yellow
} else {
    Write-Host "Building Docker image for MonsterMQ v$version..." -ForegroundColor Yellow
}

if ($Clean) {
    Write-Host "Cleaning output directories..." -ForegroundColor Yellow
    $dirsToClean = @("..\broker\target", "..\dashboard\dist", "target")
    foreach ($d in $dirsToClean) {
        $p = Join-Path $PSScriptRoot $d
        if (Test-Path $p) {
            Remove-Item -Recurse -Force $p -ErrorAction SilentlyContinue
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

# Build the broker with Maven unless -DockerOnly is set
if (-not $DockerOnly) {
    $dashboardDir = $null
    $candidates = @("..\dashboard", "..\..\dashboard", "..\..\monster-mq-dashboard", "..\monster-mq-dashboard")
    foreach ($cand in $candidates) {
        $pkg = Join-Path $PSScriptRoot "$cand\package.json"
        if (Test-Path $pkg) {
            $dashboardDir = (Resolve-Path (Join-Path $PSScriptRoot $cand)).Path
            break
        }
    }

    if (-not $dashboardDir) {
        Write-Host "Error: Dashboard checkout not found in any standard location." -ForegroundColor Red
        Write-Host "Please create a symlink or clone https://github.com/vogler75/monster-mq-dashboard into a sibling folder." -ForegroundColor Yellow
        exit 1
    }

    Write-Host "Building dashboard from $dashboardDir..." -ForegroundColor Yellow
    if ($Container) {
        Write-Host "Running npm build in Docker container (Node.js LTS)..." -ForegroundColor Yellow
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
            Write-Error "Dashboard build failed!"
            exit 1
        }
    } else {
        Push-Location $dashboardDir
        try {
            & npm ci
            if ($LASTEXITCODE -ne 0) { & npm install }
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
            & npm run build
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        } finally {
            Pop-Location
        }
    }

    $brokerResources = (Resolve-Path (Join-Path $PSScriptRoot "..\broker\src\main\resources")).Path
    $targetDash = Join-Path $brokerResources "dashboard"
    if (Test-Path $targetDash) { Remove-Item -Recurse -Force $targetDash }
    New-Item -ItemType Directory -Path $targetDash -Force | Out-Null
    Copy-Item -Path "$dashboardDir\dist\*" -Destination $targetDash -Recurse -Force
    $brokersJson = Join-Path $targetDash "config\brokers.json"
    if (Test-Path $brokersJson) { Remove-Item -Force $brokersJson }
    Write-Host "[OK] Dashboard built and copied to broker resources" -ForegroundColor Green

    if ($Container) {
        Write-Host "Running mvn package in Docker volume (GraalVM 21)..." -ForegroundColor Yellow
        $brokerDir = (Resolve-Path (Join-Path $PSScriptRoot "..\broker")).Path
        $mavenBuilderImage = "monstermq-maven-builder:graalvm21"
        $imageExists = (& docker image ls -q $mavenBuilderImage)
        if (-not $imageExists) {
            Write-Host "Building Maven builder image (first time only)..." -ForegroundColor Yellow
            & docker build -t $mavenBuilderImage -f (Join-Path $PSScriptRoot "Dockerfile.maven-builder") $PSScriptRoot
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        }

        & docker run --rm `
            -v "${brokerDir}:/build" `
            -v "${BrokerVolume}:/build/target" `
            -v "${M2Volume}:/root/.m2" `
            -e "MAVEN_OPTS=-XX:MaxRAMPercentage=75.0 -Duser.home=/root" `
            -w /build `
            $mavenBuilderImage `
            mvn package -DskipTests
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

        Write-Host "Copying JAR and dependencies from build volume..." -ForegroundColor Yellow
        $tempCid = (& docker create -v "${BrokerVolume}:/build/target" $mavenBuilderImage sh).Trim()
        $targetLocal = Join-Path $PSScriptRoot "target"
        if (Test-Path $targetLocal) { Remove-Item -Recurse -Force $targetLocal }
        New-Item -ItemType Directory -Path $targetLocal -Force | Out-Null
        & docker cp "${tempCid}:/build/target/broker-1.0-SNAPSHOT.jar" "$targetLocal/"
        & docker cp "${tempCid}:/build/target/dependencies" "$targetLocal/"
        & docker rm $tempCid 2>$null | Out-Null
    } else {
        Write-Host "Running mvn package in broker directory..." -ForegroundColor Yellow
        Push-Location (Join-Path $PSScriptRoot "..\broker")
        try {
            $mvnCmd = if (Get-Command "mvn.cmd" -ErrorAction SilentlyContinue) { "mvn.cmd" } else { "mvn" }
            & $mvnCmd package -DskipTests
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        } finally {
            Pop-Location
        }
    }
} else {
    Write-Host "Skipping Maven build (-DockerOnly flag set)" -ForegroundColor Yellow
}

# Copy files from broker/target or dist/ to docker/target
$dockerTarget = Join-Path $PSScriptRoot "target"
if ($DockerOnly -or -not $Container) {
    if (Test-Path $dockerTarget) { Remove-Item -Recurse -Force $dockerTarget }
    New-Item -ItemType Directory -Path $dockerTarget -Force | Out-Null

    $brokerJar = Join-Path $PSScriptRoot "..\broker\target\broker-1.0-SNAPSHOT.jar"
    $brokerDeps = Join-Path $PSScriptRoot "..\broker\target\dependencies"
    $distZip = Join-Path $PSScriptRoot "..\dist\monstermq-broker-$version.zip"

    if ((Test-Path $brokerJar) -and (Test-Path $brokerDeps)) {
        Write-Host "Copying JAR and dependencies from broker/target..." -ForegroundColor Yellow
        Copy-Item -Path $brokerJar -Destination "$dockerTarget\broker-1.0-SNAPSHOT.jar" -Force
        Copy-Item -Path $brokerDeps -Destination "$dockerTarget\dependencies" -Recurse -Force
    } elseif (Test-Path $distZip) {
        Write-Host "Extracting JAR and dependencies from dist/monstermq-broker-$version.zip..." -ForegroundColor Yellow
        Expand-Archive -Path $distZip -DestinationPath "$dockerTarget\extracted" -Force
        $extractedBase = Join-Path $dockerTarget "extracted\monstermq-broker-$version"
        Copy-Item -Path "$extractedBase\monstermq-broker-$version.jar" -Destination "$dockerTarget\broker-1.0-SNAPSHOT.jar" -Force
        Copy-Item -Path "$extractedBase\dependencies" -Destination "$dockerTarget\dependencies" -Recurse -Force
        Remove-Item -Recurse -Force "$dockerTarget\extracted"
    } else {
        Write-Host "Error: Neither broker/target nor dist/monstermq-broker-$version.zip found!" -ForegroundColor Red
        Write-Host "Please build the broker first with .\build.ps1 -Broker (or .\build.ps1 -All)" -ForegroundColor Red
        exit 1
    }
}

$isDev = $version.EndsWith("-dev") -or $version.EndsWith("-beta") -or $version.EndsWith("-alpha")

Write-Host "Building Docker image (local, native platform only)..." -ForegroundColor Yellow
$tags = @("-t", "rocworks/monstermq:$version")
if ($Testing) {
    $tags = @("-t", "rocworks/monstermq:testing")
} elseif (-not $isDev) {
    $tags += @("-t", "rocworks/monstermq:latest")
}

& docker build @tags .
if ($LASTEXITCODE -ne 0) {
    Write-Error "Docker build failed"
    exit $LASTEXITCODE
}

Write-Host "[OK] Docker image built successfully (native platform)" -ForegroundColor Green
if ($Testing) {
    Write-Host "  - rocworks/monstermq:testing" -ForegroundColor Green
} else {
    Write-Host "  - rocworks/monstermq:$version" -ForegroundColor Green
    if (-not $isDev) {
        Write-Host "  - rocworks/monstermq:latest" -ForegroundColor Green
    }
}

# Build compat image (GraalVM JDK 21) if not testing
if (-not $Testing) {
    Write-Host "Building compat Docker image (GraalVM JDK 21)..." -ForegroundColor Yellow
    $compatTags = @("-t", "rocworks/monstermq:${version}-jdk21")
    if (-not $isDev) {
        $compatTags += @("-t", "rocworks/monstermq:latest-jdk21")
    }

    & docker build --build-arg BASE_IMAGE=ghcr.io/graalvm/jdk-community:21 @compatTags .
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Compat Docker build failed"
        exit $LASTEXITCODE
    }
    Write-Host "[OK] Compat Docker image built successfully" -ForegroundColor Green
    Write-Host "  - rocworks/monstermq:${version}-jdk21" -ForegroundColor Green
    if (-not $isDev) {
        Write-Host "  - rocworks/monstermq:latest-jdk21" -ForegroundColor Green
    }
}

Write-Host "Cleaning up temporary files..." -ForegroundColor Yellow
if (Test-Path $dockerTarget) {
    Remove-Item -Recurse -Force $dockerTarget -ErrorAction SilentlyContinue
}
Write-Host "[OK] Cleanup complete" -ForegroundColor Green
