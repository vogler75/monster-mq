#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Initialize databases and run Phase 5 message expiry tests
.DESCRIPTION
    This script:
    1. Starts test databases
    2. Waits for them to be fully initialized
    3. Starts the broker with each database to create schema
    4. Runs Phase 5 message expiry tests
    5. Generates test report
#>

param(
    [switch]$SkipCleanup,
    [switch]$VerboseBroker
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$brokerDir = Join-Path $projectRoot "broker"
$testsDir = Join-Path $projectRoot "tests"

# Color output
function Write-Header($msg) { Write-Host "`n======================================" -ForegroundColor Cyan; Write-Host $msg -ForegroundColor Cyan; Write-Host "======================================`n" -ForegroundColor Cyan }
function Write-OK($msg) { Write-Host "[OK] $msg" -ForegroundColor Green }
function Write-Fail($msg) { Write-Host "[FAIL] $msg" -ForegroundColor Red }
function Write-Info($msg) { Write-Host "[i] $msg" -ForegroundColor Yellow }

function Test-PortListening {
    param([int]$Port, [int]$TimeoutSeconds = 30)
    
    $endTime = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $endTime) {
        $connection = netstat -ano | findstr ":$Port"
        if ($connection -match "LISTENING") {
            return $true
        }
        Start-Sleep -Milliseconds 500
    }
    return $false
}

# Test results
$results = @{
    SQLite = @{ Success = $false; Duration = 0 }
    PostgreSQL = @{ Success = $false; Duration = 0 }
    CrateDB = @{ Success = $false; Duration = 0 }
    MongoDB = @{ Success = $false; Duration = 0 }
}

# Step 1: Build broker
Write-Header "STEP 1: BUILD BROKER"
Write-Info "Stopping any running brokers..."
Get-Process java -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

Write-Info "Building broker (mvn clean package -DskipTests)..."
Push-Location $brokerDir
try {
    $output = mvn clean package -DskipTests -q 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-OK "Broker built successfully"
    } else {
        Write-Fail "Build failed"
        Write-Host $output
        exit 1
    }
} finally {
    Pop-Location
}

# Step 2: Start databases
Write-Header "STEP 2: START TEST DATABASES"
Write-Info "Starting PostgreSQL, CrateDB, and MongoDB..."
Push-Location $projectRoot
try {
    docker-compose -f docker-compose-test.yml up -d
    if ($LASTEXITCODE -eq 0) {
        Write-OK "Databases started"
    } else {
        Write-Fail "Failed to start databases"
        exit 1
    }
} finally {
    Pop-Location
}

Write-Info "Waiting for databases to initialize (30 seconds)..."
Start-Sleep -Seconds 30

# Verify database health
$containers = @("monstermq-test-postgres", "monstermq-test-cratedb", "monstermq-test-mongodb")
foreach ($container in $containers) {
    $health = docker inspect --format='{{.State.Health.Status}}' $container 2>$null
    if ($health -eq "healthy") {
        Write-OK "$container is healthy"
    } else {
        Write-Info "$container status: $health"
    }
}

# Step 3: Test SQLite (baseline)
Write-Header "STEP 3: TEST WITH SQLITE (BASELINE)"
$startTime = Get-Date

Push-Location $brokerDir
try {
    # Ensure SQLite config
    if (Test-Path "config.yaml") {
        $content = Get-Content "config.yaml" -Raw
        if ($content -notmatch "DefaultStoreType:\s*SQLITE") {
            Write-Info "Updating config to use SQLite..."
            $content = $content -replace "DefaultStoreType:\s*\w+", "DefaultStoreType: SQLITE"
            Set-Content "config.yaml" -Value $content
        }
    }
    
    # Clean SQLite database
    Remove-Item sqlite\*.db* -Force -ErrorAction SilentlyContinue
    
    Write-Info "Starting broker with SQLite..."
    $brokerProc = Start-Process cmd -ArgumentList "/c", "run.bat" -PassThru -WindowStyle Hidden
    
    Write-Info "Waiting for broker to start (checking port 1883)..."
    $brokerStarted = Test-PortListening -Port 1883 -TimeoutSeconds 30
    
    if ($brokerStarted) {
        Write-OK "Broker started and listening on port 1883"
        
        Write-Info "Running Phase 5 test..."
        Push-Location $testsDir
        try {
            $testOutput = python test_mqtt5_phase5_message_expiry.py 2>&1 | Out-String
            if ($LASTEXITCODE -eq 0) {
                Write-OK "SQLite test PASSED"
                $results.SQLite.Success = $true
            } else {
                Write-Fail "SQLite test FAILED"
                Write-Host $testOutput
            }
        } finally {
            Pop-Location
        }
        
        # Stop broker (find java process listening on 1883)
        $javaPids = Get-NetTCPConnection -LocalPort 1883 -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique
        if ($javaPids) {
            $javaPids | ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }
        }
    } else {
        Write-Fail "Broker failed to start (port 1883 not listening)"
    }
} finally {
    Pop-Location
    Get-Process java -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
}

$results.SQLite.Duration = ((Get-Date) - $startTime).TotalSeconds

# Step 4: Test PostgreSQL
Write-Header "STEP 4: TEST WITH POSTGRESQL"
$startTime = Get-Date

Push-Location $brokerDir
try {
    Copy-Item "configs\test-postgres.yaml" "config.yaml" -Force
    Write-OK "PostgreSQL config loaded"
    
    Write-Info "Starting broker with PostgreSQL (schema will be created)..."
    $brokerProc = Start-Process cmd -ArgumentList "/c", "run.bat" -PassThru -WindowStyle Hidden
    
    Write-Info "Waiting for broker to start and create schema (checking port 1883)..."
    $brokerStarted = Test-PortListening -Port 1883 -TimeoutSeconds 30
    
    if ($brokerStarted) {
        Write-OK "Broker started and listening on port 1883"
        
        # Give extra time for schema initialization
        Write-Info "Waiting for schema to stabilize..."
        Start-Sleep -Seconds 5
        
        Write-Info "Running Phase 5 test..."
        Push-Location $testsDir
        try {
            $testOutput = python test_mqtt5_phase5_message_expiry.py 2>&1 | Out-String
            if ($LASTEXITCODE -eq 0) {
                Write-OK "PostgreSQL test PASSED"
                $results.PostgreSQL.Success = $true
            } else {
                Write-Fail "PostgreSQL test FAILED"
                Write-Host $testOutput
            }
        } finally {
            Pop-Location
        }
        
        # Stop broker
        $javaPids = Get-NetTCPConnection -LocalPort 1883 -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique
        if ($javaPids) {
            $javaPids | ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }
        }
    } else {
        Write-Fail "Broker failed to start with PostgreSQL (port 1883 not listening)"
    }
} finally {
    Pop-Location
    Get-Process java -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
}

$results.PostgreSQL.Duration = ((Get-Date) - $startTime).TotalSeconds

# Step 5: Test CrateDB
Write-Header "STEP 5: TEST WITH CRATEDB"
$startTime = Get-Date

Push-Location $brokerDir
try {
    Copy-Item "configs\test-cratedb.yaml" "config.yaml" -Force
    Write-OK "CrateDB config loaded"
    
    Write-Info "Starting broker with CrateDB (schema will be created)..."
    $brokerProc = Start-Process cmd -ArgumentList "/c", "run.bat" -PassThru -WindowStyle Hidden
    
    Write-Info "Waiting for broker to start and create schema (checking port 1883)..."
    $brokerStarted = Test-PortListening -Port 1883 -TimeoutSeconds 30
    
    if ($brokerStarted) {
        Write-OK "Broker started and listening on port 1883"
        
        Write-Info "Waiting for schema to stabilize..."
        Start-Sleep -Seconds 5
        
        Write-Info "Running Phase 5 test..."
        Push-Location $testsDir
        try {
            $testOutput = python test_mqtt5_phase5_message_expiry.py 2>&1 | Out-String
            if ($LASTEXITCODE -eq 0) {
                Write-OK "CrateDB test PASSED"
                $results.CrateDB.Success = $true
            } else {
                Write-Fail "CrateDB test FAILED"
                Write-Host $testOutput
            }
        } finally {
            Pop-Location
        }
        
        # Stop broker
        $javaPids = Get-NetTCPConnection -LocalPort 1883 -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique
        if ($javaPids) {
            $javaPids | ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }
        }
    } else {
        Write-Fail "Broker failed to start with CrateDB (port 1883 not listening)"
    }
} finally {
    Pop-Location
    Get-Process java -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
}

$results.CrateDB.Duration = ((Get-Date) - $startTime).TotalSeconds

# Step 6: Test MongoDB
Write-Header "STEP 6: TEST WITH MONGODB"
$startTime = Get-Date

Push-Location $brokerDir
try {
    Copy-Item "configs\test-mongodb.yaml" "config.yaml" -Force
    Write-OK "MongoDB config loaded"
    
    Write-Info "Starting broker with MongoDB (collections will be created)..."
    $brokerProc = Start-Process cmd -ArgumentList "/c", "run.bat" -PassThru -WindowStyle Hidden
    
    Write-Info "Waiting for broker to start and create collections (checking port 1883)..."
    $brokerStarted = Test-PortListening -Port 1883 -TimeoutSeconds 30
    
    if ($brokerStarted) {
        Write-OK "Broker started and listening on port 1883"
        
        Write-Info "Waiting for collection initialization..."
        Start-Sleep -Seconds 5
        
        Write-Info "Running Phase 5 test..."
        Push-Location $testsDir
        try {
            $testOutput = python test_mqtt5_phase5_message_expiry.py 2>&1 | Out-String
            if ($LASTEXITCODE -eq 0) {
                Write-OK "MongoDB test PASSED"
                $results.MongoDB.Success = $true
            } else {
                Write-Fail "MongoDB test FAILED"
                Write-Host $testOutput
            }
        } finally {
            Pop-Location
        }
        
        # Stop broker
        $javaPids = Get-NetTCPConnection -LocalPort 1883 -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique
        if ($javaPids) {
            $javaPids | ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }
        }
    } else {
        Write-Fail "Broker failed to start with MongoDB (port 1883 not listening)"
    }
} finally {
    Pop-Location
    Get-Process java -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
}

$results.MongoDB.Duration = ((Get-Date) - $startTime).TotalSeconds

# Generate Report
Write-Header "TEST SUMMARY"

$allPassed = $true
foreach ($db in @("SQLite", "PostgreSQL", "CrateDB", "MongoDB")) {
    $result = $results[$db]
    if ($result.Success) {
        Write-Host "$db : PASSED ($([math]::Round($result.Duration, 2)) sec)" -ForegroundColor Green
    } else {
        Write-Host "$db : FAILED ($([math]::Round($result.Duration, 2)) sec)" -ForegroundColor Red
        $allPassed = $false
    }
}

Write-Host ""

if ($allPassed) {
    Write-OK "ALL TESTS PASSED!"
    Write-Host ""
    Write-Header "PHASE 5 VALIDATION COMPLETE"
    Write-Info "SQLite: VALIDATED"
    Write-Info "PostgreSQL: VALIDATED"
    Write-Info "CrateDB: VALIDATED"
    Write-Info "MongoDB: VALIDATED"
} else {
    Write-Fail "SOME TESTS FAILED"
}

# Cleanup
if (-not $SkipCleanup) {
    Write-Host ""
    Write-Info "Cleaning up databases..."
    Push-Location $projectRoot
    try {
        docker-compose -f docker-compose-test.yml down -v
        Write-OK "Databases stopped and volumes removed"
    } finally {
        Pop-Location
    }
} else {
    Write-Info "Databases left running (use -SkipCleanup to preserve)"
}

# Restore SQLite config
Push-Location $brokerDir
try {
    if (Test-Path "config.yaml") {
        $content = Get-Content "config.yaml" -Raw
        if ($content -notmatch "DefaultStoreType:\s*SQLITE") {
            $content = $content -replace "DefaultStoreType:\s*\w+", "DefaultStoreType: SQLITE"
            Set-Content "config.yaml" -Value $content
            Write-Info "Restored SQLite configuration"
        }
    }
} finally {
    Pop-Location
}

Write-Host ""
exit $(if ($allPassed) { 0 } else { 1 })
