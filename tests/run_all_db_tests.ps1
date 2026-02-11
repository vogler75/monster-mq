#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Test MQTT v5 Phase 5 Message Expiry with all database backends
.DESCRIPTION
    This script:
    1. Starts test databases using docker-compose-test.yml
    2. Runs MonsterMQ broker with each database configuration
    3. Executes Phase 5 message expiry tests for each backend
    4. Generates a comprehensive test report
#>

param(
    [switch]$SkipDockerStart,
    [switch]$KeepDatabases,
    [string]$TestScript = "test_mqtt5_phase5_message_expiry.py"
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$brokerDir = Join-Path $projectRoot "broker"
$testsDir = Join-Path $projectRoot "tests"

# Color output functions
function Write-Header($message) {
    Write-Host "`n======================================================================" -ForegroundColor Cyan
    Write-Host $message -ForegroundColor Cyan
    Write-Host "======================================================================`n" -ForegroundColor Cyan
}

function Write-SuccessMsg($message) {
    Write-Host "[OK] $message" -ForegroundColor Green
}

function Write-ErrorMsg($message) {
    Write-Host "[X] $message" -ForegroundColor Red
}

function Write-Info($message) {
    Write-Host "[i] $message" -ForegroundColor Yellow
}

# Test results tracking
$testResults = @{
    PostgreSQL = @{ Success = $false; Duration = 0; Output = "" }
    CrateDB = @{ Success = $false; Duration = 0; Output = "" }
    MongoDB = @{ Success = $false; Duration = 0; Output = "" }
}

# Start databases
if (-not $SkipDockerStart) {
    Write-Header "STARTING TEST DATABASES"
    Write-Info "Starting PostgreSQL, CrateDB, and MongoDB..."
    
    Push-Location $projectRoot
    try {
        docker-compose -f docker-compose-test.yml up -d
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to start databases"
        }
        Write-SuccessMsg "Databases started"
        
        Write-Info "Waiting for databases to be ready..."
        Start-Sleep -Seconds 15
        
        # Check health status
        $healthChecks = @("monstermq-test-postgres", "monstermq-test-cratedb", "monstermq-test-mongodb")
        foreach ($container in $healthChecks) {
            $health = docker inspect --format='{{.State.Health.Status}}' $container 2>$null
            if ($health -eq "healthy") {
                Write-SuccessMsg "$container is healthy"
            } else {
                Write-Info "$container status: $health (may still be starting)"
            }
        }
        
        Start-Sleep -Seconds 10
    } finally {
        Pop-Location
    }
} else {
    Write-Info "Skipping database startup (using existing containers)"
}

# Test function for a specific database
function Test-Database {
    param(
        [string]$DbName,
        [string]$ConfigFile
    )
    
    Write-Header "TESTING: $DbName"
    
    $startTime = Get-Date
    $brokerProcess = $null
    $testOutput = ""
    
    try {
        # Copy config file
        $configPath = Join-Path $brokerDir "configs\$ConfigFile"
        $targetConfig = Join-Path $brokerDir "config.yaml"
        
        if (-not (Test-Path $configPath)) {
            Write-ErrorMsg "Config file not found: $configPath"
            return $false
        }
        
        Copy-Item $configPath $targetConfig -Force
        Write-Info "Using config: $ConfigFile"
        
        # Stop any existing broker
        Write-Info "Stopping any existing broker..."
        Get-Process java -ErrorAction SilentlyContinue | Where-Object {
            $_.MainWindowTitle -like "*monster*" -or $_.CommandLine -like "*broker*"
        } | Stop-Process -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 2
        
        # Start broker
        Write-Info "Starting MonsterMQ broker with $DbName..."
        Push-Location $brokerDir
        try {
            $brokerProcess = Start-Process -FilePath "cmd.exe" -ArgumentList "/c", "run.bat" -PassThru -WindowStyle Hidden
            Start-Sleep -Seconds 8  # Wait for broker to initialize
            
            if ($brokerProcess.HasExited) {
                Write-ErrorMsg "Broker failed to start"
                return $false
            }
            
            Write-SuccessMsg "Broker started (PID: $($brokerProcess.Id))"
        } finally {
            Pop-Location
        }
        
        # Run test
        Write-Info "Running Phase 5 message expiry test..."
        Push-Location $testsDir
        try {
            $testOutput = & python $TestScript 2>&1 | Out-String
            $testSuccess = $LASTEXITCODE -eq 0
            
            Write-Host $testOutput
            
            if ($testSuccess) {
                Write-SuccessMsg "$DbName test PASSED"
            } else {
                Write-ErrorMsg "$DbName test FAILED"
            }
            
            return $testSuccess
        } finally {
            Pop-Location
        }
        
    } catch {
        Write-ErrorMsg "Error testing ${DbName}: $_"
        $testOutput = $_.Exception.Message
        return $false
        
    } finally {
        # Stop broker
        if ($brokerProcess -and -not $brokerProcess.HasExited) {
            Write-Info "Stopping broker..."
            Stop-Process -Id $brokerProcess.Id -Force -ErrorAction SilentlyContinue
            Start-Sleep -Seconds 2
        }
        
        # Kill any remaining Java processes
        Get-Process java -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
        
        $duration = (Get-Date) - $startTime
        $testResults[$DbName].Duration = $duration.TotalSeconds
        $testResults[$DbName].Output = $testOutput
    }
}

# Run tests for each database
Write-Header "MQTT V5.0 PHASE 5 - DATABASE BACKEND TESTING"
Write-Info "Testing Message Expiry with PostgreSQL, CrateDB, and MongoDB"

$testResults.PostgreSQL.Success = Test-Database -DbName "PostgreSQL" -ConfigFile "test-postgres.yaml"
Start-Sleep -Seconds 3

$testResults.CrateDB.Success = Test-Database -DbName "CrateDB" -ConfigFile "test-cratedb.yaml"
Start-Sleep -Seconds 3

$testResults.MongoDB.Success = Test-Database -DbName "MongoDB" -ConfigFile "test-mongodb.yaml"

# Generate summary report
Write-Header "TEST SUMMARY"

$allPassed = $true
foreach ($db in $testResults.Keys) {
    $result = $testResults[$db]
    $status = if ($result.Success) { "PASSED" } else { "FAILED" }
    $color = if ($result.Success) { "Green" } else { "Red" }
    $duration = [math]::Round($result.Duration, 2)
    
    Write-Host ('{0} : {1} ({2} sec)' -f $db, $status, $duration) -ForegroundColor $color
    
    if (-not $result.Success) {
        $allPassed = $false
    }
}

Write-Host ""

if ($allPassed) {
    Write-SuccessMsg "ALL DATABASE TESTS PASSED!"
    Write-Host ""
    Write-Header "PHASE 5 VALIDATION COMPLETE"
    Write-Info "PostgreSQL: VALIDATED"
    Write-Info "CrateDB: VALIDATED"
    Write-Info "MongoDB: VALIDATED"
} else {
    Write-ErrorMsg "SOME TESTS FAILED - Review output above"
}

# Cleanup
if (-not $KeepDatabases) {
    Write-Host ""
    Write-Info "Stopping test databases..."
    Push-Location $projectRoot
    try {
        docker-compose -f docker-compose-test.yml down -v
        Write-SuccessMsg "Databases stopped and volumes removed"
    } finally {
        Pop-Location
    }
} else {
    Write-Info "Keeping databases running (use -KeepDatabases to preserve)"
    Write-Info "To stop: docker-compose -f docker-compose-test.yml down -v"
}

Write-Host ""

exit $(if ($allPassed) { 0 } else { 1 })
