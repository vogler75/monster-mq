#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Test a single database backend for Phase 5 message expiry
.PARAMETER Database
    The database to test: SQLite, PostgreSQL, CrateDB, or MongoDB
#>
param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("SQLite", "PostgreSQL", "CrateDB", "MongoDB")]
    [string]$Database
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$brokerDir = Join-Path $projectRoot "broker"
$testsDir = Join-Path $projectRoot "tests"

function Write-Info($msg) { Write-Host "[i] $msg" -ForegroundColor Yellow }
function Write-OK($msg) { Write-Host "[OK] $msg" -ForegroundColor Green }
function Write-Fail($msg) { Write-Host "[FAIL] $msg" -ForegroundColor Red }

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Testing $Database Backend" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Step 1: Stop any running brokers
Write-Info "Stopping any running brokers..."
Get-Process java -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 3

# Step 2: Build broker if needed
if (-not (Test-Path "$brokerDir\target\broker-1.0-SNAPSHOT-fat.jar")) {
    Write-Info "Building broker..."
    Push-Location $brokerDir
    try {
        mvn clean package -DskipTests -q
        if ($LASTEXITCODE -ne 0) {
            Write-Fail "Build failed"
            exit 1
        }
        Write-OK "Broker built"
    } finally {
        Pop-Location
    }
} else {
    Write-OK "Broker already built"
}

# Step 3: Start test databases if needed (except SQLite)
if ($Database -ne "SQLite") {
    Write-Info "Starting test databases..."
    Push-Location $projectRoot
    try {
        docker-compose -f docker-compose-test.yml up -d 2>&1 | Out-Null
        Write-OK "Databases started"
        Write-Info "Waiting 30 seconds for database initialization..."
        Start-Sleep -Seconds 30
    } finally {
        Pop-Location
    }
}

# Step 4: Configure broker
Push-Location $brokerDir
try {
    if ($Database -eq "SQLite") {
        Write-Info "Using SQLite configuration..."
        Remove-Item sqlite\*.db* -Force -ErrorAction SilentlyContinue
    } else {
        Write-Info "Loading $Database configuration..."
        Copy-Item "configs\test-$($Database.ToLower()).yaml" "config.yaml" -Force
    }
    Write-OK "Configuration loaded"
    
    # Step 5: Start broker
    Write-Info "Starting broker..."
    $brokerProc = Start-Process cmd -ArgumentList "/c", "run.bat" -PassThru -WindowStyle Minimized
    
    Write-Info "Waiting for broker to start (checking port 1883)..."
    $timeout = 60
    $elapsed = 0
    $started = $false
    
    while ($elapsed -lt $timeout) {
        $connection = netstat -ano | findstr ":1883.*LISTENING"
        if ($connection) {
            $started = $true
            break
        }
        Start-Sleep -Seconds 2
        $elapsed += 2
        Write-Host "." -NoNewline
    }
    Write-Host ""
    
    if (-not $started) {
        Write-Fail "Broker failed to start within $timeout seconds"
        
        # Check for errors
        if (Test-Path "console.txt") {
            Write-Info "Last 20 lines of console.txt:"
            Get-Content "console.txt" -Tail 20 | Write-Host
        }
        exit 1
    }
    
    Write-OK "Broker started and listening on port 1883"
    
    # Wait for schema creation
    Write-Info "Waiting 10 seconds for schema/collection creation..."
    Start-Sleep -Seconds 10
    
    # Step 6: Run tests
    Write-Info "Running Phase 5 message expiry tests..."
    Pop-Location
    Push-Location $testsDir
    
    python test_mqtt5_phase5_message_expiry.py
    $testResult = $LASTEXITCODE
    
    Pop-Location
    Push-Location $brokerDir
    
    if ($testResult -eq 0) {
        Write-OK "$Database tests PASSED!"
    } else {
        Write-Fail "$Database tests FAILED"
    }
    
    # Step 7: Stop broker
    Write-Info "Stopping broker..."
    $javaPids = Get-NetTCPConnection -LocalPort 1883 -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique
    if ($javaPids) {
        $javaPids | ForEach-Object { Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue }
    }
    Start-Sleep -Seconds 2
    
    Write-OK "Test complete"
    
    exit $testResult
    
} finally {
    Pop-Location
    # Cleanup
    Get-Process java -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
}
