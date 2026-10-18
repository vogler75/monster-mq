<#
.SYNOPSIS
    Cross-compiler for MonsterMQ Setup Executables on Windows / PowerShell.

.DESCRIPTION
    Builds setup binaries for Windows (amd64, arm64), macOS (arm64, amd64), and Linux (amd64, arm64).
    Converted from build.sh.
#>

[CmdletBinding()]
param(
    [switch]$Clean,
    [Alias("h", "?")]
    [switch]$Help
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

if ($Help) {
    Write-Host "Usage: .\build.ps1 [-Clean] [-Help]"
    exit 0
}

if ($Clean) {
    Write-Host "Cleaning setup bin/ directory..." -ForegroundColor Yellow
    if (Test-Path "bin") {
        Remove-Item -Recurse -Force "bin"
    }
    Write-Host "[OK] Cleaned" -ForegroundColor Green
    exit 0
}

Write-Host "=== Building MonsterMQ Setup Binaries ===" -ForegroundColor Green

# Ensure schema.json is up to date
$brokerSchema = Join-Path $PSScriptRoot "..\broker\yaml-json-schema.json"
$localSchema = Join-Path $PSScriptRoot "schema.json"

if (Test-Path $brokerSchema) {
    Copy-Item -Path $brokerSchema -Destination $localSchema -Force
} elseif (-not (Test-Path $localSchema)) {
    Write-Host "Error: schema.json not found and ..\broker\yaml-json-schema.json is missing!" -ForegroundColor Red
    exit 1
}

$binDir = Join-Path $PSScriptRoot "bin"
if (-not (Test-Path $binDir)) {
    New-Item -ItemType Directory -Path $binDir -Force | Out-Null
}

$targets = @(
    @{ Name = "Windows x64 (setup.exe)"; Os = "windows"; Arch = "amd64"; Out = "bin/setup.exe" },
    @{ Name = "Windows ARM64 (setup-win-arm64.exe)"; Os = "windows"; Arch = "arm64"; Out = "bin/setup-win-arm64.exe" },
    @{ Name = "macOS ARM64 (setup-mac-arm64)"; Os = "darwin"; Arch = "arm64"; Out = "bin/setup-mac-arm64" },
    @{ Name = "macOS x64 (setup-mac-x64)"; Os = "darwin"; Arch = "amd64"; Out = "bin/setup-mac-x64" },
    @{ Name = "Linux x64 (setup-linux-amd64)"; Os = "linux"; Arch = "amd64"; Out = "bin/setup-linux-amd64" },
    @{ Name = "Linux ARM64 (setup-linux-arm64)"; Os = "linux"; Arch = "arm64"; Out = "bin/setup-linux-arm64" }
)

foreach ($t in $targets) {
    Write-Host "Compiling $($t.Name)..." -ForegroundColor Yellow
    $env:GOOS = $t.Os
    $env:GOARCH = $t.Arch
    $env:CGO_ENABLED = "0"
    & go build -buildvcs=false -ldflags="-s -w" -o (Join-Path $PSScriptRoot $t.Out) .
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Build failed for $($t.Name)"
        exit $LASTEXITCODE
    }
    Write-Host "[OK] $($t.Out)" -ForegroundColor Green
}

Remove-Item env:GOOS -ErrorAction SilentlyContinue
Remove-Item env:GOARCH -ErrorAction SilentlyContinue
Remove-Item env:CGO_ENABLED -ErrorAction SilentlyContinue

Write-Host "`n=== Build Complete! Output binaries in setup/bin/ ===" -ForegroundColor Green
Get-ChildItem -Path $binDir | Format-Table Name, Length, LastWriteTime
