# ==============================================================================
# MonsterMQ Java Broker - Windows PowerShell Installer
#
# Quick install:
#   irm https://raw.githubusercontent.com/vogler75/monster-mq/main/install.ps1 | iex
#
# Options:
#   powershell -c "& { irm https://raw.githubusercontent.com/vogler75/monster-mq/main/install.ps1 | iex } -Dir 'C:\monstermq'"
# ==============================================================================

param(
    [string]$Dir = ".\monstermq",
    [string]$Version = "latest",
    [switch]$Start,
    [switch]$Yes,
    [switch]$Help
)

if ($Help) {
    Write-Host "MonsterMQ Java Broker Installer for Windows" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Usage: install.ps1 [-Dir <path>] [-Version <tag>] [-Start] [-Yes]"
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -Dir <path>       Installation directory (default: .\monstermq)"
    Write-Host "  -Version <tag>    Specific version (e.g. 1.8.27, default: latest)"
    Write-Host "  -Start            Start broker immediately after install"
    Write-Host "  -Yes              Non-interactive mode (auto-confirm)"
    Write-Host "  -Help             Show this help message"
    exit 0
}

$ErrorActionPreference = "Stop"

Write-Host "  __  __                  _              __  __  ____ " -ForegroundColor Cyan
Write-Host " |  \/  | ___  _ __  ___ | |_  ___  _ __|  \/  |/ __ \" -ForegroundColor Cyan
Write-Host " | |\/| |/ _ \| '_ \/ __|| __|/ _ \| '__| |\/| | / / |" -ForegroundColor Cyan
Write-Host " | |  | | (_) | | | \__ \| |_|  __/| |  | |  | | \ \_|" -ForegroundColor Cyan
Write-Host " |_|  |_|\___/|_| |_|___/ \__|\___||_|  |_|  |_|\___\_\" -ForegroundColor Cyan
Write-Host ""
Write-Host "MonsterMQ Java Broker Installation (Windows)" -ForegroundColor White
Write-Host "----------------------------------------------------"

# 1. Check Java requirement
Write-Host -NoNewline "Checking Java... "
$javaCmd = Get-Command java -ErrorAction SilentlyContinue
if (-not $javaCmd) {
    Write-Host "not found" -ForegroundColor Red
    Write-Host ""
    Write-Host "Error: Java is not installed or not in your PATH." -ForegroundColor Red
    Write-Host "MonsterMQ requires Java 21 or higher." -ForegroundColor Yellow
    Write-Host ""
    Write-Host "To install Java 21 on Windows, run in an Administrator prompt:" -ForegroundColor Yellow
    Write-Host "  winget install EclipseAdoptium.Temurin.21.JRE" -ForegroundColor White
    Write-Host "  or: choco install openjdk21" -ForegroundColor White
    Write-Host "  or download from: https://adoptium.net/temurin/releases/?version=21" -ForegroundColor White
    exit 1
}

$javaVerRaw = & java -version 2>&1 | Out-String
$javaVerMatch = [regex]::Match($javaVerRaw, 'version "(.*?)"')
$javaVerStr = if ($javaVerMatch.Success) { $javaVerMatch.Groups[1].Value } else { "unknown" }

$majorVer = 0
if ($javaVerStr -match '^1\.([0-9]+)') {
    $majorVer = [int]$matches[1]
} elseif ($javaVerStr -match '^([0-9]+)') {
    $majorVer = [int]$matches[1]
}

if ($majorVer -lt 21) {
    Write-Host "$javaVerStr (too old)" -ForegroundColor Red
    Write-Host "Error: Java 21 or higher is required, but found version $javaVerStr." -ForegroundColor Red
    Write-Host "Please upgrade Java to version 21+ and run this installer again." -ForegroundColor Yellow
    exit 1
}

Write-Host "Java $javaVerStr (OK)" -ForegroundColor Green

# 2. Resolve release version and download URL
$Repo = "vogler75/monster-mq"
Write-Host -NoNewline "Resolving release... "

$tag = ""
$downloadUrl = ""

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

if ($Version -eq "latest") {
    try {
        $apiUri = "https://api.github.com/repos/$Repo/releases/latest"
        $releaseInfo = Invoke-RestMethod -Uri $apiUri -Headers @{ "User-Agent" = "MonsterMQ-Installer" }
        $tag = $releaseInfo.tag_name
        $asset = $releaseInfo.assets | Where-Object { $_.name -like "monstermq-broker-*.zip" } | Select-Object -First 1
        if ($asset) {
            $downloadUrl = $asset.browser_download_url
        }
    } catch {
        # Fallback to direct redirect
    }

    if (-not $downloadUrl) {
        $req = [System.Net.WebRequest]::Create("https://github.com/$Repo/releases/latest")
        $req.AllowAutoRedirect = $false
        try {
            $resp = $req.GetResponse()
            $location = $resp.GetResponseHeader("Location")
            $tag = $location.Split('/')[-1]
            $verNum = $tag.TrimStart('v')
            $downloadUrl = "https://github.com/$Repo/releases/download/$tag/monstermq-broker-$verNum.zip"
        } catch {
            Write-Host "failed" -ForegroundColor Red
            Write-Host "Could not resolve latest release from GitHub." -ForegroundColor Red
            exit 1
        }
    }
} else {
    $tag = "v" + $Version.TrimStart('v')
    $verNum = $tag.TrimStart('v')
    $downloadUrl = "https://github.com/$Repo/releases/download/$tag/monstermq-broker-$verNum.zip"
}

Write-Host "$tag" -ForegroundColor Green

# 3. Setup Target Directory
$targetDir = [System.IO.Path]::GetFullPath($Dir)
Write-Host "Install target   : $targetDir" -ForegroundColor White
Write-Host "Download package : $downloadUrl" -ForegroundColor Yellow

if (-not $Yes -and [Environment]::UserInteractive) {
    $confirm = Read-Host "Proceed with installation? (Y/n)"
    if ($confirm -match '^[Nn]') {
        Write-Host "Installation cancelled." -ForegroundColor Yellow
        exit 0
    }
}

# 4. Download and Extract
$tempZip = Join-Path $env:TEMP "monstermq-broker-$tag.zip"
$tempExtract = Join-Path $env:TEMP "monstermq-extract-$tag"

if (Test-Path $tempExtract) {
    Remove-Item -Recurse -Force $tempExtract
}
New-Item -ItemType Directory -Path $tempExtract | Out-Null
New-Item -ItemType Directory -Path $targetDir -Force | Out-Null

Write-Host "Downloading MonsterMQ $tag..." -ForegroundColor Cyan
Invoke-WebRequest -Uri $downloadUrl -OutFile $tempZip -UseBasicParsing

Write-Host "Extracting files to $targetDir..." -ForegroundColor Cyan
Expand-Archive -Path $tempZip -DestinationPath $tempExtract -Force

$extractedItems = Get-ChildItem -Path $tempExtract
if ($extractedItems.Count -eq 1 -and $extractedItems[0].PSIsContainer) {
    Copy-Item -Path "$($extractedItems[0].FullName)\*" -Destination $targetDir -Recurse -Force
} else {
    Copy-Item -Path "$tempExtract\*" -Destination $targetDir -Recurse -Force
}

# Clean temp
Remove-Item -Force $tempZip -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force $tempExtract -ErrorAction SilentlyContinue

# Ensure directories
New-Item -ItemType Directory -Path (Join-Path $targetDir "sqlite") -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $targetDir "log") -Force | Out-Null

# 5. Ensure config.yaml and yaml-json-schema.json
$configPath = Join-Path $targetDir "config.yaml"
$schemaPath = Join-Path $targetDir "yaml-json-schema.json"

if (-not (Test-Path $schemaPath)) {
    try {
        $schemaUrl = "https://raw.githubusercontent.com/$Repo/main/broker/yaml-json-schema.json"
        Invoke-WebRequest -Uri $schemaUrl -OutFile $schemaPath -UseBasicParsing -ErrorAction SilentlyContinue
    } catch {}
}

if (-not (Test-Path $configPath)) {
    try {
        $configUrl = "https://raw.githubusercontent.com/$Repo/main/broker/config-default.yaml"
        Invoke-WebRequest -Uri $configUrl -OutFile $configPath -UseBasicParsing -ErrorAction SilentlyContinue
    } catch {}
}

Write-Host ""
Write-Host "✓ MonsterMQ $tag installed successfully!" -ForegroundColor Green
Write-Host "----------------------------------------------------"
Write-Host "Quickstart:" -ForegroundColor White
Write-Host "  cd `"$targetDir`"" -ForegroundColor Yellow
Write-Host "  .\run.bat" -ForegroundColor Yellow
Write-Host ""
Write-Host "Endpoints:" -ForegroundColor White
Write-Host "  • Web Dashboard & GraphQL : http://localhost:4000/" -ForegroundColor Cyan
Write-Host "  • MQTT Broker             : mqtt://localhost:1883" -ForegroundColor Cyan
Write-Host "  • MQTT WebSocket          : ws://localhost:1884" -ForegroundColor Cyan
Write-Host "  • MCP AI Server           : http://localhost:3000/" -ForegroundColor Cyan
Write-Host "----------------------------------------------------"

if ($Start) {
    Write-Host "Starting MonsterMQ..." -ForegroundColor Green
    Set-Location $targetDir
    & .\run.bat
}
