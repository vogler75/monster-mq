# ==============================================================================
# MonsterMQ Setup & Installer Launcher (Windows PowerShell)
#
# Downloads and launches the native MonsterMQ Setup executable (setup.exe)
# with interactive browser wizard, schema-driven config editor, and Java 21+ check.
#
# Quick install:
#   irm https://raw.githubusercontent.com/vogler75/monster-mq/main/install.ps1 | iex
#
# Options:
#   powershell -c "& { irm https://raw.githubusercontent.com/vogler75/monster-mq/main/install.ps1 | iex } -Cli"
# ==============================================================================

param(
    [switch]$Cli,
    [switch]$Unattended,
    [string]$Dir = "",
    [string]$Version = "latest",
    [switch]$Help
)

if ($Help) {
    Write-Host "MonsterMQ Setup & Installer Launcher for Windows" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Usage: install.ps1 [-Cli] [-Unattended] [-Dir <path>] [-Version <tag>]"
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -Cli          Run in terminal CLI mode instead of web browser"
    Write-Host "  -Unattended   Run non-interactive automatic installation"
    Write-Host "  -Dir <path>   Target installation directory"
    Write-Host "  -Version <tag> Specific version to install (default: latest)"
    Write-Host "  -Help         Show this help message"
    exit 0
}

$ErrorActionPreference = "Stop"

Write-Host "  __  __                  _              __  __  ____ " -ForegroundColor Cyan
Write-Host " |  \/  | ___  _ __  ___ | |_  ___  _ __|  \/  |/ __ \" -ForegroundColor Cyan
Write-Host " | |\/| |/ _ \| '_ \/ __|| __|/ _ \| '__| |\/| | / / |" -ForegroundColor Cyan
Write-Host " | |  | | (_) | | | \__ \| |_|  __/| |  | |  | | \ \_|" -ForegroundColor Cyan
Write-Host " |_|  |_|\___/|_| |_|___/ \__|\___||_|  |_|  |_|\___\_\" -ForegroundColor Cyan
Write-Host "                                      Setup & Installer" -ForegroundColor Cyan
Write-Host ""

$Repo = "vogler75/monster-mq"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

# 1. Detect Architecture
$arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
$binName = "setup.exe"
if ($arch -eq [System.Runtime.InteropServices.Architecture]::Arm64) {
    $binName = "setup-win-arm64.exe"
}

Write-Host "Platform detected : Windows ($arch) -> $binName" -ForegroundColor White

# 2. Resolve Release & Download URL
Write-Host -NoNewline "Resolving latest release... "
$tag = ""
$downloadUrl = ""

if ($Version -eq "latest") {
    try {
        $apiUri = "https://api.github.com/repos/$Repo/releases/latest"
        $releaseInfo = Invoke-RestMethod -Uri $apiUri -Headers @{ "User-Agent" = "MonsterMQ-Setup-Launcher" }
        $tag = $releaseInfo.tag_name
        $asset = $releaseInfo.assets | Where-Object { $_.name -eq $binName } | Select-Object -First 1
        if ($asset) {
            $downloadUrl = $asset.browser_download_url
        }
    } catch {}

    if (-not $downloadUrl) {
        $req = [System.Net.WebRequest]::Create("https://github.com/$Repo/releases/latest")
        $req.AllowAutoRedirect = $false
        try {
            $resp = $req.GetResponse()
            $location = $resp.GetResponseHeader("Location")
            $tag = $location.Split('/')[-1]
            $downloadUrl = "https://github.com/$Repo/releases/download/$tag/$binName"
        } catch {
            Write-Host "failed" -ForegroundColor Red
            Write-Host "Could not resolve latest release from GitHub." -ForegroundColor Red
            exit 1
        }
    }
} else {
    $tag = "v" + $Version.TrimStart('v')
    $downloadUrl = "https://github.com/$Repo/releases/download/$tag/$binName"
}

Write-Host "$tag" -ForegroundColor Green

# 3. Download Setup Executable
$tempSetup = Join-Path $env:TEMP "monstermq-$binName"
Write-Host "Downloading MonsterMQ Setup ($tag)..." -ForegroundColor Cyan
Invoke-WebRequest -Uri $downloadUrl -OutFile $tempSetup -UseBasicParsing

# 4. Launch Setup Executable
$passArgs = @()
if ($Cli) { $passArgs += "-cli" }
if ($Unattended) { $passArgs += "-unattended" }
if ($Dir) { $passArgs += "-dir"; $passArgs += $Dir }
if ($Version -and $Version -ne "latest") { $passArgs += "-version"; $passArgs += $Version }

Write-Host "Launching MonsterMQ Setup..." -ForegroundColor Green
try {
    & $tempSetup @passArgs
} finally {
    Remove-Item -Force $tempSetup -ErrorAction SilentlyContinue
}
