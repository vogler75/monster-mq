# MonsterMQ Run Script (PowerShell)
# Usage: .\run.ps1 [SCRIPT_OPTIONS] [-- MONSTER_OPTIONS]
#
# Script Options (before --):
#   -build, -b          Build dashboard and package broker with Maven before starting
#   -compile, -c        Only compile broker (mvn compile, no package, no dashboard)
#   -norun, -n          Do not run the broker (useful with -b/-c for build-only)
#   -nokill, -nk        Do not kill existing instances before starting
#   -dashboard, -d      Build and serve dashboard from dashboard\dist\ (live development)
#   -help, --help, -h   Show this help message
#
# All arguments after -- are passed directly to MonsterMQ.

$BUILD_FIRST = $false
$COMPILE_ONLY = $false
$NO_RUN = $false
$NO_KILL = $false
$DASHBOARD_DEV = $false
$FOUND_SEPARATOR = $false
$MONSTER_ARGS = [System.Collections.Generic.List[string]]::new()

function Show-Help {
    Write-Host "MonsterMQ Run Script"
    Write-Host ""
    Write-Host "Usage: .\run.ps1 [SCRIPT_OPTIONS] [-- MONSTER_OPTIONS]"
    Write-Host ""
    Write-Host "Script Options (before --):"
    Write-Host "  -build, -b          Build dashboard and package broker with Maven before starting"
    Write-Host "  -compile, -c        Only compile broker (mvn compile, no package, no dashboard)"
    Write-Host "  -norun, -n          Do not run the broker (useful with -b/-c for build-only)"
    Write-Host "  -nokill, -nk        Do not kill existing instances before starting"
    Write-Host "  -dashboard, -d      Build and serve dashboard from dashboard\dist\ (live development)"
    Write-Host "  -help, --help, -h   Show this help message"
    Write-Host ""
    Write-Host "Monster Options (after --):"
    Write-Host "  All arguments after -- are passed directly to MonsterMQ."
    Write-Host "  Use -- -help to see MonsterMQ options."
    Write-Host ""
    Write-Host "Examples:"
    Write-Host "  .\run.ps1                                         Start with default config"
    Write-Host "  .\run.ps1 -b                                      Build first, then start"
    Write-Host "  .\run.ps1 -c                                      Compile only (fast), then start"
    Write-Host "  .\run.ps1 -b -n                                   Build only, do not start"
    Write-Host "  .\run.ps1 -- -cluster                             Start with broker options"
    Write-Host "  .\run.ps1 -b -- -cluster                          Build and start in cluster mode"
    Write-Host "  .\run.ps1 -- -log FINEST                          Start with temporary CLI log override"
    Write-Host "  .\run.ps1 -b -- -archiveConfigs archives.json     Build and import configs"
    Write-Host "  .\run.ps1 -d                                      Serve dashboard from filesystem"
    Write-Host "  .\run.ps1 -- -help                                Show MonsterMQ help"
    exit 0
}

# Parse script arguments
$i = 0
while ($i -lt $args.Count) {
    $arg = $args[$i]
    if ($FOUND_SEPARATOR) {
        $MONSTER_ARGS.Add($arg)
    } elseif ($arg -eq "--") {
        $FOUND_SEPARATOR = $true
    } elseif ($arg -in "-build", "-b", "--build") {
        $BUILD_FIRST = $true
    } elseif ($arg -in "-compile", "-c", "--compile") {
        $COMPILE_ONLY = $true
    } elseif ($arg -in "-norun", "-n", "--norun") {
        $NO_RUN = $true
    } elseif ($arg -in "-nokill", "-nk", "--nokill") {
        $NO_KILL = $true
    } elseif ($arg -in "-dashboard", "-d", "--dashboard") {
        $DASHBOARD_DEV = $true
    } elseif ($arg -in "-help", "--help", "-h", "/?") {
        Show-Help
    } else {
        $MONSTER_ARGS.Add($arg)
    }
    $i++
}

# Resolve script directory
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $ScriptDir) { $ScriptDir = (Get-Location).Path }
$ResourcesDir = Join-Path $ScriptDir "src\main\resources\dashboard"

# Resolve dashboard directory (supports monorepo, sibling checkouts, or separate repo names)
function Resolve-DashboardDir {
    $candidates = @(
        (Join-Path $ScriptDir "..\dashboard"),
        (Join-Path $ScriptDir "..\..\dashboard"),
        (Join-Path $ScriptDir "..\..\monster-mq-dashboard"),
        (Join-Path $ScriptDir "..\monster-mq-dashboard")
    )
    foreach ($cand in $candidates) {
        if (Test-Path (Join-Path $cand "package.json")) {
            return (Resolve-Path $cand).Path
        }
    }
    return $null
}

$DashboardDir = Resolve-DashboardDir

# Build dashboard and broker if -build is specified
if ($BUILD_FIRST) {
    if ($DashboardDir -and (Test-Path (Join-Path $DashboardDir "package.json"))) {
        Write-Host "Building dashboard from $DashboardDir..." -ForegroundColor Cyan
        Push-Location $DashboardDir
        try {
            & npm install
            if ($LASTEXITCODE -ne 0) {
                Write-Error "Dashboard install failed!"
                exit 1
            }
            & npm run build
            if ($LASTEXITCODE -ne 0) {
                Write-Error "Dashboard build failed!"
                exit 1
            }
        } finally {
            Pop-Location
        }

        Write-Host "Copying dashboard to broker resources..." -ForegroundColor Cyan
        if (Test-Path $ResourcesDir) {
            Remove-Item -Recurse -Force $ResourcesDir
        }
        Copy-Item -Recurse -Force (Join-Path $DashboardDir "dist") $ResourcesDir
        Write-Host "Dashboard build completed." -ForegroundColor Green
    } else {
        Write-Error "Error: Dashboard checkout not found in any standard location (..\dashboard, ..\..\dashboard, ..\..\monster-mq-dashboard)."
        Write-Host "Please create a symlink or clone https://github.com/vogler75/monster-mq-dashboard into a sibling folder."
        exit 1
    }

    Write-Host "Building MonsterMQ..." -ForegroundColor Cyan
    & mvn package -DskipTests
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Build failed!"
        exit 1
    }
    Write-Host "Build completed successfully." -ForegroundColor Green
}

# Compile broker if -compile is specified (and -b was not)
if ($COMPILE_ONLY -and -not $BUILD_FIRST) {
    $depsDir = Join-Path $ScriptDir "target\dependencies"
    if (-not (Test-Path $depsDir)) {
        Write-Error "target\dependencies\ not found. Run with -b once to populate it."
        exit 1
    }
    Write-Host "Compiling MonsterMQ (no package)..." -ForegroundColor Cyan
    & mvn compile -DskipTests
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Compile failed!"
        exit 1
    }
    Write-Host "Compile completed successfully." -ForegroundColor Green
}

# Exit if -norun is set
if ($NO_RUN) {
    exit 0
}

# Kill existing MonsterMQ instances (skip in cluster mode or with -nokill)
$skipKill = $NO_KILL
if (-not $skipKill) {
    if ($MONSTER_ARGS -contains "-cluster") {
        $skipKill = $true
    }
}

if ($skipKill) {
    Write-Host "Skipping kill of existing instances." -ForegroundColor Yellow
} else {
    Write-Host "Checking for existing MonsterMQ instances..." -ForegroundColor Cyan
    try {
        $procs = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
            $_.CommandLine -like "*at.rocworks.MonsterKt*" -and $_.ProcessId -ne $PID
        }
        if ($procs) {
            $pids = ($procs | ForEach-Object { $_.ProcessId }) -join ", "
            Write-Host "Found existing MonsterMQ processes: $pids" -ForegroundColor Yellow
            Write-Host "Stopping existing instances..." -ForegroundColor Yellow
            foreach ($p in $procs) {
                Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
            }
            Start-Sleep -Seconds 1
            Write-Host "All existing MonsterMQ instances stopped." -ForegroundColor Green
        } else {
            Write-Host "No existing MonsterMQ instances found." -ForegroundColor DarkGray
        }
    } catch {
        # Non-fatal if process query fails
    }
}

Write-Host "Starting MonsterMQ..." -ForegroundColor Cyan

# Detect if Java is GraalVM
$javaOpts = [System.Collections.Generic.List[string]]::new()
try {
    $javaVerOutput = (& java -version 2>&1 | Out-String)
    if ($javaVerOutput -match "GraalVM") {
        Write-Host "Detected GraalVM - enabling JVMCI for optimal JavaScript performance" -ForegroundColor Green
        $javaOpts.Add("-XX:+EnableJVMCI")
        $javaOpts.Add("-XX:+UseJVMCICompiler")
    } else {
        Write-Host "Using standard JVM (GraalVM not detected)" -ForegroundColor DarkGray
    }
} catch {
    Write-Host "Using standard JVM (GraalVM detection skipped)" -ForegroundColor DarkGray
}

$javaOpts.Add("--enable-native-access=ALL-UNNAMED")

# Serve dashboard from filesystem for development
if ($DASHBOARD_DEV) {
    if ($DashboardDir) {
        $dashboardDist = Join-Path $DashboardDir "dist"
        if (-not $BUILD_FIRST) {
            if (Test-Path (Join-Path $DashboardDir "package.json")) {
                Write-Host "Always build requested by -d, building dashboard from $DashboardDir..." -ForegroundColor Cyan
                Push-Location $DashboardDir
                try {
                    & npm install
                    if ($LASTEXITCODE -ne 0) {
                        Write-Error "Dashboard install failed!"
                        exit 1
                    }
                    & npm run build
                    if ($LASTEXITCODE -ne 0) {
                        Write-Error "Dashboard build failed!"
                        exit 1
                    }
                } finally {
                    Pop-Location
                }
            }
        }

        if (Test-Path $dashboardDist) {
            Write-Host "Dashboard serving from filesystem: $dashboardDist" -ForegroundColor Green
            $MONSTER_ARGS.Add("-dashboardPath")
            $MONSTER_ARGS.Add($dashboardDist)
        } else {
            Write-Error "Error: $dashboardDist not found after build attempt."
            exit 1
        }
    } else {
        Write-Error "Error: Dashboard checkout not found in any standard location (..\dashboard, ..\..\dashboard, ..\..\monster-mq-dashboard)."
        Write-Host "Please create a symlink or clone https://github.com/vogler75/monster-mq-dashboard into a sibling folder."
        exit 1
    }
}

# Start MonsterMQ
$classpath = "$ScriptDir\target\classes;$ScriptDir\target\dependencies\*"
Write-Host "java $($javaOpts -join ' ') -classpath `"$classpath`" at.rocworks.MonsterKt $($MONSTER_ARGS -join ' ')" -ForegroundColor DarkCyan
& java @javaOpts -classpath $classpath at.rocworks.MonsterKt @MONSTER_ARGS
exit $LASTEXITCODE
