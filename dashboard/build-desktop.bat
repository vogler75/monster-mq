@echo off
setlocal enabledelayedexpansion

REM Build script for MonsterMQ Desktop Apps (Batch version)
cd /d "%~dp0"

set BUILD_MAC=false
set BUILD_WIN=false
set BUILD_LINUX=false

REM Parse arguments
:parse_args
if "%~1"=="" goto after_args

if /i "%~1"=="--win" set BUILD_WIN=true
if /i "%~1"=="win" set BUILD_WIN=true
if /i "%~1"=="-w" set BUILD_WIN=true

if /i "%~1"=="--mac" set BUILD_MAC=true
if /i "%~1"=="mac" set BUILD_MAC=true
if /i "%~1"=="-m" set BUILD_MAC=true

if /i "%~1"=="--linux" set BUILD_LINUX=true
if /i "%~1"=="linux" set BUILD_LINUX=true
if /i "%~1"=="-l" set BUILD_LINUX=true

if /i "%~1"=="--all" (
    set BUILD_WIN=true
    set BUILD_MAC=true
    set BUILD_LINUX=true
)
if /i "%~1"=="all" (
    set BUILD_WIN=true
    set BUILD_MAC=true
    set BUILD_LINUX=true
)

shift
goto parse_args

:after_args

REM If no specific platform was selected, default to Windows only on batch script
if "%BUILD_MAC%"=="false" if "%BUILD_WIN%"=="false" if "%BUILD_LINUX%"=="false" (
    set BUILD_WIN=true
)

echo === Building MonsterMQ Desktop App ===

REM Sync package.json version with broker version in version.txt if available
set "VERSION_FILE="
if exist "..\version.txt" set "VERSION_FILE=..\version.txt"
if not defined VERSION_FILE if exist "version.txt" set "VERSION_FILE=version.txt"

if defined VERSION_FILE (
    set /p RAW_BROKER_VERSION=<"!VERSION_FILE!"
    for /f "tokens=1 delims=+" %%a in ("!RAW_BROKER_VERSION!") do set "BROKER_VERSION=%%a"
    if defined BROKER_VERSION (
        echo Syncing package.json version from !VERSION_FILE!: !BROKER_VERSION!
        call npm version !BROKER_VERSION! --no-git-tag-version --allow-same-version >nul 2>&1
    )
)


REM Copy the app logo if available
if not exist "build" mkdir build
if not exist "build\icon.png" (
    if exist "appicon.png" (
        copy /Y "appicon.png" "build\icon.png" >nul
        echo Application icon copied from dashboard/appicon.png to dashboard/build/icon.png
    ) else if exist "appicon-option1.png" (
        copy /Y "appicon-option1.png" "build\icon.png" >nul
        echo Application icon copied from dashboard/appicon-option1.png to dashboard/build/icon.png
    ) else if exist "..\logos\appicon.png" (
        copy /Y "..\logos\appicon.png" "build\icon.png" >nul
        echo Application icon copied from logos/appicon.png to dashboard/build/icon.png
    ) else if exist "..\logos\Logo-v2.png" (
        copy /Y "..\logos\Logo-v2.png" "build\icon.png" >nul
        echo Application icon copied to dashboard/build/icon.png
    )
)

echo Installing npm dependencies...
call npm install
if errorlevel 1 goto error

echo Building web dashboard assets...
call npm run build
if errorlevel 1 goto error

REM Construct builder arguments
set BUILD_FLAGS=--x64 --arm64 --publish never

if "%BUILD_MAC%"=="true" set BUILD_FLAGS=!BUILD_FLAGS! --mac
if "%BUILD_WIN%"=="true" set BUILD_FLAGS=!BUILD_FLAGS! --win
if "%BUILD_LINUX%"=="true" set BUILD_FLAGS=!BUILD_FLAGS! --linux

echo Packaging desktop app with flags: !BUILD_FLAGS!
call npx electron-builder !BUILD_FLAGS!
if errorlevel 1 goto error

REM Post-processing rename for macOS and Windows build artifacts for consistency (without version numbers)
if "%BUILD_MAC%"=="true" (
    echo Checking macOS build artifacts...
    if exist "dist-desktop\MonsterMQ-Dashboard-x64.dmg" (
        ren "dist-desktop\MonsterMQ-Dashboard-x64.dmg" "MonsterMQ-Dashboard-mac-x64.dmg"
        echo Renamed dist-desktop\MonsterMQ-Dashboard-x64.dmg to dist-desktop\MonsterMQ-Dashboard-mac-x64.dmg
    )
    if exist "dist-desktop\MonsterMQ-Dashboard-arm64.dmg" (
        ren "dist-desktop\MonsterMQ-Dashboard-arm64.dmg" "MonsterMQ-Dashboard-mac-arm64.dmg"
        echo Renamed dist-desktop\MonsterMQ-Dashboard-arm64.dmg to dist-desktop\MonsterMQ-Dashboard-mac-arm64.dmg
    )
)

if "%BUILD_WIN%"=="true" (
    echo Checking Windows build artifacts...
    if exist "dist-desktop\MonsterMQ-Dashboard Setup.exe" (
        ren "dist-desktop\MonsterMQ-Dashboard Setup.exe" "MonsterMQ-Dashboard-win-x64-setup.exe"
        echo Renamed Setup exe to dist-desktop\MonsterMQ-Dashboard-win-x64-setup.exe
    )
    if exist "dist-desktop\MonsterMQ-Dashboard Setup arm64.exe" (
        ren "dist-desktop\MonsterMQ-Dashboard Setup arm64.exe" "MonsterMQ-Dashboard-win-arm64-setup.exe"
        echo Renamed Setup exe to dist-desktop\MonsterMQ-Dashboard-win-arm64-setup.exe
    )
)

echo === Build Completed Successfully ===
echo Desktop packages are located in dist-desktop/
exit /b 0

:error
echo === Build Failed ===
exit /b 1
