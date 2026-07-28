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

REM Copy the app logo if available
if not exist "build" mkdir build
if exist "..\logos\Logo-v2.png" (
    copy /Y "..\logos\Logo-v2.png" "build\icon.png" >nul
    echo Application icon copied to dashboard/build/icon.png
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

REM Post-processing rename for x64 macOS files if built
if "%BUILD_MAC%"=="true" (
    echo Renaming macOS x64 build artifacts for clarity...
    for /f "tokens=*" %%v in ('node -e "console.log(require('./package.json').version)"') do set VERSION=%%v
    if exist "dist-desktop\MonsterMQ-!VERSION!.dmg" (
        ren "dist-desktop\MonsterMQ-!VERSION!.dmg" "MonsterMQ-!VERSION!-intel-x64.dmg"
        echo Renamed dist-desktop\MonsterMQ-!VERSION!.dmg to dist-desktop\MonsterMQ-!VERSION!-intel-x64.dmg
    )
    if exist "dist-desktop\MonsterMQ-!VERSION!-mac.zip" (
        ren "dist-desktop\MonsterMQ-!VERSION!-mac.zip" "MonsterMQ-!VERSION!-intel-x64.zip"
        echo Renamed dist-desktop\MonsterMQ-!VERSION!-mac.zip to dist-desktop\MonsterMQ-!VERSION!-intel-x64.zip
    )
    if exist "dist-desktop\MonsterMQ-!VERSION!.dmg.blockmap" (
        ren "dist-desktop\MonsterMQ-!VERSION!.dmg.blockmap" "MonsterMQ-!VERSION!-intel-x64.dmg.blockmap"
    )
    if exist "dist-desktop\MonsterMQ-!VERSION!-mac.zip.blockmap" (
        ren "dist-desktop\MonsterMQ-!VERSION!-mac.zip.blockmap" "MonsterMQ-!VERSION!-intel-x64.zip.blockmap"
    )
)

echo === Build Completed Successfully ===
echo Desktop packages are located in dist-desktop/
exit /b 0

:error
echo === Build Failed ===
exit /b 1
