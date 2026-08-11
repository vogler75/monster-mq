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
    echo Renaming macOS build artifacts for clarity...
    if exist "dist-desktop\MonsterMQ-Dashboard-x64.dmg" (
        ren "dist-desktop\MonsterMQ-Dashboard-x64.dmg" "MonsterMQ-Dashboard-mac-x64.dmg"
        echo Renamed dist-desktop\MonsterMQ-Dashboard-x64.dmg to dist-desktop\MonsterMQ-Dashboard-mac-x64.dmg
    ) else if exist "dist-desktop\MonsterMQ-Dashboard.dmg" (
        ren "dist-desktop\MonsterMQ-Dashboard.dmg" "MonsterMQ-Dashboard-mac-x64.dmg"
        echo Renamed dist-desktop\MonsterMQ-Dashboard.dmg to dist-desktop\MonsterMQ-Dashboard-mac-x64.dmg
    )
    if exist "dist-desktop\MonsterMQ-Dashboard-x64.zip" (
        ren "dist-desktop\MonsterMQ-Dashboard-x64.zip" "MonsterMQ-Dashboard-mac-x64.zip"
        echo Renamed dist-desktop\MonsterMQ-Dashboard-x64.zip to dist-desktop\MonsterMQ-Dashboard-mac-x64.zip
    ) else if exist "dist-desktop\MonsterMQ-Dashboard-mac.zip" (
        ren "dist-desktop\MonsterMQ-Dashboard-mac.zip" "MonsterMQ-Dashboard-mac-x64.zip"
        echo Renamed dist-desktop\MonsterMQ-Dashboard-mac.zip to dist-desktop\MonsterMQ-Dashboard-mac-x64.zip
    )
    if exist "dist-desktop\MonsterMQ-Dashboard-arm64.dmg" (
        ren "dist-desktop\MonsterMQ-Dashboard-arm64.dmg" "MonsterMQ-Dashboard-mac-arm64.dmg"
        echo Renamed dist-desktop\MonsterMQ-Dashboard-arm64.dmg to dist-desktop\MonsterMQ-Dashboard-mac-arm64.dmg
    )
    if exist "dist-desktop\MonsterMQ-Dashboard-arm64-mac.zip" (
        ren "dist-desktop\MonsterMQ-Dashboard-arm64-mac.zip" "MonsterMQ-Dashboard-mac-arm64.zip"
        echo Renamed dist-desktop\MonsterMQ-Dashboard-arm64-mac.zip to dist-desktop\MonsterMQ-Dashboard-mac-arm64.zip
    )
)

if "%BUILD_WIN%"=="true" (
    echo Renaming Windows build artifacts for clarity...
    if exist "dist-desktop\MonsterMQ-Dashboard Setup.exe" (
        ren "dist-desktop\MonsterMQ-Dashboard Setup.exe" "MonsterMQ-Dashboard-win-x64-setup.exe"
        echo Renamed Setup exe to dist-desktop\MonsterMQ-Dashboard-win-x64-setup.exe
    ) else if exist "dist-desktop\MonsterMQ-Dashboard-x64.exe" (
        ren "dist-desktop\MonsterMQ-Dashboard-x64.exe" "MonsterMQ-Dashboard-win-x64-setup.exe"
        echo Renamed Setup exe to dist-desktop\MonsterMQ-Dashboard-win-x64-setup.exe
    )
    if exist "dist-desktop\MonsterMQ-Dashboard-win.zip" (
        ren "dist-desktop\MonsterMQ-Dashboard-win.zip" "MonsterMQ-Dashboard-win-x64.zip"
        echo Renamed dist-desktop\MonsterMQ-Dashboard-win.zip to dist-desktop\MonsterMQ-Dashboard-win-x64.zip
    )
    if exist "dist-desktop\MonsterMQ-Dashboard-arm64-win.zip" (
        ren "dist-desktop\MonsterMQ-Dashboard-arm64-win.zip" "MonsterMQ-Dashboard-win-arm64.zip"
        echo Renamed dist-desktop\MonsterMQ-Dashboard-arm64-win.zip to dist-desktop\MonsterMQ-Dashboard-win-arm64.zip
    )
)

echo === Build Completed Successfully ===
echo Desktop packages are located in dist-desktop/
exit /b 0

:error
echo === Build Failed ===
exit /b 1
