@echo off
setlocal enabledelayedexpansion

REM MonsterMQ Run Script (Windows)
REM Usage: run.bat [SCRIPT_OPTIONS] [-- MONSTER_OPTIONS]
REM
REM Script Options (before --):
REM   -build, -b          Build dashboard and package broker with Maven before starting
REM   -compile, -c        Only compile broker (mvn compile, no package, no dashboard)
REM   -norun, -n          Do not run the broker (useful with -b/-c for build-only)
REM   -dashboard, -d      Build and serve dashboard from dashboard\dist\ (live development)
REM   -help, --help, -h   Show this help message
REM
REM All arguments after -- are passed directly to MonsterMQ.

set "BUILD_FIRST=false"
set "COMPILE_ONLY=false"
set "NO_RUN=false"
set "NO_KILL=false"
set "DASHBOARD_DEV=false"
set "FOUND_SEPARATOR=false"
set "REMAINING_ARGS="

REM Parse script options (before --) and monster options (after --)
:parse_args
if "%~1"=="" goto end_parse
if "!FOUND_SEPARATOR!"=="true" (
    set REMAINING_ARGS=!REMAINING_ARGS! %1
    shift
    goto parse_args
)
if "%~1"=="--" (
    set "FOUND_SEPARATOR=true"
) else if /I "%~1"=="-build" (
    set "BUILD_FIRST=true"
) else if /I "%~1"=="-b" (
    set "BUILD_FIRST=true"
) else if /I "%~1"=="-compile" (
    set "COMPILE_ONLY=true"
) else if /I "%~1"=="-c" (
    set "COMPILE_ONLY=true"
) else if /I "%~1"=="-norun" (
    set "NO_RUN=true"
) else if /I "%~1"=="-n" (
    set "NO_RUN=true"
) else if /I "%~1"=="-nokill" (
    set "NO_KILL=true"
) else if /I "%~1"=="-nk" (
    set "NO_KILL=true"
) else if /I "%~1"=="-dashboard" (
    set "DASHBOARD_DEV=true"
) else if /I "%~1"=="-d" (
    set "DASHBOARD_DEV=true"
) else if /I "%~1"=="-help" (
    goto show_help
) else if /I "%~1"=="--help" (
    goto show_help
) else if /I "%~1"=="-h" (
    goto show_help
) else (
    set REMAINING_ARGS=!REMAINING_ARGS! %1
)
shift
goto parse_args
:end_parse

REM Resolve script-relative directories
set "SCRIPT_DIR=%~dp0"
if "%SCRIPT_DIR:~-1%"=="\" set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"
set "DASHBOARD_DIR=%SCRIPT_DIR%\..\dashboard"
set "RESOURCES_DIR=%SCRIPT_DIR%\src\main\resources\dashboard"

REM If -build option is specified, build dashboard and broker
if "!BUILD_FIRST!"=="true" (
    if exist "%DASHBOARD_DIR%\package.json" (
        echo Building dashboard...
        pushd "%DASHBOARD_DIR%"
        call npm install
        if errorlevel 1 (
            echo Dashboard install failed!
            popd
            exit /b 1
        )
        call npm run build
        if errorlevel 1 (
            echo Dashboard build failed!
            popd
            exit /b 1
        )
        popd
        echo Copying dashboard to broker resources...
        if exist "%RESOURCES_DIR%" rmdir /S /Q "%RESOURCES_DIR%"
        xcopy /E /I /Y "%DASHBOARD_DIR%\dist" "%RESOURCES_DIR%" >nul
        if errorlevel 1 (
            echo Dashboard copy failed!
            exit /b 1
        )
        echo Dashboard build completed.
    ) else (
        echo Dashboard not found, skipping.
    )

    echo Building MonsterMQ...
    call mvn package -DskipTests
    if errorlevel 1 (
        echo Build failed!
        exit /b 1
    )
    echo Build completed successfully.
)

REM If -compile option is specified (and -b was not), only run mvn compile
if "!COMPILE_ONLY!"=="true" if "!BUILD_FIRST!"=="false" (
    if not exist "%SCRIPT_DIR%\target\dependencies" (
        echo target\dependencies\ not found. Run with -b once to populate it.
        exit /b 1
    )
    echo Compiling MonsterMQ ^(no package^)...
    call mvn compile -DskipTests
    if errorlevel 1 (
        echo Compile failed!
        exit /b 1
    )
    echo Compile completed successfully.
)

REM Exit if -norun is set
if "!NO_RUN!"=="true" exit /b 0

echo Starting MonsterMQ...

REM Detect if Java is GraalVM
set "JAVA_OPTS="
java -version 2>&1 | findstr /I "GraalVM" >nul
if not errorlevel 1 (
    echo Detected GraalVM - enabling JVMCI for optimal JavaScript performance
    set "JAVA_OPTS=-XX:+EnableJVMCI -XX:+UseJVMCICompiler"
) else (
    echo Using standard JVM ^(GraalVM not detected^)
)

set "JAVA_OPTS=!JAVA_OPTS! --enable-native-access=ALL-UNNAMED"

REM Serve dashboard from filesystem for development
if "!DASHBOARD_DEV!"=="true" (
    set "DASHBOARD_DIST=%DASHBOARD_DIR%\dist"

    if "!BUILD_FIRST!"=="false" (
        if exist "%DASHBOARD_DIR%\package.json" (
            echo Always build requested by -d, building dashboard...
            pushd "%DASHBOARD_DIR%"
            call npm install
            if errorlevel 1 (
                echo Dashboard install failed!
                popd
                exit /b 1
            )
            call npm run build
            if errorlevel 1 (
                echo Dashboard build failed!
                popd
                exit /b 1
            )
            popd
        ) else (
            echo Warning: Dashboard directory or package.json not found, skipping build.
        )
    )

    if exist "!DASHBOARD_DIST!" (
        echo Dashboard serving from filesystem: !DASHBOARD_DIST!
        set REMAINING_ARGS=!REMAINING_ARGS! -dashboardPath "!DASHBOARD_DIST!"
    ) else (
        echo Warning: dashboard\dist\ not found after build attempt.
    )
)

REM Note: Protobuf version upgraded to 4.28.3 for Java 21+ compatibility

REM Start MonsterMQ
echo java !JAVA_OPTS! -classpath "%SCRIPT_DIR%\target\classes;%SCRIPT_DIR%\target\dependencies\*" at.rocworks.MonsterKt !REMAINING_ARGS!
java !JAVA_OPTS! -classpath "%SCRIPT_DIR%\target\classes;%SCRIPT_DIR%\target\dependencies\*" at.rocworks.MonsterKt !REMAINING_ARGS!
exit /b %ERRORLEVEL%

:show_help
echo MonsterMQ Run Script
echo.
echo Usage: run.bat [SCRIPT_OPTIONS] [-- MONSTER_OPTIONS]
echo.
echo Script Options (before --):
echo   -build, -b          Build dashboard and package broker with Maven before starting
echo   -compile, -c        Only compile broker (mvn compile, no package, no dashboard)
echo   -norun, -n          Do not run the broker (useful with -b/-c for build-only)
echo   -dashboard, -d      Build and serve dashboard from dashboard\dist\ (live development)
echo   -help, --help, -h   Show this help message
echo.
echo Monster Options (after --):
echo   All arguments after -- are passed directly to MonsterMQ.
echo   Use -- -help to see MonsterMQ options.
echo.
echo Examples:
echo   run.bat                                         Start with default config
echo   run.bat -b                                      Build first, then start
echo   run.bat -c                                      Compile only (fast), then start
echo   run.bat -b -n                                   Build only, do not start
echo   run.bat -- -cluster                             Start with broker options
echo   run.bat -b -- -cluster                          Build and start in cluster mode
echo   run.bat -- -log FINEST                          Start with temporary CLI log override
echo   run.bat -b -- -archiveConfigs archives.json     Build and import configs
echo   run.bat -d                                      Serve dashboard from filesystem
echo   run.bat -- -help                                Show MonsterMQ help
exit /b 0
