@echo off
setlocal enabledelayedexpansion

:: Check if -build argument is provided
set "BUILD_REQUESTED=false"
set "JAVA_ARGS="

:: Parse arguments
:parse_args
if "%~1"=="" goto :done_parsing
if "%~1"=="-build" (
    set "BUILD_REQUESTED=true"
    shift
    goto :parse_args
)
:: Collect remaining arguments for Java command
set "JAVA_ARGS=!JAVA_ARGS! %~1"
shift
goto :parse_args

:done_parsing

:: Execute Maven build if requested
if "!BUILD_REQUESTED!"=="true" (
    echo Running Maven clean package...
    call mvn clean package
    if errorlevel 1 (
        echo Maven build failed!
        exit /b 1
    )
    echo Maven build completed successfully.
    echo.
)

:: Run the Java application with collected arguments
echo Starting MonsterMQ...
java -classpath target/classes;target/dependencies/* at.rocworks.MonsterKt !JAVA_ARGS!

