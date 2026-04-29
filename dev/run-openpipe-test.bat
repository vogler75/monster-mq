@echo off
REM Run dev\OpenPipeTest.java with whichever JDK 11+ is available.
REM Usage: run-openpipe-test.bat [filter]
REM Example: run-openpipe-test.bat "HMI_Tag_*"

setlocal

REM Resolve script directory (so this works regardless of cwd)
set "SCRIPT_DIR=%~dp0"
if "%SCRIPT_DIR:~-1%"=="\" set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"

REM Pick a JDK >=11 from the usual local installs. Add more candidates as needed.
set "JAVA_EXE="
for %%P in (
    "C:\Tools\logstash-8.17.3\jdk\bin\java.exe"
    "C:\Program Files\Amazon Corretto\jdk21.0.0_0\bin\java.exe"
    "C:\Program Files\Amazon Corretto\jdk17.0.8_8\bin\java.exe"
    "C:\Program Files\Amazon Corretto\jdk11.0.17_8\bin\java.exe"
) do (
    if exist %%P set "JAVA_EXE=%%~P"
)

if "%JAVA_EXE%"=="" (
    echo No JDK 11+ found at the known paths. Edit run-openpipe-test.bat to add yours.
    exit /b 1
)

echo Using java: %JAVA_EXE%
"%JAVA_EXE%" "%SCRIPT_DIR%\OpenPipeTest.java" %*
