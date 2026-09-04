# Running MonsterMQ as a Windows Service (WinSW)

This guide explains how to install, configure, and run the MonsterMQ Java Broker as a background Windows Service using **WinSW (Windows Service Wrapper)**.

---

## Overview

[WinSW](https://github.com/winsw/winsw) is an open-source utility that wraps any executable or batch script as a native Windows service. It provides automatic startup on boot, failure recovery with auto-restart, graceful shutdown, and stdout/stderr log rolling.

---

## Prerequisites

1. **Java 21+** (OpenJDK, Eclipse Temurin, or GraalVM) installed and accessible in the system `PATH` (or specified via `JAVA_HOME`).
2. **MonsterMQ Broker** installed (for example, in `C:\Program Files\MonsterMQ` or `C:\monstermq`).
3. **Administrator Privileges** on the Windows machine.

---

## Step 1: Download WinSW

1. Download the latest `WinSW-x64.exe` binary from the [WinSW GitHub Releases](https://github.com/winsw/winsw/releases).
2. Copy the executable into your MonsterMQ directory (e.g. `C:\Program Files\MonsterMQ\`).
3. Rename `WinSW-x64.exe` to `monstermq-service.exe` (or `MonsterMQ.exe`).

> **Note:** WinSW looks for an XML configuration file with the **exact same name** as the executable (e.g., `monstermq-service.xml` for `monstermq-service.exe`).

---

## Step 2: Create the Service Configuration (`monstermq-service.xml`)

In the same directory where `monstermq-service.exe` resides, create `monstermq-service.xml`:

```xml
<service>
  <id>MonsterMQ</id>
  <name>MonsterMQ Broker</name>
  <description>MonsterMQ MQTT Broker</description>

  <executable>C:\Windows\System32\cmd.exe</executable>
  <arguments>/c "C:\Program Files\MonsterMQ\run.bat"</arguments>
  <workingdirectory>C:\Program Files\MonsterMQ</workingdirectory>

  <startmode>Automatic</startmode>

  <onfailure action="restart" delay="10 sec"/>
  <onfailure action="restart" delay="30 sec"/>
  <onfailure action="restart" delay="60 sec"/>

  <logpath>C:\Program Files\MonsterMQ\log</logpath>
  <log mode="roll"></log>

  <stoptimeout>30sec</stoptimeout>
</service>
```

### Configuration Breakdown

| Element | Description |
|---|---|
| `<id>` | Unique Windows Service identifier (used with `sc`, `net`, and PowerShell). |
| `<name>` | Friendly display name shown in Windows Services (`services.msc`). |
| `<description>` | Description text shown in Windows Services Manager. |
| `<executable>` | The command processor `cmd.exe` to execute the broker's batch script. |
| `<arguments>` | Command arguments passing `/c` and the full path to `run.bat`. |
| `<workingdirectory>` | Working directory where MonsterMQ finds its configs, storage, and libraries. |
| `<startmode>` | `Automatic` to start when Windows boots (alternatives: `Manual`, `Delayed`). |
| `<onfailure>` | Recovery rules: automatically restarts the broker after 10s, 30s, and 60s if it terminates unexpectedly. |
| `<logpath>` | Directory where WinSW writes wrapper stdout/stderr log files. |
| `<log mode="roll">` | Automatically rolls log files to prevent unbounded disk usage. |
| `<stoptimeout>` | Grants up to 30 seconds for graceful shutdown before terminating the process. |

---

## Step 3: Advanced Options (Optional)

### Custom Configuration File or Options
To start with a specific configuration file (e.g. `config-postgres.yaml`) or broker options, add the arguments after `--` in the `<arguments>` tag:

```xml
<arguments>/c "C:\Program Files\MonsterMQ\run.bat -- -config configs\config-postgres.yaml -log INFO"</arguments>
```

### Direct Java Execution (Bypassing `run.bat`)
If you prefer launching `java.exe` directly instead of `cmd.exe /c run.bat`:

```xml
<service>
  <id>MonsterMQ</id>
  <name>MonsterMQ Broker</name>
  <description>MonsterMQ MQTT Broker</description>

  <executable>java</executable>
  <arguments>-Xms512m -Xmx2g --enable-native-access=ALL-UNNAMED -classpath "target\classes;target\dependencies\*" at.rocworks.MonsterKt -config config.yaml</arguments>
  <workingdirectory>C:\Program Files\MonsterMQ</workingdirectory>

  <startmode>Automatic</startmode>

  <onfailure action="restart" delay="10 sec"/>
  <onfailure action="restart" delay="30 sec"/>
  <onfailure action="restart" delay="60 sec"/>

  <logpath>C:\Program Files\MonsterMQ\log</logpath>
  <log mode="roll"></log>

  <stoptimeout>30sec</stoptimeout>
</service>
```

### Explicit `JAVA_HOME` or Environment Variables
If Java is not in the system-wide `PATH` for the `LocalSystem` account, define environment variables inside `<service>`:

```xml
<env name="JAVA_HOME" value="C:\Program Files\Eclipse Adoptium\jdk-21.0.x" />
<env name="PATH" value="%JAVA_HOME%\bin;%PATH%" />
```

---

## Step 4: Install and Manage the Service

Open **PowerShell** or **Command Prompt** as **Administrator** and navigate to your MonsterMQ directory:

```powershell
cd "C:\Program Files\MonsterMQ"
```

### 1. Install the Service
```powershell
.\monstermq-service.exe install
```

### 2. Start the Service
```powershell
.\monstermq-service.exe start
```
*(Or via Windows standard commands: `net start MonsterMQ` or `Start-Service MonsterMQ`)*

### 3. Check Service Status
```powershell
.\monstermq-service.exe status
```
*(Or via PowerShell: `Get-Service MonsterMQ`)*

### 4. Stop the Service
```powershell
.\monstermq-service.exe stop
```
*(Or `net stop MonsterMQ` / `Stop-Service MonsterMQ`)*

### 5. Restart the Service
```powershell
.\monstermq-service.exe restart
```
*(Or `Restart-Service MonsterMQ`)*

### 6. Uninstall the Service
```powershell
.\monstermq-service.exe stop
.\monstermq-service.exe uninstall
```

---

## Step 5: Verify Broker Operation

1. **Check the Dashboard / GraphQL API:** Open `http://localhost:4000/` in your browser.
2. **Check Logs:**
   - **WinSW wrapper logs:** `C:\Program Files\MonsterMQ\log\monstermq-service.out.log` and `monstermq-service.err.log`
   - **MonsterMQ application logs:** `C:\Program Files\MonsterMQ\log\monstermq.log`
3. **Test MQTT Connectivity:**
   ```powershell
   # Using Mosquitto or any MQTT client:
   mosquitto_pub -h localhost -p 1883 -t "test/status" -m "Service running"
   ```

---

## Troubleshooting

### Service Fails to Start
- **Java Not in PATH:** When running as a service, the `LocalSystem` account uses system environment variables, not user environment variables. Ensure `JAVA_HOME` is set under **System Variables**, or explicitly configure `<env name="JAVA_HOME" ... />` in `monstermq-service.xml`.
- **Working Directory Incorrect:** Verify that `<workingdirectory>` points to the exact folder where MonsterMQ and its `target\` or `broker\` folders are located.
- **Port Conflict:** Check if port 1883 or 4000 is occupied by another process using `netstat -ano | findstr 1883`.

### Log Inspection
- If the service stops immediately after starting, inspect `log\monstermq-service.err.log` to see Java errors, class loading issues, or missing configuration files.
