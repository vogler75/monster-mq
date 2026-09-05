# Running MonsterMQ as a Windows service

[WinSW](https://github.com/winsw/winsw) can run the Java broker as a Windows service.
This example uses the WinSW 2.x XML format and a broker built from source. It needs
Java 21+, an administrator terminal for service installation, and a writable
configuration/data directory accessible to the service account.

## Prepare the broker

Build the broker as described in [Installation](installation.md). The example
assumes `C:\monstermq` contains `config.yaml`, `target\classes`, and
`target\dependencies`. If using a release distribution, use the classpath or JAR
layout from that distribution's `run.bat` instead. Test the Java command in a
terminal before installing the service.

Download the appropriate WinSW binary from its
[releases](https://github.com/winsw/winsw/releases), rename it to
`monstermq-service.exe`, and place it beside `monstermq-service.xml`.
The executable and XML basenames must match.

## Service configuration

Replace the Java path below with the installed `java.exe` path:

```xml
<service>
  <id>MonsterMQ</id>
  <name>MonsterMQ Broker</name>
  <description>MonsterMQ MQTT Broker</description>
  <executable>C:\Program Files\Eclipse Adoptium\jdk-21\bin\java.exe</executable>
  <arguments>-Xms512m -Xmx2g --enable-native-access=ALL-UNNAMED -classpath "target\classes;target\dependencies\*" at.rocworks.MonsterKt -config config.yaml</arguments>
  <workingdirectory>C:\monstermq</workingdirectory>
  <startmode>Automatic</startmode>
  <onfailure action="restart" delay="10 sec"/>
  <onfailure action="restart" delay="30 sec"/>
  <onfailure action="restart" delay="60 sec"/>
  <logpath>C:\monstermq\log</logpath>
  <log mode="roll-by-size">
    <sizeThreshold>10240</sizeThreshold>
    <keepFiles>8</keepFiles>
  </log>
  <stoptimeout>30sec</stoptimeout>
</service>
```

The heap sizes are examples; size them for the workload. Relative broker paths
resolve from `workingdirectory`. Give the service account access to that directory,
SQLite data, certificates, and any configured external resources. A service may
have a different PATH and environment from your interactive login.

For delayed startup, keep `startmode` as `Automatic` and add
`<delayedAutoStart>true</delayedAutoStart>`; `Delayed` is not a start mode.
`roll-by-size` rotates at the configured threshold in KB; plain `roll` only moves
logs at startup. See the [WinSW 2.x XML reference](https://github.com/winsw/winsw/blob/v2.12.0/doc/xmlConfigFile.md)
for these settings and service-account configuration.

Broker arguments such as `-cluster` or `-log INFO` go directly after the main
class in this Java command. The `--` separator belongs to the repository's
`run.bat` wrapper and is not needed for direct Java execution.

## Install and manage

In an administrator PowerShell terminal:

```powershell
Set-Location C:\monstermq
.\monstermq-service.exe install
.\monstermq-service.exe start
.\monstermq-service.exe status
```

Subsequent operations:

```powershell
.\monstermq-service.exe restart
.\monstermq-service.exe stop
```

To remove the service registration while retaining broker files and data:

```powershell
.\monstermq-service.exe stop
.\monstermq-service.exe uninstall
```

## Verify and troubleshoot

Open `http://localhost:4000/` when GraphQL/dashboard is enabled. Check
`log\monstermq-service.wrapper.log`, `log\monstermq-service.out.log`, and
`log\monstermq-service.err.log`. MonsterMQ's default application logger writes to
the console, which the wrapper captures; a separate `monstermq.log` is not created
by the default logging configuration.

Subscribe before publishing a connectivity test, in two terminals:

```powershell
mosquitto_sub -h localhost -p 1883 -t "test/status"
```

```powershell
mosquitto_pub -h localhost -p 1883 -t "test/status" -m "Service running"
```

Add MQTT credentials if required. For startup failures, check the Java executable,
classpath layout, working directory permissions, configuration errors, and port
conflicts (`Get-NetTCPConnection -LocalPort 1883,4000`).
