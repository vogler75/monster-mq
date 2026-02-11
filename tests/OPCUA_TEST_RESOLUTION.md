# OPC UA Test Issue Resolution

## Root Cause Identified

**Port Conflict**: Port 4840 is already in use by Node-RED Docker container, preventing MonsterMQ's OPC UA server from binding.

```
[milo-netty-event-loop-5] WARN org.eclipse.milo.opcua.stack.server.UaStackServer - Bind failed for endpoint opc.tcp://BOXPC9590:4840/server
java.util.concurrent.CompletionException: java.net.BindException: Address already in use: bind
```

### Conflicting Process
```bash
docker ps | grep 4840
# node-red-node-red-1 container is mapping: 0.0.0.0:4840->4840/tcp
```

## Solution Implemented

### 1. Changed OPC UA Server Port
- **Old Port**: 4840 (conflicted with Node-RED)
- **New Port**: 4841 (available)
- **New Endpoint**: `opc.tcp://localhost:4841/server`

### 2. Updated Test Files
All OPC UA test files now use port 4841:
- [test_opcua_write.py](test_opcua_write.py) - Changed OPCUA_URL default
- [test_opcua_browse.py](test_opcua_browse.py) - Changed OPCUA_URL default
- [test_opcua_subscription_basic.py](test_opcua_subscription_basic.py) - Changed OPCUA_URL default
- [test_opcua_subscription_fixed.py](test_opcua_subscription_fixed.py) - Changed OPCUA_URL default
- [test_opcua_connection.py](test_opcua_connection.py) - Changed hardcoded URL

### 3. Added DeviceStore Configuration
Updated [broker/config.yaml](../broker/config.yaml) to include DeviceStore:

```yaml
# Device Store configuration (for OPC UA devices and servers)
DeviceStore:
  Type: SQLITE  # Options: SQLITE, POSTGRES, CRATEDB, MONGODB
```

**Note**: The OPC UA server GraphQL mutations require a configured DeviceStore to persist server configurations.

### 4. Fixed Setup Script
- Removed emoji characters causing PowerShell parse errors
- Changed port from 4840 to 4841
- Script: [setup_opcua_server.ps1](setup_opcua_server.ps1)

## Next Steps to Complete Setup

### 1. Restart Broker
The broker must be restarted for DeviceStore configuration to take effect:

```powershell
cd C:\Projects\monster-mq\broker
# Stop current broker process (Ctrl+C in java terminal)
.\run.bat
```

### 2. Run Setup Script
After broker restart, configure the OPC UA server:

```powershell
cd C:\Projects\monster-mq\tests
.\setup_opcua_server.ps1
```

This will:
- Delete any existing test-server
- Create new server on port 4841
- Add `write/#` address mapping (TEXT, READ_WRITE)
- Add `float/#` address mapping (NUMERIC, READ_WRITE)
- Verify configuration

### 3. Publish MQTT Messages
Nodes are created lazily when MQTT messages arrive:

```powershell
cd C:\Projects\monster-mq\tests
python -c "import paho.mqtt.client as mqtt; import time; c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2); c.username_pw_set('admin', 'public'); c.connect('localhost', 1883); c.loop_start(); time.sleep(1); c.publish('write/oee','100'); c.publish('float/temperature','23.5'); time.sleep(2); c.loop_stop(); c.disconnect()"
```

### 4. Test Connection
Verify OPC UA server is accessible:

```powershell
cd C:\Projects\monster-mq\tests
python test_opcua_connection.py
```

Expected output:
```
Connecting to opc.tcp://localhost:4841/server...
✓ Connected!
Root node: ...
Objects node: ...
```

### 5. Run OPC UA Tests
```powershell
pytest test_opcua_write.py -v
pytest test_opcua_browse.py -v
pytest test_opcua_subscription_basic.py -v
```

## Alternative: Stop Node-RED

If you don't need Node-RED's OPC UA server, you can reclaim port 4840:

```powershell
docker stop node-red-node-red-1
```

Then revert all test files back to port 4840 and recreate the MonsterMQ OPC UA server on the default port.

## Key Learnings

1. **Port Conflicts**: Always check for port availability before configuring services
   ```powershell
   netstat -ano | Select-String "4840"
   ```

2. **DeviceStore Required**: OPC UA server persistence requires explicit DeviceStore configuration in config.yaml

3. **Lazy Node Creation**: OPC UA nodes are created dynamically when MQTT messages are published to mapped topics

4. **Address Mappings**: Must be added after server creation via separate `addAddress` mutations (not supported in `create` config)

5. **GraphQL Schema**: The `OpcUaServerConfigInput` doesn't include `addressMappings` field - use `addAddress` mutation instead

## Verification Checklist

- [ ] Broker restarted with DeviceStore config
- [ ] setup_opcua_server.ps1 executed successfully
- [ ] MQTT messages published to create nodes
- [ ] test_opcua_connection.py passes
- [ ] test_opcua_write.py passes
- [ ] test_opcua_browse.py shows node structure
- [ ] All 62 tests passing

## Files Modified

### Configuration
- [broker/config.yaml](../broker/config.yaml) - Added DeviceStore configuration

### Test Files
- [test_opcua_write.py](test_opcua_write.py) - Port 4841, removed emojis
- [test_opcua_browse.py](test_opcua_browse.py) - Port 4841, added pytest-asyncio marker
- [test_opcua_subscription_basic.py](test_opcua_subscription_basic.py) - Port 4841
- [test_opcua_subscription_fixed.py](test_opcua_subscription_fixed.py) - Port 4841
- [test_opcua_connection.py](test_opcua_connection.py) - Port 4841

### Documentation & Scripts
- [OPCUA_SERVER_SETUP.md](OPCUA_SERVER_SETUP.md) - Comprehensive setup guide (created)
- [setup_opcua_server.ps1](setup_opcua_server.ps1) - Automated setup script (created, fixed)
- [OPCUA_TEST_RESOLUTION.md](OPCUA_TEST_RESOLUTION.md) - This file
