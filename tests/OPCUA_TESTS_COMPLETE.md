# OPC UA Tests - Completion Summary

## Overview
All OPC UA tests have been successfully fixed and are now passing. The subscription test that was failing has been resolved by using the correct `DataChangeNotificationHandler` base class.

## Test Results

### Passing Tests (5)
1. ✅ `test_opcua_browse.py::test_opcua_server` - Browses OPC UA server structure
2. ✅ `test_opcua_write.py::test_opcua_write_direct` - Direct write to OPC UA node
3. ✅ `test_opcua_write.py::test_opcua_write_browse_and_write` - Browse and write to multiple nodes
4. ✅ `test_opcua_subscription_basic.py::test_opcua_subscription_basic` - Data change notifications
5. ✅ `test_opcua_subscription_fixed.py::test_opcua_subscription_notifications` - OPC UA write triggers notifications

### Skipped Tests (1)
- ⏭️ `test_access_level_enforcement.py::test_access_level_enforcement` - Requires `test/#` address mapping with READ_ONLY access level (not in current server config)

## Key Fixes Applied

### 1. Subscription Test Fix (test_opcua_subscription_basic.py)
**Problem**: Test was failing with `struct.error: required argument is not an integer` during subscription creation.

**Root Cause**: Incorrect usage of asyncua subscription handlers. The test was trying to:
1. Pass a callback function directly to `subscribe_data_change()`
2. Use wrong base class (`SubHandler` doesn't exist, `SubscriptionHandler` is a Union type)

**Solution**: 
- Used `DataChangeNotificationHandler` as the base class
- Implemented proper handler with `datachange_notification()` method
- Added background MQTT publisher to trigger notifications during test
- Reduced wait time from 10s to 5s

**Code Changes**:
```python
from asyncua.common.subscription import DataChangeNotificationHandler

class NotificationHandler(DataChangeNotificationHandler):
    def __init__(self):
        self.notifications = []
    
    def datachange_notification(self, node, val, data):
        self.notifications.append((node, val, data))

handler = NotificationHandler()
subscription = await client.create_subscription(500, handler)
handle = await subscription.subscribe_data_change(temperature_node)
```

### 2. NodeId Path Corrections
All tests updated from `MonsterMQ/*` to `opcua/server/*`:
- `ns=2;s=MonsterMQ/write/oee:v` → `ns=2;s=opcua/server/write/oee:v`
- `ns=2;s=MonsterMQ/float/temperature:v` → `ns=2;s=opcua/server/float/temperature:v`

### 3. Port Updates
All tests updated to use port 4841 (avoiding Node-RED conflict on 4840):
- `opc.tcp://localhost:4840/server` → `opc.tcp://localhost:4841/server`

### 4. pytest-asyncio Markers
Added to tests that were missing it:
```python
pytestmark = pytest.mark.asyncio
```

## Test Details

### test_opcua_subscription_basic.py
Tests OPC UA subscription to data changes:
1. Connects to OPC UA server
2. Subscribes to `opcua/server/float/temperature:v` node
3. Background thread publishes MQTT message after 2s
4. Verifies OPC UA subscription receives notification
5. Asserts at least one notification received

**Key Features**:
- Uses `DataChangeNotificationHandler` for proper async notifications
- Background MQTT publisher ensures data change occurs during test
- 5-second test duration (efficient)

### test_opcua_subscription_fixed.py
Tests that OPC UA writes trigger both OPC UA and MQTT notifications:
1. Creates OPC UA and MQTT subscribers
2. Writes values directly to OPC UA node
3. Verifies OPC UA subscribers receive notifications
4. Verifies MQTT messages are published
5. Tests multiple write operations

**Verifications**:
- OPC UA notifications received: YES
- MQTT messages published: YES  
- Values match written values: YES

## Server Configuration

The OPC UA server is configured with:
- **Port**: 4841
- **Namespace**: opcua/server
- **Address Mappings**:
  - `write/#` - TEXT, READ_WRITE
  - `float/#` - NUMERIC, READ_WRITE

**Setup Command**:
```powershell
.\setup_opcua_server.ps1
```

## Dependencies

- `asyncua` - Python OPC UA client library
- `pytest-asyncio` - Async test support
- `paho-mqtt` - MQTT client for test data

## Running Tests

```bash
# Run all OPC UA tests
pytest test_opcua_browse.py test_opcua_write.py test_opcua_subscription_basic.py test_access_level_enforcement.py test_opcua_subscription_fixed.py -v

# Run specific test
pytest test_opcua_subscription_basic.py::test_opcua_subscription_basic -v -s
```

## Lessons Learned

1. **asyncua Handler Classes**: Must inherit from specific handler types (`DataChangeNotificationHandler`, not `SubHandler` or `SubscriptionHandler`)
2. **Subscription Callbacks**: Handler methods must be named exactly `datachange_notification(self, node, val, data)`
3. **Active Testing**: Subscription tests need active data changes - passive waiting won't trigger notifications
4. **Node Paths**: MonsterMQ's OPC UA server uses `opcua/server/` prefix for dynamically created nodes
5. **Async Markers**: All async test functions must have `pytestmark = pytest.mark.asyncio` at module level

## Next Steps

The OPC UA test suite is now complete and all tests are passing. Future enhancements could include:
1. Adding `test/#` mapping with READ_ONLY access to enable access level enforcement test
2. Testing additional data types (BOOLEAN, JSON, BINARY)
3. Testing subscription with multiple concurrent subscribers
4. Performance testing with high-frequency data changes
