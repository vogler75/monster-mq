---
name: test-developer
description: >
  Guide for writing Python integration tests for the MonsterMQ broker. Use this skill whenever
  the user wants to create, modify, or run test scripts for MonsterMQ. This covers MQTT protocol
  tests (v3.1.1 and v5), GraphQL API tests, OPC UA tests, database backend tests, and any
  other integration/functional testing. Trigger on mentions of "write a test", "add test cases",
  "test MQTT", "test GraphQL", "pytest", "paho-mqtt", "test script", "integration test",
  "test the broker", "verify behavior", or any work on files in the tests/ directory.
---

# MonsterMQ Test Development Skill

You are helping a developer write Python integration tests for the MonsterMQ broker.
Tests run against a live broker instance and verify end-to-end behavior.

## Test Environment

### Setup
```bash
cd tests
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Running Tests
```bash
cd tests

# Run all tests
pytest

# Run specific test categories
pytest mqtt3/           # MQTT v3.1.1 tests
pytest mqtt5/           # MQTT v5 tests
pytest graphql/         # GraphQL API tests
pytest opcua/           # OPC UA tests
pytest database/        # Database backend tests

# Run single file
pytest mqtt3/test_basic_pubsub.py -v

# Run single test
pytest mqtt3/test_basic_pubsub.py::test_basic_pubsub_qos0 -v

# Run by marker
pytest -m mqtt5
pytest -m "mqtt3 and not slow"
```

### Configuration
Tests connect to the broker using environment variables with defaults:
```
MQTT_BROKER=localhost    MQTT_PORT=1883
MQTT_USERNAME=Test       MQTT_PASSWORD=Test
GRAPHQL_URL=http://localhost:4000/graphql
```

### pytest.ini
Located at `tests/pytest.ini`. Key settings:
- Test paths: `mqtt3 mqtt5 opcua graphql database`
- Markers: `mqtt3`, `mqtt5`, `asyncio`, `retain`, `qos`, `properties`, `slow`, `integration`
- Output: verbose, short tracebacks, strict markers, color

## Project Structure

```
tests/
  conftest.py               # Shared fixtures (broker_config, mqtt_client, etc.)
  pytest.ini                # pytest configuration
  requirements.txt          # Dependencies (paho-mqtt, asyncua, requests, pytest plugins)
  mqtt3/
    test_basic_pubsub.py    # Basic pub/sub at QoS 0, 1
    test_basic_retained.py  # Retained message tests
    test_live_messaging.py  # Live message delivery
    test_mqtt_publish.py    # Publishing edge cases
    test_mqtt_publish_bulk_retained.py
    test_mqtt_publish_rejection.py    # ACL rejection tests
    test_mqtt_subscription_rejection.py
  mqtt5/
    test_mqtt5_connection.py          # MQTT5 connect properties
    test_mqtt5_properties.py          # User properties, content type, etc.
    test_mqtt5_reason_codes.py        # Reason codes in CONNACK, SUBACK, etc.
    test_mqtt5_topic_alias.py         # Topic alias support
    test_mqtt5_will_delay.py          # Will delay interval
    test_mqtt5_message_expiry.py      # Message expiry interval
    test_mqtt5_no_local.py            # No Local subscription option
    test_mqtt5_retain_handling.py     # Retain handling options
    test_mqtt5_flow_control.py        # Receive maximum
    test_mqtt5_request_response.py    # Request/response pattern
    test_mqtt5_rap_pytest.py          # Retain as Published
    ...
  graphql/
    test_graphql_publisher.py         # GraphQL publish mutation
    test_graphql_topic_subscriptions.py
    test_graphql_bulk_subscriptions.py
    test_bulk_e2e.py
    test_graphql_system_logs.py
  opcua/
    test_opcua_connection.py          # OPC UA server connection
    test_opcua_browse.py              # Node browsing
    test_opcua_subscription_basic.py  # Data subscriptions
    test_opcua_write.py               # Write operations
    test_node_ids.py
    test_display_names.py
    test_access_level_enforcement.py
    test_live_subscription.py
  database/
    test_all_backends_phase5.py       # Multi-backend tests
```

## Shared Fixtures (conftest.py)

The `tests/conftest.py` provides these fixtures:

### `broker_config`
Returns dict with `host`, `port`, `username`, `password`.

### `mqtt_client`
Provides a configured but **not connected** MQTT v5 client. Handles cleanup.

### `connected_client`
Provides a **connected** MQTT v5 client with loop started. Waits for CONNACK.

### `clean_topic`
Factory fixture that registers topic names and clears their retained messages after the test.
```python
def test_retained(connected_client, clean_topic):
    topic = clean_topic("test/my_retained_topic")
    connected_client.publish(topic, "hello", retain=True)
```

### `message_collector`
Instance of `MessageCollector` class with helpers:
- `.on_connect`, `.on_subscribe`, `.on_message` — assign as client callbacks
- `.wait_for_connection(timeout)`, `.wait_for_subscription(timeout)`, `.wait_for_messages(count, timeout)`
- `.messages` — list of received message dicts with `topic`, `payload`, `retain`, `qos`, `properties`

## Writing Tests

### MQTT v3.1.1 Tests

```python
import pytest
import paho.mqtt.client as mqtt
import threading
import time
import uuid

pytestmark = pytest.mark.mqtt3

def _make_client(client_id, broker_config):
    """Create MQTTv311 client with threading events for CONNACK and SUBACK."""
    unique_id = f"{client_id}_{uuid.uuid4().hex[:8]}"
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, unique_id, protocol=mqtt.MQTTv311)
    c.username_pw_set(broker_config["username"], broker_config["password"])
    c._connack_event = threading.Event()
    c._suback_event = threading.Event()

    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            client._connack_event.set()

    def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        client._suback_event.set()

    c.on_connect = on_connect
    c.on_subscribe = on_subscribe
    return c

def test_my_feature(broker_config):
    """Test description."""
    sub = _make_client("test_sub", broker_config)
    pub = _make_client("test_pub", broker_config)
    received = []

    def on_message(c, u, msg):
        received.append(msg.payload.decode())

    sub.on_message = on_message

    try:
        sub.connect(broker_config["host"], broker_config["port"])
        sub.loop_start()
        assert sub._connack_event.wait(5.0), "Subscriber failed to connect"

        sub.subscribe("test/topic", qos=1)
        assert sub._suback_event.wait(5.0), "Subscribe failed"
        time.sleep(0.5)

        pub.connect(broker_config["host"], broker_config["port"])
        pub.loop_start()
        assert pub._connack_event.wait(5.0), "Publisher failed to connect"

        pub.publish("test/topic", b"test payload", qos=1)
        time.sleep(2)

        assert len(received) > 0, "No messages received"
        assert received[0] == "test payload"
    finally:
        pub.loop_stop()
        pub.disconnect()
        sub.loop_stop()
        sub.disconnect()
```

### MQTT v5 Tests

Use the fixtures from conftest.py:

```python
import pytest
import time

pytestmark = pytest.mark.mqtt5

def test_v5_feature(connected_client, message_collector, clean_topic):
    """Test MQTT v5 feature."""
    topic = clean_topic("test/v5/feature")

    connected_client.on_message = message_collector.on_message
    connected_client.subscribe(topic, qos=1)
    time.sleep(0.5)

    connected_client.publish(topic, b"v5 test", qos=1)

    assert message_collector.wait_for_messages(1, timeout=3.0)
    msg = message_collector.messages[0]
    assert msg['payload'] == "v5 test"
```

For v5 properties:
```python
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

properties = Properties(PacketTypes.PUBLISH)
properties.MessageExpiryInterval = 60
properties.ContentType = "application/json"
connected_client.publish(topic, payload, qos=1, properties=properties)
```

### GraphQL Tests

```python
import requests
import pytest

GRAPHQL_URL = "http://localhost:4000/graphql"

def test_graphql_query():
    """Test GraphQL query."""
    query = """
    query {
        brokers {
            nodeId
            version
        }
    }
    """
    response = requests.post(GRAPHQL_URL, json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert "errors" not in data
    assert len(data["data"]["brokers"]) > 0
```

### OPC UA Tests

```python
import asyncio
from asyncua import Client

async def test_opcua_browse():
    """Test OPC UA node browsing."""
    client = Client(url="opc.tcp://localhost:4841/server", timeout=10)
    await client.connect()
    try:
        objects = client.get_objects_node()
        children = await objects.get_children()
        assert len(children) > 0
    finally:
        await client.disconnect()
```

## Test Conventions

1. **File naming**: `test_<feature>.py` — one file per feature area
2. **Test naming**: `test_<specific_behavior>` — descriptive, not generic
3. **Markers**: Apply `pytestmark = pytest.mark.<category>` at module level
4. **Unique client IDs**: Append UUID suffix to avoid collisions between tests
5. **Cleanup**: Always use try/finally to disconnect clients. Use `clean_topic` for retained messages.
6. **Timeouts**: Use reasonable waits (2-5s for message delivery). Don't use `time.sleep` without a reason — prefer event-based waiting (threading.Event, MessageCollector).
7. **Assertions**: Include descriptive failure messages: `assert condition, "what went wrong"`
8. **Print output**: Include `print()` statements for debugging — pytest captures them and shows on failure

## Dependencies

From `tests/requirements.txt`:
- `paho-mqtt>=2.0.0` — MQTT client (v2 callback API)
- `asyncua>=1.1.3,<2.0.0` — OPC UA client
- `requests` — HTTP/GraphQL tests
- `websockets` — WebSocket tests
- `pytest>=7.4.3` with plugins: `pytest-xdist`, `pytest-timeout`, `pytest-html`, `pytest-cov`, `pytest-mock`

## Key paho-mqtt v2 Notes

The project uses paho-mqtt v2 with `CallbackAPIVersion.VERSION2`:
- Callbacks have signature: `(client, userdata, flags, reason_code, properties)`
- `reason_code` may be a `ReasonCode` object — check `.value` attribute
- Use `mqtt.MQTTv5` or `mqtt.MQTTv311` for protocol version
- `client.subscribe()` returns `(result, mid)` — check `result == mqtt.MQTT_ERR_SUCCESS`
