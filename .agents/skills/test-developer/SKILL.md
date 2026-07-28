---
name: test-developer
description: >
  Guide for writing Python integration tests for the MonsterMQ broker. Use this skill whenever
  the user wants to create, modify, or run test scripts for MonsterMQ. This covers MQTT protocol
  tests (v3.1.1 and v5), GraphQL API tests, OPC UA tests, database backend tests, flow engine tests,
  REST API tests, Kafka tests, and latency/queuing tests. Trigger on mentions of "write a test",
  "add test cases", "test MQTT", "test GraphQL", "pytest", "paho-mqtt", "test script",
  "integration test", "test the broker", "verify behavior", or any work on files in the tests/ directory.
---

# MonsterMQ Test Development Skill

This skill provides instructions, conventions, fixtures, and code examples for writing and executing Python integration tests against a running MonsterMQ broker.

---

## Test Environment Setup & Execution

### Setup Virtual Environment
```bash
cd tests
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Running Tests (`pytest`)
All integration tests are located in `tests/pytest_tests/`.

```bash
cd tests

# Run all pytest integration tests
pytest pytest_tests/

# Run specific test categories
pytest pytest_tests/mqtt3/          # MQTT v3.1.1 tests
pytest pytest_tests/mqtt5/          # MQTT v5 tests
pytest pytest_tests/graphql/        # GraphQL API tests
pytest pytest_tests/opcua/          # OPC UA integration tests
pytest pytest_tests/database/       # Database backend tests
pytest pytest_tests/flow/           # Flow engine tests
pytest pytest_tests/i3x/            # I3X API tests
pytest pytest_tests/kafka/          # Kafka bridge tests
pytest pytest_tests/queuing/        # Queue store tests
pytest pytest_tests/rest/           # REST API tests

# Run a single test file
pytest pytest_tests/mqtt3/test_basic_pubsub.py -v

# Run a specific test function
pytest pytest_tests/mqtt3/test_basic_pubsub.py::test_basic_pubsub_qos0 -v

# Run using convenience script
./run.sh
```

### Environment Configuration
Tests read connection settings from environment variables with sensible defaults:
```env
MQTT_BROKER=localhost
MQTT_PORT=1883
MQTT_USERNAME=Test
MQTT_PASSWORD=Test
GRAPHQL_URL=http://localhost:4000/graphql
```

---

## Directory Structure (`tests/pytest_tests/`)

```
tests/
  requirements.txt               # Dependencies (paho-mqtt, asyncua, requests, pytest)
  pytest.ini                     # Global pytest settings & markers
  run.sh                         # Test execution helper script
  pytest_tests/
    conftest.py                  # Shared fixtures (broker_config, mqtt_client, message_collector)
    mqtt3/                       # MQTT v3.1.1 protocol tests
      test_basic_pubsub.py
      test_basic_retained.py
      test_live_messaging.py
      test_mqtt_publish_rejection.py
      test_mqtt_subscription_rejection.py
    mqtt5/                       # MQTT v5 protocol & properties tests
      test_mqtt5_connection.py
      test_mqtt5_properties.py
      test_mqtt5_reason_codes.py
      test_mqtt5_topic_alias.py
      test_mqtt5_message_expiry.py
      test_mqtt5_no_local.py
      test_mqtt5_retain_handling.py
    graphql/                     # GraphQL query & mutation tests
      test_graphql_publisher.py
      test_graphql_topic_subscriptions.py
      test_graphql_bulk_subscriptions.py
      test_graphql_system_logs.py
    opcua/                       # OPC UA integration tests
      test_opcua_connection.py
      test_opcua_browse.py
      test_opcua_subscription_basic.py
      test_opcua_write.py
    database/                    # Multi-database backend tests (PostgreSQL, MongoDB, SQLite)
      test_all_backends_phase5.py
    flow/                        # Flow engine workflow tests
    i3x/                         # CESMII I3X manufacturing API tests
    kafka/                       # Kafka bridge tests
    queuing/                     # Message queue & offline client tests
    rest/                        # HTTP REST API & InfluxDB Line Protocol tests
    latency/                     # Throughput & latency benchmarking
```

---

## Shared Fixtures (`pytest_tests/conftest.py`)

### `broker_config`
Returns dictionary containing `host`, `port`, `username`, and `password`.

### `mqtt_client`
Provides an un-connected paho MQTT v5 client pre-configured with authentication and clean session. Automatically handles cleanup on tear-down.

### `connected_client`
Provides an actively connected MQTT v5 client with background event loop running.

### `clean_topic`
Factory fixture that registers topic names during a test and automatically deletes/clears retained messages from the broker after test completion.

```python
def test_retained_behavior(connected_client, clean_topic):
    topic = clean_topic("test/my_retained_topic")
    connected_client.publish(topic, b"hello", retain=True)
```

### `message_collector`
Instance of `MessageCollector` with helper methods:
- `.on_message` callback
- `.wait_for_messages(count=1, timeout=3.0)`: Blocks until expected count of messages arrive.
- `.messages`: List of received message dictionaries with `topic`, `payload`, `qos`, `retain`, `properties`.

---

## Writing Test Cases

### MQTT v3.1.1 Test Pattern
```python
import pytest
import paho.mqtt.client as mqtt
import threading
import uuid

pytestmark = pytest.mark.mqtt3

def _make_client(client_id, broker_config):
    unique_id = f"{client_id}_{uuid.uuid4().hex[:8]}"
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, unique_id, protocol=mqtt.MQTTv311)
    c.username_pw_set(broker_config["username"], broker_config["password"])
    c._connack_event = threading.Event()

    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            client._connack_event.set()

    c.on_connect = on_connect
    return c

def test_basic_publish(broker_config):
    client = _make_client("test_pub", broker_config)
    try:
        client.connect(broker_config["host"], broker_config["port"])
        client.loop_start()
        assert client._connack_event.wait(5.0), "Connection failed"
        res = client.publish("sensors/temp", b"23.5", qos=0)
        assert res.rc == mqtt.MQTT_ERR_SUCCESS
    finally:
        client.loop_stop()
        client.disconnect()
```

### MQTT v5 Test Pattern
```python
import pytest
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

pytestmark = pytest.mark.mqtt5

def test_user_properties(connected_client, message_collector, clean_topic):
    topic = clean_topic("test/v5/props")
    connected_client.on_message = message_collector.on_message
    connected_client.subscribe(topic, qos=1)

    props = Properties(PacketTypes.PUBLISH)
    props.UserProperty = [("source", "sensor1"), ("quality", "good")]

    connected_client.publish(topic, b"data", qos=1, properties=props)

    assert message_collector.wait_for_messages(1, timeout=3.0)
    msg = message_collector.messages[0]
    assert msg['payload'] == "data"
```

### GraphQL API Test Pattern
```python
import requests
import pytest

GRAPHQL_URL = "http://localhost:4000/graphql"

def test_query_broker_metrics():
    query = """
    query {
      broker {
        nodeId
        version
        isLeader
        metrics {
          messagesIn
          messagesOut
        }
      }
    }
    """
    resp = requests.post(GRAPHQL_URL, json={"query": query})
    assert resp.status_code == 200
    data = resp.json()
    assert "errors" not in data
    assert data["data"]["broker"]["version"] != ""
```

---

## Best Practices & Guidelines

1. **Clean Up State**: Always use `clean_topic` for retained messages and `try/finally` blocks to disconnect clients.
2. **Unique Client IDs**: Always generate random suffixes for client IDs (`uuid.uuid4().hex[:8]`) to avoid session collisions.
3. **Avoid Hardcoded Sleeping**: Use `threading.Event` or `message_collector.wait_for_messages()` instead of static `time.sleep()`.
4. **Targeted Pytest Markers**: Apply `pytestmark = pytest.mark.mqtt5` or `pytestmark = pytest.mark.graphql` at the module level.
