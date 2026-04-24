#!/usr/bin/env python3
"""
End-to-end test for the MonsterMQ Flow Engine.

Ported from tests/flows/flow-test-combine-inputs.sh. Creates a flow class with
a JavaScript combiner node, wires it to two MQTT input topics and one output
topic via a flow instance, publishes to both inputs, and asserts the merged
JSON reaches the output.
"""

import json
import os
import time
import uuid

import paho.mqtt.client as mqtt
import pytest
import requests
from paho.mqtt.client import CallbackAPIVersion


pytestmark = [
    pytest.mark.graphql,
    pytest.mark.external,
    pytest.mark.integration,
    pytest.mark.flow,
]

GRAPHQL_URL = os.getenv("GRAPHQL_URL", "http://localhost:4000/graphql")
GRAPHQL_USERNAME = os.getenv("GRAPHQL_USERNAME", os.getenv("MQTT_USERNAME", "Admin"))
GRAPHQL_PASSWORD = os.getenv("GRAPHQL_PASSWORD", os.getenv("MQTT_PASSWORD", "Admin"))
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "Test")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "Test")
REQUEST_TIMEOUT = 10

COMBINER_SCRIPT = """
if (inputs.input1 && inputs.input2) {
  const combined = {
    input1: inputs.input1.value,
    input2: inputs.input2.value,
    timestamp: Date.now(),
    combined_at: new Date().toISOString()
  };
  outputs.send("output", combined);
}
"""


def _graphql(query, variables=None, headers=None, allow_errors=False):
    request_headers = {"Content-Type": "application/json"}
    if headers:
        request_headers.update(headers)
    response = requests.post(
        GRAPHQL_URL,
        json={"query": query, "variables": variables or {}},
        headers=request_headers,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    result = response.json()
    if not allow_errors:
        assert "errors" not in result, f"GraphQL errors: {result['errors']}"
    return result


def _broker_available() -> bool:
    try:
        response = requests.post(
            GRAPHQL_URL,
            json={"query": "{ __typename }"},
            timeout=2,
        )
        return response.status_code == 200
    except Exception:
        return False


def _flow_engine_enabled() -> bool:
    try:
        data = _graphql("{ broker { nodeId enabledFeatures } }")["data"]["broker"]
        return "FlowEngine" in (data.get("enabledFeatures") or [])
    except Exception:
        return False


def _current_node_id() -> str:
    data = _graphql("{ broker { nodeId } }")["data"]["broker"]
    return data["nodeId"]


def _credential_candidates():
    candidates = [
        (os.getenv("GRAPHQL_USERNAME"), os.getenv("GRAPHQL_PASSWORD")),
        (os.getenv("MQTT_USERNAME"), os.getenv("MQTT_PASSWORD")),
        (GRAPHQL_USERNAME, GRAPHQL_PASSWORD),
        ("Admin", "Admin"),
        ("Test", "Test"),
    ]
    seen = set()
    for username, password in candidates:
        if not username or not password:
            continue
        key = (username, password)
        if key in seen:
            continue
        seen.add(key)
        yield key


@pytest.fixture(scope="module", autouse=True)
def require_flow_engine():
    if not _broker_available():
        pytest.skip(f"GraphQL endpoint not reachable at {GRAPHQL_URL}")
    if not _flow_engine_enabled():
        pytest.skip("FlowEngine feature is not enabled on this broker")


@pytest.fixture(scope="module")
def auth_headers():
    login_query = """
    mutation Login($username: String!, $password: String!) {
        login(username: $username, password: $password) { success token }
    }
    """
    for username, password in _credential_candidates():
        result = _graphql(
            login_query,
            {"username": username, "password": password},
            allow_errors=True,
        )
        login = result.get("data", {}).get("login") or {}
        token = login.get("token")
        if token:
            return {"Authorization": f"Bearer {token}"}
    return {}


@pytest.fixture
def combine_flow(auth_headers):
    run_id = uuid.uuid4().hex[:8]
    class_name = f"pytest-combine-{run_id}"
    instance_name = f"pytest-combine-inst-{run_id}"
    input1_topic = f"test/pytest/flow/{run_id}/input1"
    input2_topic = f"test/pytest/flow/{run_id}/input2"
    output_topic = f"test/pytest/flow/{run_id}/output"
    node_id = _current_node_id()

    created_class = False
    created_instance = False
    try:
        _graphql(
            """
            mutation CreateClass($input: FlowClassInput!) {
                flow { createClass(input: $input) { name } }
            }
            """,
            {
                "input": {
                    "name": class_name,
                    "namespace": "test",
                    "version": "1.0.0",
                    "description": "pytest combine-inputs",
                    "nodes": [
                        {
                            "id": "combiner",
                            "type": "function",
                            "name": "Input Combiner",
                            "language": "javascript",
                            "inputs": ["input1", "input2"],
                            "outputs": ["output"],
                            "config": {"script": COMBINER_SCRIPT},
                            "position": {"x": 100, "y": 100},
                        }
                    ],
                    "connections": [],
                }
            },
            headers=auth_headers,
        )
        created_class = True

        _graphql(
            """
            mutation CreateInstance($input: FlowInstanceInput!) {
                flow { createInstance(input: $input) { name } }
            }
            """,
            {
                "input": {
                    "name": instance_name,
                    "namespace": "test",
                    "nodeId": node_id,
                    "flowClassId": class_name,
                    "enabled": True,
                    "inputMappings": [
                        {
                            "nodeInput": "combiner.input1",
                            "type": "TOPIC",
                            "value": input1_topic,
                        },
                        {
                            "nodeInput": "combiner.input2",
                            "type": "TOPIC",
                            "value": input2_topic,
                        },
                    ],
                    "outputMappings": [
                        {"nodeOutput": "combiner.output", "topic": output_topic},
                    ],
                    "variables": {"description": "pytest instance"},
                }
            },
            headers=auth_headers,
        )
        created_instance = True

        yield {
            "class_name": class_name,
            "instance_name": instance_name,
            "input1": input1_topic,
            "input2": input2_topic,
            "output": output_topic,
        }
    finally:
        delete_mutation = (
            "mutation D($n: String!) { flow { %s(name: $n) } }"
        )
        if created_instance:
            _graphql(
                delete_mutation % "deleteInstance",
                {"n": instance_name},
                headers=auth_headers,
                allow_errors=True,
            )
        if created_class:
            _graphql(
                delete_mutation % "deleteClass",
                {"n": class_name},
                headers=auth_headers,
                allow_errors=True,
            )


def _connect_mqtt():
    state = {"connected": False}

    def on_connect(client, userdata, flags, reason_code, properties):
        state["connected"] = reason_code == 0 or (
            hasattr(reason_code, "value") and reason_code.value == 0
        )

    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5
    )
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.connect(MQTT_BROKER, MQTT_PORT)
    client.loop_start()
    deadline = time.time() + 5
    while not state["connected"] and time.time() < deadline:
        time.sleep(0.05)
    assert state["connected"], "MQTT client did not receive CONNACK within 5s"
    return client


def test_flow_combines_two_inputs_and_publishes_output(combine_flow):
    received = []
    sub_ready = {"ok": False}

    def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        def _ok(rc):
            return rc.value < 128 if hasattr(rc, "value") else rc == 0
        if isinstance(reason_code_list, list):
            sub_ready["ok"] = all(_ok(rc) for rc in reason_code_list)
        else:
            sub_ready["ok"] = _ok(reason_code_list)

    def on_message(client, userdata, msg):
        received.append(msg.payload.decode("utf-8"))

    subscriber = _connect_mqtt()
    subscriber.on_subscribe = on_subscribe
    subscriber.on_message = on_message
    subscriber.subscribe(combine_flow["output"], qos=1)

    publisher = _connect_mqtt()

    try:
        deadline = time.time() + 5
        while not sub_ready["ok"] and time.time() < deadline:
            time.sleep(0.05)
        assert sub_ready["ok"], "Subscriber never received SUBACK"

        marker1 = f"in1-{uuid.uuid4().hex[:8]}"
        marker2 = f"in2-{uuid.uuid4().hex[:8]}"

        # FlowEngine's internal subscriptions don't receive retained messages
        # (see SessionHandler.subscribeInternalClient), so retry publishing
        # live until the flow instance has finished subscribing and emits.
        deadline = time.time() + 20
        while not received and time.time() < deadline:
            publisher.publish(
                combine_flow["input1"], json.dumps({"v": marker1}), qos=1
            ).wait_for_publish(2)
            publisher.publish(
                combine_flow["input2"], json.dumps({"v": marker2}), qos=1
            ).wait_for_publish(2)
            inner_deadline = time.time() + 1.5
            while not received and time.time() < inner_deadline:
                time.sleep(0.1)
    finally:
        publisher.loop_stop()
        publisher.disconnect()
        subscriber.loop_stop()
        subscriber.disconnect()

    assert received, "Flow did not emit a combined output within 20s"

    body = json.loads(received[0])
    assert {"input1", "input2", "timestamp", "combined_at"}.issubset(body.keys()), body
    assert isinstance(body["timestamp"], (int, float))
    assert isinstance(body["combined_at"], str) and "T" in body["combined_at"]
    assert marker1 in json.dumps(body["input1"]), body["input1"]
    assert marker2 in json.dumps(body["input2"]), body["input2"]
