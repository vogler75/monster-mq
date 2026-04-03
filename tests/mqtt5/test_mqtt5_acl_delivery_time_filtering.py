#!/usr/bin/env python3
"""
MQTT v5 test for ACL delivery-time filtering (AclCheckOnSubscription: false).

Mirrors mqtt3/test_acl_delivery_time_filtering.py but uses MQTT v5 protocol
to exercise the separate v5 code path in MqttClient.subscribeHandler().

When AclCheckOnSubscription is false (Mosquitto-compatible mode):
- Wildcard subscriptions are accepted even if no ACL rule matches the filter
- Messages are filtered at delivery time against the concrete topic
- Clients only receive messages on topics their ACL rules allow
- Exact (non-wildcard) subscriptions are still checked at subscribe time
- MQTT v5 SUBACK uses reason codes (NOT_AUTHORIZED, etc.)

Prerequisites:
- Broker running locally on 1883 with UserManagement enabled
- AclCheckOnSubscription: false in config.yaml
- GraphQL API available for user/ACL setup

Testing Both Modes:
To test the default subscribe-time mode (AclCheckOnSubscription: true),
configure config.yaml accordingly and run the standard ACL tests. These tests
specifically validate delivery-time filtering behavior with AclCheckOnSubscription: false.
"""

import os
import time
import uuid
import threading
import pytest
import requests

import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion

pytestmark = [pytest.mark.mqtt5, pytest.mark.integration]

# Configuration
BROKER_HOST = os.getenv("MQTT_BROKER", "localhost")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
GRAPHQL_URL = os.getenv("GRAPHQL_URL", "http://localhost:4000/graphql")
ADMIN_USER = os.getenv("MQTT_ADMIN_USER", "Admin")
ADMIN_PASS = os.getenv("MQTT_ADMIN_PASS", "Admin")

# Test user credentials
TEST_USER = f"acl_v5_user_{uuid.uuid4().hex[:8]}"
TEST_PASS = "TestPass123"
ALLOWED_TOPIC_PATTERN = "acl_v5_test/#"


def _get_admin_token():
    """Get JWT token for Admin user."""
    login_query = """
        mutation($username: String!, $password: String!) {
            login(username: $username, password: $password) {
                token
            }
        }
    """
    payload = {
        "query": login_query,
        "variables": {"username": ADMIN_USER, "password": ADMIN_PASS}
    }
    resp = requests.post(GRAPHQL_URL, json=payload, timeout=10)
    resp.raise_for_status()
    body = resp.json()
    if "errors" in body:
        raise RuntimeError(f"GraphQL login error: {body['errors']}")
    return body["data"]["login"]["token"]


def _graphql(query, variables=None, token=None):
    """Execute a GraphQL query/mutation and return data or raise on error."""
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    resp = requests.post(GRAPHQL_URL, json=payload, headers=headers, timeout=10)
    resp.raise_for_status()
    body = resp.json()
    if "errors" in body:
        raise RuntimeError(f"GraphQL error: {body['errors']}")
    return body.get("data")


def _make_v5_client(client_id, username, password):
    """Create an MQTT v5 client with threading events."""
    unique_id = f"{client_id}_{uuid.uuid4().hex[:8]}"
    c = mqtt.Client(CallbackAPIVersion.VERSION2, unique_id, protocol=mqtt.MQTTv5)
    c.username_pw_set(username, password)
    c._connack_event = threading.Event()
    c._suback_event = threading.Event()
    c._suback_reason_codes = []

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0 or (hasattr(reason_code, 'value') and reason_code.value == 0):
            client._connack_event.set()

    def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        client._suback_reason_codes = reason_code_list if isinstance(reason_code_list, list) else [reason_code_list]
        client._suback_event.set()

    c.on_connect = on_connect
    c.on_subscribe = on_subscribe
    return c


def _is_granted(rc):
    """Check if a SUBACK reason code indicates success (QoS 0/1/2)."""
    val = rc.value if hasattr(rc, 'value') else rc
    return val < 128


@pytest.fixture(scope="module")
def setup_test_user():
    """Create a test user with a specific ACL rule via GraphQL. Cleanup after all tests."""
    try:
        requests.post(GRAPHQL_URL, json={"query": "{ __typename }"}, timeout=2)
    except Exception:
        pytest.skip(f"GraphQL endpoint not reachable at {GRAPHQL_URL}")

    token = _get_admin_token()

    _graphql("""
        mutation($input: CreateUserInput!) {
            user { createUser(input: $input) { success message } }
        }
    """, variables={
        "input": {
            "username": TEST_USER,
            "password": TEST_PASS,
            "enabled": True,
            "canSubscribe": True,
            "canPublish": True,
            "isAdmin": False,
        }
    }, token=token)

    _graphql("""
        mutation($input: CreateAclRuleInput!) {
            user { createAclRule(input: $input) { success message } }
        }
    """, variables={
        "input": {
            "username": TEST_USER,
            "topicPattern": ALLOWED_TOPIC_PATTERN,
            "canSubscribe": True,
            "canPublish": True,
            "priority": 0,
        }
    }, token=token)

    time.sleep(2)
    yield TEST_USER

    try:
        token = _get_admin_token()
        _graphql("""
            mutation($username: String!) {
                user { deleteUser(username: $username) { success message } }
            }
        """, variables={"username": TEST_USER}, token=token)
    except Exception as e:
        print(f"Warning: cleanup failed: {e}")


def test_v5_wildcard_subscription_accepted(setup_test_user, broker_config):
    """
    MQTT v5: subscribing to '#' should succeed when AclCheckOnSubscription=false.
    SUBACK should contain a granted QoS reason code, not NOT_AUTHORIZED.
    """
    client = _make_v5_client("v5_acl_wildcard", TEST_USER, TEST_PASS)
    try:
        client.connect(BROKER_HOST, BROKER_PORT)
        client.loop_start()
        assert client._connack_event.wait(5.0), "Failed to connect"

        client._suback_event.clear()
        client.subscribe("#", qos=1)
        assert client._suback_event.wait(5.0), "No SUBACK received"

        rcs = client._suback_reason_codes
        print(f"MQTT v5 SUBACK for '#': {rcs}")

        if _is_granted(rcs[0]):
            print("'#' subscription ACCEPTED via MQTT v5 — AclCheckOnSubscription is likely false")
        else:
            rc_val = rcs[0].value if hasattr(rcs[0], 'value') else rcs[0]
            print(f"'#' subscription REJECTED (reason code {rc_val}) — AclCheckOnSubscription is likely true")
            pytest.skip("AclCheckOnSubscription=true (default); v5 wildcard test not applicable")
    finally:
        client.loop_stop()
        client.disconnect()


def test_v5_delivery_filtering_allowed(setup_test_user, broker_config):
    """
    MQTT v5: subscribe to '#', publish to allowed topic — message should be delivered.
    """
    sub = _make_v5_client("v5_sub_allowed", TEST_USER, TEST_PASS)
    pub = _make_v5_client("v5_pub_allowed", ADMIN_USER, ADMIN_PASS)
    received = []

    def on_message(c, u, msg):
        received.append({"topic": msg.topic, "payload": msg.payload.decode()})

    sub.on_message = on_message

    try:
        sub.connect(BROKER_HOST, BROKER_PORT)
        sub.loop_start()
        assert sub._connack_event.wait(5.0), "Subscriber failed to connect"

        sub._suback_event.clear()
        sub.subscribe("#", qos=1)
        assert sub._suback_event.wait(5.0), "No SUBACK received"
        if not _is_granted(sub._suback_reason_codes[0]):
            pytest.skip("AclCheckOnSubscription=true; delivery-time filtering not active")

        time.sleep(0.5)

        pub.connect(BROKER_HOST, BROKER_PORT)
        pub.loop_start()
        assert pub._connack_event.wait(5.0), "Publisher failed to connect"

        allowed_topic = f"acl_v5_test/sensor_{uuid.uuid4().hex[:6]}"
        pub.publish(allowed_topic, b"v5_allowed", qos=1)
        time.sleep(2)

        matching = [m for m in received if m["topic"] == allowed_topic]
        assert len(matching) == 1, f"Expected 1 message on '{allowed_topic}', got {len(matching)}"
        assert matching[0]["payload"] == "v5_allowed"
        print(f"MQTT v5: allowed topic '{allowed_topic}' delivered correctly")
    finally:
        pub.loop_stop()
        pub.disconnect()
        sub.loop_stop()
        sub.disconnect()


def test_v5_delivery_filtering_denied(setup_test_user, broker_config):
    """
    MQTT v5: subscribe to '#', publish to denied topic — message should be dropped.
    Also publish to allowed topic as control to confirm delivery works.
    """
    sub = _make_v5_client("v5_sub_denied", TEST_USER, TEST_PASS)
    pub = _make_v5_client("v5_pub_denied", ADMIN_USER, ADMIN_PASS)
    received = []

    def on_message(c, u, msg):
        received.append({"topic": msg.topic, "payload": msg.payload.decode()})

    sub.on_message = on_message

    try:
        sub.connect(BROKER_HOST, BROKER_PORT)
        sub.loop_start()
        assert sub._connack_event.wait(5.0), "Subscriber failed to connect"

        sub._suback_event.clear()
        sub.subscribe("#", qos=1)
        assert sub._suback_event.wait(5.0), "No SUBACK received"
        if not _is_granted(sub._suback_reason_codes[0]):
            pytest.skip("AclCheckOnSubscription=true; delivery-time filtering not active")

        time.sleep(0.5)

        pub.connect(BROKER_HOST, BROKER_PORT)
        pub.loop_start()
        assert pub._connack_event.wait(5.0), "Publisher failed to connect"

        denied_topic = f"forbidden/v5_secret_{uuid.uuid4().hex[:6]}"
        pub.publish(denied_topic, b"v5_secret", qos=1)

        control_topic = f"acl_v5_test/control_{uuid.uuid4().hex[:6]}"
        pub.publish(control_topic, b"v5_control", qos=1)
        time.sleep(2)

        denied_msgs = [m for m in received if m["topic"] == denied_topic]
        assert len(denied_msgs) == 0, f"Should NOT have received '{denied_topic}', but got: {denied_msgs}"

        control_msgs = [m for m in received if m["topic"] == control_topic]
        assert len(control_msgs) == 1, f"Expected control message on '{control_topic}', got {len(control_msgs)}"

        print(f"MQTT v5: denied topic '{denied_topic}' correctly dropped")
        print(f"MQTT v5: allowed topic '{control_topic}' correctly delivered")
    finally:
        pub.loop_stop()
        pub.disconnect()
        sub.loop_stop()
        sub.disconnect()


def test_v5_exact_subscription_denied(setup_test_user, broker_config):
    """
    MQTT v5: exact subscription to unauthorized topic should return NOT_AUTHORIZED reason code.
    """
    client = _make_v5_client("v5_exact_denied", TEST_USER, TEST_PASS)
    try:
        client.connect(BROKER_HOST, BROKER_PORT)
        client.loop_start()
        assert client._connack_event.wait(5.0), "Failed to connect"

        client._suback_event.clear()
        client.subscribe("forbidden/exact/v5topic", qos=1)
        assert client._suback_event.wait(5.0), "No SUBACK received"

        rcs = client._suback_reason_codes
        print(f"MQTT v5 SUBACK for 'forbidden/exact/v5topic': {rcs}")

        assert not _is_granted(rcs[0]), \
            "Exact subscription to uncovered topic should be rejected even with AclCheckOnSubscription=false"
        print("MQTT v5: exact subscription to unauthorized topic correctly rejected")
    finally:
        client.loop_stop()
        client.disconnect()


def test_v5_exact_subscription_allowed(setup_test_user, broker_config):
    """
    MQTT v5: exact subscription to ACL-covered topic should succeed.
    """
    client = _make_v5_client("v5_exact_allowed", TEST_USER, TEST_PASS)
    try:
        client.connect(BROKER_HOST, BROKER_PORT)
        client.loop_start()
        assert client._connack_event.wait(5.0), "Failed to connect"

        client._suback_event.clear()
        client.subscribe("acl_v5_test/temperature", qos=1)
        assert client._suback_event.wait(5.0), "No SUBACK received"

        rcs = client._suback_reason_codes
        print(f"MQTT v5 SUBACK for 'acl_v5_test/temperature': {rcs}")

        assert _is_granted(rcs[0]), "Subscription to ACL-covered exact topic should be accepted"
        print("MQTT v5: exact subscription to authorized topic correctly accepted")
    finally:
        client.loop_stop()
        client.disconnect()


def test_v5_user_subscribe_flag_enforced(setup_test_user, broker_config):
    """
    MQTT v5: user with canSubscribe=false should still be denied wildcard subscriptions.
    """
    disabled_user = f"v5_disabled_{uuid.uuid4().hex[:8]}"
    try:
        token = _get_admin_token()
        _graphql("""
            mutation($input: CreateUserInput!) {
                user { createUser(input: $input) { success message } }
            }
        """, variables={
            "input": {
                "username": disabled_user,
                "password": TEST_PASS,
                "enabled": True,
                "canSubscribe": False,
                "canPublish": True,
                "isAdmin": False,
            }
        }, token=token)
        time.sleep(2)

        client = _make_v5_client("v5_disabled_sub", disabled_user, TEST_PASS)
        client.connect(BROKER_HOST, BROKER_PORT)
        client.loop_start()
        assert client._connack_event.wait(5.0), "Failed to connect"

        client._suback_event.clear()
        client.subscribe("#", qos=1)
        assert client._suback_event.wait(5.0), "No SUBACK received"

        rcs = client._suback_reason_codes
        print(f"MQTT v5 SUBACK for '#' with canSubscribe=false: {rcs}")

        assert not _is_granted(rcs[0]), \
            "Wildcard subscription should be denied when user canSubscribe=false"
        print("MQTT v5: user-level canSubscribe=false correctly enforced")

        client.loop_stop()
        client.disconnect()
    finally:
        try:
            token = _get_admin_token()
            _graphql("""
                mutation($username: String!) {
                    user { deleteUser(username: $username) { success message } }
                }
            """, variables={"username": disabled_user}, token=token)
        except Exception:
            pass

