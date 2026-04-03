#!/usr/bin/env python3
"""
Test ACL delivery-time filtering (AclCheckOnSubscription: false).

When AclCheckOnSubscription is false (Mosquitto-compatible mode):
- Wildcard subscriptions are accepted even if no ACL rule matches the filter
- Messages are filtered at delivery time against the concrete topic
- Clients only receive messages on topics their ACL rules allow
- Exact (non-wildcard) subscriptions are still checked at subscribe time

Prerequisites:
- Broker running locally on 1883 with UserManagement enabled
- AclCheckOnSubscription: false in config.yaml
- A test user with ACL rule allowing 'acl_test/#' (canSubscribe=true)
- GraphQL API available for user/ACL setup

The test suite creates its own test user and ACL rules via GraphQL,
then verifies filtering behavior over MQTT.
"""

import os
import sys
import time
import uuid
import threading
import pytest
import requests

import paho.mqtt.client as mqtt

pytestmark = [pytest.mark.mqtt3, pytest.mark.integration]

# Configuration
BROKER_HOST = os.getenv("MQTT_BROKER", "localhost")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
GRAPHQL_URL = os.getenv("GRAPHQL_URL", "http://localhost:4000/graphql")
ADMIN_USER = os.getenv("MQTT_ADMIN_USER", "Admin")
ADMIN_PASS = os.getenv("MQTT_ADMIN_PASS", "Admin")

# Test user credentials
TEST_USER = f"acl_test_user_{uuid.uuid4().hex[:8]}"
TEST_PASS = "TestPass123"
ALLOWED_TOPIC_PATTERN = "acl_test/#"


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


def _make_client(client_id, username, password):
    """Create an MQTTv311 client with threading events."""
    unique_id = f"{client_id}_{uuid.uuid4().hex[:8]}"
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, unique_id, protocol=mqtt.MQTTv311)
    c.username_pw_set(username, password)
    c._connack_event = threading.Event()
    c._suback_event = threading.Event()
    c._suback_results = []

    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            client._connack_event.set()

    def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        client._suback_results = reason_code_list if isinstance(reason_code_list, list) else [reason_code_list]
        client._suback_event.set()

    c.on_connect = on_connect
    c.on_subscribe = on_subscribe
    return c


@pytest.fixture(scope="module")
def setup_test_user():
    """Create a test user with a specific ACL rule via GraphQL. Cleanup after all tests."""
    # Check GraphQL is reachable
    try:
        requests.post(GRAPHQL_URL, json={"query": "{ __typename }"}, timeout=2)
    except Exception:
        pytest.skip(f"GraphQL endpoint not reachable at {GRAPHQL_URL}")

    # Get admin token
    token = _get_admin_token()

    # Create test user
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
    print(f"Created test user: {TEST_USER}")

    # Create ACL rule: allow subscribe to acl_test/#
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
    print(f"Created ACL rule: {ALLOWED_TOPIC_PATTERN} for {TEST_USER}")

    # Wait for ACL cache to pick up the new user/rule
    time.sleep(2)

    yield TEST_USER

    # Cleanup: delete user (cascades ACL rules)
    try:
        token = _get_admin_token()
        _graphql("""
            mutation($username: String!) {
                user { deleteUser(username: $username) { success message } }
            }
        """, variables={"username": TEST_USER}, token=token)
        print(f"Deleted test user: {TEST_USER}")
    except Exception as e:
        print(f"Warning: cleanup failed: {e}")


def test_wildcard_subscription_accepted(setup_test_user, broker_config):
    """
    When AclCheckOnSubscription=false, subscribing to '#' should succeed
    even though no ACL rule explicitly covers '#'.

    If AclCheckOnSubscription=true (default), this subscription would be denied.
    The test detects which mode is active and validates accordingly.
    """
    client = _make_client("acl_wildcard_sub", TEST_USER, TEST_PASS)
    try:
        client.connect(BROKER_HOST, BROKER_PORT)
        client.loop_start()
        assert client._connack_event.wait(5.0), "Failed to connect"

        client._suback_event.clear()
        client.subscribe("#", qos=0)
        assert client._suback_event.wait(5.0), "No SUBACK received"

        results = client._suback_results
        print(f"SUBACK for '#': {results}")

        # Determine if subscription was accepted
        # In paho v2: reason codes are ReasonCode objects or ints
        def _granted(rc):
            val = rc.value if hasattr(rc, 'value') else rc
            return val < 128

        if _granted(results[0]):
            print("'#' subscription ACCEPTED — AclCheckOnSubscription is likely false")
        else:
            print("'#' subscription REJECTED — AclCheckOnSubscription is likely true (default)")
            pytest.skip("AclCheckOnSubscription=true (default); wildcard test not applicable")
    finally:
        client.loop_stop()
        client.disconnect()


def test_delivery_time_filtering_allowed_topic(setup_test_user, broker_config):
    """
    With AclCheckOnSubscription=false, subscribe to '#' and verify that
    messages on allowed topics (matching acl_test/#) ARE delivered.
    """
    sub = _make_client("acl_sub_allowed", TEST_USER, TEST_PASS)
    pub = _make_client("acl_pub", ADMIN_USER, ADMIN_PASS)
    received = []

    def on_message(c, u, msg):
        received.append({"topic": msg.topic, "payload": msg.payload.decode()})

    sub.on_message = on_message

    try:
        sub.connect(BROKER_HOST, BROKER_PORT)
        sub.loop_start()
        assert sub._connack_event.wait(5.0), "Subscriber failed to connect"

        sub._suback_event.clear()
        sub.subscribe("#", qos=0)
        assert sub._suback_event.wait(5.0), "No SUBACK received"

        results = sub._suback_results
        def _granted(rc):
            val = rc.value if hasattr(rc, 'value') else rc
            return val < 128

        if not _granted(results[0]):
            pytest.skip("AclCheckOnSubscription=true; delivery-time filtering not active")

        time.sleep(0.5)

        # Publish via admin to an allowed topic
        pub.connect(BROKER_HOST, BROKER_PORT)
        pub.loop_start()
        assert pub._connack_event.wait(5.0), "Publisher failed to connect"

        allowed_topic = f"acl_test/sensor_{uuid.uuid4().hex[:6]}"
        pub.publish(allowed_topic, b"allowed_payload", qos=0)
        time.sleep(2)

        # Should receive the message (ACL allows acl_test/#)
        matching = [m for m in received if m["topic"] == allowed_topic]
        assert len(matching) == 1, f"Expected 1 message on allowed topic '{allowed_topic}', got {len(matching)}. All received: {received}"
        assert matching[0]["payload"] == "allowed_payload"
        print(f"Message on allowed topic '{allowed_topic}' delivered correctly")
    finally:
        pub.loop_stop()
        pub.disconnect()
        sub.loop_stop()
        sub.disconnect()


def test_delivery_time_filtering_denied_topic(setup_test_user, broker_config):
    """
    With AclCheckOnSubscription=false, subscribe to '#' and verify that
    messages on denied topics (NOT matching acl_test/#) are silently dropped.
    """
    sub = _make_client("acl_sub_denied", TEST_USER, TEST_PASS)
    pub = _make_client("acl_pub_denied", ADMIN_USER, ADMIN_PASS)
    received = []

    def on_message(c, u, msg):
        received.append({"topic": msg.topic, "payload": msg.payload.decode()})

    sub.on_message = on_message

    try:
        sub.connect(BROKER_HOST, BROKER_PORT)
        sub.loop_start()
        assert sub._connack_event.wait(5.0), "Subscriber failed to connect"

        sub._suback_event.clear()
        sub.subscribe("#", qos=0)
        assert sub._suback_event.wait(5.0), "No SUBACK received"

        results = sub._suback_results
        def _granted(rc):
            val = rc.value if hasattr(rc, 'value') else rc
            return val < 128

        if not _granted(results[0]):
            pytest.skip("AclCheckOnSubscription=true; delivery-time filtering not active")

        time.sleep(0.5)

        # Publish via admin to a denied topic (not under acl_test/)
        pub.connect(BROKER_HOST, BROKER_PORT)
        pub.loop_start()
        assert pub._connack_event.wait(5.0), "Publisher failed to connect"

        denied_topic = f"forbidden/secret_{uuid.uuid4().hex[:6]}"
        pub.publish(denied_topic, b"secret_payload", qos=0)

        # Also publish to an allowed topic as a control
        control_topic = f"acl_test/control_{uuid.uuid4().hex[:6]}"
        pub.publish(control_topic, b"control_payload", qos=0)
        time.sleep(2)

        # Should NOT receive the denied topic message
        denied_msgs = [m for m in received if m["topic"] == denied_topic]
        assert len(denied_msgs) == 0, f"Should NOT have received message on denied topic '{denied_topic}', but got: {denied_msgs}"

        # Should receive the control (allowed) topic message
        control_msgs = [m for m in received if m["topic"] == control_topic]
        assert len(control_msgs) == 1, f"Expected control message on '{control_topic}', got {len(control_msgs)}"

        print(f"Message on denied topic '{denied_topic}' correctly dropped")
        print(f"Message on allowed topic '{control_topic}' correctly delivered")
    finally:
        pub.loop_stop()
        pub.disconnect()
        sub.loop_stop()
        sub.disconnect()


def test_exact_subscription_still_checked(setup_test_user, broker_config):
    """
    Even when AclCheckOnSubscription=false, exact (non-wildcard) subscriptions
    should still be ACL-checked at subscribe time.

    Subscribing to an exact topic NOT covered by ACL should be rejected.
    """
    client = _make_client("acl_exact_sub", TEST_USER, TEST_PASS)
    try:
        client.connect(BROKER_HOST, BROKER_PORT)
        client.loop_start()
        assert client._connack_event.wait(5.0), "Failed to connect"

        # Subscribe to an exact topic not covered by ACL
        client._suback_event.clear()
        client.subscribe("forbidden/exact/topic", qos=0)
        assert client._suback_event.wait(5.0), "No SUBACK received"

        results = client._suback_results
        print(f"SUBACK for 'forbidden/exact/topic': {results}")

        def _granted(rc):
            val = rc.value if hasattr(rc, 'value') else rc
            return val < 128

        # Exact topic not in ACL should be rejected regardless of AclCheckOnSubscription
        assert not _granted(results[0]), \
            "Exact subscription to uncovered topic should be rejected even with AclCheckOnSubscription=false"
        print("Exact subscription to unauthorized topic correctly rejected")
    finally:
        client.loop_stop()
        client.disconnect()


def test_allowed_exact_subscription_works(setup_test_user, broker_config):
    """
    Subscribing to an exact topic covered by ACL (acl_test/something) should succeed.
    """
    client = _make_client("acl_exact_allowed", TEST_USER, TEST_PASS)
    try:
        client.connect(BROKER_HOST, BROKER_PORT)
        client.loop_start()
        assert client._connack_event.wait(5.0), "Failed to connect"

        client._suback_event.clear()
        client.subscribe("acl_test/temperature", qos=0)
        assert client._suback_event.wait(5.0), "No SUBACK received"

        results = client._suback_results
        print(f"SUBACK for 'acl_test/temperature': {results}")

        def _granted(rc):
            val = rc.value if hasattr(rc, 'value') else rc
            return val < 128

        assert _granted(results[0]), "Subscription to ACL-covered exact topic should be accepted"
        print("Exact subscription to authorized topic correctly accepted")
    finally:
        client.loop_stop()
        client.disconnect()


def test_single_level_wildcard_delivery_filtering(setup_test_user, broker_config):
    """
    With AclCheckOnSubscription=false, subscribe to 'acl_test/+' and verify
    that single-level wildcard also gets delivery-time filtering for topics
    outside the ACL scope.
    """
    sub = _make_client("acl_sub_plus", TEST_USER, TEST_PASS)
    pub = _make_client("acl_pub_plus", ADMIN_USER, ADMIN_PASS)
    received = []

    def on_message(c, u, msg):
        received.append({"topic": msg.topic, "payload": msg.payload.decode()})

    sub.on_message = on_message

    try:
        sub.connect(BROKER_HOST, BROKER_PORT)
        sub.loop_start()
        assert sub._connack_event.wait(5.0), "Subscriber failed to connect"

        # Subscribe to acl_test/+ — this is within ACL scope (acl_test/#)
        # so it should be accepted in both modes
        sub._suback_event.clear()
        sub.subscribe("acl_test/+", qos=0)
        assert sub._suback_event.wait(5.0), "No SUBACK received"

        results = sub._suback_results
        def _granted(rc):
            val = rc.value if hasattr(rc, 'value') else rc
            return val < 128

        assert _granted(results[0]), "Subscription to 'acl_test/+' should be accepted (within ACL scope)"

        time.sleep(0.5)

        pub.connect(BROKER_HOST, BROKER_PORT)
        pub.loop_start()
        assert pub._connack_event.wait(5.0), "Publisher failed to connect"

        topic = f"acl_test/sensor_{uuid.uuid4().hex[:6]}"
        pub.publish(topic, b"plus_payload", qos=0)
        time.sleep(2)

        matching = [m for m in received if m["topic"] == topic]
        assert len(matching) == 1, f"Expected message on '{topic}', got {len(matching)}"
        print(f"Single-level wildcard delivery on '{topic}' works correctly")
    finally:
        pub.loop_stop()
        pub.disconnect()
        sub.loop_stop()
        sub.disconnect()


def test_user_subscribe_flag_still_enforced(setup_test_user, broker_config):
    """
    Even with AclCheckOnSubscription=false, a user with canSubscribe=false
    should still be denied wildcard subscriptions (user-level kill switch).
    """
    # Create a user with canSubscribe=false
    disabled_user = f"acl_disabled_{uuid.uuid4().hex[:8]}"
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
        time.sleep(2)  # Wait for cache refresh

        client = _make_client("acl_disabled_sub", disabled_user, TEST_PASS)
        client.connect(BROKER_HOST, BROKER_PORT)
        client.loop_start()
        assert client._connack_event.wait(5.0), "Failed to connect"

        client._suback_event.clear()
        client.subscribe("#", qos=0)
        assert client._suback_event.wait(5.0), "No SUBACK received"

        results = client._suback_results
        print(f"SUBACK for '#' with canSubscribe=false: {results}")

        def _granted(rc):
            val = rc.value if hasattr(rc, 'value') else rc
            return val < 128

        assert not _granted(results[0]), \
            "Wildcard subscription should be denied when user canSubscribe=false"
        print("User-level canSubscribe=false correctly enforced for wildcard subscription")

        client.loop_stop()
        client.disconnect()
    finally:
        # Cleanup
        try:
            token = _get_admin_token()
            _graphql("""
                mutation($username: String!) {
                    user { deleteUser(username: $username) { success message } }
                }
            """, variables={"username": disabled_user}, token=token)
        except Exception:
            pass
