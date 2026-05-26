#!/usr/bin/env python3
"""
Pytest integration test for In-Flight session metrics reporting.
Handles authentication fallbacks and avoids paho-mqtt v2 protocol bugs.
"""

import os
import time
import uuid
import threading
import pytest
import requests
import paho.mqtt.client as mqtt

pytestmark = [pytest.mark.graphql, pytest.mark.external, pytest.mark.integration]

GRAPHQL_URL = os.getenv("GRAPHQL_URL", "http://localhost:4000/graphql")
MQTT_HOST = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "Test")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "Test")


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


@pytest.fixture(scope="module", autouse=True)
def require_graphql():
    if not _broker_available():
        pytest.skip(f"GraphQL endpoint not reachable at {GRAPHQL_URL}")


def _graphql(query, variables=None, headers=None):
    request_headers = {"Content-Type": "application/json"}
    if headers:
        request_headers.update(headers)
    response = requests.post(
        GRAPHQL_URL,
        json={"query": query, "variables": variables or {}},
        headers=request_headers,
        timeout=10,
    )
    response.raise_for_status()
    result = response.json()
    assert "errors" not in result, f"GraphQL errors: {result['errors']}"
    return result


def _get_auth_headers():
    """Attempt to log in and get JWT token if user management is enabled."""
    candidates = [
        (os.getenv("GRAPHQL_USERNAME"), os.getenv("GRAPHQL_PASSWORD")),
        (os.getenv("MQTT_USERNAME"), os.getenv("MQTT_PASSWORD")),
        ("Admin", "Admin"),
        ("Test", "Test"),
        ("system", "manager")
    ]
    seen = set()
    for username, password in candidates:
        if not username or not password:
            continue
        key = (username, password)
        if key in seen:
            continue
        seen.add(key)
        try:
            response = requests.post(
                GRAPHQL_URL,
                json={
                    "query": """
                    mutation Login($username: String!, $password: String!) {
                        login(username: $username, password: $password) {
                            success
                            token
                        }
                    }
                    """,
                    "variables": {"username": username, "password": password}
                },
                timeout=3
            )
            if response.status_code == 200:
                data = response.json()
                login = data.get("data", {}).get("login") or {}
                token = login.get("token")
                if token:
                    print(f"[AUTH] Successfully logged in to GraphQL as '{username}'")
                    return {"Authorization": f"Bearer {token}"}
        except Exception as e:
            print(f"[AUTH] Failed login probe for '{username}': {e}")
    print("[AUTH] Continuing without auth headers (guest mode/anonymous)...")
    return {}


def test_inflight_metrics_reporting():
    """Test that SessionMetrics contains inFlightMessagesSnd and inFlightMessagesRcv and reports them correctly."""
    client_id = f"test_inflight_{uuid.uuid4().hex[:8]}"

    connack_event = threading.Event()
    suback_event = threading.Event()
    connection_rc = [None]

    def on_connect(c, userdata, flags, reason_code, properties=None):
        rc = reason_code.value if hasattr(reason_code, 'value') else reason_code
        print(f"\n[CLIENT] on_connect callback received: rc={rc}")
        connection_rc[0] = rc
        if rc == 0:
            connack_event.set()
        else:
            print(f"[CLIENT] Connection refused with return code: {rc}")
            connack_event.set()  # set to release the thread block and evaluate code

    def on_subscribe(c, userdata, mid, reason_codes, properties=None):
        print(f"[CLIENT] on_subscribe callback received: reason_codes={reason_codes}")
        suback_event.set()

    # Credentials candidates to try connecting to the broker TCP listener
    credentials_to_try = [
        (MQTT_USERNAME, MQTT_PASSWORD),
        (os.getenv("MQTT_ADMIN_USER", "Admin"), os.getenv("MQTT_ADMIN_PASS", "Admin")),
        ("Admin", "Admin"),
        ("Test", "Test"),
        ("", "")  # Anonymous connection
    ]

    client = None
    connected = False
    last_tried_user = ""

    # Probe connection credentials using MQTT v3.1.1 to bypass the paho-mqtt v5 CONNACK KeyError crash
    for user, password in credentials_to_try:
        connack_event.clear()
        connection_rc[0] = None
        last_tried_user = user

        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt.MQTTv311
        )
        client.on_connect = on_connect
        client.on_subscribe = on_subscribe

        if user:
            client.username_pw_set(user, password)

        user_label = user if user else "[Anonymous]"
        print(f"\nConnecting client {client_id} to {MQTT_HOST}:{MQTT_PORT} using user '{user_label}'...")
        try:
            client.connect(MQTT_HOST, MQTT_PORT)
            client.loop_start()

            # Wait for connection callback to fire
            if connack_event.wait(3.0) and connection_rc[0] == 0:
                print(f"[CLIENT] Successfully connected as user '{user_label}'!")
                connected = True
                break
            else:
                print(f"[CLIENT] Refused/Failed to connect as user '{user_label}' (rc={connection_rc[0]})")
                client.loop_stop()
                client.disconnect()
        except Exception as e:
            print(f"[CLIENT] Exception connecting as user '{user_label}': {e}")
            try:
                client.loop_stop()
                client.disconnect()
            except:
                pass

    assert connected, f"Failed to connect to broker using any credentials (last tried user: '{last_tried_user}', rc={connection_rc[0]})"

    try:
        # Subscribe to a unique topic
        topic = f"test/inflight/{uuid.uuid4().hex[:8]}"
        client.subscribe(topic, qos=1)

        # Wait for subscription acknowledgment
        assert suback_event.wait(5.0), "Timed out waiting for subscription to complete"
        time.sleep(0.2)  # allow broker state to settle

        # Log in to get GraphQL JWT headers
        headers = _get_auth_headers()

        # Query GraphQL for session metrics
        query = """
        query GetSessionMetrics($clientId: String!) {
            session(clientId: $clientId) {
                clientId
                connected
                metrics {
                    messagesIn
                    messagesOut
                    inFlightMessagesSnd
                    inFlightMessagesRcv
                }
            }
        }
        """

        print(f"Querying GraphQL for session: {client_id}...")
        result = _graphql(query, {"clientId": client_id}, headers=headers)
        session = result["data"]["session"]

        assert session is not None, f"Session with client ID {client_id} was not found"
        assert session["connected"] is True

        metrics = session["metrics"]
        assert len(metrics) > 0
        latest = metrics[0]

        # Verify that our newly added in-flight properties exist and are valid numbers
        assert "inFlightMessagesSnd" in latest
        assert "inFlightMessagesRcv" in latest
        assert latest["inFlightMessagesSnd"] is not None
        assert latest["inFlightMessagesRcv"] is not None

        # Verify default/idle values are correct
        assert latest["inFlightMessagesSnd"] == 0
        assert latest["inFlightMessagesRcv"] == 0

        print(f"✓ Verification Succeeded! In-Flight Metrics - Snd: {latest['inFlightMessagesSnd']}, Rcv: {latest['inFlightMessagesRcv']}")

    finally:
        client.loop_stop()
        client.disconnect()
        print("Disconnected test client.")
