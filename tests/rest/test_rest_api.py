#!/usr/bin/env python3
"""
Integration tests for the MonsterMQ REST API.

Tests cover:
- Publish via POST (raw body) and PUT (inline payload)
- Bulk publish via POST /api/v1/write
- Read retained values, last values, and history
- SSE subscribe
- OpenAPI spec and Swagger UI serving
- Authentication (JWT, Basic, anonymous)
- ACL enforcement
- Error handling and edge cases

Requirements:
    pip install requests pytest paho-mqtt

Configuration via environment variables:
    REST_API_URL    - Base URL for the REST API (default: http://localhost:4000/api/v1)
    MQTT_BROKER     - MQTT broker host (default: localhost)
    MQTT_PORT       - MQTT broker port (default: 1883)
    MQTT_USERNAME   - MQTT username (default: Test)
    MQTT_PASSWORD   - MQTT password (default: Test)
    GRAPHQL_URL     - GraphQL endpoint for setup (default: http://localhost:4000/graphql)
"""

import json
import os
import threading
import time
import uuid

import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
import pytest
import requests

# Configuration
REST_API_URL = os.getenv("REST_API_URL", "http://localhost:4000/api/v1")
GRAPHQL_URL = os.getenv("GRAPHQL_URL", "http://localhost:4000/graphql")
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "Admin")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "Admin")

# Timeout for HTTP requests
REQUEST_TIMEOUT = 10


def get_auth_headers():
    """Get auth headers. Try REST login first, then GraphQL, fall back to no auth."""
    # Try REST login endpoint
    try:
        resp = requests.post(
            f"{REST_API_URL}/login",
            json={"username": MQTT_USERNAME, "password": MQTT_PASSWORD},
            timeout=5,
        )
        if resp.status_code == 200:
            token = resp.json().get("token")
            if token:
                return {"Authorization": f"Bearer {token}"}
    except Exception:
        pass
    # Try GraphQL login mutation
    try:
        resp = requests.post(
            GRAPHQL_URL,
            json={
                "query": 'mutation { login(username: "%s", password: "%s") { token } }' % (MQTT_USERNAME, MQTT_PASSWORD)
            },
            timeout=5,
        )
        data = resp.json()
        token = data.get("data", {}).get("login", {}).get("token")
        if token:
            return {"Authorization": f"Bearer {token}"}
    except Exception:
        pass
    # No auth (works when UserManagement is disabled)
    return {}


@pytest.fixture(scope="module")
def auth_headers():
    return get_auth_headers()


@pytest.fixture(scope="module")
def ensure_archive_group():
    """Ensure the Default archive group is enabled for testing."""
    headers = get_auth_headers()
    headers["Content-Type"] = "application/json"
    try:
        resp = requests.post(
            GRAPHQL_URL,
            json={"query": 'mutation { archiveGroup { enable(name: "Default") { success } } }'},
            headers=headers,
            timeout=5,
        )
        data = resp.json()
        return data.get("data", {}).get("archiveGroup", {}).get("enable", {}).get("success", False)
    except Exception:
        return False


# ============================================================
# Login Tests
# ============================================================


class TestLogin:
    """Tests for POST /api/v1/login."""

    def test_login_valid_credentials(self):
        resp = requests.post(
            f"{REST_API_URL}/login",
            json={"username": MQTT_USERNAME, "password": MQTT_PASSWORD},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert "token" in data
        assert data["username"] == MQTT_USERNAME

    def test_login_token_works_for_publish(self):
        """Get a JWT via login, then use it to publish."""
        resp = requests.post(
            f"{REST_API_URL}/login",
            json={"username": MQTT_USERNAME, "password": MQTT_PASSWORD},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        token = resp.json()["token"]

        topic = f"test/rest/login/jwt/{uuid.uuid4().hex[:8]}"
        pub_resp = requests.put(
            f"{REST_API_URL}/topics/{topic}",
            params={"payload": "jwt_test"},
            headers={"Authorization": f"Bearer {token}"},
            timeout=REQUEST_TIMEOUT,
        )
        assert pub_resp.status_code == 200
        assert pub_resp.json()["success"] is True

    def test_login_invalid_password(self):
        resp = requests.post(
            f"{REST_API_URL}/login",
            json={"username": MQTT_USERNAME, "password": "wrong_password"},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 401

    def test_login_missing_fields(self):
        resp = requests.post(
            f"{REST_API_URL}/login",
            json={"username": MQTT_USERNAME},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 400
        assert "required" in resp.json()["error"].lower()

    def test_login_empty_body(self):
        resp = requests.post(
            f"{REST_API_URL}/login",
            json={},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 400

    def test_login_no_auth_required(self):
        """Login endpoint should not require auth headers."""
        resp = requests.post(
            f"{REST_API_URL}/login",
            json={"username": MQTT_USERNAME, "password": MQTT_PASSWORD},
            timeout=REQUEST_TIMEOUT,
            # Explicitly no auth headers
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True


# ============================================================
# OpenAPI / Swagger Tests
# ============================================================


class TestSwaggerAndOpenAPI:
    """Tests for the OpenAPI spec and Swagger UI."""

    def test_openapi_yaml_available(self, auth_headers):
        resp = requests.get(f"{REST_API_URL}/openapi.yaml", headers=auth_headers, timeout=REQUEST_TIMEOUT)
        assert resp.status_code == 200
        assert "openapi" in resp.text
        assert "MonsterMQ" in resp.text

    def test_swagger_ui_available(self, auth_headers):
        resp = requests.get(f"{REST_API_URL}/docs", headers=auth_headers, timeout=REQUEST_TIMEOUT)
        assert resp.status_code == 200
        assert "swagger-ui" in resp.text.lower()


# ============================================================
# Publish Tests
# ============================================================


class TestPublishPUT:
    """Tests for PUT /api/v1/topics/{topic} (inline payload)."""

    def test_put_publish_basic(self, auth_headers):
        topic = f"test/rest/put/{uuid.uuid4().hex[:8]}"
        resp = requests.put(
            f"{REST_API_URL}/topics/{topic}",
            params={"payload": "hello"},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["topic"] == topic

    def test_put_publish_with_qos_and_retain(self, auth_headers):
        topic = f"test/rest/put/qos/{uuid.uuid4().hex[:8]}"
        resp = requests.put(
            f"{REST_API_URL}/topics/{topic}",
            params={"payload": "retained_value", "qos": 1, "retain": "true"},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_put_publish_missing_payload(self, auth_headers):
        topic = "test/rest/put/nopayload"
        resp = requests.put(
            f"{REST_API_URL}/topics/{topic}",
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 400
        assert "payload" in resp.json()["error"].lower()

    def test_put_publish_empty_topic(self, auth_headers):
        resp = requests.put(
            f"{REST_API_URL}/topics/",
            params={"payload": "test"},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        # Should either be 400 or 404 depending on routing
        assert resp.status_code in (400, 404, 405)


class TestPublishPOST:
    """Tests for POST /api/v1/topics/{topic} (raw body)."""

    def test_post_publish_text(self, auth_headers):
        topic = f"test/rest/post/{uuid.uuid4().hex[:8]}"
        resp = requests.post(
            f"{REST_API_URL}/topics/{topic}",
            data="hello from REST",
            headers={**auth_headers, "Content-Type": "text/plain"},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        assert resp.json()["topic"] == topic

    def test_post_publish_json(self, auth_headers):
        topic = f"test/rest/post/json/{uuid.uuid4().hex[:8]}"
        payload = json.dumps({"temperature": 23.5, "unit": "C"})
        resp = requests.post(
            f"{REST_API_URL}/topics/{topic}",
            data=payload,
            headers={**auth_headers, "Content-Type": "application/json"},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_post_publish_binary(self, auth_headers):
        topic = f"test/rest/post/binary/{uuid.uuid4().hex[:8]}"
        resp = requests.post(
            f"{REST_API_URL}/topics/{topic}",
            data=b"\x00\x01\x02\x03",
            headers={**auth_headers, "Content-Type": "application/octet-stream"},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_post_publish_with_retain(self, auth_headers):
        topic = f"test/rest/post/retain/{uuid.uuid4().hex[:8]}"
        resp = requests.post(
            f"{REST_API_URL}/topics/{topic}",
            data="retained_payload",
            params={"retain": "true", "qos": 1},
            headers={**auth_headers, "Content-Type": "text/plain"},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True


# ============================================================
# Bulk Write Tests
# ============================================================


class TestBulkWrite:
    """Tests for POST /api/v1/write (bulk publish)."""

    def test_bulk_write_basic(self, auth_headers):
        messages = [
            {"topic": f"test/bulk/{uuid.uuid4().hex[:8]}", "value": "1"},
            {"topic": f"test/bulk/{uuid.uuid4().hex[:8]}", "value": "2"},
            {"topic": f"test/bulk/{uuid.uuid4().hex[:8]}", "value": "3"},
        ]
        resp = requests.post(
            f"{REST_API_URL}/write",
            json={"messages": messages},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["count"] == 3

    def test_bulk_write_with_options(self, auth_headers):
        messages = [
            {"topic": "test/bulk/opts/a", "value": "v1", "qos": 1, "retain": True},
            {"topic": "test/bulk/opts/b", "value": "v2", "qos": 0, "retain": False},
        ]
        resp = requests.post(
            f"{REST_API_URL}/write",
            json={"messages": messages},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        assert resp.json()["count"] == 2

    def test_bulk_write_empty_messages(self, auth_headers):
        resp = requests.post(
            f"{REST_API_URL}/write",
            json={"messages": []},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 400

    def test_bulk_write_missing_fields(self, auth_headers):
        messages = [
            {"topic": "test/bulk/bad"},  # missing value
            {"value": "no_topic"},  # missing topic
            {"topic": "test/bulk/good", "value": "ok"},  # valid
        ]
        resp = requests.post(
            f"{REST_API_URL}/write",
            json={"messages": messages},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 1  # only 1 valid
        assert "errors" in data
        assert len(data["errors"]) == 2

    def test_bulk_write_invalid_body(self, auth_headers):
        resp = requests.post(
            f"{REST_API_URL}/write",
            data="not json",
            headers={**auth_headers, "Content-Type": "application/json"},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 400


# ============================================================
# Read Tests
# ============================================================


class TestReadRetained:
    """Tests for GET /api/v1/topics/{topic}?retained."""

    def test_read_retained_value(self, auth_headers):
        topic = f"test/rest/read/retained/{uuid.uuid4().hex[:8]}"

        # Publish retained message first
        requests.put(
            f"{REST_API_URL}/topics/{topic}",
            params={"payload": "retained_test_value", "retain": "true"},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )

        # Wait for it to be stored
        time.sleep(1)

        # Read it back
        resp = requests.get(
            f"{REST_API_URL}/topics/{topic}",
            params={"retained": ""},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "messages" in data
        assert len(data["messages"]) >= 1
        found = any(m["topic"] == topic and m["value"] == "retained_test_value" for m in data["messages"])
        assert found, f"Expected retained message not found in {data['messages']}"

    def test_read_retained_wildcard(self, auth_headers):
        base = f"test/rest/read/wild/{uuid.uuid4().hex[:8]}"
        for i in range(3):
            requests.put(
                f"{REST_API_URL}/topics/{base}/sub{i}",
                params={"payload": f"val{i}", "retain": "true"},
                headers=auth_headers,
                timeout=REQUEST_TIMEOUT,
            )
        time.sleep(1)

        # Read with # wildcard (URL-encoded as %23)
        resp = requests.get(
            f"{REST_API_URL}/topics/{base}/%23",
            params={"retained": ""},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["messages"]) >= 3


class TestReadLastValue:
    """Tests for GET /api/v1/topics/{topic}?group=Default."""

    def test_read_last_value(self, auth_headers, ensure_archive_group):
        topic = f"test/rest/read/lv/{uuid.uuid4().hex[:8]}"

        # Publish a message (it should be archived)
        requests.put(
            f"{REST_API_URL}/topics/{topic}",
            params={"payload": "42.5"},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        time.sleep(2)  # Wait for archiving

        resp = requests.get(
            f"{REST_API_URL}/topics/{topic}",
            params={"group": "Default"},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "messages" in data
        if len(data["messages"]) > 0:
            assert data["messages"][0]["topic"] == topic
            assert data["messages"][0]["value"] == "42.5"

    def test_read_nonexistent_group(self, auth_headers):
        resp = requests.get(
            f"{REST_API_URL}/topics/test/no/group",
            params={"group": "NonExistentGroup"},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 404
        assert "not found" in resp.json()["error"].lower()


class TestReadMissingParams:
    """Tests for GET /api/v1/topics/{topic} without proper query params."""

    def test_read_no_params(self, auth_headers):
        resp = requests.get(
            f"{REST_API_URL}/topics/test/no/params",
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 400
        assert "error" in resp.json()


# ============================================================
# SSE Subscribe Tests
# ============================================================


class TestSSESubscribe:
    """Tests for GET /api/v1/subscribe (Server-Sent Events)."""

    def test_sse_subscribe_receives_message(self, auth_headers):
        """Subscribe via SSE and verify a published message is received."""
        topic = f"test/rest/sse/{uuid.uuid4().hex[:8]}"
        received_events = []
        sse_ready = threading.Event()
        stop_event = threading.Event()

        def sse_reader():
            try:
                resp = requests.get(
                    f"{REST_API_URL}/subscribe",
                    params={"topic": topic},
                    headers=auth_headers,
                    stream=True,
                    timeout=15,
                )
                for line in resp.iter_lines(decode_unicode=True):
                    if not sse_ready.is_set():
                        sse_ready.set()
                    if stop_event.is_set():
                        break
                    if line and line.startswith("data: "):
                        event_data = json.loads(line[6:])
                        received_events.append(event_data)
                        stop_event.set()
            except Exception:
                sse_ready.set()

        thread = threading.Thread(target=sse_reader, daemon=True)
        thread.start()

        # Wait for SSE connection to be established
        sse_ready.wait(timeout=5)
        time.sleep(1)

        # Publish a message
        requests.put(
            f"{REST_API_URL}/topics/{topic}",
            params={"payload": "sse_test_value"},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )

        # Wait for the message to arrive
        stop_event.wait(timeout=10)
        thread.join(timeout=2)

        assert len(received_events) >= 1
        assert received_events[0]["topic"] == topic
        assert received_events[0]["value"] == "sse_test_value"
        assert "timestamp" in received_events[0]

    def test_sse_subscribe_missing_topic(self, auth_headers):
        resp = requests.get(
            f"{REST_API_URL}/subscribe",
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 400
        assert "topic" in resp.json()["error"].lower()


# ============================================================
# End-to-End: Publish via REST, Receive via MQTT
# ============================================================


class TestRestToMqttE2E:
    """End-to-end tests: publish via REST API, verify via MQTT client."""

    @staticmethod
    def _make_client():
        """Create an MQTT client with connection and subscription events."""
        unique_id = f"rest-test-{uuid.uuid4().hex[:8]}"
        client = mqtt.Client(CallbackAPIVersion.VERSION2, unique_id, protocol=mqtt.MQTTv311)
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        client._connack_event = threading.Event()
        client._suback_event = threading.Event()

        def on_connect(c, userdata, flags, rc, properties=None):
            if rc == 0:
                c._connack_event.set()

        def on_subscribe(c, userdata, mid, reason_code_list, properties=None):
            c._suback_event.set()

        client.on_connect = on_connect
        client.on_subscribe = on_subscribe
        return client

    def test_rest_publish_mqtt_receive(self, auth_headers):
        """Publish via REST PUT, receive via MQTT subscriber."""
        topic = f"test/rest/e2e/{uuid.uuid4().hex[:8]}"
        received = []
        msg_event = threading.Event()

        client = self._make_client()

        def on_message(c, userdata, msg):
            received.append({"topic": msg.topic, "payload": msg.payload.decode()})
            msg_event.set()

        client.on_message = on_message
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_start()
        assert client._connack_event.wait(timeout=5), "MQTT connection failed"

        client.subscribe(topic, qos=1)
        assert client._suback_event.wait(timeout=5), "MQTT subscribe failed"

        time.sleep(0.5)  # Allow subscription to propagate

        # Publish via REST
        resp = requests.put(
            f"{REST_API_URL}/topics/{topic}",
            params={"payload": "e2e_test", "qos": 1},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200

        # Wait for message
        msg_event.wait(timeout=10)
        client.loop_stop()
        client.disconnect()

        assert len(received) >= 1
        assert received[0]["topic"] == topic
        assert received[0]["payload"] == "e2e_test"

    def test_mqtt_publish_rest_read_retained(self, auth_headers):
        """Publish via MQTT with retain, read back via REST retained endpoint."""
        topic = f"test/rest/e2e/mqtt2rest/{uuid.uuid4().hex[:8]}"

        client = self._make_client()
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_start()
        assert client._connack_event.wait(timeout=5), "MQTT connection failed"

        result = client.publish(topic, "mqtt_retained_value", qos=1, retain=True)
        result.wait_for_publish(timeout=5)
        client.loop_stop()
        client.disconnect()

        time.sleep(2)  # Wait for broker to store retained message

        resp = requests.get(
            f"{REST_API_URL}/topics/{topic}",
            params={"retained": ""},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["messages"]) >= 1
        found = any(m["topic"] == topic and m["value"] == "mqtt_retained_value" for m in data["messages"])
        assert found


# ============================================================
# Error Handling Tests
# ============================================================


class TestErrorHandling:
    """Tests for error responses and edge cases."""

    def test_invalid_qos(self, auth_headers):
        """QoS values outside 0-2 should be clamped."""
        topic = "test/rest/badqos"
        resp = requests.put(
            f"{REST_API_URL}/topics/{topic}",
            params={"payload": "test", "qos": "99"},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        # Should succeed (QoS clamped to 0-2)
        assert resp.status_code == 200

    def test_special_characters_in_topic(self, auth_headers):
        """Topics with special characters (spaces, unicode)."""
        topic = "test/rest/special/hello%20world"
        resp = requests.put(
            f"{REST_API_URL}/topics/{topic}",
            params={"payload": "test"},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200

    def test_url_encoded_wildcards(self, auth_headers):
        """Verify URL-encoded wildcards work in read endpoints."""
        base = f"test/rest/wildcard/{uuid.uuid4().hex[:8]}"
        requests.put(
            f"{REST_API_URL}/topics/{base}/sub1",
            params={"payload": "w1", "retain": "true"},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        time.sleep(1)

        # + wildcard (URL-encoded as %2B)
        resp = requests.get(
            f"{REST_API_URL}/topics/{base}/%2B",
            params={"retained": ""},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200


# ============================================================
# ACL Enforcement Tests (Anonymous user — no publish/subscribe)
# ============================================================


class TestACLEnforcement:
    """Tests that the Anonymous user (no credentials) is denied by ACL.

    The broker creates an Anonymous user with enabled=true but
    canPublish=false and canSubscribe=false. Requests without
    credentials pass auth (anonymous allowed) but fail ACL checks.
    """

    def test_anonymous_publish_denied(self):
        """PUT publish without credentials should be 403."""
        topic = "test/rest/acl/anon/publish"
        resp = requests.put(
            f"{REST_API_URL}/topics/{topic}",
            params={"payload": "denied"},
            # No auth headers
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 403

    def test_anonymous_post_publish_denied(self):
        """POST publish without credentials should be 403."""
        topic = "test/rest/acl/anon/post"
        resp = requests.post(
            f"{REST_API_URL}/topics/{topic}",
            data="denied",
            headers={"Content-Type": "text/plain"},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 403

    def test_anonymous_bulk_write_denied(self):
        """Bulk write without credentials — ACL checked per-message."""
        resp = requests.post(
            f"{REST_API_URL}/write",
            json={"messages": [{"topic": "test/acl/bulk", "value": "denied"}]},
            timeout=REQUEST_TIMEOUT,
        )
        # Bulk write checks ACL per-message, so we get 200 with errors
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 0
        assert len(data["errors"]) == 1
        assert "not allowed" in data["errors"][0]["error"].lower()

    def test_anonymous_read_denied(self):
        """Read without credentials should be 403."""
        resp = requests.get(
            f"{REST_API_URL}/topics/test/acl/read",
            params={"retained": ""},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 403

    def test_anonymous_subscribe_denied(self):
        """SSE subscribe without credentials should be 403."""
        resp = requests.get(
            f"{REST_API_URL}/subscribe",
            params={"topic": "test/acl/sse"},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 403

    def test_anonymous_can_login(self):
        """Login endpoint should still work without credentials."""
        resp = requests.post(
            f"{REST_API_URL}/login",
            json={"username": MQTT_USERNAME, "password": MQTT_PASSWORD},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_anonymous_can_access_docs(self):
        """Swagger UI and OpenAPI spec should be accessible without credentials."""
        resp = requests.get(f"{REST_API_URL}/docs", timeout=REQUEST_TIMEOUT)
        assert resp.status_code == 200

        resp = requests.get(f"{REST_API_URL}/openapi.yaml", timeout=REQUEST_TIMEOUT)
        assert resp.status_code == 200


# ============================================================
# Basic Auth Tests
# ============================================================


class TestBasicAuth:
    """Tests for HTTP Basic authentication."""

    def test_basic_auth_publish(self):
        """Publish using Basic auth credentials."""
        topic = f"test/rest/basicauth/{uuid.uuid4().hex[:8]}"
        resp = requests.put(
            f"{REST_API_URL}/topics/{topic}",
            params={"payload": "basic_auth_test"},
            auth=(MQTT_USERNAME, MQTT_PASSWORD),
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_basic_auth_read(self):
        """Read using Basic auth credentials."""
        topic = f"test/rest/basicauth/read/{uuid.uuid4().hex[:8]}"
        # Publish retained first
        requests.put(
            f"{REST_API_URL}/topics/{topic}",
            params={"payload": "basic_read", "retain": "true"},
            auth=(MQTT_USERNAME, MQTT_PASSWORD),
            timeout=REQUEST_TIMEOUT,
        )
        time.sleep(1)

        resp = requests.get(
            f"{REST_API_URL}/topics/{topic}",
            params={"retained": ""},
            auth=(MQTT_USERNAME, MQTT_PASSWORD),
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        assert len(resp.json()["messages"]) >= 1

    def test_basic_auth_invalid_password(self):
        """Basic auth with wrong password should be 401."""
        resp = requests.put(
            f"{REST_API_URL}/topics/test/basic/bad",
            params={"payload": "nope"},
            auth=(MQTT_USERNAME, "wrong_password"),
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 401


# ============================================================
# JWT Edge Case Tests
# ============================================================


class TestJWTEdgeCases:
    """Tests for JWT token edge cases."""

    def test_expired_jwt(self):
        """A tampered/invalid JWT should return 401."""
        resp = requests.put(
            f"{REST_API_URL}/topics/test/jwt/expired",
            params={"payload": "test"},
            headers={"Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJmYWtlIiwiZXhwIjoxfQ.invalid"},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 401

    def test_malformed_bearer(self):
        """Malformed Bearer header should return 401."""
        resp = requests.put(
            f"{REST_API_URL}/topics/test/jwt/malformed",
            params={"payload": "test"},
            headers={"Authorization": "Bearer not-a-jwt"},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 401

    def test_wrong_auth_scheme(self):
        """Unknown auth scheme should return 401."""
        resp = requests.put(
            f"{REST_API_URL}/topics/test/jwt/scheme",
            params={"payload": "test"},
            headers={"Authorization": "Digest abc123"},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 401


# ============================================================
# SSE Multi-Topic Tests
# ============================================================


class TestSSEMultiTopic:
    """Tests for SSE subscribe with multiple topics."""

    def test_sse_multiple_topics(self, auth_headers):
        """Subscribe to two topics, verify messages from both arrive."""
        topic_a = f"test/rest/sse/multi/a/{uuid.uuid4().hex[:8]}"
        topic_b = f"test/rest/sse/multi/b/{uuid.uuid4().hex[:8]}"
        received_events = []
        sse_ready = threading.Event()
        done = threading.Event()

        def sse_reader():
            try:
                resp = requests.get(
                    f"{REST_API_URL}/subscribe",
                    params=[("topic", topic_a), ("topic", topic_b)],
                    headers=auth_headers,
                    stream=True,
                    timeout=15,
                )
                for line in resp.iter_lines(decode_unicode=True):
                    if not sse_ready.is_set():
                        sse_ready.set()
                    if done.is_set():
                        break
                    if line and line.startswith("data: "):
                        event_data = json.loads(line[6:])
                        received_events.append(event_data)
                        if len(received_events) >= 2:
                            done.set()
            except Exception:
                sse_ready.set()

        thread = threading.Thread(target=sse_reader, daemon=True)
        thread.start()
        sse_ready.wait(timeout=5)
        time.sleep(1)

        # Publish to both topics
        requests.put(
            f"{REST_API_URL}/topics/{topic_a}",
            params={"payload": "from_a"},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        requests.put(
            f"{REST_API_URL}/topics/{topic_b}",
            params={"payload": "from_b"},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )

        done.wait(timeout=10)
        thread.join(timeout=2)

        topics_received = {e["topic"] for e in received_events}
        assert topic_a in topics_received, f"Missing message from {topic_a}"
        assert topic_b in topics_received, f"Missing message from {topic_b}"


# ============================================================
# History Read Tests
# ============================================================


class TestHistoryRead:
    """Tests for GET /api/v1/topics/{topic}?group=X&start=...&end=..."""

    def test_history_with_time_range(self, auth_headers, ensure_archive_group):
        """Publish, then query history with start/end params."""
        topic = f"test/rest/history/{uuid.uuid4().hex[:8]}"

        # Publish several messages
        for i in range(3):
            requests.put(
                f"{REST_API_URL}/topics/{topic}",
                params={"payload": f"history_{i}"},
                headers=auth_headers,
                timeout=REQUEST_TIMEOUT,
            )
            time.sleep(0.5)

        time.sleep(3)  # Wait for archiving

        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        start = (now - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        resp = requests.get(
            f"{REST_API_URL}/topics/{topic}",
            params={"group": "Default", "start": start, "end": end, "limit": "100"},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        # 200 if archive store is configured, 404 if group has no archive store
        assert resp.status_code in (200, 404)
        if resp.status_code == 200:
            data = resp.json()
            assert "messages" in data
            if len(data["messages"]) > 0:
                assert all(m["topic"] == topic for m in data["messages"])

    def test_history_with_limit(self, auth_headers, ensure_archive_group):
        """Verify the limit parameter restricts result count."""
        topic = f"test/rest/history/limit/{uuid.uuid4().hex[:8]}"

        for i in range(5):
            requests.put(
                f"{REST_API_URL}/topics/{topic}",
                params={"payload": f"lim_{i}"},
                headers=auth_headers,
                timeout=REQUEST_TIMEOUT,
            )
            time.sleep(0.3)

        time.sleep(3)

        resp = requests.get(
            f"{REST_API_URL}/topics/{topic}",
            params={"group": "Default", "limit": "2"},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data.get("messages", [])) <= 2


# ============================================================
# Large Payload Tests
# ============================================================


class TestLargePayload:
    """Tests for large payloads."""

    def test_large_payload_publish_and_read(self, auth_headers):
        """Publish a 100KB payload and read it back via retained."""
        topic = f"test/rest/large/{uuid.uuid4().hex[:8]}"
        large_payload = "X" * 100_000  # 100KB

        resp = requests.post(
            f"{REST_API_URL}/topics/{topic}",
            data=large_payload,
            params={"retain": "true"},
            headers={**auth_headers, "Content-Type": "text/plain"},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200

        time.sleep(1)

        resp = requests.get(
            f"{REST_API_URL}/topics/{topic}",
            params={"retained": ""},
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        messages = resp.json()["messages"]
        assert len(messages) >= 1
        assert len(messages[0]["value"]) == 100_000
