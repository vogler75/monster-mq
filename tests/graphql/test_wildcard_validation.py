#!/usr/bin/env python3
"""
Tests for wildcard character validation in MQTT topic names.

Per MQTT spec [MQTT-3.3.2-2]: Topic names in PUBLISH packets MUST NOT contain
wildcard characters (+ or #). These are only valid in subscription topic filters.

Tests cover three validation layers:
  1. GraphQL publishMessage mutation (publish via API)
  2. GraphQL device configuration mutations (save invalid config via API)
  3. MQTT protocol-level rejection (direct MQTT publish)

Prerequisites:
  - Broker running locally with MQTT port 1883 and GraphQL on 4000
  - Test/Test credentials OR UserManagement disabled
"""

import os
import threading
import time
import uuid

import paho.mqtt.client as mqtt
import pytest
import requests

GRAPHQL_URL = os.getenv("GRAPHQL_URL", "http://localhost:4000/graphql")
BROKER_HOST = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
USERNAME = os.getenv("MQTT_USERNAME", "Test")
PASSWORD = os.getenv("MQTT_PASSWORD", "Test")

pytestmark = pytest.mark.graphql


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _broker_available() -> bool:
    try:
        requests.post(GRAPHQL_URL, json={"query": "{ __typename }"}, timeout=2)
        return True
    except Exception:
        return False


@pytest.fixture(scope="module", autouse=True)
def require_broker():
    if not _broker_available():
        pytest.skip(f"Broker GraphQL endpoint not reachable at {GRAPHQL_URL}")


def _graphql(query: str, variables: dict = None) -> dict:
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    response = requests.post(
        GRAPHQL_URL,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


def _make_mqtt_client(client_id: str) -> mqtt.Client:
    c = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        f"{client_id}_{uuid.uuid4().hex[:8]}",
        protocol=mqtt.MQTTv311,
        clean_session=True,
    )
    c.username_pw_set(USERNAME, PASSWORD)
    c._connack_event = threading.Event()
    c._puback_event = threading.Event()
    c._puback_rc = None

    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            client._connack_event.set()

    def on_publish(client, userdata, mid, reason_code=None, properties=None):
        client._puback_rc = getattr(reason_code, "value", 0)
        client._puback_event.set()

    c.on_connect = on_connect
    c.on_publish = on_publish
    return c


# ---------------------------------------------------------------------------
# 1. GraphQL publishMessage mutation
# ---------------------------------------------------------------------------

PUBLISH_MUTATION = """
    mutation PublishMessage($input: PublishInput!) {
        publish(input: $input) {
            success
            topic
            error
        }
    }
"""


class TestGraphQLPublishWildcardValidation:
    """GraphQL publish mutation must reject wildcard topics."""

    def _publish(self, topic: str) -> dict:
        result = _graphql(PUBLISH_MUTATION, {"input": {"topic": topic, "payload": "test", "qos": 0}})
        return result.get("data", {}).get("publish", {})

    def test_publish_single_level_wildcard_rejected(self):
        """publish to 'sensor/+/temperature' must fail."""
        data = self._publish("sensor/+/temperature")
        assert data.get("success") is False, (
            f"Expected publish to 'sensor/+/temperature' to fail, got: {data}"
        )

    def test_publish_multi_level_wildcard_rejected(self):
        """publish to 'sensor/test/#' must fail."""
        data = self._publish("sensor/test/#")
        assert data.get("success") is False, (
            f"Expected publish to 'sensor/test/#' to fail, got: {data}"
        )

    def test_publish_wildcard_only_rejected(self):
        """publish to '#' must fail."""
        data = self._publish("#")
        assert data.get("success") is False, (
            f"Expected publish to '#' to fail, got: {data}"
        )

    def test_publish_mixed_wildcards_rejected(self):
        """publish to 'a/+/b/#' must fail."""
        data = self._publish("a/+/b/#")
        assert data.get("success") is False, (
            f"Expected publish to 'a/+/b/#' to fail, got: {data}"
        )

    def test_publish_valid_topic_succeeds(self):
        """publish to a normal topic must succeed."""
        topic = f"test/wildcard-validation/{uuid.uuid4().hex[:8]}"
        data = self._publish(topic)
        assert data.get("success") is True, (
            f"Expected publish to '{topic}' to succeed, got: {data}"
        )

    def test_publish_valid_topic_with_slash_succeeds(self):
        """publish to multi-level topic without wildcards must succeed."""
        topic = f"sensor/room1/temperature/{uuid.uuid4().hex[:8]}"
        data = self._publish(topic)
        assert data.get("success") is True, (
            f"Expected publish to '{topic}' to succeed, got: {data}"
        )


# ---------------------------------------------------------------------------
# 2. GraphQL device configuration mutations
# ---------------------------------------------------------------------------

class TestGraphQLMqttClientAddressValidation:
    """GraphQL addAddress mutation for MQTT Client must reject wildcard publish destinations."""

    DEVICE_NAME = "__wildcard_test_mqtt__"  # Nonexistent device – expected to fail at device lookup

    ADD_ADDRESS_MUTATION = """
        mutation AddMqttClientAddress($deviceName: String!, $input: MqttClientAddressInput!) {
            mqttClient {
                addAddress(deviceName: $deviceName, input: $input) {
                    success
                    errors
                }
            }
        }
    """

    def _add_address(self, mode: str, remote: str, local: str) -> dict:
        result = _graphql(self.ADD_ADDRESS_MUTATION, {
            "deviceName": self.DEVICE_NAME,
            "input": {
                "mode": mode,
                "remoteTopic": remote,
                "localTopic": local,
                "qos": 0,
                "removePath": False,
            }
        })
        return result.get("data", {}).get("mqttClient", {}).get("addAddress", {})

    def test_publish_mode_wildcard_remote_topic_rejected(self):
        """PUBLISH mode: remote topic is the destination - wildcards must be rejected."""
        data = self._add_address("PUBLISH", "sensor/+/temperature", "local/sensor/room1")
        assert data.get("success") is False, (
            f"Expected wildcard in PUBLISH remote topic to fail: {data}"
        )
        errors = data.get("errors", [])
        assert any("wildcard" in e.lower() or "remoteTopic" in e or "+" in e or "#" in e
                   for e in errors), f"Error message should mention wildcards: {errors}"

    def test_publish_mode_valid_topics_passes_to_device_check(self):
        """PUBLISH mode with valid topics should pass topic validation (may fail at device lookup)."""
        data = self._add_address("PUBLISH", "sensor/room1/temperature", "local/sensor/+/temp")
        # If device not found, we expect success=False but with device-not-found error, not wildcard error
        errors = data.get("errors", [])
        wildcard_error = any("wildcard" in e.lower() or "contain" in e.lower() for e in errors)
        assert not wildcard_error, (
            f"Valid topics should not trigger wildcard validation error: {errors}"
        )

    def test_subscribe_mode_wildcard_local_topic_rejected(self):
        """SUBSCRIBE mode: local topic is the destination - wildcards must be rejected."""
        data = self._add_address("SUBSCRIBE", "sensor/+/temperature", "local/sensor/#")
        assert data.get("success") is False, (
            f"Expected wildcard in SUBSCRIBE local topic to fail: {data}"
        )
        errors = data.get("errors", [])
        assert any("wildcard" in e.lower() or "localTopic" in e or "+" in e or "#" in e
                   for e in errors), f"Error message should mention wildcards: {errors}"

    def test_subscribe_mode_wildcard_remote_topic_allowed(self):
        """SUBSCRIBE mode: remote topic is the source - wildcards are allowed in subscription filters."""
        data = self._add_address("SUBSCRIBE", "sensor/+/temperature", "local/sensor/room1/temp")
        # Should NOT fail due to wildcard in remote topic
        errors = data.get("errors", [])
        wildcard_error = any("wildcard" in e.lower() and "remoteTopic" in e for e in errors)
        assert not wildcard_error, (
            f"Wildcards in SUBSCRIBE remote topic should be allowed: {errors}"
        )


class TestGraphQLRedisClientAddressValidation:
    """GraphQL addAddress for Redis client must reject wildcard MQTT topics."""

    DEVICE_NAME = "__wildcard_test_redis__"

    ADD_ADDRESS_MUTATION = """
        mutation AddRedisClientAddress($deviceName: String!, $input: RedisClientAddressInput!) {
            redisClient {
                addAddress(deviceName: $deviceName, input: $input) {
                    success
                    errors
                }
            }
        }
    """

    def _add_address(self, mqtt_topic: str) -> dict:
        result = _graphql(self.ADD_ADDRESS_MUTATION, {
            "deviceName": self.DEVICE_NAME,
            "input": {
                "mode": "SUBSCRIBE",
                "redisChannel": "test:channel",
                "mqttTopic": mqtt_topic,
                "qos": 0,
            }
        })
        return result.get("data", {}).get("redisClient", {}).get("addAddress", {})

    def test_single_level_wildcard_rejected(self):
        """Redis mqttTopic with '+' must be rejected."""
        data = self._add_address("sensor/+/temperature")
        errors = data.get("errors", [])
        wildcard_error = any("wildcard" in e.lower() or "+" in e for e in errors)
        assert wildcard_error, (
            f"Expected wildcard error for 'sensor/+/temperature': success={data.get('success')}, errors={errors}"
        )

    def test_multi_level_wildcard_rejected(self):
        """Redis mqttTopic with '#' must be rejected."""
        data = self._add_address("sensor/test/#")
        errors = data.get("errors", [])
        wildcard_error = any("wildcard" in e.lower() or "#" in e for e in errors)
        assert wildcard_error, (
            f"Expected wildcard error for 'sensor/test/#': success={data.get('success')}, errors={errors}"
        )

    def test_valid_topic_passes_to_device_check(self):
        """Valid Redis mqttTopic should not trigger wildcard error."""
        data = self._add_address("sensor/room1/temperature")
        errors = data.get("errors", [])
        wildcard_error = any("wildcard" in e.lower() for e in errors)
        assert not wildcard_error, (
            f"Unexpected wildcard error for valid topic: {errors}"
        )


class TestGraphQLNatsClientAddressValidation:
    """GraphQL addAddress for NATS client must reject wildcard MQTT topics in PUBLISH mode."""

    DEVICE_NAME = "__wildcard_test_nats__"

    ADD_ADDRESS_MUTATION = """
        mutation AddNatsClientAddress($deviceName: String!, $input: NatsClientAddressInput!) {
            natsClient {
                addAddress(deviceName: $deviceName, input: $input) {
                    success
                    errors
                }
            }
        }
    """

    def _add_address(self, mode: str, mqtt_topic: str) -> dict:
        result = _graphql(self.ADD_ADDRESS_MUTATION, {
            "deviceName": self.DEVICE_NAME,
            "input": {
                "mode": mode,
                "natsSubject": "test.subject",
                "mqttTopic": mqtt_topic,
                "qos": 0,
            }
        })
        return result.get("data", {}).get("natsClient", {}).get("addAddress", {})

    def test_publish_mode_wildcard_rejected(self):
        """NATS PUBLISH mode: mqttTopic is the destination - wildcards must be rejected."""
        data = self._add_address("PUBLISH", "sensor/+/temperature")
        errors = data.get("errors", [])
        wildcard_error = any("wildcard" in e.lower() or "+" in e for e in errors)
        assert wildcard_error, (
            f"Expected wildcard error in NATS PUBLISH mode: success={data.get('success')}, errors={errors}"
        )

    def test_subscribe_mode_wildcard_allowed(self):
        """NATS SUBSCRIBE mode: mqttTopic is the subscription source - wildcards are allowed."""
        data = self._add_address("SUBSCRIBE", "sensor/+/temperature")
        errors = data.get("errors", [])
        wildcard_error = any("wildcard" in e.lower() for e in errors)
        assert not wildcard_error, (
            f"Wildcards in NATS SUBSCRIBE mode should be allowed: {errors}"
        )


# ---------------------------------------------------------------------------
# 3. Direct MQTT protocol publish (bypasses GraphQL)
# ---------------------------------------------------------------------------

class TestMqttProtocolWildcardRejection:
    """Direct MQTT publish with wildcard topics must be blocked.

    The MQTT codec rejects wildcards at the protocol level (codec validation).
    The SessionHandler provides a second defense for any bridge connectors
    that publish programmatically.
    """

    @pytest.fixture
    def mqtt_client_v311(self):
        c = _make_mqtt_client("wildcard_proto_test")
        c.connect(BROKER_HOST, MQTT_PORT, keepalive=60)
        c.loop_start()
        assert c._connack_event.wait(5.0), "MQTT connection timeout"
        yield c
        c.loop_stop()
        c.disconnect()

    def _publish_and_check_not_received(self, client: mqtt.Client, topic: str) -> None:
        """Publish to a wildcard topic and verify a subscriber on a valid topic does NOT receive it."""
        received_messages = []
        valid_topic = f"test/wildcard-check/{uuid.uuid4().hex[:8]}"

        sub_client = _make_mqtt_client("wildcard_sub_check")
        sub_client._messages = []
        sub_client.on_message = lambda c, u, msg: received_messages.append(msg.topic)
        sub_client.connect(BROKER_HOST, MQTT_PORT, keepalive=60)
        sub_client.loop_start()
        assert sub_client._connack_event.wait(5.0)
        sub_client.subscribe(valid_topic, qos=0)
        time.sleep(0.3)

        try:
            # Attempt to publish to wildcard topic - codec or SessionHandler will reject
            client.publish(topic, b"should-not-arrive", qos=0)
            time.sleep(0.5)

            # Also publish to a valid topic to confirm subscriber is working
            client.publish(valid_topic, b"canary", qos=0)
            time.sleep(0.5)

            # The canary should arrive; wildcard publish must not cause issues
            assert valid_topic in received_messages, (
                f"Canary message on '{valid_topic}' should have been received"
            )
            # Nothing on the wildcard topic should have been delivered
            wildcard_received = any(
                "+" in t or "#" in t for t in received_messages
            )
            assert not wildcard_received, (
                f"Should not receive message on wildcard topic, received: {received_messages}"
            )
        finally:
            sub_client.loop_stop()
            sub_client.disconnect()

    def test_single_level_wildcard_not_delivered(self, mqtt_client_v311):
        """Messages 'published' to sensor/+/temp must never be delivered to subscribers."""
        self._publish_and_check_not_received(mqtt_client_v311, "sensor/+/temp")

    def test_multi_level_wildcard_not_delivered(self, mqtt_client_v311):
        """Messages 'published' to data/test/# must never be delivered to subscribers."""
        self._publish_and_check_not_received(mqtt_client_v311, "data/test/#")

    def test_valid_topic_delivered(self, mqtt_client_v311):
        """Messages on valid topics without wildcards must be delivered normally."""
        topic = f"test/wildcard-valid/{uuid.uuid4().hex[:8]}"
        received = threading.Event()

        sub_client = _make_mqtt_client("wildcard_valid_sub")
        sub_client.on_message = lambda c, u, msg: received.set()
        sub_client.connect(BROKER_HOST, MQTT_PORT, keepalive=60)
        sub_client.loop_start()
        assert sub_client._connack_event.wait(5.0)
        sub_client.subscribe(topic, qos=1)
        time.sleep(0.3)

        try:
            mqtt_client_v311.publish(topic, b"hello", qos=1)
            assert received.wait(5.0), f"Expected message on valid topic '{topic}' to be received"
        finally:
            sub_client.loop_stop()
            sub_client.disconnect()
