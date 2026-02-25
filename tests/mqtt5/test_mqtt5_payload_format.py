"""
MQTT v5.0 Payload Format Indicator Test

Tests the Payload Format Indicator property (property ID 1):
- When payloadFormatIndicator = 0 (or absent): Binary payload, no validation
- When payloadFormatIndicator = 1: UTF-8 payload, broker validates encoding

Per MQTT v5.0 spec 3.3.2.3.3: The Payload Format Indicator is a One Byte Integer
that identifies the type of payload: 0 = unspecified bytes, 1 = UTF-8 encoded payload.

Test Scenarios:
1. Publish with payloadFormatIndicator=1 and valid UTF-8 - should succeed
2. Publish with payloadFormatIndicator=1 and invalid UTF-8 - should log warning
3. Publish with payloadFormatIndicator=0 (binary) - should succeed regardless
4. Subscriber receives payloadFormatIndicator property correctly
"""

import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
import time
import pytest

pytestmark = pytest.mark.mqtt5

# Configuration
TEST_TOPIC = "test/payload/format"


def _wait_for_connack(client, timeout=5.0):
    """Poll until the client's connected flag is set (set by on_connect with rc==0)."""
    start = time.time()
    while not client._connected and (time.time() - start) < timeout:
        time.sleep(0.1)
    return client._connected


def _make_client(client_id, broker_config, userdata=None):
    """Create a client with a _connected flag and on_connect that sets it."""
    c = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv5,
        userdata=userdata,
    )
    c._connected = False

    def on_connect(client, ud, flags, rc, properties=None):
        if rc == 0:
            client._connected = True

    c.on_connect = on_connect
    c.username_pw_set(broker_config["username"], broker_config["password"])
    return c


def test_payload_format_utf8_valid(broker_config):
    """Test 1: Valid UTF-8 payload with payloadFormatIndicator=1"""
    messages_received = []
    sub_ready = False

    def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        nonlocal sub_ready
        sub_ready = True

    def on_message(client, userdata, msg):
        payload_format = None
        if hasattr(msg, 'properties') and msg.properties:
            if hasattr(msg.properties, 'PayloadFormatIndicator'):
                payload_format = msg.properties.PayloadFormatIndicator

        messages_received.append({
            'topic': msg.topic,
            'payload': msg.payload,
            'payloadFormatIndicator': payload_format
        })

    def on_publish(client, userdata, mid, reason_code=None, properties=None):
        pass

    # Create subscriber
    subscriber = _make_client("subscriber1", broker_config, userdata="Subscriber1")
    subscriber.on_subscribe = on_subscribe
    subscriber.on_message = on_message

    # Create publisher
    publisher = _make_client("publisher1", broker_config, userdata="Publisher1")
    publisher.on_publish = on_publish

    try:
        subscriber.connect(broker_config["host"], broker_config["port"], 60)
        subscriber.loop_start()
        start = time.time()
        while not subscriber._connected and (time.time() - start) < 5.0:
            time.sleep(0.1)
        assert subscriber._connected, "Subscriber did not connect"

        subscriber.subscribe(TEST_TOPIC, qos=1)
        start = time.time()
        while not sub_ready and (time.time() - start) < 5.0:
            time.sleep(0.1)
        assert sub_ready, "Subscription did not complete"

        time.sleep(0.5)  # Allow broker to fully register subscription
        publisher.connect(broker_config["host"], broker_config["port"], 60)
        publisher.loop_start()
        start = time.time()
        while not publisher._connected and (time.time() - start) < 5.0:
            time.sleep(0.1)
        assert publisher._connected, "Publisher did not connect"

        # Publish with valid UTF-8 and payloadFormatIndicator=1
        valid_utf8_payload = "Hello, MQTT v5! 你好 مرحبا".encode('utf-8')

        props = Properties(PacketTypes.PUBLISH)
        props.PayloadFormatIndicator = 1  # UTF-8

        result = publisher.publish(TEST_TOPIC, valid_utf8_payload, qos=1, properties=props)
        result.wait_for_publish()
        time.sleep(1)

        # Verify message received
        assert len(messages_received) == 1, f"Expected 1 message, got {len(messages_received)}"
        msg = messages_received[0]
        assert msg['payloadFormatIndicator'] == 1, f"Expected format indicator 1, got {msg['payloadFormatIndicator']}"
        assert msg['payload'] == valid_utf8_payload, "Payload mismatch"

    finally:
        subscriber.loop_stop()
        subscriber.disconnect()
        publisher.loop_stop()
        publisher.disconnect()
        time.sleep(0.5)


def test_payload_format_binary(broker_config):
    """Test 2: Binary payload with payloadFormatIndicator=0"""
    messages_received = []
    sub_ready = False

    def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        nonlocal sub_ready
        sub_ready = True

    def on_message(client, userdata, msg):
        payload_format = None
        if hasattr(msg, 'properties') and msg.properties:
            if hasattr(msg.properties, 'PayloadFormatIndicator'):
                payload_format = msg.properties.PayloadFormatIndicator

        messages_received.append({
            'topic': msg.topic,
            'payload': msg.payload,
            'payloadFormatIndicator': payload_format
        })

    def on_publish(client, userdata, mid, reason_code=None, properties=None):
        pass

    # Create subscriber
    subscriber = _make_client("subscriber2", broker_config, userdata="Subscriber2")
    subscriber.on_subscribe = on_subscribe
    subscriber.on_message = on_message

    # Create publisher
    publisher = _make_client("publisher2", broker_config, userdata="Publisher2")
    publisher.on_publish = on_publish

    try:
        subscriber.connect(broker_config["host"], broker_config["port"], 60)
        subscriber.loop_start()
        start = time.time()
        while not subscriber._connected and (time.time() - start) < 5.0:
            time.sleep(0.1)
        assert subscriber._connected, "Subscriber did not connect"

        subscriber.subscribe(TEST_TOPIC, qos=1)
        start = time.time()
        while not sub_ready and (time.time() - start) < 5.0:
            time.sleep(0.1)
        assert sub_ready, "Subscription did not complete"

        time.sleep(0.5)  # Allow broker to fully register subscription
        publisher.connect(broker_config["host"], broker_config["port"], 60)
        publisher.loop_start()
        start = time.time()
        while not publisher._connected and (time.time() - start) < 5.0:
            time.sleep(0.1)
        assert publisher._connected, "Publisher did not connect"

        # Publish binary data with payloadFormatIndicator=0
        binary_payload = bytes([0xFF, 0xFE, 0xFD, 0x00, 0x01, 0x02])  # Invalid UTF-8

        props = Properties(PacketTypes.PUBLISH)
        props.PayloadFormatIndicator = 0  # Binary/Unspecified

        result = publisher.publish(TEST_TOPIC, binary_payload, qos=1, properties=props)
        result.wait_for_publish()
        time.sleep(1)

        # Verify message received
        assert len(messages_received) == 1, f"Expected 1 message, got {len(messages_received)}"
        msg = messages_received[0]
        assert msg['payloadFormatIndicator'] == 0, f"Expected format indicator 0, got {msg['payloadFormatIndicator']}"
        assert msg['payload'] == binary_payload, "Payload mismatch"

    finally:
        subscriber.loop_stop()
        subscriber.disconnect()
        publisher.loop_stop()
        publisher.disconnect()
        time.sleep(0.5)


def test_payload_format_default(broker_config):
    """Test 3: No payload format indicator (default behavior)"""
    messages_received = []
    sub_ready = False

    def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        nonlocal sub_ready
        sub_ready = True

    def on_message(client, userdata, msg):
        payload_format = None
        if hasattr(msg, 'properties') and msg.properties:
            if hasattr(msg.properties, 'PayloadFormatIndicator'):
                payload_format = msg.properties.PayloadFormatIndicator

        messages_received.append({
            'topic': msg.topic,
            'payload': msg.payload,
            'payloadFormatIndicator': payload_format
        })

    def on_publish(client, userdata, mid, reason_code=None, properties=None):
        pass

    # Create subscriber
    subscriber = _make_client("subscriber3", broker_config, userdata="Subscriber3")
    subscriber.on_subscribe = on_subscribe
    subscriber.on_message = on_message

    # Create publisher
    publisher = _make_client("publisher3", broker_config, userdata="Publisher3")
    publisher.on_publish = on_publish

    try:
        subscriber.connect(broker_config["host"], broker_config["port"], 60)
        subscriber.loop_start()
        start = time.time()
        while not subscriber._connected and (time.time() - start) < 5.0:
            time.sleep(0.1)
        assert subscriber._connected, "Subscriber did not connect"

        subscriber.subscribe(TEST_TOPIC, qos=1)
        start = time.time()
        while not sub_ready and (time.time() - start) < 5.0:
            time.sleep(0.1)
        assert sub_ready, "Subscription did not complete"

        time.sleep(0.5)  # Allow broker to fully register subscription
        publisher.connect(broker_config["host"], broker_config["port"], 60)
        publisher.loop_start()
        start = time.time()
        while not publisher._connected and (time.time() - start) < 5.0:
            time.sleep(0.1)
        assert publisher._connected, "Publisher did not connect"

        # Publish without specifying payload format indicator
        payload = b"Default payload format"

        result = publisher.publish(TEST_TOPIC, payload, qos=1)
        result.wait_for_publish()
        time.sleep(1)

        # Verify message received
        assert len(messages_received) == 1, f"Expected 1 message, got {len(messages_received)}"
        msg = messages_received[0]
        assert msg['payloadFormatIndicator'] is None, f"Expected None format indicator, got {msg['payloadFormatIndicator']}"
        assert msg['payload'] == payload, "Payload mismatch"

    finally:
        subscriber.loop_stop()
        subscriber.disconnect()
        publisher.loop_stop()
        publisher.disconnect()
        time.sleep(0.5)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
