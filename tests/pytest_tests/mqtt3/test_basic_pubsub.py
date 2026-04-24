#!/usr/bin/env python3
"""Basic MQTT publish/subscribe functionality tests"""
import paho.mqtt.client as mqtt
import threading
import time
import uuid
import pytest

pytestmark = pytest.mark.mqtt3


def _make_client(client_id, broker_config):
    """Create an MQTTv311 client with threading.Event for CONNACK and SUBACK."""
    unique_id = f"{client_id}_{uuid.uuid4().hex[:8]}"
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, unique_id, protocol=mqtt.MQTTv311)
    c.username_pw_set(broker_config["username"], broker_config["password"])
    c._connack_event = threading.Event()
    c._suback_event = threading.Event()

    def on_connect(client, userdata, flags, rc, properties=None):
        print(f"[{unique_id}] on_connect: rc={rc} flags={flags}")
        if rc == 0:
            client._connack_event.set()

    def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        print(f"[{unique_id}] on_subscribe: mid={mid} reason_codes={reason_code_list}")
        client._suback_event.set()

    def on_disconnect(client, userdata, disconnect_flags, rc, properties=None):
        print(f"[{unique_id}] on_disconnect: rc={rc}")

    c.on_connect = on_connect
    c.on_subscribe = on_subscribe
    c.on_disconnect = on_disconnect
    return c


def _wait_for_connack(client, timeout=5.0):
    return client._connack_event.wait(timeout=timeout)


def _wait_for_suback(client, timeout=5.0):
    return client._suback_event.wait(timeout=timeout)


def test_basic_publish_only(broker_config):
    """Test basic MQTT publish without subscription (QoS 0)"""
    client = _make_client("test_pub_only", broker_config)

    try:
        client.connect(broker_config["host"], broker_config["port"], 60)
        client.loop_start()
        assert _wait_for_connack(client), "Failed to connect to broker"

        result = client.publish("test/verify", b"test message", qos=0)
        result.wait_for_publish()
        time.sleep(1)

        assert result.rc == mqtt.MQTT_ERR_SUCCESS, f"Publish failed with rc={result.rc}"

    finally:
        client.loop_stop()
        client.disconnect()
        time.sleep(0.5)


def test_basic_pubsub_qos0(broker_config):
    """Test basic publish/subscribe with QoS 0 (at most once delivery)"""
    messages_received = []

    sub = _make_client("test_sub_qos0", broker_config)

    def on_message(c, u, msg):
        print(f"[SUB] on_message: topic={msg.topic} payload={msg.payload!r}")
        messages_received.append(msg.payload.decode())

    sub.on_message = on_message

    pub = _make_client("test_pub_qos0", broker_config)

    try:
        print(f"[SUB] Connecting to {broker_config['host']}:{broker_config['port']} ...")
        sub.connect(broker_config["host"], broker_config["port"], 60)
        sub.loop_start()
        assert _wait_for_connack(sub), "Subscriber failed to connect"
        print(f"[SUB] Connected (client_id={sub._client_id})")

        sub.subscribe("test/qos0", qos=0)
        assert _wait_for_suback(sub), "Subscriber failed to subscribe"
        print("[SUB] Subscribed to test/qos0")
        time.sleep(0.5)

        print(f"[PUB] Connecting to {broker_config['host']}:{broker_config['port']} ...")
        pub.connect(broker_config["host"], broker_config["port"], 60)
        pub.loop_start()
        assert _wait_for_connack(pub), "Publisher failed to connect"
        print(f"[PUB] Connected (client_id={pub._client_id})")

        result = pub.publish("test/qos0", b"qos0 test message", qos=0)
        print(f"[PUB] publish() returned rc={result.rc} mid={result.mid}")
        print("[WAIT] Sleeping 2s for delivery ...")
        time.sleep(2)
        print(f"[RESULT] messages_received={messages_received}")

        assert len(messages_received) > 0, "No messages received"
        assert messages_received[0] == "qos0 test message", f"Wrong message: {messages_received[0]}"

    finally:
        pub.loop_stop()
        pub.disconnect()
        sub.loop_stop()
        sub.disconnect()
        time.sleep(0.5)


def test_basic_pubsub_qos1(broker_config):
    """Test basic publish/subscribe with QoS 1 (at least once delivery)"""
    received = []

    sub = _make_client("test_sub_qos1", broker_config)

    def on_message(client, userdata, message):
        received.append(message)
        print(f'Received: {message.payload}, retain={message.retain}')

    sub.on_message = on_message

    pub = _make_client("test_pub_qos1", broker_config)

    try:
        sub.connect(broker_config["host"], broker_config["port"])
        sub.loop_start()
        assert _wait_for_connack(sub), "Subscriber failed to connect"

        sub.subscribe('test/qos1', qos=1)
        assert _wait_for_suback(sub), "Subscriber failed to subscribe"
        time.sleep(0.5)

        pub.connect(broker_config["host"], broker_config["port"])
        pub.loop_start()
        assert _wait_for_connack(pub), "Publisher failed to connect"

        result = pub.publish('test/qos1', b'qos1 test', qos=1, retain=False)
        assert result.rc == 0, f"Publish failed with rc={result.rc}"
        time.sleep(3)

    finally:
        pub.loop_stop()
        pub.disconnect()
        sub.loop_stop()
        sub.disconnect()

    assert len(received) > 0, "No messages received"
    print(f'✓ Received {len(received)} message(s) with QoS 1')


def test_publish_multiple_topics(broker_config):
    """Test publishing to multiple different topics without subscription"""
    published_mids = []

    def on_publish(client, userdata, mid, *args):
        print(f"Message {mid} published successfully")
        published_mids.append(mid)

    client = _make_client("test-multi-topic", broker_config)
    client.on_publish = on_publish

    client.connect(broker_config["host"], broker_config["port"], 60)
    client.loop_start()
    assert _wait_for_connack(client), "Failed to connect to broker"

    print("Publishing test messages...")
    result_a = client.publish("test/a", "value_a")
    result_b = client.publish("test/b", "value_b")

    assert result_a.rc == 0, f"Publish to test/a failed with rc={result_a.rc}"
    assert result_b.rc == 0, f"Publish to test/b failed with rc={result_b.rc}"

    time.sleep(2)
    client.loop_stop()
    client.disconnect()

    assert len(published_mids) == 2, f"Expected 2 publish confirmations, got {len(published_mids)}"
    print("✓ Test messages published successfully to multiple topics")


def test_pubsub_multiple_messages(broker_config):
    """Test publishing and receiving multiple messages in sequence"""
    received = []

    def on_message(client, userdata, msg):
        print(f"✓ Received: {msg.payload.decode()}")
        received.append(msg.payload.decode())

    client = _make_client("test-multiple-client", broker_config)
    client.on_message = on_message

    # Subscribe inside on_connect so SUBACK is guaranteed before publishing
    def on_connect(c, userdata, flags, rc, properties=None):
        print(f"[on_connect] rc={rc}")
        if rc == 0:
            c._connack_event.set()
            c.subscribe("test/multiple", qos=1)

    def on_subscribe(c, userdata, mid, reason_code_list, properties=None):
        print(f"[on_subscribe] mid={mid} reason_codes={reason_code_list}")
        c._suback_event.set()

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe

    client.connect(broker_config["host"], broker_config["port"], 60)
    client.loop_start()

    assert _wait_for_connack(client), "Failed to connect to broker"
    assert _wait_for_suback(client), "Failed to subscribe"
    time.sleep(0.5)

    print("\nPublishing 3 messages...")
    for i in range(3):
        msg = f"Test message {i+1}"
        result = client.publish("test/multiple", msg, qos=1)
        assert result.rc == 0, f"Publish failed with rc={result.rc}"
        print(f"  Published: {msg}")
        time.sleep(0.2)

    time.sleep(3)  # Wait for messages

    client.loop_stop()
    client.disconnect()

    assert len(received) == 3, f"Expected 3 messages, received {len(received)}"
    print(f"✓ Successfully received all {len(received)}/3 messages")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
