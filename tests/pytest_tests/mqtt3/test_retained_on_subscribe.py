#!/usr/bin/env python3
"""
Regression test: retained messages must be delivered when a client subscribes.

This covers a bug where the SQLite retained store used async queries inside
a method that callers expected to be synchronous (findMatchingMessages).
The result was that retained messages were stored in the DB but never
published to newly subscribing clients.

Tests both exact-topic and wildcard subscriptions on MQTT v3.1.1.
"""
import paho.mqtt.client as mqtt
import threading
import time
import uuid
import pytest

pytestmark = [pytest.mark.mqtt3, pytest.mark.retain]

TOPIC_PREFIX = f"test/retained_regress/{uuid.uuid4().hex[:8]}"


def _make_client(tag, broker_config, protocol=mqtt.MQTTv311):
    cid = f"{tag}_{uuid.uuid4().hex[:8]}"
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, cid, protocol=protocol)
    c.username_pw_set(broker_config["username"], broker_config["password"])
    c._connack = threading.Event()
    c._suback = threading.Event()

    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            client._connack.set()

    def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        client._suback.set()

    c.on_connect = on_connect
    c.on_subscribe = on_subscribe
    return c


def _connect(client, broker_config):
    client.connect(broker_config["host"], broker_config["port"])
    client.loop_start()
    assert client._connack.wait(5), "Client failed to connect"


def _publish_retained(broker_config, topic, payload, qos=1):
    """Publish a retained message and disconnect."""
    pub = _make_client("pub", broker_config)
    _connect(pub, broker_config)
    result = pub.publish(topic, payload.encode(), qos=qos, retain=True)
    result.wait_for_publish(timeout=5)
    time.sleep(0.3)
    pub.loop_stop()
    pub.disconnect()


def _clear_retained(broker_config, topic):
    """Clear a retained message by publishing empty payload."""
    pub = _make_client("clr", broker_config)
    _connect(pub, broker_config)
    result = pub.publish(topic, b"", qos=1, retain=True)
    result.wait_for_publish(timeout=5)
    time.sleep(0.3)
    pub.loop_stop()
    pub.disconnect()


def test_retained_delivered_on_exact_subscribe(broker_config):
    """Retained message on an exact topic must be delivered when a new client subscribes."""
    topic = f"{TOPIC_PREFIX}/exact"
    payload = "retained-exact"

    _publish_retained(broker_config, topic, payload)

    # New client subscribes after the retained message is stored
    msgs = []
    sub = _make_client("sub", broker_config)
    sub.on_message = lambda c, u, m: msgs.append(m)
    _connect(sub, broker_config)
    sub.subscribe(topic, qos=1)
    assert sub._suback.wait(5), "Subscribe failed"

    time.sleep(1.5)  # allow retained delivery
    sub.loop_stop()
    sub.disconnect()
    _clear_retained(broker_config, topic)

    print(f"Received {len(msgs)} message(s)")
    for i, m in enumerate(msgs):
        print(f"  [{i}] topic={m.topic} retain={m.retain} payload={m.payload}")

    assert len(msgs) >= 1, "No retained message received on subscribe"
    assert msgs[0].payload.decode() == payload, f"Wrong payload: {msgs[0].payload}"
    assert msgs[0].retain is True, "Retained flag must be set on subscription-time delivery"


def test_retained_delivered_on_single_level_wildcard(broker_config):
    """Retained messages must be delivered when subscribing with single-level wildcard (+)."""
    topic_a = f"{TOPIC_PREFIX}/wild/a"
    topic_b = f"{TOPIC_PREFIX}/wild/b"
    sub_pattern = f"{TOPIC_PREFIX}/wild/+"

    _publish_retained(broker_config, topic_a, "val-a")
    _publish_retained(broker_config, topic_b, "val-b")

    msgs = []
    sub = _make_client("sub", broker_config)
    sub.on_message = lambda c, u, m: msgs.append(m)
    _connect(sub, broker_config)
    sub.subscribe(sub_pattern, qos=1)
    assert sub._suback.wait(5), "Subscribe failed"

    time.sleep(1.5)
    sub.loop_stop()
    sub.disconnect()
    _clear_retained(broker_config, topic_a)
    _clear_retained(broker_config, topic_b)

    topics_received = {m.topic for m in msgs}
    payloads = {m.topic: m.payload.decode() for m in msgs}
    print(f"Received {len(msgs)} message(s): {payloads}")

    assert topic_a in topics_received, f"Missing retained for {topic_a}"
    assert topic_b in topics_received, f"Missing retained for {topic_b}"
    assert all(m.retain for m in msgs), "All retained messages must have retain=True"


def test_retained_delivered_on_multi_level_wildcard(broker_config):
    """Retained messages must be delivered when subscribing with multi-level wildcard (#)."""
    topic = f"{TOPIC_PREFIX}/multi/deep/value"
    sub_pattern = f"{TOPIC_PREFIX}/multi/#"

    _publish_retained(broker_config, topic, "deep-val")

    msgs = []
    sub = _make_client("sub", broker_config)
    sub.on_message = lambda c, u, m: msgs.append(m)
    _connect(sub, broker_config)
    sub.subscribe(sub_pattern, qos=1)
    assert sub._suback.wait(5), "Subscribe failed"

    time.sleep(1.5)
    sub.loop_stop()
    sub.disconnect()
    _clear_retained(broker_config, topic)

    print(f"Received {len(msgs)} message(s)")
    assert len(msgs) >= 1, "No retained message received on multi-level wildcard subscribe"
    assert msgs[0].topic == topic
    assert msgs[0].payload.decode() == "deep-val"
    assert msgs[0].retain is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
