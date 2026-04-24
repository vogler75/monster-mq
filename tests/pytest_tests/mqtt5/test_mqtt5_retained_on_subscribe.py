#!/usr/bin/env python3
"""
Regression test (MQTT v5): retained messages must be delivered when a client subscribes.

Same scenario as the v3.1.1 counterpart but exercises the MQTT v5 code path,
which has separate subscription option handling (retainHandling).
"""
import paho.mqtt.client as mqtt
import threading
import time
import uuid
import pytest

pytestmark = [pytest.mark.mqtt5, pytest.mark.retain]

TOPIC_PREFIX = f"test/retained_regress5/{uuid.uuid4().hex[:8]}"


def _make_client(tag, broker_config):
    cid = f"{tag}_{uuid.uuid4().hex[:8]}"
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, cid, protocol=mqtt.MQTTv5)
    c.username_pw_set(broker_config["username"], broker_config["password"])
    c._connack = threading.Event()
    c._suback = threading.Event()

    def on_connect(client, userdata, flags, rc, properties=None):
        if hasattr(rc, "value"):
            rc = rc.value
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
    pub = _make_client("pub", broker_config)
    _connect(pub, broker_config)
    result = pub.publish(topic, payload.encode(), qos=qos, retain=True)
    result.wait_for_publish(timeout=5)
    time.sleep(0.3)
    pub.loop_stop()
    pub.disconnect()


def _clear_retained(broker_config, topic):
    pub = _make_client("clr", broker_config)
    _connect(pub, broker_config)
    result = pub.publish(topic, b"", qos=1, retain=True)
    result.wait_for_publish(timeout=5)
    time.sleep(0.3)
    pub.loop_stop()
    pub.disconnect()


def test_v5_retained_delivered_on_exact_subscribe(broker_config):
    """MQTT v5 retained message on exact topic must be delivered on subscribe (retainHandling=0)."""
    topic = f"{TOPIC_PREFIX}/exact"
    payload = "v5-retained-exact"

    _publish_retained(broker_config, topic, payload)

    msgs = []
    sub = _make_client("sub", broker_config)
    sub.on_message = lambda c, u, m: msgs.append(m)
    _connect(sub, broker_config)
    sub.subscribe(topic, qos=1)
    assert sub._suback.wait(5), "Subscribe failed"

    time.sleep(1.5)
    sub.loop_stop()
    sub.disconnect()
    _clear_retained(broker_config, topic)

    print(f"Received {len(msgs)} message(s)")
    for i, m in enumerate(msgs):
        print(f"  [{i}] topic={m.topic} retain={m.retain} payload={m.payload}")

    assert len(msgs) >= 1, "No retained message received on v5 subscribe"
    assert msgs[0].payload.decode() == payload
    assert msgs[0].retain is True


def test_v5_retained_delivered_on_wildcard_subscribe(broker_config):
    """MQTT v5 retained messages must be delivered on wildcard subscribe."""
    topic_a = f"{TOPIC_PREFIX}/wild/a"
    topic_b = f"{TOPIC_PREFIX}/wild/b"
    sub_pattern = f"{TOPIC_PREFIX}/wild/+"

    _publish_retained(broker_config, topic_a, "v5-a")
    _publish_retained(broker_config, topic_b, "v5-b")

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
    print(f"Received topics: {topics_received}")

    assert topic_a in topics_received, f"Missing retained for {topic_a}"
    assert topic_b in topics_received, f"Missing retained for {topic_b}"


def test_v5_retained_delivered_on_multi_level_wildcard(broker_config):
    """MQTT v5 retained messages must be delivered on # wildcard subscribe."""
    topic = f"{TOPIC_PREFIX}/multi/deep/val"
    sub_pattern = f"{TOPIC_PREFIX}/multi/#"

    _publish_retained(broker_config, topic, "v5-deep")

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
    assert len(msgs) >= 1, "No retained message received on v5 multi-level wildcard subscribe"
    assert msgs[0].topic == topic
    assert msgs[0].payload.decode() == "v5-deep"
    assert msgs[0].retain is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
