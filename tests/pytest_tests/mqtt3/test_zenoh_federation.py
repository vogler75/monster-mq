"""Two-broker Zenoh federation acceptance tests.

Set ZENOH_BROKER_A_PORT and ZENOH_BROKER_B_PORT to enable this suite. Both brokers must
use the same Zenoh prefix/router and distinct NodeName values.
"""

import os
import threading
import time
import uuid

import paho.mqtt.client as mqtt
import pytest


pytestmark = [pytest.mark.mqtt3, pytest.mark.integration, pytest.mark.external]


def _ports():
    a = os.getenv("ZENOH_BROKER_A_PORT")
    b = os.getenv("ZENOH_BROKER_B_PORT")
    if not a or not b:
        pytest.skip("Set ZENOH_BROKER_A_PORT and ZENOH_BROKER_B_PORT")
    return int(a), int(b)


def _client(name, port, broker_config):
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        f"{name}-{uuid.uuid4().hex[:8]}",
        protocol=mqtt.MQTTv311,
    )
    client.username_pw_set(broker_config["username"], broker_config["password"])
    connected = threading.Event()
    client.on_connect = lambda _c, _u, _f, reason, _p=None: connected.set() if reason == 0 else None
    client.connect(broker_config["host"], port)
    client.loop_start()
    assert connected.wait(5), f"{name} did not connect on port {port}"
    return client


def test_bidirectional_delivery_without_echo(broker_config):
    port_a, port_b = _ports()
    topic_ab = f"test/zenoh/{uuid.uuid4().hex}/a-to-b"
    topic_ba = f"test/zenoh/{uuid.uuid4().hex}/b-to-a"
    received_a, received_b = [], []
    sub_a = _client("zenoh-sub-a", port_a, broker_config)
    sub_b = _client("zenoh-sub-b", port_b, broker_config)
    pub_a = _client("zenoh-pub-a", port_a, broker_config)
    pub_b = _client("zenoh-pub-b", port_b, broker_config)
    try:
        sub_a.on_message = lambda _c, _u, msg: received_a.append((msg.topic, msg.payload))
        sub_b.on_message = lambda _c, _u, msg: received_b.append((msg.topic, msg.payload))
        sub_a.subscribe(topic_ba, qos=1)
        sub_b.subscribe(topic_ab.rsplit("/", 1)[0] + "/#", qos=1)
        time.sleep(0.5)

        pub_a.publish(topic_ab, b"from-a", qos=1).wait_for_publish(5)
        pub_b.publish(topic_ba, b"from-b", qos=1).wait_for_publish(5)
        deadline = time.time() + 5
        while time.time() < deadline and (len(received_a) < 1 or len(received_b) < 1):
            time.sleep(0.05)

        assert received_a == [(topic_ba, b"from-b")]
        assert received_b == [(topic_ab, b"from-a")]
        time.sleep(0.5)
        assert len(received_a) == len(received_b) == 1
    finally:
        for client in (sub_a, sub_b, pub_a, pub_b):
            client.loop_stop()
            client.disconnect()


def test_retained_message_is_available_on_remote_broker(broker_config):
    port_a, port_b = _ports()
    topic = f"test/zenoh/{uuid.uuid4().hex}/retained"
    publisher = _client("zenoh-retained-pub", port_a, broker_config)
    subscriber = None
    try:
        publisher.publish(topic, b"retained-over-zenoh", qos=1, retain=True).wait_for_publish(5)
        time.sleep(1)
        received = []
        subscriber = _client("zenoh-retained-sub", port_b, broker_config)
        subscriber.on_message = lambda _c, _u, msg: received.append(msg)
        subscriber.subscribe(topic, qos=1)
        deadline = time.time() + 5
        while time.time() < deadline and not received:
            time.sleep(0.05)
        assert len(received) == 1
        assert received[0].payload == b"retained-over-zenoh"
        assert received[0].retain
    finally:
        publisher.publish(topic, b"", qos=1, retain=True).wait_for_publish(5)
        publisher.loop_stop()
        publisher.disconnect()
        if subscriber:
            subscriber.loop_stop()
            subscriber.disconnect()


def test_denied_system_topic_stays_local(broker_config):
    port_a, port_b = _ports()
    topic = f"$SYS/test/zenoh/{uuid.uuid4().hex}"
    received = []
    subscriber = _client("zenoh-denied-sub", port_b, broker_config)
    publisher = _client("zenoh-denied-pub", port_a, broker_config)
    try:
        subscriber.on_message = lambda _c, _u, msg: received.append(msg)
        subscriber.subscribe(topic, qos=0)
        time.sleep(0.5)
        publisher.publish(topic, b"must-stay-local", qos=0).wait_for_publish(5)
        time.sleep(1)
        assert received == []
    finally:
        publisher.loop_stop()
        publisher.disconnect()
        subscriber.loop_stop()
        subscriber.disconnect()
        subscriber.loop_stop()
        subscriber.disconnect()
