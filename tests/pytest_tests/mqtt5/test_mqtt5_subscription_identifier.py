"""
MQTT v5.0 Subscription Identifier Test (monster-mq#198)

Tests the Subscription Identifier (property 11):
- CONNACK advertises Subscription Identifier Available = 1
- A PUBLISH carries the identifier of the matching subscription
- Overlapping subscriptions add all their identifiers to one PUBLISH
- Retained messages sent on subscribe carry the identifier
- A resumed persistent session keeps the identifier without re-subscribing

Per MQTT v5.0 spec §3.8.2.1.2 and §3.3.4.
"""

import time
import uuid

import paho.mqtt.client as mqtt
import pytest
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

pytestmark = pytest.mark.mqtt5


def _topic(name):
    return f"test/subid/{name}/{uuid.uuid4().hex[:8]}"


def _connect(broker_config, client_id=None, clean_start=True, session_expiry=0):
    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=client_id or f"subid-{uuid.uuid4().hex[:8]}",
        protocol=mqtt.MQTTv5,
    )
    if broker_config["username"]:
        client.username_pw_set(broker_config["username"], broker_config["password"])
    state = {"connack": None, "messages": [], "subacks": set()}

    def on_connect(c, userdata, flags, reason_code, properties):
        state["connack"] = properties

    def on_message(c, userdata, msg):
        ids = getattr(msg.properties, "SubscriptionIdentifier", []) if msg.properties else []
        state["messages"].append((msg.topic, msg.payload.decode(), sorted(ids)))

    def on_subscribe(c, userdata, mid, reason_codes, properties):
        state["subacks"].add(mid)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_subscribe = on_subscribe
    props = Properties(PacketTypes.CONNECT)
    props.SessionExpiryInterval = session_expiry
    client.connect(broker_config["host"], broker_config["port"], clean_start=clean_start, properties=props)
    client.loop_start()
    _wait(lambda: state["connack"] is not None, "CONNACK")
    return client, state


def _subscribe(client, state, topic, subscription_id=None, qos=1):
    props = None
    if subscription_id is not None:
        props = Properties(PacketTypes.SUBSCRIBE)
        props.SubscriptionIdentifier = subscription_id
    result, mid = client.subscribe(topic, options=mqtt.SubscribeOptions(qos=qos), properties=props)
    assert result == mqtt.MQTT_ERR_SUCCESS
    _wait(lambda: mid in state["subacks"], "SUBACK")


def _wait(cond, what, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.05)
    pytest.fail(f"timed out waiting for {what}")


def _stop(client):
    client.loop_stop()
    client.disconnect()


def test_connack_advertises_subscription_identifiers(broker_config):
    client, state = _connect(broker_config)
    try:
        # Absent means available (default 1); if present it must be 1
        available = getattr(state["connack"], "SubscriptionIdentifierAvailable", 1)
        assert available == 1
    finally:
        _stop(client)


def test_publish_carries_subscription_identifier(broker_config):
    topic = _topic("single")
    client, state = _connect(broker_config)
    try:
        _subscribe(client, state, topic, subscription_id=42)
        client.publish(topic, "v1", qos=1)
        _wait(lambda: state["messages"], "PUBLISH")
        assert state["messages"][0] == (topic, "v1", [42])
    finally:
        _stop(client)


def test_overlapping_subscriptions_carry_all_identifiers(broker_config):
    base = _topic("overlap")
    client, state = _connect(broker_config)
    try:
        _subscribe(client, state, f"{base}/+", subscription_id=1)
        _subscribe(client, state, f"{base}/#", subscription_id=2)
        _subscribe(client, state, f"{base}/x", subscription_id=None)
        client.publish(f"{base}/x", "v", qos=1)
        _wait(lambda: state["messages"], "PUBLISH")
        time.sleep(0.3)  # overlapping subscriptions must not produce extra copies
        assert [m[2] for m in state["messages"]] == [[1, 2]]
    finally:
        _stop(client)


def test_subscription_without_identifier_has_none(broker_config):
    topic = _topic("none")
    client, state = _connect(broker_config)
    try:
        _subscribe(client, state, topic)
        client.publish(topic, "v", qos=1)
        _wait(lambda: state["messages"], "PUBLISH")
        assert state["messages"][0][2] == []
    finally:
        _stop(client)


def test_resubscribe_replaces_identifier(broker_config):
    topic = _topic("replace")
    client, state = _connect(broker_config)
    try:
        _subscribe(client, state, topic, subscription_id=5)
        _subscribe(client, state, topic, subscription_id=6)
        client.publish(topic, "v", qos=1)
        _wait(lambda: state["messages"], "PUBLISH")
        assert state["messages"][0][2] == [6]
    finally:
        _stop(client)


def test_retained_message_on_subscribe_carries_identifier(broker_config, clean_topic):
    topic = clean_topic(_topic("retained"))
    publisher, _ = _connect(broker_config)
    try:
        publisher.publish(topic, "kept", qos=1, retain=True).wait_for_publish(5)
    finally:
        _stop(publisher)
    time.sleep(0.3)

    client, state = _connect(broker_config)
    try:
        _subscribe(client, state, topic, subscription_id=77)
        _wait(lambda: state["messages"], "retained PUBLISH")
        assert state["messages"][0] == (topic, "kept", [77])
    finally:
        _stop(client)


def test_persistent_session_keeps_identifier(broker_config):
    topic = _topic("session")
    client_id = f"subid-persist-{uuid.uuid4().hex[:8]}"

    client, state = _connect(broker_config, client_id=client_id, clean_start=True, session_expiry=300)
    _subscribe(client, state, topic, subscription_id=42)
    _stop(client)
    time.sleep(0.5)

    publisher, _ = _connect(broker_config)
    try:
        publisher.publish(topic, "offline", qos=1).wait_for_publish(5)
    finally:
        _stop(publisher)
    time.sleep(0.5)

    # Resume the session without subscribing again: queued and live messages keep the identifier
    client, state = _connect(broker_config, client_id=client_id, clean_start=False, session_expiry=300)
    try:
        _wait(lambda: state["messages"], "queued PUBLISH")
        client.publish(topic, "online", qos=1)
        _wait(lambda: len(state["messages"]) >= 2, "live PUBLISH")
        assert state["messages"][:2] == [(topic, "offline", [42]), (topic, "online", [42])]
    finally:
        _stop(client)
        # Remove the persistent session
        cleanup, _ = _connect(broker_config, client_id=client_id, clean_start=True, session_expiry=0)
        _stop(cleanup)
