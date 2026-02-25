#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import threading
import time
import uuid
import pytest


def _make_sub(broker_config):
    """Create and connect a new subscriber client, return (client, msgs, suback_event)."""
    msgs = []
    connack = threading.Event()
    suback = threading.Event()

    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f'tsub_{uuid.uuid4().hex[:8]}', protocol=mqtt.MQTTv311)
    sub.username_pw_set(broker_config["username"], broker_config["password"])

    def on_connect(client, userdata, flags, rc, properties=None):
        print(f"[SUB] on_connect: rc={rc}")
        if rc == 0:
            connack.set()

    def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        print(f"[SUB] on_subscribe: mid={mid}")
        suback.set()

    def on_message(client, userdata, message):
        print(f"[SUB] on_message: topic={message.topic} retain={message.retain} payload={message.payload}")
        msgs.append(message)

    def on_disconnect(client, userdata, disconnect_flags, rc, properties=None):
        print(f"[SUB] on_disconnect: rc={rc}")

    sub.on_connect = on_connect
    sub.on_subscribe = on_subscribe
    sub.on_message = on_message
    sub.on_disconnect = on_disconnect
    sub.connect(broker_config["host"], broker_config["port"])
    sub.loop_start()
    assert connack.wait(timeout=5), "Subscriber failed to connect"
    return sub, msgs, suback


def _make_pub(broker_config):
    """Create and connect a publisher client, return client."""
    connack = threading.Event()
    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f'tpub_{uuid.uuid4().hex[:8]}', protocol=mqtt.MQTTv311)
    pub.username_pw_set(broker_config["username"], broker_config["password"])
    pub.on_connect = lambda c, u, f, rc, p=None: connack.set() if rc == 0 else None
    pub.connect(broker_config["host"], broker_config["port"])
    pub.loop_start()
    assert connack.wait(timeout=5), "Publisher failed to connect"
    return pub


def test_live_retained_publish_has_no_retain_flag(broker_config):
    """MQTT 3.1.1 §3.3.1.3: a subscriber that is already subscribed when a retained
    message is published must receive it with retain=0 (live delivery)."""
    topic = 'test/retained'

    # Clear any previously stored retained message for this topic
    pub = _make_pub(broker_config)
    result = pub.publish(topic, b'', qos=1, retain=True)
    result.wait_for_publish()
    print("[PUB] Cleared retained message")
    time.sleep(0.5)
    pub.loop_stop()
    pub.disconnect()

    sub, msgs, suback = _make_sub(broker_config)

    # Subscribe first, before the publisher publishes.
    # Sleep after SUBACK to ensure the broker has fully registered the subscription
    # before the publish arrives.
    sub.subscribe(topic, qos=1)
    assert suback.wait(timeout=5), "Subscriber failed to subscribe"
    time.sleep(0.5)
    print("[SUB] Subscribed, now publishing...")

    # Now publish with retain=True while subscriber is already connected
    pub = _make_pub(broker_config)
    result = pub.publish(topic, b'live-data', qos=1, retain=True)
    result.wait_for_publish()
    print("[PUB] Published with retain=True")

    time.sleep(0.5)  # allow delivery
    pub.loop_stop()
    pub.disconnect()
    sub.loop_stop()
    sub.disconnect()

    print(f"[TEST] Received {len(msgs)} message(s):")
    for i, m in enumerate(msgs):
        print(f"  [{i}] topic={m.topic} retain={m.retain} payload={m.payload}")

    assert len(msgs) > 0, "No messages received by live subscriber"
    live_msg = msgs[-1]
    assert live_msg.retain == False, \
        f"Live delivery: expected retain=False, got retain={live_msg.retain}"
    print(f"✓ Live delivery: retain={live_msg.retain} (correct)")


def test_retained_message_on_resubscribe_has_retain_flag(broker_config):
    """MQTT 3.1.1 §3.3.1.3: a client that subscribes after a retained message was
    published must receive it with retain=1 (subscription-time delivery)."""
    topic = 'test/retained'

    # Publish retained message first, then disconnect
    pub = _make_pub(broker_config)
    result = pub.publish(topic, b'data', qos=1, retain=True)
    result.wait_for_publish()
    print("[PUB] Retained message published")
    time.sleep(0.5)
    pub.loop_stop()
    pub.disconnect()

    # Now subscribe — broker must deliver stored retained message with retain=1
    sub, msgs, suback = _make_sub(broker_config)
    sub.subscribe(topic, qos=1)
    assert suback.wait(timeout=5), "Subscriber failed to subscribe"
    print("[SUB] Subscribed, waiting for retained message...")
    time.sleep(0.5)
    sub.loop_stop()
    sub.disconnect()

    assert len(msgs) > 0, "No retained message received on subscription"
    assert msgs[0].retain == True, \
        f"Subscription-time delivery: expected retain=True, got retain={msgs[0].retain}"
    print(f"✓ Subscription-time delivery: retain={msgs[0].retain} (correct)")


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
