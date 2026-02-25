"""
Pytest version of MQTT v5.0 Retain As Published (RAP) tests.

Tests verify that the Retain As Published subscription option correctly
controls whether the retain flag is preserved when forwarding messages.

MQTT 5.0 §3.3.1.3 rules:
- Subscription-time delivery of stored retained messages: retain=1 ALWAYS
  (regardless of RAP setting)
- Live delivery to already-subscribed clients: retain=0 unless RAP=true
"""
import time
import pytest
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.subscribeoptions import SubscribeOptions
from conftest import MessageCollector


pytestmark = [
    pytest.mark.mqtt5,
    pytest.mark.subscription_options,
]


@pytest.fixture
def publisher_client(broker_config):
    """Provides a dedicated publisher client."""
    connected = False

    def on_connect(client, userdata, flags, reason_code, properties=None):
        nonlocal connected
        if reason_code == 0:
            connected = True

    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=f"rap_pub_{int(time.time() * 1000)}",
        protocol=mqtt.MQTTv5
    )
    client.username_pw_set(broker_config["username"], broker_config["password"])
    client.on_connect = on_connect
    client.connect(broker_config["host"], broker_config["port"])
    client.loop_start()
    start = time.time()
    while not connected and (time.time() - start) < 5.0:
        time.sleep(0.1)

    yield client

    client.loop_stop()
    client.disconnect()


@pytest.fixture
def subscriber_client(broker_config, message_collector):
    """Provides a subscriber client with message collection."""
    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=f"rap_sub_{int(time.time() * 1000)}",
        protocol=mqtt.MQTTv5
    )
    client.username_pw_set(broker_config["username"], broker_config["password"])
    client.on_connect = message_collector.on_connect
    client.on_subscribe = message_collector.on_subscribe
    client.on_message = message_collector.on_message

    yield client

    try:
        client.loop_stop()
        client.disconnect()
    except:
        pass


# ---------------------------------------------------------------------------
# Live delivery tests (subscribe first, then publish)
# RAP controls the retain flag on live-forwarded messages.
# ---------------------------------------------------------------------------

def test_rap_false_clears_retain_on_live_delivery(broker_config, clean_topic):
    """
    Live delivery with RAP=false must clear the retain flag.

    Subscribe first, then publish a retained message.  The live-forwarded
    copy must arrive with retain=False because RAP=false (the default).
    """
    topic = clean_topic("test/rap/false/live")

    # Clear any stale retained message
    pub = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    pub.username_pw_set(broker_config["username"], broker_config["password"])
    pub_connected = False

    def on_pub_connect(client, userdata, flags, reason_code, properties=None):
        nonlocal pub_connected
        if reason_code == 0:
            pub_connected = True

    pub.on_connect = on_pub_connect
    pub.connect(broker_config["host"], broker_config["port"])
    pub.loop_start()
    start = time.time()
    while not pub_connected and (time.time() - start) < 5.0:
        time.sleep(0.1)
    pub.publish(topic, "", qos=1, retain=True).wait_for_publish()
    time.sleep(0.3)

    # Subscribe with RAP=false
    collector = MessageCollector()
    sub = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5
    )
    sub.username_pw_set(broker_config["username"], broker_config["password"])
    sub.on_connect = collector.on_connect
    sub.on_subscribe = collector.on_subscribe
    sub.on_message = collector.on_message
    sub.connect(broker_config["host"], broker_config["port"])
    sub.loop_start()
    assert collector.wait_for_connection(), "Subscriber failed to connect"

    sub_options = SubscribeOptions(qos=1)
    sub_options.retainAsPublished = False
    sub.subscribe(topic, options=sub_options)
    assert collector.wait_for_subscription(), "Subscription failed"
    time.sleep(0.5)  # Allow broker to fully register subscription

    # Now publish retained — this is live delivery to an existing subscriber
    pub.publish(topic, "live_retained_msg", qos=1, retain=True)
    assert collector.wait_for_messages(1), "Did not receive live message"

    assert len(collector.messages) == 1
    msg = collector.messages[0]
    assert msg['payload'] == "live_retained_msg"
    assert msg['retain'] is False, "RAP=false: live delivery must have retain=False"

    sub.loop_stop()
    sub.disconnect()
    pub.loop_stop()
    pub.disconnect()


def test_rap_true_preserves_retain_on_live_delivery(broker_config, clean_topic):
    """
    Live delivery with RAP=true must preserve the retain flag.

    Subscribe first with RAP=true, then publish a retained message.
    The live-forwarded copy must arrive with retain=True.
    """
    topic = clean_topic("test/rap/true/live")

    # Clear any stale retained message
    pub = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    pub.username_pw_set(broker_config["username"], broker_config["password"])
    pub_connected = False

    def on_pub_connect(client, userdata, flags, reason_code, properties=None):
        nonlocal pub_connected
        if reason_code == 0:
            pub_connected = True

    pub.on_connect = on_pub_connect
    pub.connect(broker_config["host"], broker_config["port"])
    pub.loop_start()
    start = time.time()
    while not pub_connected and (time.time() - start) < 5.0:
        time.sleep(0.1)
    pub.publish(topic, "", qos=1, retain=True).wait_for_publish()
    time.sleep(0.3)

    # Subscribe with RAP=true
    collector = MessageCollector()
    sub = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5
    )
    sub.username_pw_set(broker_config["username"], broker_config["password"])
    sub.on_connect = collector.on_connect
    sub.on_subscribe = collector.on_subscribe
    sub.on_message = collector.on_message
    sub.connect(broker_config["host"], broker_config["port"])
    sub.loop_start()
    assert collector.wait_for_connection(), "Subscriber failed to connect"

    sub_options = SubscribeOptions(qos=1)
    sub_options.retainAsPublished = True
    sub.subscribe(topic, options=sub_options)
    assert collector.wait_for_subscription(), "Subscription failed"
    time.sleep(0.5)  # Allow broker to fully register subscription

    # Now publish retained — live delivery to existing subscriber with RAP=true
    pub.publish(topic, "live_retained_msg", qos=1, retain=True)
    assert collector.wait_for_messages(1), "Did not receive live message"

    assert len(collector.messages) == 1
    msg = collector.messages[0]
    assert msg['payload'] == "live_retained_msg"
    assert msg['retain'] is True, "RAP=true: live delivery must preserve retain=True"

    sub.loop_stop()
    sub.disconnect()
    pub.loop_stop()
    pub.disconnect()


# ---------------------------------------------------------------------------
# Subscription-time delivery tests (publish first, then subscribe)
# Retained messages delivered at subscription time ALWAYS have retain=1,
# regardless of RAP setting.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("rap_value", [False, True, None])
def test_subscription_time_retained_always_has_retain_flag(broker_config, clean_topic, rap_value):
    """
    Subscription-time delivery of stored retained messages must ALWAYS
    have retain=True, regardless of RAP setting (MQTT 5.0 §3.3.1.3).
    """
    label = "default" if rap_value is None else str(rap_value)
    topic = clean_topic(f"test/rap/subtime/{label}")

    # Publish retained message first
    pub = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    pub.username_pw_set(broker_config["username"], broker_config["password"])
    pub_connected = False

    def on_pub_connect(client, userdata, flags, reason_code, properties=None):
        nonlocal pub_connected
        if reason_code == 0:
            pub_connected = True

    pub.on_connect = on_pub_connect
    pub.connect(broker_config["host"], broker_config["port"])
    pub.loop_start()
    start = time.time()
    while not pub_connected and (time.time() - start) < 5.0:
        time.sleep(0.1)

    pub.publish(topic, f"retained_rap_{label}", qos=1, retain=True)
    time.sleep(0.5)
    pub.loop_stop()
    pub.disconnect()

    # Now subscribe — should receive stored retained message with retain=True
    collector = MessageCollector()
    sub = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5
    )
    sub.username_pw_set(broker_config["username"], broker_config["password"])
    sub.on_connect = collector.on_connect
    sub.on_subscribe = collector.on_subscribe
    sub.on_message = collector.on_message
    sub.connect(broker_config["host"], broker_config["port"])
    sub.loop_start()
    assert collector.wait_for_connection(), "Subscriber failed to connect"

    if rap_value is not None:
        sub_options = SubscribeOptions(qos=1)
        sub_options.retainAsPublished = rap_value
        sub.subscribe(topic, options=sub_options)
    else:
        sub.subscribe(topic, qos=1)

    assert collector.wait_for_subscription(), "Subscription failed"
    assert collector.wait_for_messages(1), "Did not receive retained message"

    assert len(collector.messages) == 1
    msg = collector.messages[0]
    assert msg['payload'] == f"retained_rap_{label}"
    assert msg['retain'] is True, \
        f"Subscription-time delivery must always have retain=True (RAP={label})"

    sub.loop_stop()
    sub.disconnect()


# ---------------------------------------------------------------------------
# Parameterized live delivery tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("rap_value,expected_retain", [
    (False, False),   # RAP=false clears retain on live delivery
    (True, True),     # RAP=true preserves retain on live delivery
    (None, False),    # Default (RAP=false) clears retain on live delivery
])
def test_rap_live_delivery(publisher_client, clean_topic, rap_value, expected_retain, broker_config):
    """
    Parameterized test for RAP behavior on live delivery.

    Subscribe first, then publish a retained message.  The retain flag
    on the live-forwarded message is controlled by RAP.
    """
    label = "default" if rap_value is None else str(rap_value)
    topic = clean_topic(f"test/rap/live/{label}")

    # Clear stale retained
    publisher_client.publish(topic, "", qos=1, retain=True).wait_for_publish()
    time.sleep(0.3)

    # Subscribe
    collector = MessageCollector()
    sub = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=f"rap_live_{label}_{int(time.time() * 1000)}",
        protocol=mqtt.MQTTv5
    )
    sub.username_pw_set(broker_config["username"], broker_config["password"])
    sub.on_connect = collector.on_connect
    sub.on_subscribe = collector.on_subscribe
    sub.on_message = collector.on_message
    sub.connect(broker_config["host"], broker_config["port"])
    sub.loop_start()
    assert collector.wait_for_connection()

    if rap_value is not None:
        sub_options = SubscribeOptions(qos=1)
        sub_options.retainAsPublished = rap_value
        sub.subscribe(topic, options=sub_options)
    else:
        sub.subscribe(topic, qos=1)

    assert collector.wait_for_subscription()
    time.sleep(0.5)  # Allow broker to fully register subscription

    # Publish retained — live delivery path
    publisher_client.publish(topic, f"message_rap_{label}", qos=1, retain=True)
    assert collector.wait_for_messages(1)

    assert len(collector.messages) == 1
    msg = collector.messages[0]
    assert msg['retain'] == expected_retain, \
        f"RAP={label}: live delivery expected retain={expected_retain}, got {msg['retain']}"

    sub.loop_stop()
    sub.disconnect()


def test_rap_with_non_retained_messages(publisher_client, clean_topic, broker_config):
    """
    Test that RAP setting doesn't affect non-retained messages.

    Non-retained messages should always have retain=False regardless of RAP setting.
    """
    topic = clean_topic("test/rap/non_retained")

    # Create two subscribers with different RAP settings
    collectors = [MessageCollector(), MessageCollector()]
    clients = []

    for i, collector in enumerate(collectors):
        client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=f"rap_non_ret_sub{i}_{int(time.time() * 1000)}",
            protocol=mqtt.MQTTv5
        )
        client.username_pw_set(broker_config["username"], broker_config["password"])
        client.on_connect = collector.on_connect
        client.on_subscribe = collector.on_subscribe
        client.on_message = collector.on_message
        client.connect(broker_config["host"], broker_config["port"])
        client.loop_start()
        clients.append(client)
        assert collector.wait_for_connection()

    # Subscribe with different RAP settings
    sub_options_false = SubscribeOptions(qos=1)
    sub_options_false.retainAsPublished = False
    clients[0].subscribe(topic, options=sub_options_false)

    sub_options_true = SubscribeOptions(qos=1)
    sub_options_true.retainAsPublished = True
    clients[1].subscribe(topic, options=sub_options_true)

    for collector in collectors:
        assert collector.wait_for_subscription()

    time.sleep(0.5)

    # Publish NON-retained message
    publisher_client.publish(topic, "live_message", qos=1, retain=False)

    # Both should receive with retain=False
    for collector in collectors:
        assert collector.wait_for_messages(1), "Did not receive message"
        assert len(collector.messages) == 1
        assert collector.messages[0]['retain'] is False

    # Cleanup
    for client in clients:
        client.loop_stop()
        client.disconnect()


def test_rap_with_wildcard_live_delivery(publisher_client, broker_config, clean_topic):
    """
    Test RAP with wildcard topic subscriptions on live delivery.

    Subscribe with wildcard and RAP=false first, then publish retained
    messages.  All live-forwarded messages must have retain=False.
    """
    base_topic = "test/rap/wildcard"
    topics = [f"{base_topic}/topic1", f"{base_topic}/topic2", f"{base_topic}/topic3"]
    for t in topics:
        clean_topic(t)  # register each sub-topic for post-test cleanup

    # Clear stale retained for all sub-topics and wait for each PUBACK
    for topic in topics:
        publisher_client.publish(topic, "", qos=1, retain=True).wait_for_publish()
    time.sleep(0.3)

    # Subscribe with wildcard and RAP=false
    collector = MessageCollector()
    sub = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=f"rap_wild_{int(time.time() * 1000)}",
        protocol=mqtt.MQTTv5
    )
    sub.username_pw_set(broker_config["username"], broker_config["password"])
    sub.on_connect = collector.on_connect
    sub.on_subscribe = collector.on_subscribe
    sub.on_message = collector.on_message
    sub.connect(broker_config["host"], broker_config["port"])
    sub.loop_start()
    assert collector.wait_for_connection()

    sub_options = SubscribeOptions(qos=1)
    sub_options.retainAsPublished = False
    sub.subscribe(f"{base_topic}/#", options=sub_options)
    assert collector.wait_for_subscription()
    time.sleep(0.5)  # Allow broker to fully register subscription

    # Publish retained messages — all are live delivery
    for i, topic in enumerate(topics):
        publisher_client.publish(topic, f"message_{i + 1}", qos=1, retain=True)

    assert collector.wait_for_messages(3, timeout=5.0), \
        f"Expected 3 messages, got {len(collector.messages)}"

    assert len(collector.messages) == 3
    for msg in collector.messages:
        assert msg['retain'] is False, \
            f"Wildcard RAP=false live delivery must have retain=False, got retain=True on {msg['topic']}"

    sub.loop_stop()
    sub.disconnect()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
