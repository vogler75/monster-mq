"""
Simple pytest example for Monster-MQ - demonstrating pytest benefits
with minimal changes from the original test structure.

MQTT 5.0 §3.3.1.3 rules:
- Subscription-time delivery of stored retained messages: retain=1 ALWAYS
- Live delivery to already-subscribed clients: retain=0 unless RAP=true
"""
import time
import pytest
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.subscribeoptions import SubscribeOptions


# Mark all tests as MQTT v5 tests
pytestmark = pytest.mark.mqtt5


@pytest.fixture
def cleanup_topic(broker_config):
    """Cleanup retained messages after test."""
    topics = []

    def _add_topic(topic):
        topics.append(topic)
        return topic

    yield _add_topic

    # Cleanup
    if topics:
        cleanup_connected = False

        def on_cleanup_connect(client, userdata, flags, reason_code, properties=None):
            nonlocal cleanup_connected
            if reason_code == 0:
                cleanup_connected = True

        client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
        client.on_connect = on_cleanup_connect
        client.username_pw_set(broker_config["username"], broker_config["password"])
        try:
            client.connect(broker_config["host"], broker_config["port"])
            client.loop_start()
            start = time.time()
            while not cleanup_connected and (time.time() - start) < 5.0:
                time.sleep(0.1)
            for topic in topics:
                client.publish(topic, "", retain=True)
            time.sleep(0.2)
            client.loop_stop()
            client.disconnect()
        except:
            pass


@pytest.mark.parametrize("rap_value,expected_retain", [
    (False, False),  # RAP=false clears retain on live delivery
    (True, True),    # RAP=true preserves retain on live delivery
])
def test_rap_live_delivery(broker_config, cleanup_topic, rap_value, expected_retain):
    """
    Test Retain As Published (RAP) on live delivery.

    Subscribe first with the given RAP setting, then publish a retained
    message.  The retain flag on the live-forwarded copy is controlled by RAP.
    """
    topic = cleanup_topic(f"test/rap/live_{rap_value}")
    messages = []
    sub_ready = False

    def on_connect(client, userdata, flags, reason_code, properties):
        sub_options = SubscribeOptions(qos=1)
        sub_options.retainAsPublished = rap_value
        client.subscribe(topic, options=sub_options)

    def on_subscribe(client, userdata, mid, reason_code_list, properties):
        nonlocal sub_ready
        sub_ready = True

    def on_message(client, userdata, msg):
        messages.append({
            'payload': msg.payload.decode('utf-8'),
            'retain': msg.retain
        })

    # Clear any stale retained message
    pub_connected = False

    def on_pub_connect(client, userdata, flags, reason_code, properties=None):
        nonlocal pub_connected
        if reason_code == 0:
            pub_connected = True

    pub = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    pub.username_pw_set(broker_config["username"], broker_config["password"])
    pub.on_connect = on_pub_connect
    pub.connect(broker_config["host"], broker_config["port"])
    pub.loop_start()
    start = time.time()
    while not pub_connected and (time.time() - start) < 5.0:
        time.sleep(0.1)
    assert pub_connected, "Publisher did not connect"

    pub.publish(topic, "", qos=1, retain=True).wait_for_publish()
    time.sleep(0.3)

    # Subscribe first
    sub = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    sub.username_pw_set(broker_config["username"], broker_config["password"])
    sub.on_connect = on_connect
    sub.on_subscribe = on_subscribe
    sub.on_message = on_message
    sub.connect(broker_config["host"], broker_config["port"])
    sub.loop_start()

    timeout = time.time() + 5
    while not sub_ready and time.time() < timeout:
        time.sleep(0.1)
    assert sub_ready, "Subscription did not complete"
    time.sleep(0.5)  # Allow broker to fully register subscription

    # Now publish retained — this is live delivery
    pub.publish(topic, f"live_{rap_value}", qos=1, retain=True)
    time.sleep(2.0)

    assert len(messages) == 1, f"Expected 1 message, got {len(messages)}"
    assert messages[0]['payload'] == f"live_{rap_value}"
    assert messages[0]['retain'] == expected_retain, \
        f"RAP={rap_value} live delivery: expected retain={expected_retain}, got {messages[0]['retain']}"

    sub.loop_stop()
    sub.disconnect()
    pub.loop_stop()
    pub.disconnect()


@pytest.mark.parametrize("rap_value", [False, True])
def test_subscription_time_retained_always_has_retain(broker_config, cleanup_topic, rap_value):
    """
    Subscription-time delivery of stored retained messages must ALWAYS
    have retain=True, regardless of RAP setting (MQTT 5.0 §3.3.1.3).
    """
    topic = cleanup_topic(f"test/rap/subtime_{rap_value}")
    messages = []
    sub_ready = False

    def on_connect(client, userdata, flags, reason_code, properties):
        sub_options = SubscribeOptions(qos=1)
        sub_options.retainAsPublished = rap_value
        client.subscribe(topic, options=sub_options)

    def on_subscribe(client, userdata, mid, reason_code_list, properties):
        nonlocal sub_ready
        sub_ready = True

    def on_message(client, userdata, msg):
        messages.append({
            'payload': msg.payload.decode('utf-8'),
            'retain': msg.retain
        })

    # Publish retained message first
    pub_connected = False

    def on_pub_connect(client, userdata, flags, reason_code, properties=None):
        nonlocal pub_connected
        if reason_code == 0:
            pub_connected = True

    pub = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    pub.username_pw_set(broker_config["username"], broker_config["password"])
    pub.on_connect = on_pub_connect
    pub.connect(broker_config["host"], broker_config["port"])
    pub.loop_start()
    start = time.time()
    while not pub_connected and (time.time() - start) < 5.0:
        time.sleep(0.1)
    assert pub_connected, "Publisher did not connect"

    pub.publish(topic, f"retained_{rap_value}", qos=1, retain=True)
    time.sleep(0.5)
    pub.loop_stop()
    pub.disconnect()

    # Now subscribe — broker must deliver stored retained message with retain=True
    sub = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    sub.username_pw_set(broker_config["username"], broker_config["password"])
    sub.on_connect = on_connect
    sub.on_subscribe = on_subscribe
    sub.on_message = on_message
    sub.connect(broker_config["host"], broker_config["port"])
    sub.loop_start()

    timeout = time.time() + 5
    while not sub_ready and time.time() < timeout:
        time.sleep(0.1)
    assert sub_ready, "Subscription did not complete"

    time.sleep(2.0)

    assert len(messages) == 1, f"Expected 1 message, got {len(messages)}"
    assert messages[0]['payload'] == f"retained_{rap_value}"
    assert messages[0]['retain'] is True, \
        f"Subscription-time delivery must always have retain=True (RAP={rap_value}), got {messages[0]['retain']}"

    sub.loop_stop()
    sub.disconnect()


def test_rap_with_live_messages(broker_config, cleanup_topic):
    """
    Test that RAP doesn't affect non-retained messages.

    Live (non-retained) messages should always have retain=False
    regardless of the RAP setting.
    """
    topic = cleanup_topic("test/rap/live")
    messages_false = []
    messages_true = []
    ready = [False, False]

    # Create two subscribers with different RAP settings
    def make_callbacks(msg_list, index):
        def on_connect(client, userdata, flags, reason_code, properties):
            sub_options = SubscribeOptions(qos=1)
            sub_options.retainAsPublished = (index == 1)  # False for 0, True for 1
            client.subscribe(topic, options=sub_options)

        def on_subscribe(client, userdata, mid, reason_code_list, properties):
            ready[index] = True

        def on_message(client, userdata, msg):
            msg_list.append(msg.retain)

        return on_connect, on_subscribe, on_message

    clients = []
    for i, msg_list in enumerate([messages_false, messages_true]):
        on_conn, on_sub, on_msg = make_callbacks(msg_list, i)

        client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
        client.username_pw_set(broker_config["username"], broker_config["password"])
        client.on_connect = on_conn
        client.on_subscribe = on_sub
        client.on_message = on_msg
        client.connect(broker_config["host"], broker_config["port"])
        client.loop_start()
        clients.append(client)

    # Wait for both subscriptions
    timeout = time.time() + 5
    while not all(ready) and time.time() < timeout:
        time.sleep(0.1)
    assert all(ready), "Not all subscriptions completed"

    time.sleep(0.5)

    # Publish non-retained message
    pub_connected2 = False

    def on_pub_connect2(client, userdata, flags, reason_code, properties=None):
        nonlocal pub_connected2
        if reason_code == 0:
            pub_connected2 = True

    pub = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    pub.username_pw_set(broker_config["username"], broker_config["password"])
    pub.on_connect = on_pub_connect2
    pub.connect(broker_config["host"], broker_config["port"])
    pub.loop_start()
    start = time.time()
    while not pub_connected2 and (time.time() - start) < 5.0:
        time.sleep(0.1)
    assert pub_connected2, "Publisher did not connect"

    pub.publish(topic, "live_message", qos=1, retain=False)
    time.sleep(1.0)

    pub.loop_stop()
    pub.disconnect()

    # Verify both received with retain=False
    assert len(messages_false) == 1, f"Sub with RAP=false got {len(messages_false)} messages"
    assert len(messages_true) == 1, f"Sub with RAP=true got {len(messages_true)} messages"
    assert messages_false[0] is False, "Live message should have retain=False (RAP=false)"
    assert messages_true[0] is False, "Live message should have retain=False (RAP=true)"

    # Cleanup
    for client in clients:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    # Can still run standalone
    pytest.main([__file__, "-v", "-s"])
