"""
Simple pytest example for Monster-MQ - demonstrating pytest benefits
with minimal changes from the original test structure.
"""
import time
import pytest
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.subscribeoptions import SubscribeOptions


# Mark all tests as MQTT v5 tests
pytestmark = pytest.mark.mqtt5


@pytest.fixture(scope="module")
def broker_config():
    """Broker connection configuration."""
    return {
        "host": "localhost",
        "port": 1883,
        "username": "Test",
        "password": "Test"
    }


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
        client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
        client.username_pw_set(broker_config["username"], broker_config["password"])
        try:
            client.connect(broker_config["host"], broker_config["port"])
            client.loop_start()
            time.sleep(0.2)
            for topic in topics:
                client.publish(topic, "", retain=True)
            time.sleep(0.2)
            client.loop_stop()
            client.disconnect()
        except:
            pass


@pytest.mark.parametrize("rap_value,expected_retain", [
    (False, False),  # RAP=false clears retain
    (True, True),    # RAP=true preserves retain
])
def test_rap_subscription_option(broker_config, cleanup_topic, rap_value, expected_retain):
    """
    Test Retain As Published (RAP) subscription option.
    
    Verifies that RAP controls whether the retain flag is preserved
    when forwarding retained messages to subscribers.
    """
    topic = cleanup_topic(f"test/rap/param_{rap_value}")
    messages = []
    sub_ready = False
    
    def on_connect(client, userdata, flags, reason_code, properties):
        """Connection callback - subscribe once connected."""
        sub_options = SubscribeOptions(qos=1)
        sub_options.retainAsPublished = rap_value
        client.subscribe(topic, options=sub_options)
    
    def on_subscribe(client, userdata, mid, reason_code_list, properties):
        """Subscribe callback."""
        nonlocal sub_ready
        sub_ready = True
    
    def on_message(client, userdata, msg):
        """Message callback."""
        messages.append({
            'payload': msg.payload.decode('utf-8'),
            'retain': msg.retain
        })
    
    # Publish retained message
    pub = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    pub.username_pw_set(broker_config["username"], broker_config["password"])
    pub.connect(broker_config["host"], broker_config["port"])
    pub.loop_start()
    time.sleep(0.3)
    
    pub.publish(topic, f"test_{rap_value}", qos=1, retain=True)
    time.sleep(0.5)
    
    pub.loop_stop()
    pub.disconnect()
    
    # Subscribe with RAP setting
    sub = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    sub.username_pw_set(broker_config["username"], broker_config["password"])
    sub.on_connect = on_connect
    sub.on_subscribe = on_subscribe
    sub.on_message = on_message
    sub.connect(broker_config["host"], broker_config["port"])
    sub.loop_start()
    
    # Wait for subscription
    timeout = time.time() + 5
    while not sub_ready and time.time() < timeout:
        time.sleep(0.1)
    assert sub_ready, "Subscription did not complete"
    
    # Wait for message
    time.sleep(2.0)
    
    # Verify
    assert len(messages) == 1, f"Expected 1 message, got {len(messages)}"
    assert messages[0]['payload'] == f"test_{rap_value}"
    assert messages[0]['retain'] == expected_retain, \
        f"RAP={rap_value} should result in retain={expected_retain}, got {messages[0]['retain']}"
    
    # Cleanup
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
    
    time.sleep(0.3)
    
    # Publish non-retained message
    pub = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
    pub.username_pw_set(broker_config["username"], broker_config["password"])
    pub.connect(broker_config["host"], broker_config["port"])
    pub.loop_start()
    time.sleep(0.2)
    
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
