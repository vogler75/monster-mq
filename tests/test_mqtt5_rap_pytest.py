"""
Pytest version of MQTT v5.0 Retain As Published (RAP) tests.

Tests verify that the Retain As Published subscription option correctly
controls whether the retain flag is preserved when forwarding messages.
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
    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=f"rap_pub_{int(time.time() * 1000)}",
        protocol=mqtt.MQTTv5
    )
    client.username_pw_set(broker_config["username"], broker_config["password"])
    client.connect(broker_config["host"], broker_config["port"])
    client.loop_start()
    time.sleep(0.3)
    
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


def test_rap_false_clears_retain_flag(broker_config, clean_topic):
    """
    Test that RAP=false clears the retain flag on forwarded messages.
    
    When a subscriber uses retainAsPublished=False, messages should be
    delivered with retain=False even if published with retain=True.
    """
    topic = clean_topic("test/rap/false")
    
    # Publish retained message
    pub_client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5
    )
    pub_client.username_pw_set(broker_config["username"], broker_config["password"])
    pub_client.connect(broker_config["host"], broker_config["port"])
    pub_client.loop_start()
    time.sleep(0.3)
    
    pub_client.publish(topic, "test_message", qos=1, retain=True)
    time.sleep(0.5)
    
    pub_client.loop_stop()
    pub_client.disconnect()
    
    # Subscribe with RAP=false
    collector = MessageCollector()
    sub_client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5
    )
    sub_client.username_pw_set(broker_config["username"], broker_config["password"])
    sub_client.on_connect = collector.on_connect
    sub_client.on_subscribe = collector.on_subscribe
    sub_client.on_message = collector.on_message
    sub_client.connect(broker_config["host"], broker_config["port"])
    sub_client.connect(broker_config["host"], broker_config["port"])
    sub_client.loop_start()
    assert collector.wait_for_connection(), "Failed to connect"
    
    sub_options = SubscribeOptions(qos=1)
    sub_options.retainAsPublished = False
    sub_client.subscribe(topic, options=sub_options)
    assert collector.wait_for_subscription(), "Subscription failed"
    
    # Wait for message
    assert collector.wait_for_messages(1), "Did not receive message"
    
    # Verify retain flag is cleared
    assert len(collector.messages) == 1
    msg = collector.messages[0]
    assert msg['payload'] == "test_message"
    assert msg['retain'] is False, "RAP=false should clear retain flag"
    
    # Cleanup
    sub_client.loop_stop()
    sub_client.disconnect()


def test_rap_true_preserves_retain_flag(publisher_client, subscriber_client, message_collector, clean_topic, broker_config):
    """
    Test that RAP=true preserves the retain flag on forwarded messages.
    
    When a subscriber uses retainAsPublished=True, messages should be
    delivered with the original retain flag intact.
    """
    topic = clean_topic("test/rap/true")
    
    # Publish retained message
    publisher_client.publish(topic, "test_message", qos=1, retain=True)
    time.sleep(0.5)
    
    # Subscribe with RAP=true
    subscriber_client.connect(broker_config["host"], broker_config["port"])
    subscriber_client.loop_start()
    assert message_collector.wait_for_connection(), "Subscriber failed to connect"
    
    sub_options = SubscribeOptions(qos=1)
    sub_options.retainAsPublished = True
    subscriber_client.subscribe(topic, options=sub_options)
    assert message_collector.wait_for_subscription(), "Subscription failed"
    
    # Wait for message
    assert message_collector.wait_for_messages(1), "Did not receive message"
    
    # Verify retain flag is preserved
    assert len(message_collector.messages) == 1
    msg = message_collector.messages[0]
    assert msg['payload'] == "test_message"
    assert msg['retain'] is True, "RAP=true should preserve retain flag"


def test_rap_with_non_retained_messages(publisher_client, clean_topic):
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
        client.username_pw_set("Test", "Test")
        client.on_connect = collector.on_connect
        client.on_subscribe = collector.on_subscribe
        client.on_message = collector.on_message
        client.connect("localhost", 1883)
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
    
    time.sleep(0.3)
    
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


@pytest.mark.parametrize("rap_value,expected_retain", [
    (False, False),  # RAP=false clears retain
    (True, True),    # RAP=true preserves retain
    (None, False),   # Default (RAP=false) clears retain
])
def test_rap_multiple_subscribers(publisher_client, clean_topic, rap_value, expected_retain):
    """
    Test RAP behavior with parameterized values.
    
    Verifies that multiple subscribers with different RAP settings
    each receive messages with the correct retain flag.
    """
    topic = clean_topic(f"test/rap/multi/{rap_value}")
    
    # Publish retained message
    publisher_client.publish(topic, f"message_rap_{rap_value}", qos=1, retain=True)
    time.sleep(0.5)
    
    # Create subscriber
    collector = MessageCollector()
    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=f"rap_multi_{rap_value}_{int(time.time() * 1000)}",
        protocol=mqtt.MQTTv5
    )
    client.username_pw_set("Test", "Test")
    client.on_connect = collector.on_connect
    client.on_subscribe = collector.on_subscribe
    client.on_message = collector.on_message
    client.connect("localhost", 1883)
    client.loop_start()
    assert collector.wait_for_connection()
    
    # Subscribe with specified RAP value
    if rap_value is not None:
        sub_options = SubscribeOptions(qos=1)
        sub_options.retainAsPublished = rap_value
        client.subscribe(topic, options=sub_options)
    else:
        # Default subscription (RAP=false)
        client.subscribe(topic, qos=1)
    
    assert collector.wait_for_subscription()
    assert collector.wait_for_messages(1)
    
    # Verify
    assert len(collector.messages) == 1
    msg = collector.messages[0]
    assert msg['retain'] == expected_retain, f"RAP={rap_value} should result in retain={expected_retain}"
    
    # Cleanup
    client.loop_stop()
    client.disconnect()


def test_rap_with_wildcard_subscriptions(publisher_client, subscriber_client, message_collector, clean_topic):
    """
    Test RAP with wildcard topic subscriptions.
    
    Verifies that RAP setting applies correctly even when using
    wildcard subscriptions (# or +).
    """
    base_topic = clean_topic("test/rap/wildcard")
    topics = [f"{base_topic}/topic1", f"{base_topic}/topic2", f"{base_topic}/topic3"]
    
    # Publish retained messages to multiple topics
    for i, topic in enumerate(topics):
        publisher_client.publish(topic, f"message_{i+1}", qos=1, retain=True)
    time.sleep(0.5)
    
    # Subscribe with wildcard and RAP=false
    subscriber_client.connect("localhost", 1883)
    subscriber_client.loop_start()
    assert message_collector.wait_for_connection()
    
    sub_options = SubscribeOptions(qos=1)
    sub_options.retainAsPublished = False
    subscriber_client.subscribe(f"{base_topic}/#", options=sub_options)
    assert message_collector.wait_for_subscription()
    
    # Wait for all messages
    assert message_collector.wait_for_messages(3, timeout=5.0), "Did not receive all messages"
    
    # Verify all messages have retain=False
    assert len(message_collector.messages) == 3
    for msg in message_collector.messages:
        assert msg['retain'] is False, "Wildcard subscription with RAP=false should clear all retain flags"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
