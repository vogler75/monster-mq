#!/usr/bin/env python3
"""Basic MQTT publish/subscribe functionality tests"""
import paho.mqtt.client as mqtt
import time
import pytest

pytestmark = pytest.mark.mqtt3


def test_basic_publish_only(broker_config):
    """Test basic MQTT publish without subscription (QoS 0)"""
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "test_pub_only", protocol=mqtt.MQTTv5)
    client.username_pw_set(broker_config["username"], broker_config["password"])
    
    try:
        client.connect(broker_config["host"], broker_config["port"], 60)
        client.loop_start()
        time.sleep(0.5)
        
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
    
    def on_message(client, userdata, msg):
        messages_received.append(msg.payload.decode())
    
    # Create subscriber
    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "test_sub_qos0", protocol=mqtt.MQTTv5)
    sub.username_pw_set(broker_config["username"], broker_config["password"])
    sub.on_message = on_message
    
    # Create publisher
    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "test_pub_qos0", protocol=mqtt.MQTTv5)
    pub.username_pw_set(broker_config["username"], broker_config["password"])
    
    try:
        sub.connect(broker_config["host"], broker_config["port"], 60)
        sub.subscribe("test/qos0", qos=0)
        sub.loop_start()
        time.sleep(1)
        
        pub.connect(broker_config["host"], broker_config["port"], 60)
        pub.publish("test/qos0", b"qos0 test message", qos=0)
        pub.disconnect()
        
        time.sleep(2)
        
        assert len(messages_received) > 0, "No messages received"
        assert messages_received[0] == "qos0 test message", f"Wrong message: {messages_received[0]}"
        
    finally:
        sub.loop_stop()
        sub.disconnect()
        time.sleep(0.5)


def test_basic_pubsub_qos1(broker_config):
    """Test basic publish/subscribe with QoS 1 (at least once delivery)"""
    received = []
    
    def on_message(client, userdata, message):
        received.append(message)
        print(f'Received: {message.payload}, retain={message.retain}')
    
    # Subscriber
    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, 'test_sub_qos1', protocol=mqtt.MQTTv5)
    sub.username_pw_set(broker_config["username"], broker_config["password"])
    sub.on_message = on_message
    sub.connect(broker_config["host"], broker_config["port"])
    sub.subscribe('test/qos1', qos=1)
    sub.loop_start()
    time.sleep(2)
    
    # Publisher
    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, 'test_pub_qos1', protocol=mqtt.MQTTv5)
    pub.username_pw_set(broker_config["username"], broker_config["password"])
    pub.connect(broker_config["host"], broker_config["port"])
    result = pub.publish('test/qos1', b'qos1 test', qos=1, retain=False)
    assert result.rc == 0, f"Publish failed with rc={result.rc}"
    time.sleep(3)
    pub.disconnect()
    
    sub.loop_stop()
    sub.disconnect()
    
    assert len(received) > 0, "No messages received"
    print(f'✓ Received {len(received)} message(s) with QoS 1')


def test_publish_multiple_topics(broker_config):
    """Test publishing to multiple different topics without subscription"""
    connected = [False]
    published_mids = []
    
    def on_connect(client, userdata, flags, rc, properties=None):
        print(f"Connected to MQTT broker with result code {rc}")
        connected[0] = (rc == 0)
    
    def on_publish(client, userdata, mid):
        print(f"Message {mid} published successfully")
        published_mids.append(mid)
    
    # Create MQTT client
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                         client_id="test-multi-topic",
                         protocol=mqtt.MQTTv5)
    client.username_pw_set(broker_config["username"], broker_config["password"])
    client.on_connect = on_connect
    client.on_publish = on_publish
    
    # Connect to broker
    client.connect(broker_config["host"], broker_config["port"], 60)
    client.loop_start()
    
    # Wait a moment for connection
    time.sleep(1)
    assert connected[0], "Failed to connect to broker"
    
    # Publish test messages to different topics
    print("Publishing test messages...")
    result_a = client.publish("test/a", "value_a")
    result_b = client.publish("test/b", "value_b")
    
    assert result_a.rc == 0, f"Publish to test/a failed with rc={result_a.rc}"
    assert result_b.rc == 0, f"Publish to test/b failed with rc={result_b.rc}"
    
    # Wait for publishing to complete
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
    
    def on_connect(client, userdata, flags, rc, properties=None):
        print(f"Connected rc={rc}")
        assert rc == 0, f"Connection failed with rc={rc}"
        client.subscribe("test/multiple", qos=1)
        print("Subscribed to test/multiple")
    
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                         client_id="test-multiple-client",
                         protocol=mqtt.MQTTv5)
    client.username_pw_set(broker_config["username"], broker_config["password"])
    client.on_connect = on_connect
    client.on_message = on_message
    
    client.connect(broker_config["host"], broker_config["port"], 60)
    client.loop_start()
    
    time.sleep(2)  # Wait for connection
    
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
