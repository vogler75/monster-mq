"""
MQTT v5.0 Payload Format Indicator Test

Tests the Payload Format Indicator property (property ID 1):
- When payloadFormatIndicator = 0 (or absent): Binary payload, no validation
- When payloadFormatIndicator = 1: UTF-8 payload, broker validates encoding

Per MQTT v5.0 spec 3.3.2.3.3: The Payload Format Indicator is a One Byte Integer
that identifies the type of payload: 0 = unspecified bytes, 1 = UTF-8 encoded payload.

Test Scenarios:
1. Publish with payloadFormatIndicator=1 and valid UTF-8 - should succeed
2. Publish with payloadFormatIndicator=1 and invalid UTF-8 - should log warning
3. Publish with payloadFormatIndicator=0 (binary) - should succeed regardless
4. Subscriber receives payloadFormatIndicator property correctly
"""

import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
import time
import pytest

pytestmark = pytest.mark.mqtt5

# Configuration
TEST_TOPIC = "test/payload/format"


def test_payload_format_utf8_valid(broker_config):
    """Test 1: Valid UTF-8 payload with payloadFormatIndicator=1"""
    # Test state
    messages_received = []
    connections = {}
    
    def on_connect(client, userdata, flags, rc, properties=None):
        client_name = userdata
        print(f"[{client_name}] Connected rc={rc}")
        connections[client_name] = True

    def on_message(client, userdata, msg):
        client_name = userdata
        payload = msg.payload
        
        # Get payload format indicator
        payload_format = None
        if hasattr(msg, 'properties') and msg.properties:
            if hasattr(msg.properties, 'PayloadFormatIndicator'):
                payload_format = msg.properties.PayloadFormatIndicator
        
        print(f"[{client_name}] Received message:")
        print(f"  Topic: {msg.topic}")
        print(f"  Payload Format Indicator: {payload_format}")
        
        messages_received.append({
            'topic': msg.topic,
            'payload': payload,
            'payloadFormatIndicator': payload_format
        })

    def on_publish(client, userdata, mid, reason_code=None, properties=None):
        client_name = userdata
        print(f"[{client_name}] Message {mid} published successfully")
    
    # Create subscriber
    subscriber = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                            client_id="subscriber1",
                            protocol=mqtt.MQTTv5,
                            userdata="Subscriber1")
    subscriber.username_pw_set(broker_config["username"], broker_config["password"])
    subscriber.on_connect = on_connect
    subscriber.on_message = on_message
    
    # Create publisher
    publisher = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                           client_id="publisher1",
                           protocol=mqtt.MQTTv5,
                           userdata="Publisher1")
    publisher.username_pw_set(broker_config["username"], broker_config["password"])
    publisher.on_connect = on_connect
    publisher.on_publish = on_publish
    
    try:
        subscriber.connect(broker_config["host"], broker_config["port"], 60)
        subscriber.loop_start()
        time.sleep(0.5)
        
        subscriber.subscribe(TEST_TOPIC, qos=1)
        time.sleep(0.5)
        
        publisher.connect(broker_config["host"], broker_config["port"], 60)
        publisher.loop_start()
        time.sleep(0.5)
        
        # Publish with valid UTF-8 and payloadFormatIndicator=1
        print("\nPublishing valid UTF-8 message with PayloadFormatIndicator=1...")
        valid_utf8_payload = "Hello, MQTT v5! 你好 مرحبا".encode('utf-8')
        
        props = Properties(PacketTypes.PUBLISH)
        props.PayloadFormatIndicator = 1  # UTF-8
        
        result = publisher.publish(TEST_TOPIC, valid_utf8_payload, qos=1, properties=props)
        result.wait_for_publish()
        time.sleep(1)
        
        # Verify message received
        assert len(messages_received) == 1, f"Expected 1 message, got {len(messages_received)}"
        msg = messages_received[0]
        assert msg['payloadFormatIndicator'] == 1, f"Expected format indicator 1, got {msg['payloadFormatIndicator']}"
        assert msg['payload'] == valid_utf8_payload, "Payload mismatch"
        print("✓ Valid UTF-8 accepted with PayloadFormatIndicator=1")
        
    finally:
        subscriber.loop_stop()
        subscriber.disconnect()
        publisher.loop_stop()
        publisher.disconnect()
        time.sleep(0.5)


def test_payload_format_binary(broker_config):
    """Test 2: Binary payload with payloadFormatIndicator=0"""
    # Test state
    messages_received = []
    connections = {}
    
    def on_connect(client, userdata, flags, rc, properties=None):
        client_name = userdata
        print(f"[{client_name}] Connected rc={rc}")
        connections[client_name] = True

    def on_message(client, userdata, msg):
        client_name = userdata
        payload = msg.payload
        
        # Get payload format indicator
        payload_format = None
        if hasattr(msg, 'properties') and msg.properties:
            if hasattr(msg.properties, 'PayloadFormatIndicator'):
                payload_format = msg.properties.PayloadFormatIndicator
        
        print(f"[{client_name}] Received message:")
        print(f"  Topic: {msg.topic}")
        print(f"  Payload Format Indicator: {payload_format}")
        
        messages_received.append({
            'topic': msg.topic,
            'payload': payload,
            'payloadFormatIndicator': payload_format
        })

    def on_publish(client, userdata, mid, reason_code=None, properties=None):
        client_name = userdata
        print(f"[{client_name}] Message {mid} published successfully")
    
    # Create subscriber
    subscriber = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                            client_id="subscriber2",
                            protocol=mqtt.MQTTv5,
                            userdata="Subscriber2")
    subscriber.username_pw_set(broker_config["username"], broker_config["password"])
    subscriber.on_connect = on_connect
    subscriber.on_message = on_message
    
    # Create publisher
    publisher = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                           client_id="publisher2",
                           protocol=mqtt.MQTTv5,
                           userdata="Publisher2")
    publisher.username_pw_set(broker_config["username"], broker_config["password"])
    publisher.on_connect = on_connect
    publisher.on_publish = on_publish
    
    try:
        subscriber.connect(broker_config["host"], broker_config["port"], 60)
        subscriber.loop_start()
        time.sleep(0.5)
        
        subscriber.subscribe(TEST_TOPIC, qos=1)
        time.sleep(0.5)
        
        publisher.connect(broker_config["host"], broker_config["port"], 60)
        publisher.loop_start()
        time.sleep(0.5)
        
        # Publish binary data with payloadFormatIndicator=0
        print("\nPublishing binary data with PayloadFormatIndicator=0...")
        binary_payload = bytes([0xFF, 0xFE, 0xFD, 0x00, 0x01, 0x02])  # Invalid UTF-8
        
        props = Properties(PacketTypes.PUBLISH)
        props.PayloadFormatIndicator = 0  # Binary/Unspecified
        
        result = publisher.publish(TEST_TOPIC, binary_payload, qos=1, properties=props)
        result.wait_for_publish()
        time.sleep(1)
        
        # Verify message received
        assert len(messages_received) == 1, f"Expected 1 message, got {len(messages_received)}"
        msg = messages_received[0]
        assert msg['payloadFormatIndicator'] == 0, f"Expected format indicator 0, got {msg['payloadFormatIndicator']}"
        assert msg['payload'] == binary_payload, "Payload mismatch"
        print("✓ Binary payload accepted with PayloadFormatIndicator=0")
        
    finally:
        subscriber.loop_stop()
        subscriber.disconnect()
        publisher.loop_stop()
        publisher.disconnect()
        time.sleep(0.5)


def test_payload_format_default(broker_config):
    """Test 3: No payload format indicator (default behavior)"""
    # Test state
    messages_received = []
    connections = {}
    
    def on_connect(client, userdata, flags, rc, properties=None):
        client_name = userdata
        print(f"[{client_name}] Connected rc={rc}")
        connections[client_name] = True

    def on_message(client, userdata, msg):
        client_name = userdata
        payload = msg.payload
        
        # Get payload format indicator
        payload_format = None
        if hasattr(msg, 'properties') and msg.properties:
            if hasattr(msg.properties, 'PayloadFormatIndicator'):
                payload_format = msg.properties.PayloadFormatIndicator
        
        print(f"[{client_name}] Received message:")
        print(f"  Topic: {msg.topic}")
        print(f"  Payload Format Indicator: {payload_format}")
        
        messages_received.append({
            'topic': msg.topic,
            'payload': payload,
            'payloadFormatIndicator': payload_format
        })

    def on_publish(client, userdata, mid, reason_code=None, properties=None):
        client_name = userdata
        print(f"[{client_name}] Message {mid} published successfully")
    
    # Create subscriber
    subscriber = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                            client_id="subscriber3",
                            protocol=mqtt.MQTTv5,
                            userdata="Subscriber3")
    subscriber.username_pw_set(broker_config["username"], broker_config["password"])
    subscriber.on_connect = on_connect
    subscriber.on_message = on_message
    
    # Create publisher
    publisher = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                           client_id="publisher3",
                           protocol=mqtt.MQTTv5,
                           userdata="Publisher3")
    publisher.username_pw_set(broker_config["username"], broker_config["password"])
    publisher.on_connect = on_connect
    publisher.on_publish = on_publish
    
    try:
        subscriber.connect(broker_config["host"], broker_config["port"], 60)
        subscriber.loop_start()
        time.sleep(0.5)
        
        subscriber.subscribe(TEST_TOPIC, qos=1)
        time.sleep(0.5)
        
        publisher.connect(broker_config["host"], broker_config["port"], 60)
        publisher.loop_start()
        time.sleep(0.5)
        
        # Publish without specifying payload format indicator
        print("\nPublishing message without PayloadFormatIndicator property...")
        payload = b"Default payload format"
        
        result = publisher.publish(TEST_TOPIC, payload, qos=1)
        result.wait_for_publish()
        time.sleep(1)
        
        # Verify message received
        assert len(messages_received) == 1, f"Expected 1 message, got {len(messages_received)}"
        msg = messages_received[0]
        assert msg['payloadFormatIndicator'] is None, f"Expected None format indicator, got {msg['payloadFormatIndicator']}"
        assert msg['payload'] == payload, "Payload mismatch"
        print("✓ Message delivered without PayloadFormatIndicator")
        
    finally:
        subscriber.loop_stop()
        subscriber.disconnect()
        publisher.loop_stop()
        publisher.disconnect()
        time.sleep(0.5)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
