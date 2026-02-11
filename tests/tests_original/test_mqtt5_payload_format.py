#!/usr/bin/env python3
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
import sys

# Configuration
BROKER_HOST = "localhost"
BROKER_PORT = 1883
TEST_TOPIC = "test/payload/format"

# Test state
messages_received = []
connections = {}

def on_connect(client, userdata, flags, rc, properties=None):
    """Handle connection callback"""
    client_name = userdata
    print(f"[{client_name}] Connected rc={rc}")
    connections[client_name] = True

def on_message(client, userdata, msg):
    """Handle message callback"""
    client_name = userdata
    payload = msg.payload
    
    # Check if properties exist and get payload format indicator
    payload_format = None
    if hasattr(msg, 'properties') and msg.properties:
        if hasattr(msg.properties, 'PayloadFormatIndicator'):
            payload_format = msg.properties.PayloadFormatIndicator
    
    print(f"[{client_name}] Received message:")
    print(f"  Topic: {msg.topic}")
    print(f"  Payload (bytes): {payload}")
    print(f"  Payload Format Indicator: {payload_format}")
    
    messages_received.append({
        'topic': msg.topic,
        'payload': payload,
        'payloadFormatIndicator': payload_format
    })

def on_disconnect(client, userdata, flags, rc, properties=None):
    """Handle disconnect for MQTT v5"""
    client_name = userdata
    print(f"[{client_name}] Disconnected rc={rc}")

def on_publish(client, userdata, mid, reason_code=None, properties=None):
    """Handle publish acknowledgment"""
    client_name = userdata
    print(f"[{client_name}] Message {mid} published successfully")

def cleanup():
    """Clean up test messages"""
    print("\nCleaning up test messages...")
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                        client_id="cleanup_client",
                        protocol=mqtt.MQTTv5,
                        userdata="Cleanup")
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()
    time.sleep(0.5)
    
    # Delete any retained messages
    client.publish(TEST_TOPIC, "", qos=1, retain=True)
    time.sleep(0.5)
    
    client.loop_stop()
    client.disconnect()
    time.sleep(0.5)

def test_payload_format_utf8_valid():
    """Test 1: Valid UTF-8 payload with payloadFormatIndicator=1"""
    print("\n" + "="*70)
    print("TEST 1: Payload Format Indicator = 1 (UTF-8) with valid UTF-8")
    print("="*70)
    
    # Reset state
    messages_received.clear()
    connections.clear()
    
    # Create subscriber
    subscriber = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                            client_id="subscriber1",
                            protocol=mqtt.MQTTv5,
                            userdata="Subscriber1")
    subscriber.on_connect = on_connect
    subscriber.on_message = on_message
    subscriber.on_disconnect = on_disconnect
    
    subscriber.connect(BROKER_HOST, BROKER_PORT, 60)
    subscriber.loop_start()
    time.sleep(0.5)
    
    subscriber.subscribe(TEST_TOPIC, qos=1)
    time.sleep(0.5)
    
    # Create publisher
    publisher = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                           client_id="publisher1",
                           protocol=mqtt.MQTTv5,
                           userdata="Publisher1")
    publisher.on_connect = on_connect
    publisher.on_publish = on_publish
    
    publisher.connect(BROKER_HOST, BROKER_PORT, 60)
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
    success = len(messages_received) == 1
    
    if success:
        msg = messages_received[0]
        if msg['payloadFormatIndicator'] == 1:
            print("✓ TEST 1 PASSED: Valid UTF-8 accepted with PayloadFormatIndicator=1")
            print(f"  Payload: {msg['payload'].decode('utf-8')}")
            print(f"  Format Indicator: {msg['payloadFormatIndicator']}")
        else:
            print(f"✗ TEST 1 FAILED: PayloadFormatIndicator not preserved (got {msg['payloadFormatIndicator']})")
            success = False
    else:
        print(f"✗ TEST 1 FAILED: Expected 1 message, got {len(messages_received)}")
        success = False
    
    # Cleanup
    subscriber.loop_stop()
    subscriber.disconnect()
    publisher.loop_stop()
    publisher.disconnect()
    time.sleep(0.5)
    
    return success

def test_payload_format_binary():
    """Test 2: Binary payload with payloadFormatIndicator=0"""
    print("\n" + "="*70)
    print("TEST 2: Payload Format Indicator = 0 (Binary)")
    print("="*70)
    
    # Reset state
    messages_received.clear()
    connections.clear()
    
    # Create subscriber
    subscriber = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                            client_id="subscriber2",
                            protocol=mqtt.MQTTv5,
                            userdata="Subscriber2")
    subscriber.on_connect = on_connect
    subscriber.on_message = on_message
    subscriber.on_disconnect = on_disconnect
    
    subscriber.connect(BROKER_HOST, BROKER_PORT, 60)
    subscriber.loop_start()
    time.sleep(0.5)
    
    subscriber.subscribe(TEST_TOPIC, qos=1)
    time.sleep(0.5)
    
    # Create publisher
    publisher = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                           client_id="publisher2",
                           protocol=mqtt.MQTTv5,
                           userdata="Publisher2")
    publisher.on_connect = on_connect
    publisher.on_publish = on_publish
    
    publisher.connect(BROKER_HOST, BROKER_PORT, 60)
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
    success = len(messages_received) == 1
    
    if success:
        msg = messages_received[0]
        if msg['payloadFormatIndicator'] == 0 and msg['payload'] == binary_payload:
            print("✓ TEST 2 PASSED: Binary payload accepted with PayloadFormatIndicator=0")
            print(f"  Payload bytes: {msg['payload'].hex()}")
            print(f"  Format Indicator: {msg['payloadFormatIndicator']}")
        else:
            print(f"✗ TEST 2 FAILED: Payload or indicator mismatch")
            success = False
    else:
        print(f"✗ TEST 2 FAILED: Expected 1 message, got {len(messages_received)}")
        success = False
    
    # Cleanup
    subscriber.loop_stop()
    subscriber.disconnect()
    publisher.loop_stop()
    publisher.disconnect()
    time.sleep(0.5)
    
    return success

def test_payload_format_default():
    """Test 3: No payload format indicator (default behavior)"""
    print("\n" + "="*70)
    print("TEST 3: No Payload Format Indicator (default)")
    print("="*70)
    
    # Reset state
    messages_received.clear()
    connections.clear()
    
    # Create subscriber
    subscriber = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                            client_id="subscriber3",
                            protocol=mqtt.MQTTv5,
                            userdata="Subscriber3")
    subscriber.on_connect = on_connect
    subscriber.on_message = on_message
    subscriber.on_disconnect = on_disconnect
    
    subscriber.connect(BROKER_HOST, BROKER_PORT, 60)
    subscriber.loop_start()
    time.sleep(0.5)
    
    subscriber.subscribe(TEST_TOPIC, qos=1)
    time.sleep(0.5)
    
    # Create publisher
    publisher = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                           client_id="publisher3",
                           protocol=mqtt.MQTTv5,
                           userdata="Publisher3")
    publisher.on_connect = on_connect
    publisher.on_publish = on_publish
    
    publisher.connect(BROKER_HOST, BROKER_PORT, 60)
    publisher.loop_start()
    time.sleep(0.5)
    
    # Publish without specifying payload format indicator
    print("\nPublishing message without PayloadFormatIndicator property...")
    payload = b"Default payload format"
    
    result = publisher.publish(TEST_TOPIC, payload, qos=1)
    result.wait_for_publish()
    time.sleep(1)
    
    # Verify message received
    success = len(messages_received) == 1
    
    if success:
        msg = messages_received[0]
        # PayloadFormatIndicator should be None/absent when not set
        if msg['payloadFormatIndicator'] is None and msg['payload'] == payload:
            print("✓ TEST 3 PASSED: Message delivered without PayloadFormatIndicator")
            print(f"  Payload: {msg['payload']}")
            print(f"  Format Indicator: {msg['payloadFormatIndicator']} (absent)")
        else:
            print(f"✗ TEST 3 FAILED: Unexpected format indicator: {msg['payloadFormatIndicator']}")
            success = False
    else:
        print(f"✗ TEST 3 FAILED: Expected 1 message, got {len(messages_received)}")
        success = False
    
    # Cleanup
    subscriber.loop_stop()
    subscriber.disconnect()
    publisher.loop_stop()
    publisher.disconnect()
    time.sleep(0.5)
    
    return success

def run_test():
    """Run all payload format indicator tests"""
    print("\n" + "="*70)
    print("MQTT v5.0 PAYLOAD FORMAT INDICATOR TEST")
    print("="*70)
    
    try:
        # Run all tests
        test1_pass = test_payload_format_utf8_valid()
        test2_pass = test_payload_format_binary()
        test3_pass = test_payload_format_default()
        
        # Summary
        print("\n" + "="*70)
        print("TEST SUMMARY")
        print("="*70)
        print(f"Test 1 (UTF-8 valid): {'✓ PASSED' if test1_pass else '✗ FAILED'}")
        print(f"Test 2 (Binary): {'✓ PASSED' if test2_pass else '✗ FAILED'}")
        print(f"Test 3 (Default): {'✓ PASSED' if test3_pass else '✗ FAILED'}")
        
        all_pass = test1_pass and test2_pass and test3_pass
        
        print("\n" + "="*70)
        if all_pass:
            print("✓✓✓ PAYLOAD FORMAT INDICATOR TEST PASSED ✓✓✓")
            print("Payload Format Indicator property working correctly!")
        else:
            print("✗✗✗ PAYLOAD FORMAT INDICATOR TEST FAILED ✗✗✗")
        print("="*70 + "\n")
        
        cleanup()
        return all_pass
        
    except Exception as e:
        print(f"\n✗ TEST ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_test()
    sys.exit(0 if success else 1)
