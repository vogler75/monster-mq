#!/usr/bin/env python3
"""Simple test to verify basic MQTT publish/subscribe works"""
import paho.mqtt.client as mqtt
import time
import pytest

print("Testing basic MQTT functionality...")

# Test 1: Simple publish (no subscribe)
try:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "test_pub_only", protocol=mqtt.MQTTv5)
    client.connect("localhost", 1883, 60)
    client.loop_start()  # Start loop to process network traffic
    time.sleep(0.5)  # Allow connection to establish
    result = client.publish("test/verify", b"test message", qos=0)
    time.sleep(1)  # Wait for publish to complete
    client.loop_stop()
    client.disconnect()
    print(f"[OK] Publish-only test passed (rc={result.rc})")
except Exception as e:
    print(f"[FAIL] Publish-only test failed: {e}")

time.sleep(1)

# Test 2: Pub/Sub with synchronous loop
messages_received = []

def on_message(client, userdata, msg):
    messages_received.append(msg.payload.decode())
    print(f"  Message received: {msg.payload.decode()}")

try:
    # Create subscriber
    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "test_sub_sync", protocol=mqtt.MQTTv5)
    sub.on_message = on_message
    sub.connect("localhost", 1883, 60)
    sub.subscribe("test/sync", qos=0)
    
    # Start loop in background
    sub.loop_start()
    time.sleep(1)  # Wait for subscription to complete
    
    # Publish message
    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "test_pub_sync", protocol=mqtt.MQTTv5)
    pub.connect("localhost", 1883, 60)
    pub.publish("test/sync", b"sync test message", qos=0)
    pub.disconnect()
    
    # Wait for message
    time.sleep(2)
    
    sub.loop_stop()
    sub.disconnect()
    
    if len(messages_received) > 0:
        print(f"[OK] Pub/Sub test passed ({len(messages_received)} messages received)")
    else:
        print("[FAIL] Pub/Sub test failed: No messages received")
        
except Exception as e:
    print(f"[FAIL] Pub/Sub test failed with exception: {e}")
    import traceback
    traceback.print_exc()

print("\nTest complete")
