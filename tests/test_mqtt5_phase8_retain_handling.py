#!/usr/bin/env python3
"""
MQTT v5.0 Phase 8 - Retain Handling Subscription Option Test

Tests the Retain Handling subscription option feature:
- Retain Handling = 0: Send retained messages at subscribe time (default)
- Retain Handling = 1: Send retained messages only if subscription doesn't exist
- Retain Handling = 2: Never send retained messages

Per MQTT v5.0 spec 3.8.3.1: Retain Handling option specifies whether retained
messages are sent when the subscription is established.
"""

import paho.mqtt.client as mqtt
import time
import sys

# Configuration
BROKER_HOST = "localhost"
BROKER_PORT = 1883
TEST_TOPIC = "test/retain/handling"

# Test state
messages_received = {}
connections = {}

def on_connect(client, userdata, flags, rc, properties=None):
    """Handle connection callback"""
    client_name = userdata
    print(f"[{client_name}] Connected rc={rc}")
    connections[client_name] = True

def on_message(client, userdata, msg):
    """Handle message callback"""
    client_name = userdata
    payload = msg.payload.decode()
    print(f"[{client_name}] Received: {payload}")
    if client_name not in messages_received:
        messages_received[client_name] = []
    messages_received[client_name].append(payload)

def on_disconnect(client, userdata, flags, rc, properties=None):
    """Handle disconnect for MQTT v5"""
    client_name = userdata
    print(f"[{client_name}] Disconnected rc={rc}")

def cleanup():
    """Clean up retained messages"""
    print("\nCleaning up retained messages...")
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                        client_id="cleanup_client",
                        protocol=mqtt.MQTTv5,
                        userdata="Cleanup")
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()
    time.sleep(0.5)
    
    # Delete retained message by publishing empty payload
    client.publish(TEST_TOPIC, "", qos=1, retain=True)
    time.sleep(0.5)
    
    client.loop_stop()
    client.disconnect()
    time.sleep(0.5)

def test_retain_handling_0():
    """Test Retain Handling = 0: Always send retained messages"""
    print("\n" + "="*70)
    print("TEST 1: Retain Handling = 0 (Always send retained messages)")
    print("="*70)
    
    # Reset state
    messages_received.clear()
    connections.clear()
    
    # Publish retained message
    publisher = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                           client_id="publisher1",
                           protocol=mqtt.MQTTv5,
                           userdata="Publisher1")
    publisher.on_connect = on_connect
    publisher.connect(BROKER_HOST, BROKER_PORT, 60)
    publisher.loop_start()
    time.sleep(0.5)
    
    print("Publishing retained message...")
    publisher.publish(TEST_TOPIC, "Retained message for test 1", qos=1, retain=True)
    time.sleep(0.5)
    
    publisher.loop_stop()
    publisher.disconnect()
    time.sleep(0.5)
    
    # Subscribe with retainHandling = 0 (default)
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
    
    # Subscribe with retainHandling = 0 (send retained)
    print("Subscribing with retainHandling=0 (send retained messages)...")
    options = mqtt.SubscribeOptions(qos=1, retainHandling=0)
    subscriber.subscribe(TEST_TOPIC, options=options)
    time.sleep(2)
    
    subscriber.loop_stop()
    subscriber.disconnect()
    
    # Verify: Should receive 1 retained message
    received = messages_received.get("Subscriber1", [])
    success = len(received) == 1 and received[0] == "Retained message for test 1"
    
    print(f"\nMessages received: {len(received)}")
    if success:
        print("✓ TEST 1 PASSED: Retained message delivered (retainHandling=0)")
    else:
        print(f"✗ TEST 1 FAILED: Expected 1 retained message, got {len(received)}")
    
    cleanup()
    return success

def test_retain_handling_2():
    """Test Retain Handling = 2: Never send retained messages"""
    print("\n" + "="*70)
    print("TEST 2: Retain Handling = 2 (Never send retained messages)")
    print("="*70)
    
    # Reset state
    messages_received.clear()
    connections.clear()
    
    # Publish retained message
    publisher = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                           client_id="publisher2",
                           protocol=mqtt.MQTTv5,
                           userdata="Publisher2")
    publisher.on_connect = on_connect
    publisher.connect(BROKER_HOST, BROKER_PORT, 60)
    publisher.loop_start()
    time.sleep(0.5)
    
    print("Publishing retained message...")
    publisher.publish(TEST_TOPIC, "Retained message for test 2", qos=1, retain=True)
    time.sleep(0.5)
    
    publisher.loop_stop()
    publisher.disconnect()
    time.sleep(0.5)
    
    # Subscribe with retainHandling = 2 (never send)
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
    
    # Subscribe with retainHandling = 2 (never send retained)
    print("Subscribing with retainHandling=2 (never send retained messages)...")
    options = mqtt.SubscribeOptions(qos=1, retainHandling=2)
    subscriber.subscribe(TEST_TOPIC, options=options)
    time.sleep(2)
    
    subscriber.loop_stop()
    subscriber.disconnect()
    
    # Verify: Should receive 0 retained messages
    received = messages_received.get("Subscriber2", [])
    success = len(received) == 0
    
    print(f"\nMessages received: {len(received)}")
    if success:
        print("✓ TEST 2 PASSED: No retained message delivered (retainHandling=2)")
    else:
        print(f"✗ TEST 2 FAILED: Expected 0 retained messages, got {len(received)}")
    
    cleanup()
    return success

def test_retain_handling_1():
    """Test Retain Handling = 1: Send only if subscription is new"""
    print("\n" + "="*70)
    print("TEST 3: Retain Handling = 1 (Send only if new subscription)")
    print("="*70)
    
    # Reset state
    messages_received.clear()
    connections.clear()
    
    # Publish retained message
    publisher = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                           client_id="publisher3",
                           protocol=mqtt.MQTTv5,
                           userdata="Publisher3")
    publisher.on_connect = on_connect
    publisher.connect(BROKER_HOST, BROKER_PORT, 60)
    publisher.loop_start()
    time.sleep(0.5)
    
    print("Publishing retained message...")
    publisher.publish(TEST_TOPIC, "Retained message for test 3", qos=1, retain=True)
    time.sleep(0.5)
    
    publisher.loop_stop()
    publisher.disconnect()
    time.sleep(0.5)
    
    # First subscription - should receive retained message (new subscription)
    subscriber = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                            client_id="subscriber3",
                            protocol=mqtt.MQTTv5,
                            userdata="Subscriber3-First")
    subscriber.on_connect = on_connect
    subscriber.on_message = on_message
    subscriber.on_disconnect = on_disconnect
    
    subscriber.connect(BROKER_HOST, BROKER_PORT, 60)
    subscriber.loop_start()
    time.sleep(0.5)
    
    print("First subscription with retainHandling=1 (new subscription)...")
    options = mqtt.SubscribeOptions(qos=1, retainHandling=1)
    subscriber.subscribe(TEST_TOPIC, options=options)
    time.sleep(2)
    
    first_received = messages_received.get("Subscriber3-First", [])
    print(f"First subscription received: {len(first_received)} messages")
    
    # Unsubscribe
    subscriber.unsubscribe(TEST_TOPIC)
    time.sleep(0.5)
    
    # Reset for second subscription
    messages_received["Subscriber3-Second"] = []
    subscriber._userdata = "Subscriber3-Second"
    
    # Second subscription - should also receive retained (new subscription after unsubscribe)
    print("Second subscription with retainHandling=1 (new subscription)...")
    subscriber.subscribe(TEST_TOPIC, options=options)
    time.sleep(2)
    
    second_received = messages_received.get("Subscriber3-Second", [])
    print(f"Second subscription received: {len(second_received)} messages")
    
    subscriber.loop_stop()
    subscriber.disconnect()
    
    # Verify: Both should receive retained message (both are "new" subscriptions)
    success = (len(first_received) == 1 and 
               len(second_received) == 1 and
               first_received[0] == "Retained message for test 3" and
               second_received[0] == "Retained message for test 3")
    
    if success:
        print("✓ TEST 3 PASSED: Retained message delivered on both new subscriptions (retainHandling=1)")
    else:
        print(f"✗ TEST 3 FAILED: First={len(first_received)}, Second={len(second_received)} (both should be 1)")
    
    cleanup()
    return success

def run_test():
    print("\n" + "="*70)
    print("MQTT v5.0 PHASE 8 - RETAIN HANDLING SUBSCRIPTION OPTION TEST")
    print("="*70)
    
    try:
        # Run all tests
        test1_pass = test_retain_handling_0()
        test2_pass = test_retain_handling_2()
        test3_pass = test_retain_handling_1()
        
        # Summary
        print("\n" + "="*70)
        print("TEST SUMMARY")
        print("="*70)
        print(f"Test 1 (retainHandling=0): {'✓ PASSED' if test1_pass else '✗ FAILED'}")
        print(f"Test 2 (retainHandling=2): {'✓ PASSED' if test2_pass else '✗ FAILED'}")
        print(f"Test 3 (retainHandling=1): {'✓ PASSED' if test3_pass else '✗ FAILED'}")
        
        all_pass = test1_pass and test2_pass and test3_pass
        
        print("\n" + "="*70)
        if all_pass:
            print("✓✓✓ PHASE 8 RETAIN HANDLING TEST PASSED ✓✓✓")
            print("Retain Handling subscription option working correctly!")
        else:
            print("✗✗✗ PHASE 8 RETAIN HANDLING TEST FAILED ✗✗✗")
        print("="*70 + "\n")
        
        return all_pass
        
    except Exception as e:
        print(f"\n✗ TEST ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_test()
    sys.exit(0 if success else 1)
