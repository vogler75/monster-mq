#!/usr/bin/env python3
"""
MQTT v5.0 Retain Handling Subscription Option Test

Tests the Retain Handling subscription option feature:
- Retain Handling = 0: Send retained messages at subscribe time (default)
- Retain Handling = 1: Send retained messages only if subscription doesn't exist
- Retain Handling = 2: Never send retained messages

Per MQTT v5.0 spec 3.8.3.1: Retain Handling option specifies whether retained
messages are sent when the subscription is established.
"""

import paho.mqtt.client as mqtt
import time
import pytest

pytestmark = pytest.mark.mqtt5

# Configuration
TEST_TOPIC = "test/retain/handling"

def test_retain_handling_0(broker_config):
    """Test Retain Handling = 0: Always send retained messages"""
    print("\n" + "="*70)
    print("TEST 1: Retain Handling = 0 (Always send retained messages)")
    print("="*70)
    
    # Test state
    messages_received = {}
    connections = {}
    
    def on_connect(client, userdata, flags, rc, properties=None):
        """Handle connection callback"""
        nonlocal connections
        client_name = userdata
        print(f"[{client_name}] Connected rc={rc}")
        connections[client_name] = True

    def on_message(client, userdata, msg):
        """Handle message callback"""
        nonlocal messages_received
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
        client.username_pw_set(broker_config["username"], broker_config["password"])
        client.connect(broker_config["host"], broker_config["port"], 60)
        client.loop_start()
        time.sleep(0.5)
        
        # Delete retained message by publishing empty payload
        client.publish(TEST_TOPIC, "", qos=1, retain=True)
        time.sleep(0.5)
        
        client.loop_stop()
        client.disconnect()
        time.sleep(0.5)
    
    # Publish retained message
    publisher = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                           client_id="publisher1",
                           protocol=mqtt.MQTTv5,
                           userdata="Publisher1")
    publisher.on_connect = on_connect
    publisher.username_pw_set(broker_config["username"], broker_config["password"])
    publisher.connect(broker_config["host"], broker_config["port"], 60)
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
    
    subscriber.username_pw_set(broker_config["username"], broker_config["password"])
    subscriber.connect(broker_config["host"], broker_config["port"], 60)
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
    
    print(f"\nMessages received: {len(received)}")
    assert len(received) == 1, f"Expected 1 retained message, got {len(received)}"
    assert received[0] == "Retained message for test 1", f"Wrong message content: {received[0]}"
    print("✓ TEST 1 PASSED: Retained message delivered (retainHandling=0)")
    
    cleanup()

def test_retain_handling_2(broker_config):
    """Test Retain Handling = 2: Never send retained messages"""
    print("\n" + "="*70)
    print("TEST 2: Retain Handling = 2 (Never send retained messages)")
    print("="*70)
    
    # Test state
    messages_received = {}
    connections = {}
    
    def on_connect(client, userdata, flags, rc, properties=None):
        """Handle connection callback"""
        nonlocal connections
        client_name = userdata
        print(f"[{client_name}] Connected rc={rc}")
        connections[client_name] = True

    def on_message(client, userdata, msg):
        """Handle message callback"""
        nonlocal messages_received
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
        client.username_pw_set(broker_config["username"], broker_config["password"])
        client.connect(broker_config["host"], broker_config["port"], 60)
        client.loop_start()
        time.sleep(0.5)
        
        # Delete retained message by publishing empty payload
        client.publish(TEST_TOPIC, "", qos=1, retain=True)
        time.sleep(0.5)
        
        client.loop_stop()
        client.disconnect()
        time.sleep(0.5)
    
    # Publish retained message
    publisher = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                           client_id="publisher2",
                           protocol=mqtt.MQTTv5,
                           userdata="Publisher2")
    publisher.on_connect = on_connect
    publisher.username_pw_set(broker_config["username"], broker_config["password"])
    publisher.connect(broker_config["host"], broker_config["port"], 60)
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
    
    subscriber.username_pw_set(broker_config["username"], broker_config["password"])
    subscriber.connect(broker_config["host"], broker_config["port"], 60)
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
    
    print(f"\nMessages received: {len(received)}")
    assert len(received) == 0, f"Expected 0 retained messages, got {len(received)}"
    print("✓ TEST 2 PASSED: No retained message delivered (retainHandling=2)")
    
    cleanup()

def test_retain_handling_1(broker_config):
    """Test Retain Handling = 1: Send only if subscription is new"""
    print("\n" + "="*70)
    print("TEST 3: Retain Handling = 1 (Send only if new subscription)")
    print("="*70)
    
    # Test state
    messages_received = {}
    connections = {}
    
    def on_connect(client, userdata, flags, rc, properties=None):
        """Handle connection callback"""
        nonlocal connections
        client_name = userdata
        print(f"[{client_name}] Connected rc={rc}")
        connections[client_name] = True

    def on_message(client, userdata, msg):
        """Handle message callback"""
        nonlocal messages_received
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
        client.username_pw_set(broker_config["username"], broker_config["password"])
        client.connect(broker_config["host"], broker_config["port"], 60)
        client.loop_start()
        time.sleep(0.5)
        
        # Delete retained message by publishing empty payload
        client.publish(TEST_TOPIC, "", qos=1, retain=True)
        time.sleep(0.5)
        
        client.loop_stop()
        client.disconnect()
        time.sleep(0.5)
    
    # Publish retained message
    publisher = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                           client_id="publisher3",
                           protocol=mqtt.MQTTv5,
                           userdata="Publisher3")
    publisher.on_connect = on_connect
    publisher.username_pw_set(broker_config["username"], broker_config["password"])
    publisher.connect(broker_config["host"], broker_config["port"], 60)
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
    
    subscriber.username_pw_set(broker_config["username"], broker_config["password"])
    subscriber.connect(broker_config["host"], broker_config["port"], 60)
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
    assert len(first_received) == 1, f"First subscription: Expected 1 message, got {len(first_received)}"
    assert len(second_received) == 1, f"Second subscription: Expected 1 message, got {len(second_received)}"
    assert first_received[0] == "Retained message for test 3", f"First subscription: Wrong message content"
    assert second_received[0] == "Retained message for test 3", f"Second subscription: Wrong message content"
    print("✓ TEST 3 PASSED: Retained message delivered on both new subscriptions (retainHandling=1)")
    
    cleanup()

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
