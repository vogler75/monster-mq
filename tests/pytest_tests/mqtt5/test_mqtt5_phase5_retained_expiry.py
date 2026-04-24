#!/usr/bin/env python3
"""
Test MQTT v5.0 Phase 5: Message Expiry Interval for Retained Messages

This test validates that retained messages with Message Expiry Interval:
1. Are delivered to new subscribers before expiry
2. Include the Message Expiry property with remaining time
3. Are NOT delivered to new subscribers after expiry
4. Are automatically removed from the broker after expiry
"""

import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
import os
import time
import argparse
import sys

# Test results
test_results = {
    "test1_received_before_expiry": False,
    "test2_expiry_property_included": False,
    "test3_not_received_after_expiry": False,
}

received_messages = {}


def _wait_for_connack(client, timeout=5.0):
    """Poll until the client's _connected flag is True."""
    start = time.time()
    while not client._connected and (time.time() - start) < timeout:
        time.sleep(0.1)
    return client._connected


def _wait_for_suback(client, timeout=5.0):
    """Poll until the client's _subscribed flag is True."""
    start = time.time()
    while not client._subscribed and (time.time() - start) < timeout:
        time.sleep(0.1)
    return client._subscribed


def _make_client(client_id, userdata, host, port, username=None, password=None):
    """Create a client with _connected/_subscribed flags and appropriate callbacks."""
    c = mqtt.Client(
        client_id=client_id,
        protocol=mqtt.MQTTv5,
        userdata=userdata,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    c._connected = False
    c._subscribed = False

    if username:
        c.username_pw_set(username, password)

    def on_connect(client, ud, flags, rc, properties=None):
        if rc == 0:
            client._connected = True
            print(f"✓ Connected to broker (client: {ud['name']})")
        else:
            print(f"✗ Connection failed with code {rc}")

    def on_subscribe(client, ud, mid, reason_code_list, properties=None):
        client._subscribed = True

    c.on_connect = on_connect
    c.on_subscribe = on_subscribe
    return c


def on_message(client, userdata, msg):
    """Callback for when a message is received"""
    test_name = userdata.get('test_name', 'unknown')
    
    # Extract Message Expiry Interval property (property ID 2)
    has_expiry = False
    expiry_value = None
    if msg.properties and hasattr(msg.properties, 'MessageExpiryInterval'):
        has_expiry = True
        expiry_value = msg.properties.MessageExpiryInterval
    
    received_messages[test_name] = {
        'topic': msg.topic,
        'payload': msg.payload.decode(),
        'qos': msg.qos,
        'retain': msg.retain,
        'has_expiry': has_expiry,
        'expiry_value': expiry_value
    }
    
    print(f"  Received: topic={msg.topic}, payload={msg.payload.decode()}, "
          f"qos={msg.qos}, retain={msg.retain}, "
          f"expiry={'Present (' + str(expiry_value) + 's)' if has_expiry else 'NOT Present'}")

def test_retained_message_expiry(broker_config):
    broker_host = broker_config["host"]
    broker_port = broker_config["port"]
    username = broker_config.get("username")
    password = broker_config.get("password")
    """Test retained messages with Message Expiry Interval"""
    
    print("\n" + "="*70)
    print("MQTT v5.0 Phase 5: Retained Message Expiry Test")
    print("="*70)
    
    # Test 1: Publish retained message with 5 second expiry, subscribe immediately
    print("\n[Test 1] Retained message delivered before expiry")
    print("-" * 70)
    
    publisher = _make_client("retained_expiry_publisher", {'name': 'publisher'},
                             broker_host, broker_port, username, password)

    print("Connecting publisher...")
    publisher.connect(broker_host, broker_port, 60)
    publisher.loop_start()
    assert _wait_for_connack(publisher), "Publisher failed to connect"
    
    # Publish retained message with 5s expiry
    topic = "test/retained/expiry"
    payload = "Retained message with 5s expiry"
    
    properties = Properties(PacketTypes.PUBLISH)
    properties.MessageExpiryInterval = 5
    
    print(f"Publishing retained message: topic={topic}, expiry=5s")
    result = publisher.publish(topic, payload, qos=1, retain=True, properties=properties)
    result.wait_for_publish()
    time.sleep(0.3)
    
    # Subscribe immediately (before expiry)
    subscriber1 = _make_client("retained_sub1",
                               {'name': 'subscriber1', 'test_name': 'test1'},
                               broker_host, broker_port, username, password)
    subscriber1.on_message = on_message
    
    print("Subscribing before expiry...")
    subscriber1.connect(broker_host, broker_port, 60)
    subscriber1.loop_start()
    assert _wait_for_connack(subscriber1), "Subscriber1 failed to connect"

    subscriber1.subscribe(topic, qos=1)
    assert _wait_for_suback(subscriber1), "Subscriber1 failed to subscribe"
    time.sleep(0.5)  # Allow broker to fully register subscription
    time.sleep(1)  # Wait for retained message delivery
    
    # Check Test 1
    if 'test1' in received_messages:
        msg = received_messages['test1']
        test_results["test1_received_before_expiry"] = True
        print(f"✓ Test 1 PASSED: Retained message received before expiry")
        
        # Check Test 2: Message Expiry property included
        if msg['has_expiry']:
            test_results["test2_expiry_property_included"] = True
            print(f"✓ Test 2 PASSED: Message Expiry property included ({msg['expiry_value']}s remaining)")
        else:
            print(f"✗ Test 2 FAILED: Message Expiry property NOT included")
    else:
        print(f"✗ Test 1 FAILED: Retained message NOT received before expiry")
    
    subscriber1.loop_stop()
    subscriber1.disconnect()
    
    # Test 3: Wait for message to expire, then subscribe again
    print("\n[Test 3] Retained message NOT delivered after expiry")
    print("-" * 70)
    print("Waiting 6 seconds for message to expire...")
    time.sleep(6)
    
    # Subscribe after expiry
    subscriber2 = _make_client("retained_sub2",
                               {'name': 'subscriber2', 'test_name': 'test3'},
                               broker_host, broker_port, username, password)
    subscriber2.on_message = on_message
    
    print("Subscribing after expiry...")
    subscriber2.connect(broker_host, broker_port, 60)
    subscriber2.loop_start()
    assert _wait_for_connack(subscriber2), "Subscriber2 failed to connect"

    subscriber2.subscribe(topic, qos=1)
    assert _wait_for_suback(subscriber2), "Subscriber2 failed to subscribe"
    time.sleep(0.5)  # Allow broker to fully register subscription
    time.sleep(2)  # Wait for potentially retained message
    
    # Check Test 3
    if 'test3' not in received_messages:
        test_results["test3_not_received_after_expiry"] = True
        print(f"✓ Test 3 PASSED: Expired retained message NOT delivered")
    else:
        print(f"✗ Test 3 FAILED: Expired retained message was delivered")
    
    subscriber2.loop_stop()
    subscriber2.disconnect()
    
    # Cleanup
    # Remove the retained message (if still exists)
    publisher.publish(topic, None, qos=1, retain=True)
    time.sleep(0.5)
    
    publisher.loop_stop()
    publisher.disconnect()
    
    # Print summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    passed = sum(1 for v in test_results.values() if v)
    total = len(test_results)
    
    for test_name, result in test_results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    print("="*70)
    
    assert passed == total, f"Only {passed}/{total} sub-tests passed"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test MQTT v5.0 Phase 5 Retained Message Expiry')
    parser.add_argument('--host', default=os.getenv("MQTT_BROKER", 'localhost'),
                        help='MQTT broker host (default: localhost)')
    parser.add_argument('--port', type=int, default=int(os.getenv("MQTT_PORT", "1883")),
                        help='MQTT broker port (default: 1883)')
    parser.add_argument('--username', default=os.getenv("MQTT_USERNAME", "Test"),
                        help='MQTT username')
    parser.add_argument('--password', default=os.getenv("MQTT_PASSWORD", "Test"),
                        help='MQTT password')
    
    args = parser.parse_args()
    
    success = test_retained_message_expiry(args.host, args.port, args.username, args.password)
    sys.exit(0 if success else 1)
