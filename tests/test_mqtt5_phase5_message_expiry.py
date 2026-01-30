#!/usr/bin/env python3
"""
Test MQTT v5.0 Phase 5: Message Expiry Interval
Tests message expiry functionality for queued messages
"""

import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
import time
import sys

# Test configuration
BROKER_HOST = "localhost"
BROKER_PORT = 1883
TEST_TIMEOUT = 20

# Test state
test_results = []
received_messages = []
connack_props = None

def on_connect(client, userdata, flags, reason_code, properties):
    """MQTT v5 connection callback"""
    global connack_props
    connack_props = properties
    print(f"✓ Connected (reason_code={reason_code})")

def on_message(client, userdata, message):
    """Message received callback"""
    msg_props = message.properties if hasattr(message, 'properties') else None
    expiry = None
    if msg_props and hasattr(msg_props, 'MessageExpiryInterval'):
        expiry = msg_props.MessageExpiryInterval
    
    received_messages.append({
        'topic': message.topic,
        'payload': message.payload.decode('utf-8'),
        'expiry': expiry,
        'time': time.time()
    })
    print(f"✓ Received: topic={message.topic}, payload={message.payload.decode('utf-8')}, expiry={expiry}")

def test_message_expiry_basic():
    """Test 1: Basic message expiry (message should expire)"""
    print("\n=== Test 1: Basic Message Expiry ===")
    
    # Create offline subscriber (persistent session)
    subscriber = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="test_phase5_sub1", protocol=mqtt.MQTTv5)
    subscriber.on_connect = on_connect
    subscriber.on_message = on_message
    
    # Subscribe and disconnect
    conn_props = Properties(PacketTypes.CONNECT)
    conn_props.SessionExpiryInterval = 300  # 5 minutes
    
    subscriber.connect(BROKER_HOST, BROKER_PORT, properties=conn_props)
    subscriber.loop_start()
    time.sleep(0.5)
    
    props = Properties(PacketTypes.SUBSCRIBE)
    subscriber.subscribe("test/phase5/expiry", qos=1, properties=props)
    time.sleep(0.5)
    
    subscriber.disconnect()
    subscriber.loop_stop()
    print("✓ Subscriber disconnected (offline)")
    
    # Publish message with 2-second expiry
    publisher = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="test_phase5_pub1", protocol=mqtt.MQTTv5)
    publisher.connect(BROKER_HOST, BROKER_PORT)
    publisher.loop_start()
    time.sleep(0.3)
    
    pub_props = Properties(PacketTypes.PUBLISH)
    pub_props.MessageExpiryInterval = 2  # Expires in 2 seconds
    
    print("✓ Publishing message with 2s expiry interval")
    publisher.publish("test/phase5/expiry", "expired_msg", qos=1, properties=pub_props)
    time.sleep(0.3)
    
    publisher.disconnect()
    publisher.loop_stop()
    
    # Wait for message to expire (3 seconds > 2 second expiry)
    print("⏳ Waiting 3 seconds for message to expire...")
    time.sleep(3)
    
    # Reconnect subscriber - should NOT receive expired message
    received_messages.clear()
    subscriber2 = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="test_phase5_sub1", protocol=mqtt.MQTTv5)
    subscriber2.on_connect = on_connect
    subscriber2.on_message = on_message
    
    conn_props2 = Properties(PacketTypes.CONNECT)
    conn_props2.SessionExpiryInterval = 300
    
    subscriber2.connect(BROKER_HOST, BROKER_PORT, properties=conn_props2)
    subscriber2.loop_start()
    
    time.sleep(2)  # Wait for potential message delivery
    
    subscriber2.disconnect()
    subscriber2.loop_stop()
    
    # Verify: Should have received NO messages (expired)
    success = len(received_messages) == 0
    test_results.append(('Message expired (not delivered)', success))
    
    if success:
        print("✓ Test 1 PASSED: Expired message was NOT delivered")
    else:
        print(f"✗ Test 1 FAILED: Expected 0 messages, got {len(received_messages)}")
    
    return success

def test_message_expiry_delivered():
    """Test 2: Message delivered before expiry"""
    print("\n=== Test 2: Message Delivered Before Expiry ===")
    
    # Create offline subscriber
    subscriber = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="test_phase5_sub2", protocol=mqtt.MQTTv5)
    subscriber.on_connect = on_connect
    subscriber.on_message = on_message
    
    conn_props = Properties(PacketTypes.CONNECT)
    conn_props.SessionExpiryInterval = 300
    
    subscriber.connect(BROKER_HOST, BROKER_PORT, properties=conn_props)
    subscriber.loop_start()
    time.sleep(0.5)
    
    props = Properties(PacketTypes.SUBSCRIBE)
    subscriber.subscribe("test/phase5/valid", qos=1, properties=props)
    time.sleep(0.5)
    
    subscriber.disconnect()
    subscriber.loop_stop()
    print("✓ Subscriber disconnected (offline)")
    
    # Publish message with 10-second expiry
    publisher = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="test_phase5_pub2", protocol=mqtt.MQTTv5)
    publisher.connect(BROKER_HOST, BROKER_PORT)
    publisher.loop_start()
    time.sleep(0.3)
    
    pub_props = Properties(PacketTypes.PUBLISH)
    pub_props.MessageExpiryInterval = 10  # Expires in 10 seconds
    
    print("✓ Publishing message with 10s expiry interval")
    publisher.publish("test/phase5/valid", "valid_msg", qos=1, properties=pub_props)
    time.sleep(0.3)
    
    publisher.disconnect()
    publisher.loop_stop()
    
    # Wait 2 seconds (less than 10 second expiry)
    print("⏳ Waiting 2 seconds (before expiry)...")
    time.sleep(2)
    
    # Reconnect subscriber - SHOULD receive message
    received_messages.clear()
    subscriber2 = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="test_phase5_sub2", protocol=mqtt.MQTTv5)
    subscriber2.on_connect = on_connect
    subscriber2.on_message = on_message
    
    conn_props2 = Properties(PacketTypes.CONNECT)
    conn_props2.SessionExpiryInterval = 300
    
    subscriber2.connect(BROKER_HOST, BROKER_PORT, properties=conn_props2)
    subscriber2.loop_start()
    
    time.sleep(2)  # Wait for message delivery
    
    subscriber2.disconnect()
    subscriber2.loop_stop()
    
    # Verify: Should have received 1 message
    success = len(received_messages) == 1 and received_messages[0]['payload'] == 'valid_msg'
    test_results.append(('Message delivered before expiry', success))
    
    if success:
        print("✓ Test 2 PASSED: Message delivered before expiry")
    else:
        print(f"✗ Test 2 FAILED: Expected 1 message, got {len(received_messages)}")
    
    return success

def test_expiry_interval_update():
    """Test 3: Expiry interval updated on forward (decremented by elapsed time)"""
    print("\n=== Test 3: Expiry Interval Update on Forward ===")
    
    # Create offline subscriber
    subscriber = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="test_phase5_sub3", protocol=mqtt.MQTTv5)
    subscriber.on_connect = on_connect
    subscriber.on_message = on_message
    
    conn_props = Properties(PacketTypes.CONNECT)
    conn_props.SessionExpiryInterval = 300
    
    subscriber.connect(BROKER_HOST, BROKER_PORT, properties=conn_props)
    subscriber.loop_start()
    time.sleep(0.5)
    
    props = Properties(PacketTypes.SUBSCRIBE)
    subscriber.subscribe("test/phase5/update", qos=1, properties=props)
    time.sleep(0.5)
    
    subscriber.disconnect()
    subscriber.loop_stop()
    print("✓ Subscriber disconnected (offline)")
    
    # Publish message with 10-second expiry
    publisher = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="test_phase5_pub3", protocol=mqtt.MQTTv5)
    publisher.connect(BROKER_HOST, BROKER_PORT)
    publisher.loop_start()
    time.sleep(0.3)
    
    pub_props = Properties(PacketTypes.PUBLISH)
    pub_props.MessageExpiryInterval = 10  # Original: 10 seconds
    
    print("✓ Publishing message with 10s expiry interval")
    publisher.publish("test/phase5/update", "update_msg", qos=1, properties=pub_props)
    time.sleep(0.3)
    
    publisher.disconnect()
    publisher.loop_stop()
    
    # Wait 3 seconds before reconnecting
    print("⏳ Waiting 3 seconds before reconnect...")
    time.sleep(3)
    
    # Reconnect subscriber - should receive message with UPDATED expiry (~7 seconds)
    received_messages.clear()
    subscriber2 = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="test_phase5_sub3", protocol=mqtt.MQTTv5)
    subscriber2.on_connect = on_connect
    subscriber2.on_message = on_message
    
    conn_props2 = Properties(PacketTypes.CONNECT)
    conn_props2.SessionExpiryInterval = 300
    
    subscriber2.connect(BROKER_HOST, BROKER_PORT, properties=conn_props2)
    subscriber2.loop_start()
    
    time.sleep(2)  # Wait for message delivery
    
    subscriber2.disconnect()
    subscriber2.loop_stop()
    
    # Verify: Should receive message with updated expiry (approximately 7 seconds, allow ±2s tolerance)
    success = False
    if len(received_messages) == 1:
        msg = received_messages[0]
        if msg['expiry'] is not None:
            # Original 10s - 3s elapsed = ~7s (allow 5-9s range for timing variations)
            if 5 <= msg['expiry'] <= 9:
                success = True
                print(f"✓ Expiry interval updated correctly: {msg['expiry']}s (expected ~7s)")
            else:
                print(f"✗ Expiry interval incorrect: {msg['expiry']}s (expected ~7s)")
        else:
            print("✗ No expiry interval in received message")
    else:
        print(f"✗ Expected 1 message, got {len(received_messages)}")
    
    test_results.append(('Expiry interval updated on forward', success))
    
    if success:
        print("✓ Test 3 PASSED: Expiry interval updated correctly")
    else:
        print("✗ Test 3 FAILED: Expiry interval not updated correctly")
    
    return success

def test_no_expiry():
    """Test 4: Message without expiry interval (should be delivered)"""
    print("\n=== Test 4: Message Without Expiry ===")
    
    # Create offline subscriber
    subscriber = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="test_phase5_sub4", protocol=mqtt.MQTTv5)
    subscriber.on_connect = on_connect
    subscriber.on_message = on_message
    
    conn_props = Properties(PacketTypes.CONNECT)
    conn_props.SessionExpiryInterval = 300
    
    subscriber.connect(BROKER_HOST, BROKER_PORT, properties=conn_props)
    subscriber.loop_start()
    time.sleep(0.5)
    
    props = Properties(PacketTypes.SUBSCRIBE)
    subscriber.subscribe("test/phase5/noexpiry", qos=1, properties=props)
    time.sleep(0.5)
    
    subscriber.disconnect()
    subscriber.loop_stop()
    print("✓ Subscriber disconnected (offline)")
    
    # Publish message WITHOUT expiry interval
    publisher = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="test_phase5_pub4", protocol=mqtt.MQTTv5)
    publisher.connect(BROKER_HOST, BROKER_PORT)
    publisher.loop_start()
    time.sleep(0.3)
    
    print("✓ Publishing message WITHOUT expiry interval")
    publisher.publish("test/phase5/noexpiry", "no_expiry_msg", qos=1)
    time.sleep(0.3)
    
    publisher.disconnect()
    publisher.loop_stop()
    
    # Wait several seconds
    print("⏳ Waiting 3 seconds...")
    time.sleep(3)
    
    # Reconnect subscriber - should receive message (no expiry)
    received_messages.clear()
    subscriber2 = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="test_phase5_sub4", protocol=mqtt.MQTTv5)
    subscriber2.on_connect = on_connect
    subscriber2.on_message = on_message
    
    conn_props2 = Properties(PacketTypes.CONNECT)
    conn_props2.SessionExpiryInterval = 300
    
    subscriber2.connect(BROKER_HOST, BROKER_PORT, properties=conn_props2)
    subscriber2.loop_start()
    
    time.sleep(2)  # Wait for message delivery
    
    subscriber2.disconnect()
    subscriber2.loop_stop()
    
    # Verify: Should receive message (no expiry set)
    success = len(received_messages) == 1 and received_messages[0]['payload'] == 'no_expiry_msg'
    test_results.append(('Message without expiry delivered', success))
    
    if success:
        print("✓ Test 4 PASSED: Message without expiry delivered")
    else:
        print(f"✗ Test 4 FAILED: Expected 1 message, got {len(received_messages)}")
    
    return success

def cleanup_sessions():
    """Clean up test sessions"""
    print("\n=== Cleanup ===")
    for i in range(1, 5):
        client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id=f"test_phase5_sub{i}", protocol=mqtt.MQTTv5)
        try:
            conn_props = Properties(PacketTypes.CONNECT)
            conn_props.SessionExpiryInterval = 0  # Clean session
            client.connect(BROKER_HOST, BROKER_PORT, properties=conn_props)
            client.loop_start()
            time.sleep(0.3)
            client.disconnect()
            client.loop_stop()
        except:
            pass

def main():
    print("=" * 60)
    print("MQTT v5.0 Phase 5: Message Expiry Interval Tests")
    print("=" * 60)
    
    try:
        # Clean up any existing test sessions before starting
        print("\n=== Pre-Test Cleanup ===")
        cleanup_sessions()
        time.sleep(2)  # Wait longer for cleanup to complete and sessions to be purged
        
        # Run tests
        test1_passed = test_message_expiry_basic()
        test2_passed = test_message_expiry_delivered()
        test3_passed = test_expiry_interval_update()
        test4_passed = test_no_expiry()
        
        # Cleanup
        cleanup_sessions()
        
        # Print summary
        print("\n" + "=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)
        
        for test_name, passed in test_results:
            status = "✓" if passed else "✗"
            print(f"{status} {test_name}")
        
        total = len(test_results)
        passed = sum(1 for _, p in test_results if p)
        
        print(f"\n{passed}/{total} tests passed")
        
        if passed == total:
            print("\n[OVERALL] ✓✓✓ PHASE 5 TEST PASSED ✓✓✓")
            return 0
        else:
            print(f"\n[OVERALL] ✗✗✗ PHASE 5 TEST FAILED ✗✗✗ ({total - passed} failures)")
            return 1
            
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        return 1
    except Exception as e:
        print(f"\n\nTest failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
