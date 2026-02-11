#!/usr/bin/env python3
"""
MQTT v5.0 Will Delay Interval Test

Tests Will Delay Interval (Property 24) in CONNECT packet.
The broker should delay publishing the Last Will message by the specified interval.

Test Scenario:
1. Subscriber connects and subscribes to 'test/will/delayed'
2. Publisher connects with:
   - Will Topic: 'test/will/delayed'
   - Will Message: 'delayed will message'
   - Will QoS: 1
   - Will Delay Interval: 5 seconds (Property 24)
3. Publisher disconnects abnormally (no DISCONNECT packet)
4. Subscriber should NOT receive will message immediately
5. After 5 seconds, subscriber should receive the will message
6. Test reconnection cancellation:
   - Publisher reconnects before delay expires
   - Subscriber should NOT receive will message (timer was canceled)

Expected Result:
✓ Will message delayed by 5 seconds
✓ Will message published after delay
✓ Will timer canceled when client reconnects
"""

import paho.mqtt.client as mqtt
import time
import sys
import struct

# Test configuration
BROKER_HOST = "localhost"
BROKER_PORT = 1883
SUBSCRIBER_CLIENT_ID = "will_delay_subscriber"
PUBLISHER_CLIENT_ID = "will_delay_publisher"
WILL_TOPIC = "test/will/delayed"
WILL_MESSAGE = "delayed will message"
WILL_DELAY_SECONDS = 5

# Test state
messages_received = []
will_received_time = None
disconnect_time = None

def on_connect_subscriber(client, userdata, flags, reason_code, properties):
    """Callback when subscriber connects"""
    print(f"[SUBSCRIBER] Connected with reason code: {reason_code}")
    if reason_code == 0:
        # Subscribe to will topic
        client.subscribe(WILL_TOPIC, qos=1)
        print(f"[SUBSCRIBER] Subscribed to: {WILL_TOPIC}")

def on_message_subscriber(client, userdata, msg):
    """Callback when subscriber receives message"""
    global will_received_time, disconnect_time
    
    payload = msg.payload.decode('utf-8')
    print(f"\n[SUBSCRIBER] Received: topic={msg.topic}, payload={payload}, QoS={msg.qos}")
    
    messages_received.append({
        'topic': msg.topic,
        'payload': payload,
        'qos': msg.qos,
        'timestamp': time.time()
    })
    
    # Record time of will message
    if payload == WILL_MESSAGE and disconnect_time is not None:
        will_received_time = time.time()
        delay_actual = will_received_time - disconnect_time
        print(f"[SUBSCRIBER] Will message received after {delay_actual:.2f} seconds")

def on_connect_publisher(client, userdata, flags, reason_code, properties):
    """Callback when publisher connects"""
    print(f"[PUBLISHER] Connected with reason code: {reason_code}")

def test_will_delay():
    """Test Will Delay Interval feature"""
    global disconnect_time, will_received_time
    
    print("=" * 80)
    print("MQTT v5.0 Will Delay Interval Test")
    print("=" * 80)
    
    # Create subscriber client
    subscriber = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=SUBSCRIBER_CLIENT_ID,
        protocol=mqtt.MQTTv5
    )
    subscriber.on_connect = on_connect_subscriber
    subscriber.on_message = on_message_subscriber
    
    print(f"\n[SUBSCRIBER] Connecting to broker at {BROKER_HOST}:{BROKER_PORT}...")
    subscriber.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    subscriber.loop_start()
    
    # Wait for subscription
    time.sleep(1)
    
    # TEST 1: Will Delay - Abnormal Disconnect
    print(f"\n{'='*80}")
    print("TEST 1: Will Delay on Abnormal Disconnect")
    print(f"{'='*80}")
    
    # Create publisher with Last Will and Will Delay Interval
    publisher = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=PUBLISHER_CLIENT_ID,
        protocol=mqtt.MQTTv5
    )
    publisher.on_connect = on_connect_publisher
    
    # Set Last Will with Will Delay Interval
    will_properties = mqtt.Properties(mqtt.PacketTypes.WILLMESSAGE)
    will_properties.WillDelayInterval = WILL_DELAY_SECONDS  # 5 second delay
    
    publisher.will_set(
        WILL_TOPIC,
        WILL_MESSAGE,
        qos=1,
        retain=False,
        properties=will_properties
    )
    
    print(f"[PUBLISHER] Connecting with Will Delay Interval: {WILL_DELAY_SECONDS}s...")
    print(f"[PUBLISHER] Will Topic: {WILL_TOPIC}")
    print(f"[PUBLISHER] Will Message: {WILL_MESSAGE}")
    
    publisher.connect(
        BROKER_HOST,
        BROKER_PORT,
        keepalive=60
    )
    publisher.loop_start()
    
    # Wait for connection
    time.sleep(1)
    
    # Abnormal disconnect (kill connection without DISCONNECT packet)
    print(f"\n[PUBLISHER] Simulating abnormal disconnect (no DISCONNECT packet)...")
    disconnect_time = time.time()
    publisher._sock.close()  # Force close socket without sending DISCONNECT
    publisher.loop_stop()
    
    # Check immediately - should NOT have will message yet
    time.sleep(1)
    print(f"\n[CHECK] 1 second after disconnect - checking for will message...")
    if len(messages_received) > 0:
        print("❌ ERROR: Will message received too early (should be delayed 5 seconds)")
        return False
    else:
        print("✓ Will message not received yet (correctly delayed)")
    
    # Wait for will delay to expire
    print(f"\n[WAITING] Waiting {WILL_DELAY_SECONDS} seconds for will delay to expire...")
    time.sleep(WILL_DELAY_SECONDS + 1)  # +1 for safety margin
    
    # Check if will message received
    if len(messages_received) == 0:
        print("❌ ERROR: Will message not received after delay")
        return False
    
    if messages_received[0]['payload'] != WILL_MESSAGE:
        print(f"❌ ERROR: Wrong message received: {messages_received[0]['payload']}")
        return False
    
    # Validate timing
    if will_received_time is not None and disconnect_time is not None:
        delay_actual = will_received_time - disconnect_time
        delay_expected = WILL_DELAY_SECONDS
        delay_tolerance = 2.0  # Allow 2 second tolerance
        
        print(f"\n[TIMING] Expected delay: {delay_expected}s")
        print(f"[TIMING] Actual delay: {delay_actual:.2f}s")
        print(f"[TIMING] Tolerance: ±{delay_tolerance}s")
        
        if abs(delay_actual - delay_expected) > delay_tolerance:
            print(f"❌ ERROR: Will delay timing incorrect (expected ~{delay_expected}s, got {delay_actual:.2f}s)")
            return False
        else:
            print(f"✓ Will delay timing correct ({delay_actual:.2f}s)")
    
    print(f"\n✓ Will message received correctly after {WILL_DELAY_SECONDS}s delay")
    
    # TEST 2: Will Delay Cancellation on Reconnect
    print(f"\n{'='*80}")
    print("TEST 2: Will Delay Cancellation on Reconnect")
    print(f"{'='*80}")
    
    # Reset state
    messages_received.clear()
    disconnect_time = None
    will_received_time = None
    
    # Create new publisher with Will Delay
    publisher2 = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=PUBLISHER_CLIENT_ID + "_cancel",
        protocol=mqtt.MQTTv5
    )
    publisher2.on_connect = on_connect_publisher
    
    will_properties2 = mqtt.Properties(mqtt.PacketTypes.WILLMESSAGE)
    will_properties2.WillDelayInterval = WILL_DELAY_SECONDS
    
    publisher2.will_set(
        WILL_TOPIC,
        "will should be canceled",
        qos=1,
        retain=False,
        properties=will_properties2
    )
    
    print(f"[PUBLISHER] Connecting with Will Delay Interval: {WILL_DELAY_SECONDS}s...")
    publisher2.connect(
        BROKER_HOST,
        BROKER_PORT,
        keepalive=60
    )
    publisher2.loop_start()
    time.sleep(1)
    
    # Abnormal disconnect
    print(f"[PUBLISHER] Disconnecting abnormally...")
    publisher2._sock.close()
    time.sleep(1)
    
    # Reconnect BEFORE will delay expires
    print(f"[PUBLISHER] Reconnecting before will delay expires...")
    publisher3 = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=PUBLISHER_CLIENT_ID + "_cancel",
        protocol=mqtt.MQTTv5
    )
    publisher3.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
    publisher3.loop_start()
    time.sleep(1)
    
    # Wait for original delay period
    print(f"[WAITING] Waiting {WILL_DELAY_SECONDS + 1}s to ensure will was canceled...")
    time.sleep(WILL_DELAY_SECONDS + 1)
    
    # Check that will was NOT sent
    if len(messages_received) > 0:
        print(f"❌ ERROR: Will message sent despite reconnection (should be canceled)")
        return False
    else:
        print("✓ Will message correctly canceled by reconnection")
    
    # Cleanup
    publisher3.disconnect()
    publisher3.loop_stop()
    subscriber.disconnect()
    subscriber.loop_stop()
    
    return True

if __name__ == "__main__":
    try:
        success = test_will_delay()
        
        print("\n" + "=" * 80)
        if success:
            print("[OVERALL] ✓✓✓ WILL DELAY INTERVAL TEST PASSED ✓✓✓")
            print("=" * 80)
            sys.exit(0)
        else:
            print("[OVERALL] ✗✗✗ WILL DELAY INTERVAL TEST FAILED ✗✗✗")
            print("=" * 80)
            sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
