#!/usr/bin/env python3
"""
Test MQTT v5.0 Reason Codes (Phase 2).

This test verifies that:
1. SUBACK returns proper reason codes for subscriptions
2. UNSUBACK returns proper reason codes for unsubscriptions
3. PUBACK returns proper reason codes for QoS 1 publishes

Usage:
- Start broker locally (host=localhost, port=1883)
- Run: python3 tests/test_mqtt5_phase2_reason_codes.py

Requirements:
- paho-mqtt >= 2.0.0 (for MQTT v5 support)
  Install: pip install paho-mqtt>=2.0.0
"""

import sys
import time
import os
from typing import Optional

try:
    import paho.mqtt.client as mqtt
    from paho.mqtt.client import MQTTv5
    from paho.mqtt.properties import Properties
    from paho.mqtt.packettypes import PacketTypes
    from paho.mqtt.reasoncodes import ReasonCode
except ImportError:
    print("ERROR: paho-mqtt >= 2.0.0 not installed.")
    print("Install with: pip install 'paho-mqtt>=2.0.0'")
    sys.exit(1)

# Configuration
BROKER_HOST = os.getenv("MQTT_BROKER", "localhost")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
KEEPALIVE = 30

# Credentials
USERNAME: Optional[str] = os.getenv("MQTT_USERNAME", "Test")
PASSWORD: Optional[str] = os.getenv("MQTT_PASSWORD", "Test")

# Test state
state = {
    "connected": False,
    "suback_reason_codes": [],
    "unsuback_reason_codes": [],
    "puback_reason_code": None,
    "publish_mid": None,
}


def on_connect(client, userdata, flags, reason_code, properties=None):
    """Called when the broker responds to our connection request (CONNACK)."""
    print(f"\n[CONNACK] Reason code: {reason_code}")
    
    if reason_code == 0:
        print("[SUCCESS] ✓ MQTT v5.0 connection accepted!")
        state["connected"] = True
    else:
        print(f"[FAILED] ✗ Connection refused with reason code: {reason_code}")


def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
    """Called when SUBACK is received."""
    print(f"\n[SUBACK] Message ID: {mid}")
    print(f"[SUBACK] Reason codes: {reason_code_list}")
    if properties:
        print(f"[SUBACK] Properties: {properties}")
    
    state["suback_reason_codes"] = reason_code_list
    
    # Check reason codes
    for i, rc in enumerate(reason_code_list):
        if isinstance(rc, ReasonCode):
            value = rc.value
            name = rc.getName()
        else:
            value = rc
            name = f"Code {rc}"
        
        print(f"  Topic {i+1}: {name} (value={value})")


def on_unsubscribe(client, userdata, mid, reason_code_list, properties=None):
    """Called when UNSUBACK is received."""
    print(f"\n[UNSUBACK] Message ID: {mid}")
    print(f"[UNSUBACK] Reason codes: {reason_code_list}")
    if properties:
        print(f"[UNSUBACK] Properties: {properties}")
    
    state["unsuback_reason_codes"] = reason_code_list
    
    # Check reason codes
    for i, rc in enumerate(reason_code_list):
        if isinstance(rc, ReasonCode):
            value = rc.value
            name = rc.getName()
        else:
            value = rc
            name = f"Code {rc}"
        
        print(f"  Topic {i+1}: {name} (value={value})")


def on_publish(client, userdata, mid, reason_code, properties=None):
    """Called when PUBACK is received (QoS 1)."""
    print(f"\n[PUBACK] Message ID: {mid}")
    print(f"[PUBACK] Reason code: {reason_code}")
    if properties:
        print(f"[PUBACK] Properties: {properties}")
    
    if mid == state.get("publish_mid"):
        state["puback_reason_code"] = reason_code
        
        if isinstance(reason_code, ReasonCode):
            value = reason_code.value
            name = reason_code.getName()
        else:
            value = reason_code
            name = f"Code {reason_code}"
        
        print(f"  PUBACK: {name} (value={value})")


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    """Called when the client disconnects."""
    print(f"\n[DISCONNECT] Reason code: {reason_code}")


def test_mqtt5_reason_codes():
    """Test MQTT v5.0 reason codes in ACK packets."""
    print("=" * 70)
    print("MQTT v5.0 Reason Codes Test (Phase 2)")
    print("=" * 70)
    print(f"Broker: {BROKER_HOST}:{BROKER_PORT}")
    print(f"Protocol: MQTT v5.0")
    print()
    
    # Create MQTT v5 client
    client_id = f"test_mqtt5_phase2_{int(time.time())}"
    print(f"Client ID: {client_id}")
    
    try:
        # Create client with MQTT v5
        client = mqtt.Client(
            client_id=client_id,
            protocol=MQTTv5,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2
        )
        
        # Set callbacks
        client.on_connect = on_connect
        client.on_subscribe = on_subscribe
        client.on_unsubscribe = on_unsubscribe
        client.on_publish = on_publish
        client.on_disconnect = on_disconnect
        
        # Set credentials if provided
        if USERNAME:
            client.username_pw_set(USERNAME, PASSWORD)
        
        print("\n[CONNECTING] Attempting MQTT v5.0 connection...")
        
        # Connect to broker with MQTT v5
        client.connect(
            host=BROKER_HOST,
            port=BROKER_PORT,
            keepalive=KEEPALIVE
        )
        
        # Start network loop
        client.loop_start()
        
        # Wait for connection
        timeout = 5.0
        start = time.time()
        while not state["connected"] and (time.time() - start) < timeout:
            time.sleep(0.1)
        
        if not state["connected"]:
            print("\n[TIMEOUT] ✗ No connection established")
            return False
        
        # Test 1: Subscribe to topics and check SUBACK reason codes
        print("\n" + "=" * 70)
        print("TEST 1: SUBACK Reason Codes")
        print("=" * 70)
        
        test_topics = [
            ("test/phase2/topic1", 1),
            ("test/phase2/topic2", 2),
        ]
        
        topic_filters = [(topic, qos) for topic, qos in test_topics]
        client.subscribe(topic_filters)
        
        # Wait for SUBACK
        time.sleep(1.0)
        
        if state["suback_reason_codes"]:
            print("\n[RESULT] ✓ SUBACK with reason codes received")
            all_success = all(
                (isinstance(rc, ReasonCode) and rc.value <= 2) or (isinstance(rc, int) and rc <= 2)
                for rc in state["suback_reason_codes"]
            )
            if all_success:
                print("[RESULT] ✓ All subscriptions granted (reason codes indicate success)")
            else:
                print("[RESULT] ⚠ Some subscriptions failed")
        else:
            print("\n[RESULT] ✗ No SUBACK reason codes received")
            return False
        
        # Test 2: Publish QoS 1 message and check PUBACK reason code
        print("\n" + "=" * 70)
        print("TEST 2: PUBACK Reason Code")
        print("=" * 70)
        
        publish_topic = "test/phase2/publish"
        publish_payload = "Test message for PUBACK"
        
        result = client.publish(
            topic=publish_topic,
            payload=publish_payload,
            qos=1,
            retain=False
        )
        
        state["publish_mid"] = result.mid
        print(f"Published message ID: {result.mid}")
        
        # Wait for PUBACK
        time.sleep(1.0)
        
        if state["puback_reason_code"] is not None:
            print("\n[RESULT] ✓ PUBACK with reason code received")
            rc = state["puback_reason_code"]
            success = (isinstance(rc, ReasonCode) and rc.value == 0) or (isinstance(rc, int) and rc == 0)
            if success:
                print("[RESULT] ✓ Publish acknowledged with SUCCESS reason code")
            else:
                print(f"[RESULT] ⚠ Publish acknowledged with reason code: {rc}")
        else:
            print("\n[RESULT] ✗ No PUBACK reason code received")
            return False
        
        # Test 3: Unsubscribe and check UNSUBACK reason codes
        print("\n" + "=" * 70)
        print("TEST 3: UNSUBACK Reason Codes")
        print("=" * 70)
        
        unsub_topics = [topic for topic, _ in test_topics]
        client.unsubscribe(unsub_topics)
        
        # Wait for UNSUBACK
        time.sleep(1.0)
        
        if state["unsuback_reason_codes"]:
            print("\n[RESULT] ✓ UNSUBACK with reason codes received")
            all_success = all(
                (isinstance(rc, ReasonCode) and rc.value == 0) or (isinstance(rc, int) and rc == 0)
                for rc in state["unsuback_reason_codes"]
            )
            if all_success:
                print("[RESULT] ✓ All unsubscriptions successful (reason code = SUCCESS)")
            else:
                print("[RESULT] ⚠ Some unsubscriptions had non-success codes")
        else:
            print("\n[RESULT] ✗ No UNSUBACK reason codes received")
            return False
        
        # Summary
        print("\n" + "=" * 70)
        print("PHASE 2 VALIDATION SUMMARY")
        print("=" * 70)
        print("  ✓ SUBACK with MQTT v5 reason codes")
        print("  ✓ PUBACK with MQTT v5 reason code")
        print("  ✓ UNSUBACK with MQTT v5 reason codes")
        
        # Graceful disconnect
        time.sleep(0.5)
        client.disconnect()
        client.loop_stop()
        
        print("\n[OVERALL] ✓✓✓ PHASE 2 TEST PASSED ✓✓✓")
        return True
        
    except Exception as e:
        print(f"\n[EXCEPTION] ✗ Error during test: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("\nStarting MQTT v5.0 Phase 2 test...")
    print("(This test validates reason codes in SUBACK, PUBACK, and UNSUBACK)\n")
    
    success = test_mqtt5_reason_codes()
    
    print("\n" + "=" * 70)
    if success:
        print("TEST RESULT: PASS ✓")
        sys.exit(0)
    else:
        print("TEST RESULT: FAIL ✗")
        sys.exit(1)
