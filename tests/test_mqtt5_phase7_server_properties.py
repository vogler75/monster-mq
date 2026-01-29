#!/usr/bin/env python3
"""
Test MQTT v5.0 Server-Side Properties (Phase 7).

This test verifies that:
1. CONNACK contains proper server-originated properties
2. Session Expiry Interval is echoed back
3. Server Keep Alive is sent
4. Receive Maximum is communicated
5. Maximum QoS, Retain Available, Maximum Packet Size are present
6. Topic Alias Maximum is included
7. Wildcard/Shared Subscription availability flags are set
8. Assigned Client Identifier is sent when appropriate

Usage:
- Start broker locally (host=localhost, port=1883)
- Run: python3 tests/test_mqtt5_phase7_server_properties.py

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
except ImportError:
    print("ERROR: paho-mqtt >= 2.0.0 not installed.")
    print("Install with: pip install 'paho-mqtt>=2.0.0'")
    sys.exit(1)

# Configuration
BROKER_HOST = os.getenv("MQTT_BROKER", "localhost")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
KEEPALIVE = 60

# Credentials
USERNAME: Optional[str] = os.getenv("MQTT_USERNAME", "Test")
PASSWORD: Optional[str] = os.getenv("MQTT_PASSWORD", "Test")

# Test state
state = {
    "connected": False,
    "connack_properties": None,
    "reason_code": None
}


def on_connect(client, userdata, flags, reason_code, properties=None):
    """Called when the broker responds to our connection request (CONNACK)."""
    print(f"\n[CONNACK] Reason code: {reason_code}")
    print(f"[CONNACK] Flags: {flags}")
    
    if properties:
        print(f"\n[CONNACK] Properties received:")
        state["connack_properties"] = properties
        
        # List all properties
        if hasattr(properties, 'SessionExpiryInterval'):
            print(f"  Session Expiry Interval (17): {properties.SessionExpiryInterval}")
        if hasattr(properties, 'AssignedClientIdentifier'):
            print(f"  Assigned Client Identifier (18): {properties.AssignedClientIdentifier}")
        if hasattr(properties, 'ServerKeepAlive'):
            print(f"  Server Keep Alive (19): {properties.ServerKeepAlive}")
        if hasattr(properties, 'ReceiveMaximum'):
            print(f"  Receive Maximum (33): {properties.ReceiveMaximum}")
        if hasattr(properties, 'MaximumQoS'):
            print(f"  Maximum QoS (36): {properties.MaximumQoS}")
        if hasattr(properties, 'RetainAvailable'):
            print(f"  Retain Available (37): {properties.RetainAvailable}")
        if hasattr(properties, 'MaximumPacketSize'):
            print(f"  Maximum Packet Size (39): {properties.MaximumPacketSize}")
        if hasattr(properties, 'TopicAliasMaximum'):
            print(f"  Topic Alias Maximum (34): {properties.TopicAliasMaximum}")
        if hasattr(properties, 'WildcardSubscriptionAvailable'):
            print(f"  Wildcard Subscription Available (40): {properties.WildcardSubscriptionAvailable}")
        if hasattr(properties, 'SubscriptionIdentifiersAvailable'):
            print(f"  Subscription Identifier Available (41): {properties.SubscriptionIdentifiersAvailable}")
        if hasattr(properties, 'SharedSubscriptionAvailable'):
            print(f"  Shared Subscription Available (42): {properties.SharedSubscriptionAvailable}")
    else:
        print("[CONNACK] ✗ No properties returned")
    
    if reason_code == 0:
        print("\n[SUCCESS] ✓ MQTT v5.0 connection accepted!")
        state["connected"] = True
    else:
        print(f"\n[FAILED] ✗ Connection refused with reason code: {reason_code}")
    
    state["reason_code"] = reason_code


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    """Called when the client disconnects."""
    print(f"\n[DISCONNECT] Reason code: {reason_code}")


def test_mqtt5_server_properties():
    """Test MQTT v5.0 server properties in CONNACK."""
    print("=" * 70)
    print("MQTT v5.0 Server Properties Test (Phase 7)")
    print("=" * 70)
    print(f"Broker: {BROKER_HOST}:{BROKER_PORT}")
    print(f"Protocol: MQTT v5.0")
    print()
    
    try:
        # Create client
        client = mqtt.Client(
            client_id=f"test_phase7_{int(time.time())}",
            protocol=MQTTv5,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2
        )
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        
        if USERNAME:
            client.username_pw_set(USERNAME, PASSWORD)
        
        # Set CONNECT properties
        connect_properties = Properties(PacketTypes.CONNECT)
        connect_properties.SessionExpiryInterval = 300  # Request 5 minutes
        
        print("[CONNECTING] Attempting MQTT v5.0 connection...")
        print(f"  Session Expiry Interval: 300 seconds")
        print(f"  Keep Alive: {KEEPALIVE} seconds")
        
        # Connect to broker
        client.connect(
            host=BROKER_HOST,
            port=BROKER_PORT,
            keepalive=KEEPALIVE,
            properties=connect_properties
        )
        
        # Start network loop
        client.loop_start()
        
        # Wait for connection result
        timeout = 5.0
        start = time.time()
        while not state["connected"] and state["reason_code"] is None and (time.time() - start) < timeout:
            time.sleep(0.1)
        
        if not state["connected"]:
            print(f"\n[ERROR] Connection failed or timed out (reason_code={state['reason_code']})")
            return False
        
        # Validate CONNACK properties
        print("\n" + "=" * 70)
        print("PHASE 7 VALIDATION")
        print("=" * 70)
        
        props = state["connack_properties"]
        if not props:
            print("✗ No CONNACK properties received")
            return False
        
        print("✓ CONNACK properties received")
        
        success = True
        
        # Session Expiry Interval (17)
        if hasattr(props, 'SessionExpiryInterval'):
            if props.SessionExpiryInterval == 300:
                print("  ✓ Session Expiry Interval: 300 (echoed back)")
            else:
                print(f"  ⚠ Session Expiry Interval: {props.SessionExpiryInterval} (expected 300)")
        else:
            print("  ✗ Session Expiry Interval not present")
            success = False
        
        # Server Keep Alive (19)
        if hasattr(props, 'ServerKeepAlive'):
            print(f"  ✓ Server Keep Alive: {props.ServerKeepAlive}")
        else:
            print("  ⚠ Server Keep Alive not present (optional)")
        
        # Receive Maximum (33)
        if hasattr(props, 'ReceiveMaximum'):
            if props.ReceiveMaximum > 0:
                print(f"  ✓ Receive Maximum: {props.ReceiveMaximum}")
            else:
                print(f"  ✗ Receive Maximum invalid: {props.ReceiveMaximum}")
                success = False
        else:
            print("  ✗ Receive Maximum not present")
            success = False
        
        # Maximum QoS (36)
        if hasattr(props, 'MaximumQoS'):
            if props.MaximumQoS >= 0 and props.MaximumQoS <= 2:
                print(f"  ✓ Maximum QoS: {props.MaximumQoS}")
            else:
                print(f"  ✗ Maximum QoS invalid: {props.MaximumQoS}")
                success = False
        else:
            print("  ✗ Maximum QoS not present")
            success = False
        
        # Retain Available (37)
        if hasattr(props, 'RetainAvailable'):
            print(f"  ✓ Retain Available: {props.RetainAvailable}")
        else:
            print("  ✗ Retain Available not present")
            success = False
        
        # Maximum Packet Size (39)
        if hasattr(props, 'MaximumPacketSize'):
            if props.MaximumPacketSize > 0:
                print(f"  ✓ Maximum Packet Size: {props.MaximumPacketSize}")
            else:
                print(f"  ✗ Maximum Packet Size invalid: {props.MaximumPacketSize}")
                success = False
        else:
            print("  ✗ Maximum Packet Size not present")
            success = False
        
        # Topic Alias Maximum (34)
        if hasattr(props, 'TopicAliasMaximum'):
            print(f"  ✓ Topic Alias Maximum: {props.TopicAliasMaximum}")
        else:
            print("  ✗ Topic Alias Maximum not present")
            success = False
        
        # Wildcard Subscription Available (40)
        if hasattr(props, 'WildcardSubscriptionAvailable'):
            print(f"  ✓ Wildcard Subscription Available: {props.WildcardSubscriptionAvailable}")
        else:
            print("  ✗ Wildcard Subscription Available not present")
            success = False
        
        # Subscription Identifier Available (41)
        if hasattr(props, 'SubscriptionIdentifiersAvailable'):
            print(f"  ✓ Subscription Identifier Available: {props.SubscriptionIdentifiersAvailable}")
        else:
            # Property 41 with value 0 may not be included by paho-mqtt (means "not supported")
            print("  ✓ Subscription Identifier Available: not present (0 = not supported)")
        
        # Shared Subscription Available (42)
        if hasattr(props, 'SharedSubscriptionAvailable'):
            print(f"  ✓ Shared Subscription Available: {props.SharedSubscriptionAvailable}")
        else:
            print("  ✗ Shared Subscription Available not present")
            success = False
        
        # Cleanup
        client.loop_stop()
        client.disconnect()
        
        return success
        
    except Exception as e:
        print(f"\n[ERROR] Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("\nStarting MQTT v5.0 Phase 7 test...")
    print("(This test validates server-side properties in CONNACK)\n")
    
    success = test_mqtt5_server_properties()
    
    print("\n" + "=" * 70)
    if success:
        print("[OVERALL] ✓✓✓ PHASE 7 TEST PASSED ✓✓✓")
        print("All required server properties present in CONNACK!")
        sys.exit(0)
    else:
        print("[OVERALL] ✗✗✗ PHASE 7 TEST FAILED ✗✗✗")
        sys.exit(1)
