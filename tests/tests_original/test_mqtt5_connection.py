#!/usr/bin/env python3
"""
Test MQTT v5.0 connection acceptance.

This test verifies that:
1. MQTT v5.0 clients can successfully connect to the broker
2. The broker accepts MQTT5 protocol version (5) without rejection
3. CONNACK is received (basic acceptance, properties implementation pending)

Usage:
- Start broker locally (host=localhost, port=1883)
- Run: python3 tests/test_mqtt5_connection.py

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
KEEPALIVE = 30

# Credentials
USERNAME: Optional[str] = os.getenv("MQTT_USERNAME", "Test")
PASSWORD: Optional[str] = os.getenv("MQTT_PASSWORD", "Test")

# Test state
state = {
    "connected": False,
    "connection_result": None,
    "connack_properties": None,
    "error": None
}


def on_connect(client, userdata, flags, reason_code, properties=None):
    """Called when the broker responds to our connection request (CONNACK)."""
    print(f"\n[CONNACK] Reason code: {reason_code}")
    print(f"[CONNACK] Flags: {flags}")
    
    if properties:
        print(f"[CONNACK] Properties: {properties}")
        state["connack_properties"] = properties
    else:
        print("[CONNACK] No properties returned")
    
    if reason_code == 0:
        print("[SUCCESS] ✓ MQTT v5.0 connection accepted!")
        state["connected"] = True
        state["connection_result"] = "success"
    else:
        print(f"[FAILED] ✗ Connection refused with reason code: {reason_code}")
        state["connection_result"] = f"refused_{reason_code}"


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    """Called when the client disconnects."""
    print(f"\n[DISCONNECT] Reason code: {reason_code}")
    print(f"[DISCONNECT] Flags: {disconnect_flags}")
    if properties:
        print(f"[DISCONNECT] Properties: {properties}")
    
    if reason_code != 0:
        state["error"] = f"Unexpected disconnect: {reason_code}"


def test_mqtt5_connection():
    """Test MQTT v5.0 connection to the broker."""
    print("=" * 70)
    print("MQTT v5.0 Connection Test")
    print("=" * 70)
    print(f"Broker: {BROKER_HOST}:{BROKER_PORT}")
    print(f"Username: {USERNAME}")
    print(f"Protocol: MQTT v5.0")
    print()
    
    # Create MQTT v5 client
    client_id = f"test_mqtt5_{int(time.time())}"
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
        client.on_disconnect = on_disconnect
        
        # Set credentials if provided
        if USERNAME:
            client.username_pw_set(USERNAME, PASSWORD)
        
        # Optional: Set CONNECT properties (MQTT v5 specific)
        connect_properties = Properties(PacketTypes.CONNECT)
        connect_properties.SessionExpiryInterval = 300  # 5 minutes
        connect_properties.ReceiveMaximum = 100
        connect_properties.MaximumPacketSize = 1024 * 1024  # 1 MB
        
        print("\n[CONNECTING] Attempting MQTT v5.0 connection...")
        print(f"  Session Expiry Interval: 300 seconds")
        print(f"  Receive Maximum: 100")
        print(f"  Maximum Packet Size: 1048576 bytes")
        
        # Connect to broker with MQTT v5
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
        while state["connection_result"] is None and (time.time() - start) < timeout:
            time.sleep(0.1)
        
        # Check results
        if state["connection_result"] is None:
            print(f"\n[TIMEOUT] ✗ No CONNACK received within {timeout} seconds")
            return False
        
        if state["connected"]:
            print("\n[VALIDATION]")
            print("  ✓ MQTT v5.0 protocol version accepted")
            print("  ✓ CONNACK received successfully")
            print("  ✓ Connection established")
            
            # Graceful disconnect
            time.sleep(0.5)
            client.disconnect()
            client.loop_stop()
            
            print("\n[OVERALL] ✓✓✓ CONNECTION TEST PASSED ✓✓✓")
            return True
        else:
            print("\n[OVERALL] ✗✗✗ CONNECTION TEST FAILED ✗✗✗")
            return False
            
    except Exception as e:
        print(f"\n[EXCEPTION] ✗ Error during test: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("\nStarting MQTT v5.0 connection test...")
    print("(This test validates basic MQTT5 connection acceptance)\n")
    
    success = test_mqtt5_connection()
    
    print("\n" + "=" * 70)
    if success:
        print("TEST RESULT: PASS ✓")
        sys.exit(0)
    else:
        print("TEST RESULT: FAIL ✗")
        sys.exit(1)
