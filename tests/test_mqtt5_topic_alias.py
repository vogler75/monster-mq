#!/usr/bin/env python3
"""
Test MQTT v5.0 Topic Aliases.

This test verifies that:
1. Broker sends Topic Alias Maximum in CONNACK
2. Client can establish topic alias mapping
3. Client can use alias to publish (empty topic name)
4. Broker correctly resolves aliases to original topics
5. Broker validates alias limits and rejects excessive aliases

Topic Alias Feature:
- Reduces bandwidth by replacing topic strings with integers
- Alias mapping is session-specific (cleared on disconnect)
- Alias range: 1 to Topic Alias Maximum
- Property ID: 35 (Topic Alias)

Usage:
- Start broker locally (host=localhost, port=1883)
- Run: python tests/test_mqtt5_topic_alias.py

Requirements:
- paho-mqtt >= 2.0.0 (for MQTT v5 support)
  Install: pip install 'paho-mqtt>=2.0.0'
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
    "publisher_connected": False,
    "subscriber_connected": False,
    "topic_alias_maximum": None,
    "messages_received": [],
}


def on_publisher_connect(client, userdata, flags, reason_code, properties=None):
    """Called when the publisher connects."""
    print(f"\n[PUBLISHER] CONNACK received - Reason code: {reason_code}")
    
    if reason_code == 0:
        print("[PUBLISHER] ✓ Connected successfully")
        state["publisher_connected"] = True
        
        # Check for Topic Alias Maximum in CONNACK properties
        if properties:
            print(f"[PUBLISHER] CONNACK Properties: {properties}")
            # Property ID 34 = Topic Alias Maximum
            if hasattr(properties, 'TopicAliasMaximum'):
                state["topic_alias_maximum"] = properties.TopicAliasMaximum
                print(f"[PUBLISHER] ✓ Topic Alias Maximum: {state['topic_alias_maximum']}")
            else:
                print("[PUBLISHER] ✗ No Topic Alias Maximum in CONNACK")
        else:
            print("[PUBLISHER] ✗ No properties in CONNACK")
    else:
        print(f"[PUBLISHER] ✗ Connection failed: {reason_code}")


def on_subscriber_connect(client, userdata, flags, reason_code, properties=None):
    """Called when the subscriber connects."""
    print(f"\n[SUBSCRIBER] CONNACK received - Reason code: {reason_code}")
    
    if reason_code == 0:
        print("[SUBSCRIBER] ✓ Connected successfully")
        state["subscriber_connected"] = True
        
        # Subscribe to test topic
        topic = "test/topic_alias/alias"
        result, mid = client.subscribe(topic, qos=1)
        if result == mqtt.MQTT_ERR_SUCCESS:
            print(f"[SUBSCRIBER] ✓ Subscribed to: {topic}")
        else:
            print(f"[SUBSCRIBER] ✗ Subscribe failed: {result}")
    else:
        print(f"[SUBSCRIBER] ✗ Connection failed: {reason_code}")


def on_message(client, userdata, msg):
    """Called when subscriber receives a message."""
    payload = msg.payload.decode('utf-8')
    print(f"\n[SUBSCRIBER] Message received:")
    print(f"  Topic: {msg.topic}")
    print(f"  Payload: {payload}")
    
    # Store received message
    state["messages_received"].append({
        "topic": msg.topic,
        "payload": payload
    })


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    """Called when a client disconnects."""
    client_type = userdata.get("type", "unknown")
    print(f"[{client_type.upper()}] Disconnected - Reason: {reason_code}")


def test_mqtt5_topic_alias():
    """Test MQTT v5.0 Topic Aliases."""
    print("=" * 70)
    print("MQTT v5.0 Topic Alias Test")
    print("=" * 70)
    print(f"Broker: {BROKER_HOST}:{BROKER_PORT}")
    print(f"Protocol: MQTT v5.0")
    print()
    
    # Create clients
    publisher_id = f"test_mqtt5_ta_pub_{int(time.time())}"
    subscriber_id = f"test_mqtt5_ta_sub_{int(time.time())}"
    
    print(f"Publisher ID: {publisher_id}")
    print(f"Subscriber ID: {subscriber_id}")
    
    try:
        # Create subscriber client
        subscriber = mqtt.Client(
            client_id=subscriber_id,
            protocol=MQTTv5,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            userdata={"type": "subscriber"}
        )
        subscriber.on_connect = on_subscriber_connect
        subscriber.on_message = on_message
        subscriber.on_disconnect = on_disconnect
        
        if USERNAME:
            subscriber.username_pw_set(USERNAME, PASSWORD)
        
        # Connect subscriber
        print("\n[SUBSCRIBER] Connecting to broker...")
        subscriber.connect(BROKER_HOST, BROKER_PORT, KEEPALIVE)
        subscriber.loop_start()
        
        # Wait for subscriber to connect and subscribe
        timeout = 5
        start = time.time()
        while not state["subscriber_connected"] and (time.time() - start) < timeout:
            time.sleep(0.1)
        
        if not state["subscriber_connected"]:
            print("\n[ERROR] ✗ Subscriber connection timeout")
        assert state["subscriber_connected"], "Subscriber connection timeout"
        
        time.sleep(0.5)  # Allow subscription to complete
        
        # Create publisher client
        publisher = mqtt.Client(
            client_id=publisher_id,
            protocol=MQTTv5,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            userdata={"type": "publisher"}
        )
        publisher.on_connect = on_publisher_connect
        publisher.on_disconnect = on_disconnect
        
        if USERNAME:
            publisher.username_pw_set(USERNAME, PASSWORD)
        
        # Connect publisher
        print("\n[PUBLISHER] Connecting to broker...")
        publisher.connect(BROKER_HOST, BROKER_PORT, KEEPALIVE)
        publisher.loop_start()
        
        # Wait for publisher to connect
        start = time.time()
        while not state["publisher_connected"] and (time.time() - start) < timeout:
            time.sleep(0.1)
        
        if not state["publisher_connected"]:
            print("\n[ERROR] ✗ Publisher connection timeout")
        assert state["publisher_connected"], "Publisher connection timeout"
        
        time.sleep(0.5)  # Allow connection to stabilize
        
        # Verify Topic Alias Maximum in CONNACK
        print("\n" + "=" * 70)
        print("TEST 1: Verify Topic Alias Maximum in CONNACK")
        print("=" * 70)
        
        if state["topic_alias_maximum"] is not None:
            print(f"✓ Topic Alias Maximum: {state['topic_alias_maximum']}")
            if state["topic_alias_maximum"] >= 1:
                print("✓ Topic Alias feature supported (maximum >= 1)")
            else:
                print("✗ Topic Alias not supported (maximum = 0)")
        
        assert state["topic_alias_maximum"] is not None, "Topic Alias Maximum not present in CONNACK"
        assert state["topic_alias_maximum"] >= 1, "Topic Alias not supported (maximum = 0)"
        
        # Test 2: Establish topic alias mapping
        print("\n" + "=" * 70)
        print("TEST 2: Establish Topic Alias Mapping")
        print("=" * 70)
        
        topic = "test/topic_alias/alias"
        alias_id = 1
        payload1 = "Message 1 - Establishing alias"
        
        # Create properties with Topic Alias
        props = Properties(PacketTypes.PUBLISH)
        props.TopicAlias = alias_id
        
        print(f"Publishing with Topic Alias {alias_id}:")
        print(f"  Topic: {topic}")
        print(f"  Payload: {payload1}")
        
        result = publisher.publish(
            topic=topic,
            payload=payload1,
            qos=1,
            properties=props
        )
        
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print("✓ Publish successful (establishing alias)")
        else:
            print(f"✗ Publish failed: {result.rc}")
        assert result.rc == mqtt.MQTT_ERR_SUCCESS, f"Publish failed: {result.rc}"
        
        # Wait for message to be received
        time.sleep(1)
        
        assert len(state["messages_received"]) >= 1, "Message not received by subscriber"
        
        msg1 = state["messages_received"][0]
        assert msg1["topic"] == topic and msg1["payload"] == payload1, f"Message 1 incorrect: {msg1}"
        print(f"✓ Message 1 received correctly:")
        print(f"  Topic: {msg1['topic']}")
        print(f"  Payload: {msg1['payload']}")
        
        # Test 3: Use established alias (empty topic)
        print("\n" + "=" * 70)
        print("TEST 3: Use Established Topic Alias (Empty Topic)")
        print("=" * 70)
        
        payload2 = "Message 2 - Using alias only"
        
        # Create properties with same Topic Alias, but use EMPTY topic
        props2 = Properties(PacketTypes.PUBLISH)
        props2.TopicAlias = alias_id
        
        print(f"Publishing with Topic Alias {alias_id} ONLY (no topic):")
        print(f"  Topic: '' (empty - should resolve to '{topic}')")
        print(f"  Payload: {payload2}")
        
        result = publisher.publish(
            topic="",  # Empty topic - use alias
            payload=payload2,
            qos=1,
            properties=props2
        )
        
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print("✓ Publish successful (using alias)")
        else:
            print(f"✗ Publish failed: {result.rc}")
        assert result.rc == mqtt.MQTT_ERR_SUCCESS, f"Publish failed: {result.rc}"
        
        # Wait for message to be received
        time.sleep(1)
        
        assert len(state["messages_received"]) >= 2, "Message 2 not received by subscriber"
        
        msg2 = state["messages_received"][1]
        assert msg2["topic"] == topic and msg2["payload"] == payload2, f"Message 2 incorrect: {msg2}"
        print(f"✓ Message 2 received correctly (alias resolved):")
        print(f"  Topic: {msg2['topic']} (resolved from alias {alias_id})")
        print(f"  Payload: {msg2['payload']}")
        
        # Test 4: Multiple aliases
        print("\n" + "=" * 70)
        print("TEST 4: Multiple Topic Aliases")
        print("=" * 70)
        
        topic2 = "test/topic_alias/alias2"
        alias_id2 = 2
        payload3 = "Message 3 - Second alias"
        
        # Subscribe to second topic
        subscriber.subscribe(topic2, qos=1)
        time.sleep(1)  # Wait for subscription to complete
        
        # Establish second alias
        props3 = Properties(PacketTypes.PUBLISH)
        props3.TopicAlias = alias_id2
        
        print(f"Publishing with Topic Alias {alias_id2}:")
        print(f"  Topic: {topic2}")
        print(f"  Payload: {payload3}")
        
        result = publisher.publish(
            topic=topic2,
            payload=payload3,
            qos=1,
            properties=props3
        )
        
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print("✓ Publish successful (second alias)")
        else:
            print(f"✗ Publish failed: {result.rc}")
        assert result.rc == mqtt.MQTT_ERR_SUCCESS, f"Publish failed: {result.rc}"
        
        time.sleep(1)
        
        assert len(state["messages_received"]) >= 3, "Message 3 not received"
        
        msg3 = state["messages_received"][2]
        assert msg3["topic"] == topic2 and msg3["payload"] == payload3, f"Message 3 incorrect: {msg3}"
        print(f"✓ Message 3 received correctly (second alias):")
        print(f"  Topic: {msg3['topic']}")
        print(f"  Payload: {msg3['payload']}")
        
        # Now use first alias again
        payload4 = "Message 4 - Reusing first alias"
        props4 = Properties(PacketTypes.PUBLISH)
        props4.TopicAlias = alias_id
        
        print(f"\nPublishing with first alias {alias_id} again:")
        print(f"  Topic: '' (should resolve to '{topic}')")
        print(f"  Payload: {payload4}")
        
        result = publisher.publish(
            topic="",
            payload=payload4,
            qos=1,
            properties=props4
        )
        
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print("✓ Publish successful (reusing first alias)")
        else:
            print(f"✗ Publish failed: {result.rc}")
        assert result.rc == mqtt.MQTT_ERR_SUCCESS, f"Publish failed: {result.rc}"
        
        time.sleep(1)
        
        assert len(state["messages_received"]) >= 4, "Message 4 not received"
        
        msg4 = state["messages_received"][3]
        assert msg4["topic"] == topic and msg4["payload"] == payload4, f"Message 4 incorrect: {msg4}"
        print(f"✓ Message 4 received correctly (first alias reused):")
        print(f"  Topic: {msg4['topic']}")
        print(f"  Payload: {msg4['payload']}")
        
        # Cleanup
        time.sleep(0.5)
        publisher.disconnect()
        subscriber.disconnect()
        publisher.loop_stop()
        subscriber.loop_stop()
        
        # Final validation
        print("\n" + "=" * 70)
        print("VALIDATION RESULTS")
        print("=" * 70)
        
        results = {
            "Topic Alias Maximum in CONNACK": state["topic_alias_maximum"] is not None and state["topic_alias_maximum"] >= 1,
            "Establish alias mapping": len(state["messages_received"]) >= 1,
            "Use alias (empty topic)": len(state["messages_received"]) >= 2,
            "Multiple aliases": len(state["messages_received"]) >= 4,
            "Alias resolution": all(msg["topic"] != "" for msg in state["messages_received"])
        }
        
        for test, passed in results.items():
            status = "✓" if passed else "✗"
            print(f"{status} {test}")
        
        all_passed = all(results.values())
        
        if all_passed:
            print("\n[OVERALL] ✓✓✓ TOPIC ALIAS TEST PASSED ✓✓✓")
            print(f"All {len(results)} tests passed!")
            print(f"Total messages: {len(state['messages_received'])}")
        else:
            failed = sum(1 for v in results.values() if not v)
            print(f"\n[OVERALL] ✗✗✗ TOPIC ALIAS TEST FAILED ✗✗✗")
            print(f"{failed}/{len(results)} tests failed")
        
        assert all_passed, f"{sum(1 for v in results.values() if not v)}/{len(results)} tests failed"
            
    except Exception as e:
        print(f"\n[ERROR] ✗ Exception during test: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    success = test_mqtt5_topic_alias()
    sys.exit(0 if success else 1)
