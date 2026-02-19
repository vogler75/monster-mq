"""
MQTT v3.1.1 Connection Test
Tests backward compatibility alongside MQTT v5 clients
"""
import paho.mqtt.client as mqtt
import time
import json
import sys

def on_connect(client, userdata, flags, rc):
    """Callback when connected to broker"""
    if rc == 0:
        print("✅ MQTT v3.1.1 connection successful!")
        print(f"   Protocol: MQTT v3.1.1")
        print(f"   Client ID: {client._client_id.decode()}")
        
        # Publish test message (no v5 properties)
        print("\n📤 Publishing test message (v3.1.1 - no properties)...")
        
        payload = json.dumps({
            "temperature": 19.8,
            "unit": "celsius",
            "sensor": "test-sensor-02",
            "status": "active"
        })
        
        result = client.publish(
            "test/mqtt3/legacy",
            payload,
            qos=1
        )
        
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print("✅ Message published successfully:")
            print(f"   Topic: test/mqtt3/legacy")
            print(f"   Properties: None (MQTT v3.1.1)")
        else:
            print(f"❌ Publish failed: {result.rc}")
            
        # Subscribe to verify
        client.subscribe("test/mqtt3/legacy", qos=1)
        print("\n📥 Subscribed to test/mqtt3/legacy")
        
    else:
        print(f"❌ Connection failed with code {rc}")
        sys.exit(1)

def on_message(client, userdata, msg):
    """Callback when message received"""
    print(f"\n📨 Message received:")
    print(f"   Topic: {msg.topic}")
    print(f"   Payload: {msg.payload.decode()}")
    print(f"   QoS: {msg.qos}")
    print(f"   Properties: None (MQTT v3.1.1)")

def on_disconnect(client, userdata, rc):
    """Callback when disconnected"""
    if rc == 0:
        print("\n✅ Disconnected cleanly")
    else:
        print(f"\n⚠️  Disconnected with code {rc}")

def main():
    """Main test function"""
    print("=" * 70)
    print("MQTT v3.1.1 Test - Legacy Client Compatibility")
    print("=" * 70)
    print()
    
    # Create MQTT v3.1.1 client
    print("🔌 Creating MQTT v3.1.1 client...")
    client = mqtt.Client(
        client_id="test-mqtt3-legacy",
        protocol=mqtt.MQTTv311,
        transport="tcp"
    )
    
    # Set callbacks
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    
    try:
        print("🔗 Connecting to broker at localhost:1883...")
        client.connect("localhost", 1883, 60)
        
        # Start network loop
        client.loop_start()
        
        # Keep connection alive for testing
        print("\n⏱️  Connection active - Testing for 30 seconds...")
        print("   → Check Dashboard to see mixed v3 + v5 statistics")
        print("   → Check Sessions page for blue 'MQTT v3.1.1' badge")
        print()
        
        # Countdown
        for remaining in range(30, 0, -5):
            print(f"   ⏰ {remaining} seconds remaining...")
            time.sleep(5)
        
        # Stop loop and disconnect
        print("\n🔌 Disconnecting...")
        client.loop_stop()
        client.disconnect()
        
        print("\n" + "=" * 70)
        print("✅ Test Complete!")
        print("=" * 70)
        print("\nNext Steps:")
        print("1. Open Dashboard and verify statistics include v3 client")
        print("2. Open Sessions page and verify blue 'MQTT v3.1.1' badge")
        print()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        client.loop_stop()
        client.disconnect()
        sys.exit(0)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
