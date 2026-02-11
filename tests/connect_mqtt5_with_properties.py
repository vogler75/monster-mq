"""
MQTT v5 Connection Test with Message Properties
Tests Phase 9 implementation: Message properties in Topic Browser and Dashboard statistics
"""
import paho.mqtt.client as mqtt
import time
import json
import sys

def on_connect(client, userdata, flags, rc, properties=None):
    """Callback when connected to broker"""
    if rc == 0:
        print("✅ MQTT v5 connection successful!")
        print(f"   Protocol: MQTT v5.0")
        print(f"   Client ID: {client._client_id.decode()}")
        
        # Publish test message with MQTT v5 properties
        print("\n📤 Publishing test message with v5 properties...")
        
        # Create message properties
        props = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
        props.MessageExpiryInterval = 3600  # 1 hour expiry
        props.ContentType = "application/json"
        props.ResponseTopic = "response/test"
        props.PayloadFormatIndicator = 1  # UTF-8 text
        props.UserProperty = [
            ("key1", "value1"),
            ("key2", "value2"),
            ("source", "phase9-test"),
            ("timestamp", str(int(time.time())))
        ]
        
        # Create test payload
        payload = json.dumps({
            "temperature": 22.5,
            "unit": "celsius",
            "sensor": "test-sensor-01",
            "status": "active"
        })
        
        # Publish with properties
        result = client.publish(
            "test/mqtt5/properties",
            payload,
            qos=1,
            properties=props
        )
        
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print("✅ Message published successfully with properties:")
            print(f"   Topic: test/mqtt5/properties")
            print(f"   Message Expiry: 3600 seconds")
            print(f"   Content Type: application/json")
            print(f"   Response Topic: response/test")
            print(f"   Payload Format: UTF-8 Text")
            print(f"   User Properties: {len(props.UserProperty)} pairs")
        else:
            print(f"❌ Publish failed: {result.rc}")
            
        # Subscribe to verify
        client.subscribe("test/mqtt5/properties", qos=1)
        print("\n📥 Subscribed to test/mqtt5/properties")
        
    else:
        print(f"❌ Connection failed with code {rc}")
        sys.exit(1)

def on_message(client, userdata, msg):
    """Callback when message received"""
    print(f"\n📨 Message received:")
    print(f"   Topic: {msg.topic}")
    print(f"   Payload: {msg.payload.decode()}")
    print(f"   QoS: {msg.qos}")
    
    # Check for v5 properties
    if hasattr(msg, 'properties') and msg.properties:
        print(f"   ✅ MQTT v5 Properties received:")
        if msg.properties.MessageExpiryInterval:
            print(f"      Message Expiry: {msg.properties.MessageExpiryInterval}s")
        if msg.properties.ContentType:
            print(f"      Content Type: {msg.properties.ContentType}")
        if msg.properties.ResponseTopic:
            print(f"      Response Topic: {msg.properties.ResponseTopic}")
        if msg.properties.PayloadFormatIndicator is not None:
            print(f"      Payload Format: {'UTF-8' if msg.properties.PayloadFormatIndicator == 1 else 'Binary'}")
        if msg.properties.UserProperty:
            print(f"      User Properties:")
            for key, value in msg.properties.UserProperty:
                print(f"         {key} = {value}")

def on_disconnect(client, userdata, rc):
    """Callback when disconnected"""
    if rc == 0:
        print("\n✅ Disconnected cleanly")
    else:
        print(f"\n⚠️  Disconnected with code {rc}")

def main():
    """Main test function"""
    print("=" * 70)
    print("MQTT v5 Phase 9 Test - Message Properties & Statistics")
    print("=" * 70)
    print()
    
    # Create MQTT v5 client
    print("🔌 Creating MQTT v5 client...")
    client = mqtt.Client(
        client_id="test-mqtt5-phase9",
        protocol=mqtt.MQTTv5,
        transport="tcp"
    )
    
    # Set callbacks
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    
    # Enable debug (optional)
    # client.enable_logger()
    
    try:
        print("🔗 Connecting to broker at localhost:1883...")
        client.connect("localhost", 1883, 60)
        
        # Start network loop
        client.loop_start()
        
        # Keep connection alive for testing
        print("\n⏱️  Connection active - Testing for 30 seconds...")
        print("   → Check Dashboard: http://localhost:8080/pages/dashboard.html")
        print("   → Check Topic Browser: http://localhost:8080/pages/topic-browser.html")
        print("   → Check Sessions: http://localhost:8080/pages/sessions.html")
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
        print("1. Open Dashboard and verify 'MQTT v5.0 Clients' card shows '1 / 1'")
        print("2. Open Topic Browser and check message properties display")
        print("3. Open Sessions page and verify purple 'MQTT v5.0' badge")
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
