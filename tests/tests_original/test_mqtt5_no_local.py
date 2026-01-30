"""
MQTT v5.0 No Local Subscription Option Test

Tests the No Local (NL) subscription option feature:
- Client subscribes with noLocal=True
- Client publishes to that topic
- Client should NOT receive its own published messages
- Other clients should receive the messages normally

Per MQTT v5.0 spec: No Local option prevents the Server from sending PUBLISH
packets to the client that originally published them.
"""

import paho.mqtt.client as mqtt
import time
import sys

# Configuration
BROKER_HOST = "localhost"
BROKER_PORT = 1883
TEST_TOPIC = "test/nolocal/messages"

# Test state
publisher_messages = []  # Messages published by client1
client1_received = []  # Messages received by client1 (publisher with noLocal)
client2_received = []  # Messages received by client2 (normal subscription)

client1_connected = False
client2_connected = False

def on_connect_client1(client, userdata, flags, rc, properties=None):
    global client1_connected
    print(f"[Client1-Publisher] Connected rc={rc}")
    if rc == 0:
        client1_connected = True
        # Subscribe with noLocal=True
        options = mqtt.SubscribeOptions(qos=1, noLocal=True)
        client.subscribe(TEST_TOPIC, options=options)
        print(f"[Client1-Publisher] Subscribed to {TEST_TOPIC} with noLocal=True")
    else:
        print(f"[Client1-Publisher] Connection failed")

def on_connect_client2(client, userdata, flags, rc, properties=None):
    global client2_connected
    print(f"[Client2-Subscriber] Connected rc={rc}")
    if rc == 0:
        client2_connected = True
        # Normal subscription (noLocal=False)
        client.subscribe(TEST_TOPIC, qos=1)
        print(f"[Client2-Subscriber] Subscribed to {TEST_TOPIC} with noLocal=False (default)")
    else:
        print(f"[Client2-Subscriber] Connection failed")

def on_message_client1(client, userdata, msg):
    """Client1 should NOT receive its own messages (noLocal=True)"""
    payload = msg.payload.decode()
    print(f"[Client1-Publisher] ❌ RECEIVED (should NOT happen): {payload}")
    client1_received.append(payload)

def on_message_client2(client, userdata, msg):
    """Client2 should receive all messages (normal subscription)"""
    payload = msg.payload.decode()
    print(f"[Client2-Subscriber] ✓ Received: {payload}")
    client2_received.append(payload)

def on_disconnect(client, userdata, flags, rc, properties=None):
    """Handle disconnect for MQTT v5 (5 parameters)"""
    print(f"[{userdata}] Disconnected rc={rc}")

def run_test():
    global client1_connected, client2_connected
    
    print("\n" + "="*70)
    print("MQTT v5.0 NO LOCAL SUBSCRIPTION OPTION TEST")
    print("="*70 + "\n")
    
    # Create clients
    client1 = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                          client_id="test-nolocal-publisher",
                          protocol=mqtt.MQTTv5)
    client1.on_connect = on_connect_client1
    client1.on_message = on_message_client1
    client1.on_disconnect = lambda c, u, f, r, p=None: on_disconnect(c, "Client1-Publisher", f, r, p)
    
    client2 = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                          client_id="test-nolocal-subscriber",
                          protocol=mqtt.MQTTv5)
    client2.on_connect = on_connect_client2
    client2.on_message = on_message_client2
    client2.on_disconnect = lambda c, u, f, r, p=None: on_disconnect(c, "Client2-Subscriber", f, r, p)
    
    try:
        # Connect both clients
        print("Connecting clients...")
        client1.connect(BROKER_HOST, BROKER_PORT, 60)
        client2.connect(BROKER_HOST, BROKER_PORT, 60)
        
        # Start network loops
        client1.loop_start()
        client2.loop_start()
        
        # Wait for both clients to connect and subscribe
        timeout = 5
        start_time = time.time()
        while (not client1_connected or not client2_connected) and (time.time() - start_time < timeout):
            time.sleep(0.1)
        
        if not client1_connected or not client2_connected:
            print("❌ FAILED: Clients did not connect within timeout")
            return False
        
        print("\n✓ Both clients connected and subscribed\n")
        time.sleep(1)  # Allow subscriptions to register
        
        # Publish messages from client1
        test_messages = [
            "Message 1 from publisher",
            "Message 2 from publisher",
            "Message 3 from publisher"
        ]
        
        print("Publishing messages from Client1 (noLocal=True)...")
        for msg in test_messages:
            result = client1.publish(TEST_TOPIC, msg, qos=1)
            result.wait_for_publish()
            publisher_messages.append(msg)
            print(f"  Published: {msg}")
            time.sleep(0.2)
        
        # Wait for message delivery
        time.sleep(2)
        
        # Verify results
        print("\n" + "="*70)
        print("TEST RESULTS")
        print("="*70)
        
        print(f"\nMessages published by Client1: {len(publisher_messages)}")
        print(f"Messages received by Client1 (noLocal=True): {len(client1_received)}")
        print(f"Messages received by Client2 (normal): {len(client2_received)}")
        
        # Test 1: Client1 should NOT receive its own messages
        test1_pass = len(client1_received) == 0
        print(f"\nTest 1 - Client1 noLocal filtering:")
        if test1_pass:
            print("  ✓ PASS: Client1 did NOT receive its own messages (noLocal working)")
        else:
            print(f"  ❌ FAIL: Client1 received {len(client1_received)} messages (should be 0)")
            print(f"         Messages: {client1_received}")
        
        # Test 2: Client2 should receive all messages
        test2_pass = len(client2_received) == len(test_messages)
        print(f"\nTest 2 - Client2 normal subscription:")
        if test2_pass:
            print("  ✓ PASS: Client2 received all messages")
        else:
            print(f"  ❌ FAIL: Client2 received {len(client2_received)}/{len(test_messages)} messages")
        
        # Overall result
        all_pass = test1_pass and test2_pass
        
        print("\n" + "="*70)
        if all_pass:
            print("✓✓✓ NO LOCAL TEST PASSED ✓✓✓")
            print("No Local subscription option working correctly!")
        else:
            print("❌❌❌ NO LOCAL TEST FAILED ❌❌❌")
        print("="*70 + "\n")
        
        return all_pass
        
    except Exception as e:
        print(f"\n❌ TEST ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Cleanup
        print("\nCleaning up...")
        client1.loop_stop()
        client2.loop_stop()
        client1.disconnect()
        client2.disconnect()
        time.sleep(1)

if __name__ == "__main__":
    success = run_test()
    sys.exit(0 if success else 1)
