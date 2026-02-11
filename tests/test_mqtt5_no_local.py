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
import pytest

pytestmark = pytest.mark.mqtt5

# Configuration
TEST_TOPIC = "test/nolocal/messages"


def test_no_local_subscription_option(broker_config):
    """
    Test that noLocal subscription option prevents receiving own messages.
    
    Tests that:
    1. Client with noLocal=True doesn't receive its own published messages
    2. Other clients receive messages normally
    """
    # Test state
    publisher_messages = []
    client1_received = []
    client2_received = []
    client1_connected = False
    client2_connected = False
    
    def on_connect_client1(client, userdata, flags, rc, properties=None):
        nonlocal client1_connected
        print(f"[Client1-Publisher] Connected rc={rc}")
        if rc == 0:
            client1_connected = True
            # Subscribe with noLocal=True
            options = mqtt.SubscribeOptions(qos=1, noLocal=True)
            client.subscribe(TEST_TOPIC, options=options)
            print(f"[Client1-Publisher] Subscribed to {TEST_TOPIC} with noLocal=True")
    
    def on_connect_client2(client, userdata, flags, rc, properties=None):
        nonlocal client2_connected
        print(f"[Client2-Subscriber] Connected rc={rc}")
        if rc == 0:
            client2_connected = True
            # Normal subscription (noLocal=False)
            client.subscribe(TEST_TOPIC, qos=1)
            print(f"[Client2-Subscriber] Subscribed to {TEST_TOPIC}")
    
    def on_message_client1(client, userdata, msg):
        """Client1 should NOT receive its own messages (noLocal=True)"""
        payload = msg.payload.decode()
        print(f"[Client1-Publisher] ❌ RECEIVED (should NOT happen): {payload}")
        client1_received.append(payload)
    
    def on_message_client2(client, userdata, msg):
        """Client2 should receive all messages (normal subscription)"""
        payload = msg.payload.decode()
        print(f"[Client2-Subscriber] \u2713 Received: {payload}")
        client2_received.append(payload)
    
    # Create clients
    client1 = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="test-nolocal-publisher",
        protocol=mqtt.MQTTv5
    )
    client1.username_pw_set(broker_config["username"], broker_config["password"])
    client1.on_connect = on_connect_client1
    client1.on_message = on_message_client1
    
    client2 = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="test-nolocal-subscriber",
        protocol=mqtt.MQTTv5
    )
    client2.username_pw_set(broker_config["username"], broker_config["password"])
    client2.on_connect = on_connect_client2
    client2.on_message = on_message_client2
    
    try:
        # Connect both clients
        client1.connect(broker_config["host"], broker_config["port"], 60)
        client2.connect(broker_config["host"], broker_config["port"], 60)
        
        client1.loop_start()
        client2.loop_start()
        
        # Wait for connections
        timeout = time.time() + 5
        while (not client1_connected or not client2_connected) and time.time() < timeout:
            time.sleep(0.1)
        
        assert client1_connected and client2_connected, "Clients did not connect"
        time.sleep(1)
        
        # Publish messages from client1
        test_messages = [
            "Message 1 from publisher",
            "Message 2 from publisher",
            "Message 3 from publisher"
        ]
        
        for msg in test_messages:
            result = client1.publish(TEST_TOPIC, msg, qos=1)
            result.wait_for_publish()
            publisher_messages.append(msg)
            time.sleep(0.2)
        
        # Wait for message delivery
        time.sleep(2)
        
        # Verify: Client1 should NOT receive its own messages
        assert len(client1_received) == 0, \
            f"Client1 received {len(client1_received)} messages (should be 0 with noLocal=True)"
        
        # Verify: Client2 should receive all messages
        assert len(client2_received) == len(test_messages), \
            f"Client2 received {len(client2_received)}/{len(test_messages)} messages"
        
    finally:
        client1.loop_stop()
        client2.loop_stop()
        client1.disconnect()
        client2.disconnect()
        time.sleep(0.5)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
