"""Simple publish/subscribe test to verify broker is working"""
import paho.mqtt.client as mqtt
import time

received = []

def on_message(client, userdata, msg):
    print(f"✓ Received: {msg.payload.decode()}")
    received.append(msg.payload.decode())

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"Connected rc={rc}")
    client.subscribe("test/simple", qos=1)
    print("Subscribed to test/simple")

client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                     client_id="test-simple-client",
                     protocol=mqtt.MQTTv5)
client.on_connect = on_connect
client.on_message = on_message

client.connect("localhost", 1883, 60)
client.loop_start()

time.sleep(2)  # Wait for connection

print("\nPublishing 3 messages...")
for i in range(3):
    msg = f"Test message {i+1}"
    client.publish("test/simple", msg, qos=1)
    print(f"  Published: {msg}")
    time.sleep(0.2)

time.sleep(3)  # Wait for messages

print(f"\nReceived {len(received)}/3 messages")
if len(received) == 3:
    print("✓✓✓ SIMPLE TEST PASSED ✓✓✓")
else:
    print("❌❌❌ SIMPLE TEST FAILED ❌❌❌")

client.loop_stop()
client.disconnect()
