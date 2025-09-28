#!/usr/bin/env python3

import paho.mqtt.client as mqtt
import time

def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker with result code {rc}")

def on_publish(client, userdata, mid):
    print(f"Message {mid} published successfully")

# Create MQTT client
client = mqtt.Client()
client.on_connect = on_connect
client.on_publish = on_publish

# Connect to broker
client.connect("localhost", 1883, 60)
client.loop_start()

# Wait a moment for connection
time.sleep(1)

# Publish test messages
print("Publishing test messages...")
client.publish("test/a", "value_a")
client.publish("test/b", "value_b")

# Wait for publishing to complete
time.sleep(2)
client.loop_stop()
client.disconnect()
print("Test messages published successfully")