#!/usr/bin/env python3

import paho.mqtt.client as mqtt
import time
import os

# Configuration from environment variables with defaults
BROKER_HOST = os.getenv("MQTT_BROKER", "localhost")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
BROKER_USERNAME = os.getenv("MQTT_USERNAME")
BROKER_PASSWORD = os.getenv("MQTT_PASSWORD")

def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker with result code {rc}")

def on_publish(client, userdata, mid):
    print(f"Message {mid} published successfully")

# Create MQTT client
client = mqtt.Client()
if BROKER_USERNAME:
    client.username_pw_set(BROKER_USERNAME, BROKER_PASSWORD or "")
client.on_connect = on_connect
client.on_publish = on_publish

# Connect to broker
client.connect(BROKER_HOST, BROKER_PORT, 60)
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