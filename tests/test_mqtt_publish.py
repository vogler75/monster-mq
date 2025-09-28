#!/usr/bin/env python3
"""
MQTT publish test to create OPC UA node
"""

import paho.mqtt.client as mqtt
import time

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")

def on_publish(client, userdata, mid):
    print(f"Message published with mid: {mid}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_publish = on_publish

try:
    print("Connecting to MQTT broker...")
    client.connect("localhost", 1883, 60)
    client.loop_start()
    time.sleep(1)  # Wait for connection

    print("Publishing message to write/oee topic...")
    result = client.publish("write/oee", "100")

    print(f"Publish result: {result.rc}")
    time.sleep(2)  # Wait for message to be processed

    client.loop_stop()
    client.disconnect()
    print("Disconnected from MQTT broker")

except Exception as e:
    print(f"Error: {e}")