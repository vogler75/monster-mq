#!/usr/bin/env python3
"""
MQTT bulk publish test - publishes retained messages to 1000 topics
Topics: test/1 to test/1000
Messages: "Hello world 1" to "Hello world 1000"
"""

import paho.mqtt.client as mqtt
import time
import sys
import os
import pytest

# Configuration from environment variables with defaults
BROKER_HOST = os.getenv("MQTT_BROKER", "localhost")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
BROKER_USERNAME = os.getenv("MQTT_USERNAME")
BROKER_PASSWORD = os.getenv("MQTT_PASSWORD")
NUM_TOPICS = 1000
TOPIC_PREFIX = "test"

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")

def on_publish(client, userdata, mid):
    # Only print progress every 100 messages to avoid spam
    if mid % 100 == 0:
        print(f"Message {mid} published")

client = mqtt.Client(client_id="bulk_publisher")
if BROKER_USERNAME:
    client.username_pw_set(BROKER_USERNAME, BROKER_PASSWORD or "")
client.on_connect = on_connect
client.on_publish = on_publish

try:
    print(f"Connecting to MQTT broker at {BROKER_HOST}:{BROKER_PORT}...")
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_start()
    time.sleep(1)  # Wait for connection

    print(f"Publishing {NUM_TOPICS} retained messages...")
    start_time = time.time()

    for i in range(1, NUM_TOPICS + 1):
        topic = f"{TOPIC_PREFIX}/{i}"
        message = f"Hello world {i}"

        # Publish with retain flag set to True
        result = client.publish(topic, message, qos=1, retain=True)

        if i % 100 == 0:
            elapsed = time.time() - start_time
            rate = i / elapsed
            print(f"Published {i}/{NUM_TOPICS} messages ({rate:.1f} msg/sec)")

    elapsed = time.time() - start_time
    print(f"All {NUM_TOPICS} messages published in {elapsed:.2f} seconds")

    # Wait a bit to ensure all messages are processed
    time.sleep(2)

    client.loop_stop()
    client.disconnect()
    print("Disconnected from MQTT broker")

except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)
