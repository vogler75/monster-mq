import paho.mqtt.client as mqtt
import time
import json
from datetime import datetime

# Define the MQTT broker details
broker_address = "linux1"  # Example broker address, you can change it to your broker's address
topic = "test/broadcast"  # Example topic

message_id = 0

# Function to publish a message
def publish_messages():
    global message_id
    while True:
        ts = datetime.now().isoformat()
        message_id = message_id + 1
        payload = { "id": message_id, "ts": ts }
        client.publish(topic, json.dumps(payload), qos=1).wait_for_publish()
        print("Message Published:", payload)
        time.sleep(1)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully")
    else:
        print(f"Connection failed with code {rc}")


# Create a client instance
client = mqtt.Client()
client.on_connect = on_connect


# Function to connect to the broker
def connect_mqtt():
    try:
        client.connect(broker_address)
    except Exception as e:
        print(f"Failed to connect to MQTT broker: {e}")
        exit(1)


if __name__ == "__main__":
    # Connect to the MQTT broker
    connect_mqtt()
    client.loop_start()
    while not client.is_connected():
        time.sleep(1)
    publish_messages()
