import paho.mqtt.client as mqtt
import time
from datetime import datetime

# Define the MQTT broker details
broker_address = "192.168.1.30"  # Example broker address, you can change it to your broker's address
topic = "test/broadcast"  # Example topic


# Function to publish a message
def publish_messages():
    while True:
        message = datetime.now().isoformat()
        client.publish(topic, message, qos=1).wait_for_publish()
        # Print the current time in ISO format
        print("Message Published at:", message)
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
