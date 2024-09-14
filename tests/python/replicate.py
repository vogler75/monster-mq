import time

import paho.mqtt.client as mqtt
import json

# Source MQTT broker details
SOURCE_BROKER = 'scada'
SOURCE_PORT = 1883
SOURCE_TOPIC = '#'

# Destination MQTT broker details
DEST_BROKER = 'localhost'
DEST_PORT = 1883


# Callback when the client connects to the source broker
def on_connect_source(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to source broker successfully")
        client.subscribe(SOURCE_TOPIC)
    else:
        print(f"Connection to source broker failed with code {rc}")


# Callback when a message is received from the source broker
def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        print(f"Replicating message from topic `{msg.topic}`: {payload}")
        dest_client.publish(msg.topic, payload, qos=msg.qos, retain=msg.retain)
    except Exception as e:
        print(f"Failed to replicate message: {e}")


# Create a client instance for the source broker
source_client = mqtt.Client(client_id="replicator", clean_session=True)
source_client.on_connect = on_connect_source
source_client.on_message = on_message

# Create a client instance for the destination broker
dest_client = mqtt.Client()

# Connect to the source broker
source_client.connect(SOURCE_BROKER, SOURCE_PORT, 60)

# Connect to the destination broker
dest_client.connect(DEST_BROKER, DEST_PORT, 60)

# Start the network loop for both clients
source_client.loop_start()
dest_client.loop_start()

# Keep the script running
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Exiting...")
    source_client.loop_stop()
    dest_client.loop_stop()
    source_client.disconnect()
    dest_client.disconnect()