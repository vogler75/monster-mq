import paho.mqtt.client as mqtt
from datetime import datetime

# MQTT settings
BROKER = 'linux0'
PORT = 1883
TOPIC = 'test/broadcast'
CLIENT_ID = "subscriber_110"


# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully")
        #client.subscribe(TOPIC)
    else:
        print(f"Connection failed with code {rc}")


# Callback when a message is received from the broker
def on_message(client, userdata, msg):
    print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
    # the content of the message is a timestamp in iso, take this and calculate the time difference between now and the time the message was sent
    sent_time = datetime.fromisoformat(msg.payload.decode())
    time_diff = datetime.now() - sent_time
    print(f"Time difference: {time_diff.total_seconds()} seconds")


# Create an MQTT client instance
client = mqtt.Client(client_id=CLIENT_ID, clean_session=False)

# Assign the callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to the broker
client.connect(BROKER, PORT, 60)

# Start the network loop
client.loop_forever()