import paho.mqtt.client as mqtt
import json
from datetime import datetime

# MQTT settings
BROKER = 'localhost'
PORT = 1883
TOPIC = 'test/broadcast'
CLIENT_ID = "test"


# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully")
        client.subscribe(TOPIC, qos=2)
    else:
        print(f"Connection failed with code {rc}")


# Callback when a message is received from the broker
def on_message(client, userdata, msg):
    # print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
    # the content of the message is a timestamp in iso, take this and calculate the time difference between now and the time the message was sent
    try:
        payload = json.loads(msg.payload.decode())
        sent_time = datetime.fromisoformat(payload['ts'])
        message_id = payload['id']
        time_diff = datetime.now() - sent_time
        print(f"Received [{msg.mid}] [{msg.retain}] `{msg.payload.decode()}` with id {message_id} from `{msg.topic}`. Time difference: {time_diff.total_seconds()} seconds")
    except Exception as e:
        print(f"Received [{msg.mid}] [{msg.retain}] `{msg.payload.decode()}` from `{msg.topic}`. Time difference: N/A")
        #exit(-1)


# Create an MQTT client instance
client = mqtt.Client(client_id=CLIENT_ID, clean_session=False)

# Assign the callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to the broker
client.connect(BROKER, PORT, 60)

# Start the network loop
client.loop_forever()