import paho.mqtt.client as mqtt
import json
from datetime import datetime
import time

# MQTT settings
BROKER = 'localhost'
PORT = 1883
TOPIC = 'test/broadcast'
CLIENT_ID = "python-test-subscriber"


# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully "+str(flags))
        # Subscribe to the topic if 'session present' flag is 0
        if not flags['session present']:
            print("Subscribing to topic", TOPIC)
            client.subscribe(TOPIC, qos=0)
    else:
        print(f"Connection failed with code {rc}")


# Callback when a message is received from the broker
last_message = None

def on_message(client, userdata, msg):
    global last_message
    message = f"[{datetime.now().isoformat()}] [{msg.mid}] [{msg.qos}] [{msg.topic}] [{msg.payload.decode()}]"
    if last_message is None:
        print("START", message)
    last_message = message


while True:
    last_message = None
    # Create an MQTT client instance
    client = mqtt.Client(client_id=CLIENT_ID, clean_session=False)

    # Assign the callback functions
    client.on_connect = on_connect
    client.on_message = on_message

    # Connect to the broker
    client.connect(BROKER, PORT, 60)

    # Start the network loop
    client.loop_start()

    while True:
        try:
            input("Press Enter to exit...\n")
            break
        except Exception as e:
            break
    client.disconnect()
    client.loop_stop()
    time.sleep(1)
    print("STOP", last_message)
    input("Press Enter to start...\n")