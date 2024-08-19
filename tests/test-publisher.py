import uuid

import paho.mqtt.client as mqtt
import time
import datetime
import config
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--host', type=str, default=config.BROKER, help='Host')
parser.add_argument('--port', type=int, default=config.PORT, help='Port number (integer)')
parser.add_argument('--nr', type=int, default=1, help='A number (integer)')
args = parser.parse_args()

print(f"Port: {args.port} Nr: {args.nr}")

# MQTT settings
TOPIC = "test/"+str(args.nr)
CLIENT_ID = 'mqtt_client_3_'+str(uuid.uuid4())
QOS = 0

start_time = time.time()

g_connected = False


# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    global g_connected
    if rc == 0:
        print("Connected successfully")
        g_connected = True
    else:
        print(f"Connection failed with code {rc}")


# Callback when a message is received from the broker
def on_message(client, userdata, msg):
    print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")


# Create an MQTT client instance
client = mqtt.Client(CLIENT_ID, clean_session=True)

# Assign the callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to the broker
client.connect(args.host, args.port, 60)

# Start the network loop
client.loop_start()

while not g_connected:
    time.sleep(1)

print("Start..")
message_counter = 0
last_time = datetime.datetime.now()
last_counter = 0
topic_nr1 = 0
topic_nr2 = 0
topic_nr3 = 0
while message_counter < 1_000_000_000:
    topic_nr3 = topic_nr3 + 1
    if topic_nr3 == 100:
        topic_nr3 = 0
        topic_nr2 = topic_nr2 + 1
        if topic_nr2 == 100:
            topic_nr2 = 0
            topic_nr1 = topic_nr1 + 1
            if topic_nr1 == 100:
                topic_nr1 = 0
    topic = f"{TOPIC}/{topic_nr1}/{topic_nr2}/{topic_nr3}"  # str(uuid.uuid4())
    #print(topic)
    message_counter = message_counter + 1
    last_counter = last_counter + 1
    retain = False
    #retain = True if message_counter % 100 == 0 else False
    #retain = True if topic_nr3 == 99 else False
    client.publish(topic, str(message_counter), qos=QOS, retain=retain).wait_for_publish()
    if message_counter % 100 == 0:
        current_time = datetime.datetime.now()
        diff = (current_time - last_time).total_seconds()
        if diff >= 1:
            throughput = float(last_counter) / diff
            print(f"Messages {message_counter} / {last_counter} / {throughput} / {diff}")
            last_counter = 0
            last_time = current_time

        time.sleep(0.5)

print("Done.")
time.sleep(1)
print("Disconnect...")
client.disconnect()
time.sleep(1)
print("Ended.")

