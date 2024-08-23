import paho.mqtt.client as mqtt
import datetime
import config
import uuid
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--host', type=str, default=config.BROKER, help='Host')
parser.add_argument('--port', type=int, default=config.PORT, help='Port number (integer)')
parser.add_argument('--nr', type=str, default="1", help='A number (integer)')
args = parser.parse_args()

# MQTT settings
TOPIC = "test/"+str(args.nr)+"/#"
CLIENT_ID = 'subscriber_'+str(uuid.uuid4())


# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully")
        client.subscribe(TOPIC)
    else:
        print(f"Connection failed with code {rc}")


# Callback when a message is received from the broker
message_counter = 0
last_message = 0
last_time = datetime.datetime.now()
last_counter = 0


def on_message(client, userdata, msg):
    #print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
    global message_counter, last_message, last_time, last_counter
    this_message = msg.payload.decode()
    #if int(last_message)+1 != int(this_message):
    #    print(f"Got wrong number {last_message} vs {this_message}")
    message_counter = message_counter + 1
    last_counter = last_counter + 1
    last_message = this_message
    if message_counter % 100 == 0:
        current_time = datetime.datetime.now()
        diff = (current_time - last_time).total_seconds()
        if diff >= 1:
            throughput = float(last_counter) / diff
            print(f"Messages {message_counter} / {last_counter} / {throughput} / {diff}")
            last_counter = 0
            last_time = current_time


# Create an MQTT client instance
client = mqtt.Client(CLIENT_ID, clean_session=True)

# Assign the callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to the broker
client.connect(args.host, args.port, 60)

# Start the network loop
client.loop_forever()
