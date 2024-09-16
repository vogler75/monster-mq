import paho.mqtt.client as mqtt
import json
from datetime import datetime

# MQTT settings
BROKER = 'scada'
PORT = 1884
TOPIC = 'test/broadcast'
CLIENT_ID = "python-simple-subscriber"


# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully "+str(flags))
        # Subscribe to the topic if 'session present' flag is 0
        if not flags['session present']:
            print("Subscribing to topic", TOPIC)
            client.subscribe(TOPIC, qos=2)
    else:
        print(f"Connection failed with code {rc}")


# Callback when a message is received from the broker
def on_message(client, userdata, msg):
    # print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
    # the content of the message is a timestamp in iso, take this and calculate the time difference between now and the time the message was sent
    try:
        received_time = datetime.now()
        payload = json.loads(msg.payload.decode())
        sent_time = datetime.fromisoformat(payload['ts'])
        message_id = payload['id']
        time_diff = received_time - sent_time
        publish = {
            "id": message_id,
            "ts": received_time.isoformat(),
            "sent": sent_time.isoformat(),
            "time_diff": time_diff.total_seconds()
        }
        print(f"Received [{msg.mid}] [{msg.retain}] `{msg.payload.decode()}` with id {message_id} from `{msg.topic}`. Time difference: {time_diff.total_seconds()} seconds")
        client.publish("test/received", json.dumps(publish), qos=0)
    except Exception as e:
        print(f"Received [{msg.mid}] [{msg.retain}] `{msg.payload.decode()}` from `{msg.topic}`. Time difference: N/A")
        #exit(-1)


# Create an MQTT client instance
client = mqtt.Client(client_id=CLIENT_ID, clean_session=True)

# Assign the callback functions
client.on_connect = on_connect
client.on_message = on_message

# add a last will message
client.will_set("test/lastwill", payload="I'm dead", qos=1, retain=True)

# Connect to the broker
client.connect(BROKER, PORT, 60)

# Start the network loop
client.loop_forever()