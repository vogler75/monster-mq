import paho.mqtt.client as mqtt
import json
from datetime import datetime
import time

# MQTT settings
BROKER = 'linux0'
PORT = 1883
TOPIC = 'test/broadcast'
CLIENT_ID = "python-simple-subscriber"
CLEAN_SESSION = False
QOS = 1

first_message = None
last_message = None
last_id = None


# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully "+str(flags))
        client.publish("test/status", payload="I'm alive", qos=1, retain=True)
        # Subscribe to the topic if 'session present' flag is 0
        if not flags['session present']:
            print("Subscribing to topic", TOPIC)
            client.subscribe(TOPIC, qos=QOS)
    else:
        print(f"Connection failed with code {rc}")


# Callback when a message is received from the broker
def on_message(client, userdata, msg):
    global last_id, last_message, first_message
    # print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
    try:
        received_time = datetime.now()
        payload = json.loads(msg.payload.decode())
        sent_time = datetime.fromisoformat(payload['ts'])
        message_id = payload['id']
        time_diff = received_time - sent_time

        if first_message is None:
            print(f"F [{msg.mid}] [{msg.qos}] [{msg.retain}] `{msg.payload.decode()}` with id {message_id} from `{msg.topic}`")
            first_message = True

        if last_id is not None and last_id != message_id - 1:
            print(f"X [{msg.mid}] [{msg.qos}] [{msg.retain}] `{msg.payload.decode()}` with id {message_id} from `{msg.topic}` => {last_message}")

        print(f"Received [{msg.mid}] [{msg.qos}] [{msg.retain}] `{msg.payload.decode()}` with id {message_id} from `{msg.topic}`. Time difference: {sent_time} {received_time} {time_diff.total_seconds()} seconds")
        last_id = message_id
        last_message = msg.payload.decode()

        publish = {
            "id": message_id,
            "ts": received_time.isoformat(),
            "sent": sent_time.isoformat(),
            "time_diff": time_diff.total_seconds()
        }
        client.publish("test/received", json.dumps(publish), qos=0)

    except Exception as e:
        print(e)
        #print(f"Received [{msg.mid}] [{msg.retain}] `{msg.payload.decode()}` from `{msg.topic}`. Time difference: N/A")
        #exit(-1)


while True:
    first_message = None
    last_message = None

    # Create an MQTT client instance
    client = mqtt.Client(client_id=CLIENT_ID, clean_session=CLEAN_SESSION)

    # Assign the callback functions
    client.on_connect = on_connect
    client.on_message = on_message

    # add a last will message
    client.will_set("test/status", payload="I'm dead", qos=1, retain=True)

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

    print(f"L [{last_message}]`")

    input("Press Enter to start...\n")