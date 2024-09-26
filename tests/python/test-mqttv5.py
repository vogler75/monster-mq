import paho.mqtt.client as mqtt

# Define the MQTT broker details
broker = 'localhost'
port = 1883
topic = "hello/v5"
client_id = "mqtt_v5_client"


# Callback function on connection
def on_connect(client, userdata, flags, reasonCode, properties):
    print(f"Connected with result code {reasonCode}")
    # Subscribing to a topic after connection
    client.subscribe(topic, qos=1)


# Callback function on message received
def on_message(client, userdata, msg):
    print(f"Received message: {msg.topic} -> {msg.payload.decode('utf-8')}")


# Callback function on subscribe
def on_subscribe(client, userdata, mid, granted_qos, properties):
    print(f"Subscribed to topic with QoS {granted_qos[0]}")


# Callback function on publish
def on_publish(client, userdata, mid):
    print(f"Message published with mid: {mid}")


# Create an MQTT client instance with protocol version 5
client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv5)

# Set the username and password if required
# client.username_pw_set(username="your_username", password="your_password")

# Attach the callback functions
client.on_connect = on_connect
client.on_message = on_message
client.on_subscribe = on_subscribe
client.on_publish = on_publish

# Connect to the MQTT broker
client.connect(broker, port, keepalive=60)

# Publish a message to the topic
client.publish(topic, payload="Hello MQTT v5", qos=1)

# Start the loop to process network traffic and dispatch callbacks
client.loop_forever()
