#!/usr/bin/env python3
"""
MQTT publish test to create OPC UA node
"""

import paho.mqtt.client as mqtt
import time
import pytest

def test_mqtt_publish_to_opcua(broker_config):
    """Test MQTT publish to write/oee topic (creates OPC UA node)"""
    connected = [False]
    published = [False]
    
    def on_connect(client, userdata, flags, rc, properties=None):
        print(f"Connected with result code {rc}")
        connected[0] = (rc == 0)
    
    def on_publish(client, userdata, mid, reason_code=None, properties=None):
        print(f"Message published with mid: {mid}")
        published[0] = True
    
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                         protocol=mqtt.MQTTv311)
    client.username_pw_set(broker_config["username"], broker_config["password"])
    client.on_connect = on_connect
    client.on_publish = on_publish
    
    print(f"Connecting to MQTT broker at {broker_config['host']}:{broker_config['port']}...")
    client.connect(broker_config["host"], broker_config["port"], 60)
    client.loop_start()

    # Poll-wait for CONNACK (up to 5 seconds)
    deadline = time.time() + 5
    while not connected[0] and time.time() < deadline:
        time.sleep(0.1)
    assert connected[0], "Failed to connect to broker"
    
    print("Publishing message to write/oee topic...")
    result = client.publish("write/oee", "100")
    
    assert result.rc == 0, f"Publish failed with rc={result.rc}"
    print(f"Publish result: {result.rc}")
    time.sleep(2)  # Wait for message to be processed
    
    client.loop_stop()
    client.disconnect()
    
    assert published[0], "Publish callback was not triggered"
    print("✓ Successfully published to write/oee and disconnected")

if __name__ == '__main__':
    pytest.main([__file__, '-v'])