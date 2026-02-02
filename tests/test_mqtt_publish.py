#!/usr/bin/env python3
"""
MQTT publish test to create OPC UA node
"""

import paho.mqtt.client as mqtt
import time
import os
import pytest

@pytest.fixture
def broker_config():
    """Broker connection configuration from environment or defaults"""
    return {
        "host": os.getenv("MQTT_BROKER", "localhost"),
        "port": int(os.getenv("MQTT_PORT", "1883")),
        "username": os.getenv("MQTT_USERNAME", "admin"),
        "password": os.getenv("MQTT_PASSWORD", "public")
    }

def test_mqtt_publish_to_opcua(broker_config):
    """Test MQTT publish to write/oee topic (creates OPC UA node)"""
    connected = [False]
    published = [False]
    
    def on_connect(client, userdata, flags, rc, properties=None):
        print(f"Connected with result code {rc}")
        connected[0] = (rc == 0)
    
    def on_publish(client, userdata, mid):
        print(f"Message published with mid: {mid}")
        published[0] = True
    
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                         protocol=mqtt.MQTTv5)
    client.username_pw_set(broker_config["username"], broker_config["password"])
    client.on_connect = on_connect
    client.on_publish = on_publish
    
    print(f"Connecting to MQTT broker at {broker_config['host']}:{broker_config['port']}...")
    client.connect(broker_config["host"], broker_config["port"], 60)
    client.loop_start()
    time.sleep(1)  # Wait for connection
    
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