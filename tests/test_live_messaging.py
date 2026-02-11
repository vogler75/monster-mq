#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import time
import threading
import pytest

@pytest.fixture
def broker_config():
    """Broker connection configuration"""
    return {"host": "localhost", "port": 1883, "username": "admin", "password": "public"}

def test_live_messaging(broker_config):
    """Test live MQTT messaging (publish after subscribe)"""
    received = []
    ready = threading.Event()
    
    def on_connect(client, userdata, flags, rc, props=None):
        print(f"Connected: {rc}")
        assert rc == 0, f"Connection failed with rc={rc}"
        client.subscribe('test/live')
        ready.set()
    
    def on_message(client, userdata, message):
        received.append(message)
        print(f'Received: {message.payload}')
    
    # Start subscriber first
    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, 'sub', protocol=mqtt.MQTTv5)
    sub.username_pw_set(broker_config["username"], broker_config["password"])
    sub.on_connect = on_connect
    sub.on_message = on_message
    sub.connect(broker_config["host"], broker_config["port"])
    sub.loop_start()
    
    # Wait for subscription
    assert ready.wait(timeout=3), "Subscription not ready"
    time.sleep(1)
    
    # Publish
    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, 'pub', protocol=mqtt.MQTTv5)
    pub.username_pw_set(broker_config["username"], broker_config["password"])
    pub.connect(broker_config["host"], broker_config["port"])
    result = pub.publish('test/live', b'live message', qos=1, retain=False)
    assert result.rc == 0, f"Publish failed with rc={result.rc}"
    time.sleep(2)
    pub.disconnect()
    
    sub.loop_stop()
    sub.disconnect()
    
    # Assertions
    assert len(received) > 0, "FAIL: No messages received!"
    print(f'✓ SUCCESS: Live messaging works - received {len(received)} message(s)')

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
