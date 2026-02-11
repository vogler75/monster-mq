#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import time
import pytest

@pytest.fixture
def broker_config():
    """Broker connection configuration"""
    return {"host": "localhost", "port": 1883, "username": "admin", "password": "public"}

def test_retained_message(broker_config):
    """Test MQTT retained message functionality"""
    # Publish retained message
    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, 'tpub', protocol=mqtt.MQTTv5)
    pub.username_pw_set(broker_config["username"], broker_config["password"])
    pub.connect(broker_config["host"], broker_config["port"])
    result = pub.publish('test/retained', b'data', qos=1, retain=True)
    result.wait_for_publish()
    time.sleep(2)
    pub.disconnect()
    
    # Subscribe and receive
    msgs = []
    def on_message(client, userdata, message):
        msgs.append(message)
        print(f'Received: retain={message.retain}')
    
    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, 'tsub', protocol=mqtt.MQTTv5)
    sub.username_pw_set(broker_config["username"], broker_config["password"])
    sub.on_message = on_message
    sub.connect(broker_config["host"], broker_config["port"])
    sub.subscribe('test/retained')
    time.sleep(2)
    sub.disconnect()
    
    # Assertions
    assert len(msgs) > 0, "NO MESSAGES RECEIVED!"
    assert msgs[0].retain == True, f"Expected retain=True, got {msgs[0].retain}"
    print(f'✓ Received {len(msgs)} message(s) with retain flag: {msgs[0].retain}')

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
