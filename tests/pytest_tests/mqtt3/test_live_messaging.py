#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import time
import threading
import uuid
import pytest

def test_live_messaging(broker_config):
    """Test live MQTT messaging (publish after subscribe)"""
    received = []
    sub_connected = threading.Event()
    sub_subscribed = threading.Event()

    def on_connect(client, userdata, flags, rc, props=None):
        print(f"Connected: {rc}")
        assert rc == 0, f"Connection failed with rc={rc}"
        sub_connected.set()
        client.subscribe('test/live')

    def on_subscribe(client, userdata, mid, reason_code_list, props=None):
        sub_subscribed.set()

    def on_message(client, userdata, message):
        received.append(message)
        print(f'Received: {message.payload}')

    # Start subscriber first
    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f'sub_{uuid.uuid4().hex[:8]}', protocol=mqtt.MQTTv311)
    sub.username_pw_set(broker_config["username"], broker_config["password"])
    sub.on_connect = on_connect
    sub.on_subscribe = on_subscribe
    sub.on_message = on_message
    sub.connect(broker_config["host"], broker_config["port"])
    sub.loop_start()

    # Wait for subscription to be confirmed (SUBACK)
    assert sub_subscribed.wait(timeout=5), "Subscription not ready"
    time.sleep(0.5)

    # Publisher — wait for CONNACK before publishing
    pub_connected = threading.Event()

    def pub_on_connect(client, userdata, flags, rc, props=None):
        if rc == 0:
            pub_connected.set()

    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f'pub_{uuid.uuid4().hex[:8]}', protocol=mqtt.MQTTv311)
    pub.username_pw_set(broker_config["username"], broker_config["password"])
    pub.on_connect = pub_on_connect
    pub.connect(broker_config["host"], broker_config["port"])
    pub.loop_start()
    assert pub_connected.wait(timeout=5), "Publisher failed to connect"

    result = pub.publish('test/live', b'live message', qos=1, retain=False)
    assert result.rc == 0, f"Publish failed with rc={result.rc}"
    time.sleep(2)
    pub.loop_stop()
    pub.disconnect()

    sub.loop_stop()
    sub.disconnect()

    # Assertions
    assert len(received) > 0, "FAIL: No messages received!"
    print(f'✓ SUCCESS: Live messaging works - received {len(received)} message(s)')

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
