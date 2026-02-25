#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import threading
import time
import uuid
import pytest

@pytest.mark.xfail(
    strict=True,
    reason="MonsterMQ broker does not set retain=1 on retained messages delivered at subscription time (MQTT 3.1.1 §3.3.1.3 violation)"
)
def test_retained_message(broker_config):
    """Test MQTT retained message functionality"""

    # Publish retained message
    pub_connack = threading.Event()
    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f'tpub_{uuid.uuid4().hex[:8]}', protocol=mqtt.MQTTv311)
    pub.username_pw_set(broker_config["username"], broker_config["password"])

    def pub_on_connect(client, userdata, flags, rc, properties=None):
        print(f"[PUB] on_connect: rc={rc}")
        if rc == 0:
            pub_connack.set()

    def pub_on_disconnect(client, userdata, disconnect_flags, rc, properties=None):
        print(f"[PUB] on_disconnect: rc={rc}")

    pub.on_connect = pub_on_connect
    pub.on_disconnect = pub_on_disconnect
    pub.connect(broker_config["host"], broker_config["port"])
    pub.loop_start()
    assert pub_connack.wait(timeout=5), "Publisher failed to connect"
    print("[PUB] Connected")

    result = pub.publish('test/retained', b'data', qos=1, retain=True)
    result.wait_for_publish()
    print("[PUB] Retained message published and acknowledged")
    time.sleep(0.5)  # Let broker fully store the retained message before disconnecting
    pub.loop_stop()
    pub.disconnect()

    # Subscribe and receive retained message
    msgs = []
    sub_connack = threading.Event()
    sub_suback = threading.Event()

    def on_message(client, userdata, message):
        print(f'[SUB] on_message: topic={message.topic} retain={message.retain}')
        msgs.append(message)

    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f'tsub_{uuid.uuid4().hex[:8]}', protocol=mqtt.MQTTv311)
    sub.username_pw_set(broker_config["username"], broker_config["password"])
    sub.on_message = on_message

    def sub_on_connect(client, userdata, flags, rc, properties=None):
        print(f"[SUB] on_connect: rc={rc}")
        if rc == 0:
            sub_connack.set()

    def sub_on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        print(f"[SUB] on_subscribe: mid={mid} reason_codes={reason_code_list}")
        sub_suback.set()

    def sub_on_disconnect(client, userdata, disconnect_flags, rc, properties=None):
        print(f"[SUB] on_disconnect: rc={rc}")

    sub.on_connect = sub_on_connect
    sub.on_subscribe = sub_on_subscribe
    sub.on_disconnect = sub_on_disconnect
    sub.connect(broker_config["host"], broker_config["port"])
    sub.loop_start()
    assert sub_connack.wait(timeout=5), "Subscriber failed to connect"
    print("[SUB] Connected")

    sub.subscribe('test/retained')
    assert sub_suback.wait(timeout=5), "Subscriber failed to subscribe"
    print("[SUB] Subscribed, waiting for retained message...")
    time.sleep(0.5)  # Allow broker time to deliver retained message after SUBACK

    sub.loop_stop()
    sub.disconnect()

    # Assertions
    assert len(msgs) > 0, "NO MESSAGES RECEIVED!"
    assert msgs[0].retain == True, f"Expected retain=True, got {msgs[0].retain}"
    print(f'✓ Received {len(msgs)} message(s) with retain flag: {msgs[0].retain}')

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
