#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import time

received = []

def on_message(c, u, m):
    received.append(m)
    print(f'Received: {m.payload}, retain={m.retain}')

# Subscriber
sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, 'testsub', protocol=mqtt.MQTTv5)
sub.on_message = on_message
sub.connect('localhost', 1883)
sub.subscribe('test/simple', qos=1)
sub.loop_start()
time.sleep(2)

# Publish
pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, 'testpub', protocol=mqtt.MQTTv5)
pub.connect('localhost', 1883)
result = pub.publish('test/simple', b'hello', qos=1, retain=False)
print(f'Published: {result.rc}')
time.sleep(3)
pub.disconnect()

sub.loop_stop()
sub.disconnect()

print(f'Total received: {len(received)}')
if received:
    print('SUCCESS')
else:
    print('FAILED')
