#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import time

# Publish retained message
pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, 'tpub', protocol=mqtt.MQTTv5)
pub.connect('localhost', 1883)
pub.publish('test/retained', b'data', qos=1, retain=True)
time.sleep(2)
pub.disconnect()
print('Published retained message')

# Subscribe and receive
msgs = []
def on_message(c, u, m):
    msgs.append(m)
    print(f'Received: retain={m.retain}')

sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, 'tsub', protocol=mqtt.MQTTv5)
sub.on_message = on_message
sub.connect('localhost', 1883)
sub.subscribe('test/retained')
time.sleep(2)
sub.disconnect()

print(f'Total: {len(msgs)} messages')
if msgs:
    print(f'Retain flag: {msgs[0].retain}')
else:
    print('NO MESSAGES RECEIVED!')
