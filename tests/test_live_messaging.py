#!/usr/bin/env python3
import paho.mqtt.client as mqtt
import time
import threading
import pytest

received = []
ready = threading.Event()

def on_connect(c, u, f, rc, props=None):
    print(f"Connected: {rc}")
    c.subscribe('test/live')
    ready.set()

def on_message(c, u, m):
    received.append(m)
    print(f'Received: {m.payload}')

# Start subscriber first
sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, 'sub', protocol=mqtt.MQTTv5)
sub.on_connect = on_connect
sub.on_message = on_message
sub.connect('localhost', 1883)
sub.loop_start()

# Wait for subscription
ready.wait(timeout=3)
time.sleep(1)

# Publish
pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, 'pub', protocol=mqtt.MQTTv5)
pub.connect('localhost', 1883)
pub.publish('test/live', b'live message', qos=1, retain=False)
time.sleep(2)
pub.disconnect()

sub.loop_stop()
sub.disconnect()

print(f'Total: {len(received)} messages')
if not received:
    print('FAIL: No messages received!')
else:
    print('SUCCESS: Live messaging works')
