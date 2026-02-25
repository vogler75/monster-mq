#!/usr/bin/env python3
"""
MQTT bulk publish test - publishes retained messages to 1000 topics
Topics: test/1 to test/1000
Messages: "Hello world 1" to "Hello world 1000"
"""

import paho.mqtt.client as mqtt
import time
import sys
import uuid
import pytest

@pytest.mark.slow
def test_mqtt_bulk_publish_retained(broker_config):
    """Test bulk publishing of 1000 retained messages"""
    NUM_TOPICS = 1000
    TOPIC_PREFIX = "test"
    
    connected = [False]
    publish_count = [0]
    
    def on_connect(client, userdata, flags, rc, properties=None):
        print(f"Connected with result code {rc}")
        connected[0] = (rc == 0)
    
    def on_publish(client, userdata, mid, reason_code=None, properties=None):
        publish_count[0] += 1
        # Only print progress every 100 messages to avoid spam
        if mid % 100 == 0:
            print(f"Message {mid} published")
    
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                         client_id=f"bulk_publisher_{uuid.uuid4().hex[:8]}",
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
    
    print(f"Publishing {NUM_TOPICS} retained messages...")
    start_time = time.time()
    
    failed_publishes = []
    for i in range(1, NUM_TOPICS + 1):
        topic = f"{TOPIC_PREFIX}/{i}"
        message = f"Hello world {i}"
        
        # Publish with retain flag set to True
        result = client.publish(topic, message, qos=1, retain=True)
        
        if result.rc != 0:
            failed_publishes.append((i, result.rc))
        
        if i % 100 == 0:
            elapsed = time.time() - start_time
            rate = i / elapsed
            print(f"Published {i}/{NUM_TOPICS} messages ({rate:.1f} msg/sec)")
    
    elapsed = time.time() - start_time
    print(f"All {NUM_TOPICS} messages published in {elapsed:.2f} seconds")
    
    # Wait a bit to ensure all messages are processed
    time.sleep(2)
    
    client.loop_stop()
    client.disconnect()
    
    # Assertions
    assert len(failed_publishes) == 0, f"Failed publishes: {failed_publishes[:10]}"
    print(f"✓ Successfully published {NUM_TOPICS} retained messages")
    print(f"  Publish callbacks received: {publish_count[0]}")

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
