"""
Pytest configuration and shared fixtures for Monster-MQ tests.
"""
import os
import time
import pytest
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes


# Test configuration
BROKER_HOST = os.getenv("MQTT_BROKER", "localhost")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
USERNAME = os.getenv("MQTT_USERNAME", "Test")
PASSWORD = os.getenv("MQTT_PASSWORD", "Test")


@pytest.fixture
def broker_config():
    """Provides broker connection configuration."""
    return {
        "host": BROKER_HOST,
        "port": BROKER_PORT,
        "username": USERNAME,
        "password": PASSWORD,
    }


@pytest.fixture
def mqtt_client():
    """Provides a configured MQTT v5 client (not connected)."""
    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5
    )
    if USERNAME:
        client.username_pw_set(USERNAME, PASSWORD)
    
    yield client
    
    # Cleanup
    try:
        client.loop_stop()
        client.disconnect()
    except:
        pass


@pytest.fixture
def connected_client(mqtt_client, broker_config):
    """Provides a connected MQTT v5 client with loop started."""
    mqtt_client.connect(broker_config["host"], broker_config["port"])
    mqtt_client.loop_start()
    time.sleep(0.3)  # Give connection time to establish
    
    yield mqtt_client
    
    mqtt_client.loop_stop()
    mqtt_client.disconnect()


@pytest.fixture
def clean_topic():
    """Provides a unique topic name and cleans up retained messages after test."""
    topics = []
    
    def _make_topic(base_name):
        topic = f"{base_name}/{int(time.time() * 1000)}"
        topics.append(topic)
        return topic
    
    yield _make_topic
    
    # Cleanup: Clear retained messages
    if topics:
        cleanup_client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            protocol=mqtt.MQTTv5
        )
        if USERNAME:
            cleanup_client.username_pw_set(USERNAME, PASSWORD)
        try:
            cleanup_client.connect(BROKER_HOST, BROKER_PORT)
            cleanup_client.loop_start()
            time.sleep(0.2)
            for topic in topics:
                cleanup_client.publish(topic, "", retain=True)
            time.sleep(0.2)
            cleanup_client.loop_stop()
            cleanup_client.disconnect()
        except:
            pass


class MessageCollector:
    """Helper class to collect messages received by a client."""
    
    def __init__(self):
        self.messages = []
        self.connected = False
        self.subscribed = False
    
    def on_connect(self, client, userdata, flags, reason_code, properties):
        """Connection callback."""
        self.connected = (reason_code == 0)
    
    def on_subscribe(self, client, userdata, mid, reason_code_list, properties):
        """Subscribe callback."""
        if isinstance(reason_code_list, list):
            self.subscribed = all(rc == 0 for rc in reason_code_list)
        else:
            # Single reason code
            self.subscribed = (reason_code_list == 0)
    
    def on_message(self, client, userdata, msg):
        """Message callback."""
        self.messages.append({
            'topic': msg.topic,
            'payload': msg.payload.decode('utf-8') if msg.payload else None,
            'retain': msg.retain,
            'qos': msg.qos,
            'properties': msg.properties if hasattr(msg, 'properties') else None,
        })
    
    def wait_for_connection(self, timeout=5.0):
        """Wait for connection to be established."""
        start = time.time()
        while not self.connected and (time.time() - start) < timeout:
            time.sleep(0.1)
        return self.connected
    
    def wait_for_subscription(self, timeout=5.0):
        """Wait for subscription to be confirmed."""
        start = time.time()
        while not self.subscribed and (time.time() - start) < timeout:
            time.sleep(0.1)
        return self.subscribed
    
    def wait_for_messages(self, count=1, timeout=3.0):
        """Wait for specific number of messages."""
        start = time.time()
        while len(self.messages) < count and (time.time() - start) < timeout:
            time.sleep(0.1)
        return len(self.messages) >= count
    
    def clear(self):
        """Clear collected messages."""
        self.messages.clear()


@pytest.fixture
def message_collector():
    """Provides a MessageCollector instance."""
    return MessageCollector()
