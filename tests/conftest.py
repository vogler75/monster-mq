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
    """Provides a connected MQTT v5 client with loop started, waiting for CONNACK."""
    connected = False

    def on_connect(client, userdata, flags, reason_code, properties):
        nonlocal connected
        connected = (reason_code == 0)

    mqtt_client.on_connect = on_connect
    mqtt_client.connect(broker_config["host"], broker_config["port"])
    mqtt_client.loop_start()

    # Wait for CONNACK before yielding
    start = time.time()
    while not connected and (time.time() - start) < 5.0:
        time.sleep(0.05)
    assert connected, "connected_client fixture: CONNACK not received within 5s"

    yield mqtt_client

    mqtt_client.loop_stop()
    mqtt_client.disconnect()


@pytest.fixture
def clean_topic():
    """Registers a topic name and clears its retained message after the test."""
    topics = []
    
    def _make_topic(base_name):
        topics.append(base_name)
        return base_name
    
    yield _make_topic
    
    # Cleanup: Clear retained messages
    if topics:
        cleanup_client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            protocol=mqtt.MQTTv5
        )
        if USERNAME:
            cleanup_client.username_pw_set(USERNAME, PASSWORD)
        cleanup_connected = False

        def _on_connect(client, userdata, flags, reason_code, properties):
            nonlocal cleanup_connected
            cleanup_connected = (reason_code == 0)

        cleanup_client.on_connect = _on_connect
        try:
            cleanup_client.connect(BROKER_HOST, BROKER_PORT)
            cleanup_client.loop_start()
            start = time.time()
            while not cleanup_connected and (time.time() - start) < 5.0:
                time.sleep(0.05)
            if cleanup_connected:
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
    
    def on_subscribe(self, client, userdata, mid, reason_code_list, properties=None):
        """Subscribe callback."""
        def _is_success(rc):
            # ReasonCode objects: value < 128 means success (Granted QoS 0/1/2)
            # Plain int 0 also means success (MQTTv3 style)
            if hasattr(rc, 'value'):
                return rc.value < 128
            return rc == 0
        if isinstance(reason_code_list, list):
            self.subscribed = all(_is_success(rc) for rc in reason_code_list)
        else:
            self.subscribed = _is_success(reason_code_list)
    
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
