"""
Pytest version: MQTT v5.0 Connection Test

Tests basic MQTT v5.0 connection acceptance and CONNACK handling.
"""
import time
import pytest
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes


pytestmark = pytest.mark.mqtt5


def test_mqtt5_basic_connection(broker_config):
    """Test that MQTT v5.0 clients can connect successfully."""
    connected = False
    connack_received = False
    
    def on_connect(client, userdata, flags, reason_code, properties):
        nonlocal connected, connack_received
        connack_received = True
        connected = (reason_code == 0)
    
    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5
    )
    client.username_pw_set(broker_config["username"], broker_config["password"])
    client.on_connect = on_connect
    
    client.connect(broker_config["host"], broker_config["port"])
    client.loop_start()
    
    # Wait for connection
    timeout = time.time() + 5
    while not connack_received and time.time() < timeout:
        time.sleep(0.1)
    
    client.loop_stop()
    client.disconnect()
    
    assert connack_received, "Did not receive CONNACK"
    assert connected, "Connection was refused"


def test_mqtt5_connection_with_properties(broker_config):
    """Test MQTT v5.0 connection with CONNECT properties."""
    connected = False
    server_props = None
    
    def on_connect(client, userdata, flags, reason_code, properties):
        nonlocal connected, server_props
        connected = (reason_code == 0)
        server_props = properties
    
    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=f"test_props_{int(time.time())}",
        protocol=mqtt.MQTTv5
    )
    client.username_pw_set(broker_config["username"], broker_config["password"])
    client.on_connect = on_connect
    
    # Set CONNECT properties
    connect_props = Properties(PacketTypes.CONNECT)
    connect_props.SessionExpiryInterval = 300
    connect_props.ReceiveMaximum = 100
    connect_props.MaximumPacketSize = 1024 * 1024
    
    client.connect(
        broker_config["host"],
        broker_config["port"],
        properties=connect_props
    )
    client.loop_start()
    
    timeout = time.time() + 5
    while not connected and time.time() < timeout:
        time.sleep(0.1)
    
    client.loop_stop()
    client.disconnect()
    
    assert connected, "Connection failed"
    # Server properties may or may not be present depending on implementation
    # Just verify we got a successful connection


@pytest.mark.parametrize("session_expiry", [0, 60, 300, 3600])
def test_mqtt5_session_expiry_intervals(broker_config, session_expiry):
    """Test connection with various session expiry intervals."""
    connected = False
    
    def on_connect(client, userdata, flags, reason_code, properties):
        nonlocal connected
        connected = (reason_code == 0)
    
    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=f"test_se_{session_expiry}_{int(time.time())}",
        protocol=mqtt.MQTTv5
    )
    client.username_pw_set(broker_config["username"], broker_config["password"])
    client.on_connect = on_connect
    
    connect_props = Properties(PacketTypes.CONNECT)
    connect_props.SessionExpiryInterval = session_expiry
    
    client.connect(
        broker_config["host"],
        broker_config["port"],
        properties=connect_props
    )
    client.loop_start()
    
    timeout = time.time() + 5
    while not connected and time.time() < timeout:
        time.sleep(0.1)
    
    client.loop_stop()
    client.disconnect()
    
    assert connected, f"Failed to connect with SessionExpiryInterval={session_expiry}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
