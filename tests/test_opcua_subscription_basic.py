#!/usr/bin/env python3
"""
Basic OPC UA Subscription Test
Tests subscribing to OPC UA node data changes.
"""

import asyncio
import pytest
from asyncua import Client, ua
import logging
import os
import paho.mqtt.client as mqtt
import threading
import time

# Enable detailed logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables with defaults
OPCUA_URL = os.getenv("OPCUA_URL", "opc.tcp://localhost:4841/server")

pytestmark = pytest.mark.asyncio


async def test_opcua_subscription_basic():
    """Test subscribing to a float/temperature node for data change notifications"""
    url = OPCUA_URL

    async with Client(url=url) as client:
        print("Connected to OPC UA server")

        # Get the float/temperature node by direct NodeId
        node_id = ua.NodeId("opcua/server/float/temperature:v", 2)
        temperature_node = client.get_node(node_id)

        print(f"Found node: {temperature_node}")
        print(f"NodeId: {temperature_node.nodeid}")

        # Read current value
        current_value = await temperature_node.read_value()
        print(f"Current value: {current_value}")

        # Create subscription with proper handler
        from asyncua.common.subscription import DataChangeNotificationHandler
        
        class NotificationHandler(DataChangeNotificationHandler):
            def __init__(self):
                self.notifications = []
            
            def datachange_notification(self, node, val, data):
                notification = f"Node: {node}, Value: {val}"
                print(f"Data change notification - {notification}")
                self.notifications.append((node, val, data))

        handler = NotificationHandler()
        subscription = await client.create_subscription(500, handler)
        print("Created subscription")

        # Subscribe to the node
        handle = await subscription.subscribe_data_change(temperature_node)
        print(f"Subscribed to node with handle: {handle}")

        # Publish MQTT message in background to trigger notification
        def publish_mqtt():
            time.sleep(2)  # Wait for subscription to be ready
            c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            c.username_pw_set("admin", "public")
            c.connect("localhost", 1883)
            c.loop_start()
            time.sleep(0.5)
            c.publish("float/temperature", "25.5")
            print("Published MQTT message: float/temperature = 25.5")
            time.sleep(1)
            c.loop_stop()
            c.disconnect()

        # Start MQTT publisher in background thread
        mqtt_thread = threading.Thread(target=publish_mqtt, daemon=True)
        mqtt_thread.start()

        # Wait for notifications (reduced time)
        print("Waiting 5 seconds for subscription notifications...")
        await asyncio.sleep(5)

        # Unsubscribe and cleanup
        await subscription.unsubscribe(handle)
        await subscription.delete()
        print("Unsubscribed and cleaned up")

        # Verify we received notification
        print(f"Received {len(handler.notifications)} notification(s)")
        assert len(handler.notifications) > 0, "Expected to receive at least one notification from MQTT publish"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
