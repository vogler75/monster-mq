#!/usr/bin/env python3
"""
Test OPC UA subscription notifications when writing to nodes.
This test verifies that when an OPC UA client writes to a node:
1. The local OPC UA node value is updated
2. OPC UA subscribers receive notifications
3. MQTT messages are published correctly
"""

import asyncio
import pytest
from asyncua import Client, ua
import paho.mqtt.client as mqtt
import threading
import time
import logging
import os

# Mark all async tests
pytestmark = pytest.mark.asyncio

# Enable detailed logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables with defaults
OPCUA_URL = os.getenv("OPCUA_URL", "opc.tcp://localhost:4841/server")
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

class MQTTSubscriber:
    def __init__(self):
        self.received_messages = []
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, rc):
        logger.info(f"MQTT subscriber connected with result code {rc}")
        client.subscribe("write/oee")

    def on_message(self, client, userdata, msg):
        message = msg.payload.decode()
        logger.info(f"📨 MQTT received: topic={msg.topic}, payload={message}")
        self.received_messages.append({
            'topic': msg.topic,
            'payload': message,
            'timestamp': time.time()
        })

    def start(self):
        if MQTT_USERNAME:
            self.client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD or "")
        self.client.connect(MQTT_BROKER, MQTT_PORT, 60)
        self.client.loop_start()

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()

class OPCUASubscriptionHandler:
    def __init__(self):
        self.received_notifications = []

    def datachange_notification(self, node, val, data):
        logger.info(f"📡 OPC UA subscription received: node={node}, value={val}")
        self.received_notifications.append({
            'node': str(node),
            'value': val,
            'timestamp': time.time()
        })

async def test_opcua_subscription_notifications():
    """Test that OPC UA subscriptions work when writing to nodes"""
    url = OPCUA_URL

    print("🧪 OPC UA Subscription Notification Test")
    print("=" * 60)
    print("Testing that OPC UA writes trigger both OPC UA and MQTT notifications")
    print("=" * 60)

    # Start MQTT subscriber
    mqtt_subscriber = MQTTSubscriber()
    mqtt_subscriber.start()
    time.sleep(1)  # Wait for MQTT connection

    try:
        async with Client(url=url) as client:
            print("✅ Connected to OPC UA server")

            # Get the write/oee node
            node_id = ua.NodeId("opcua/server/write/oee:v", 2)
            node = client.get_node(node_id)

            # Read current value
            current_value = await node.read_value()
            print(f"📊 Current node value: {current_value}")

            # Create subscription for OPC UA notifications
            handler = OPCUASubscriptionHandler()
            subscription = await client.create_subscription(500, handler)
            await subscription.subscribe_data_change(node)
            print("✅ Created OPC UA subscription")

            # Wait a moment for subscription to be fully established
            await asyncio.sleep(1)

            # Clear any existing notifications
            handler.received_notifications.clear()
            mqtt_subscriber.received_messages.clear()

            print("\n🔄 Performing test writes...")

            # Test 1: Write a new value
            test_value_1 = "1234"
            print(f"\n✍️  Writing value: {test_value_1}")
            await node.write_value(test_value_1)

            # Wait for notifications
            await asyncio.sleep(2)

            # Check results
            opcua_notifications = handler.received_notifications
            mqtt_messages = mqtt_subscriber.received_messages

            print(f"\n📊 Results after first write:")
            print(f"   OPC UA notifications received: {len(opcua_notifications)}")
            print(f"   MQTT messages received: {len(mqtt_messages)}")

            if opcua_notifications:
                latest_opcua = opcua_notifications[-1]
                print(f"   Latest OPC UA notification: value={latest_opcua['value']}")

            if mqtt_messages:
                latest_mqtt = mqtt_messages[-1]
                print(f"   Latest MQTT message: payload={latest_mqtt['payload']}")

            # Verify the node value was updated
            updated_value = await node.read_value()
            print(f"   Node value after write: {updated_value}")

            # Test 2: Write another value
            test_value_2 = "5678"
            print(f"\n✍️  Writing value: {test_value_2}")

            # Clear previous results
            before_opcua_count = len(opcua_notifications)
            before_mqtt_count = len(mqtt_messages)

            await node.write_value(test_value_2)
            await asyncio.sleep(2)

            # Check new results
            new_opcua_notifications = len(opcua_notifications) - before_opcua_count
            new_mqtt_messages = len(mqtt_messages) - before_mqtt_count

            print(f"\n📊 Results after second write:")
            print(f"   New OPC UA notifications: {new_opcua_notifications}")
            print(f"   New MQTT messages: {new_mqtt_messages}")

            if opcua_notifications:
                latest_opcua = opcua_notifications[-1]
                print(f"   Latest OPC UA notification: value={latest_opcua['value']}")

            if mqtt_messages:
                latest_mqtt = mqtt_messages[-1]
                print(f"   Latest MQTT message: payload={latest_mqtt['payload']}")

            # Final verification
            final_value = await node.read_value()
            print(f"   Node value after second write: {final_value}")

            # Summary
            print(f"\n🎯 Test Summary:")
            total_opcua = len(opcua_notifications)
            total_mqtt = len(mqtt_messages)

            print(f"   Total OPC UA notifications: {total_opcua}")
            print(f"   Total MQTT messages: {total_mqtt}")

            opcua_working = total_opcua >= 2
            mqtt_working = total_mqtt >= 2
            values_correct = str(final_value) == test_value_2

            print(f"   ✅ OPC UA subscriptions working: {opcua_working}")
            print(f"   ✅ MQTT publishing working: {mqtt_working}")
            print(f"   ✅ Node values updated correctly: {values_correct}")

            # Assert test results
            assert opcua_working, f"OPC UA subscriptions not working properly (got {total_opcua} notifications, expected >= 2)"
            assert mqtt_working, f"MQTT publishing not working properly (got {total_mqtt} messages, expected >= 2)"
            assert values_correct, f"Node values not updated correctly (expected {test_value_2}, got {final_value})"
            
            print(f"\n🎉 ALL TESTS PASSED! OPC UA write handling is working correctly.")
            print(f"   - OPC UA writes update local node values ✅")
            print(f"   - OPC UA subscribers receive notifications ✅")
            print(f"   - MQTT messages are published ✅")

    finally:
        mqtt_subscriber.stop()

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])