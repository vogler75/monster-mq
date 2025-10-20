#!/usr/bin/env python3

import asyncio
from asyncua import Client
import logging
import time
import os

# Enable detailed logging
logging.basicConfig(level=logging.INFO)

# Configuration from environment variables with defaults
OPCUA_URL = os.getenv("OPCUA_URL", "opc.tcp://localhost:4840/server")

class SubscriptionTest:
    def __init__(self):
        self.updates_received = 0
        self.values_received = []

    def data_change_notification(self, node, val, data):
        self.updates_received += 1
        self.values_received.append(val)
        print(f"🔔 Data change notification #{self.updates_received}: Value = {val}, Node = {node}")

async def test_live_subscription():
    url = OPCUA_URL
    test = SubscriptionTest()

    async with Client(url=url) as client:
        print("✅ Connected to OPC UA server")

        # Get the float/a node
        try:
            objects = await client.get_root_node().get_child("0:Objects")
            monster_folder = await objects.get_child("2:MonsterMQ")
            float_folder = await monster_folder.get_child("2:float")
            a_node = await float_folder.get_child("2:a")

            print(f"✅ Found node: {a_node.nodeid}")

            # Read current value
            current_value = await a_node.read_value()
            print(f"📊 Current value: {current_value}")

            # Create subscription
            subscription = await client.create_subscription(500, None)
            print("✅ Created subscription")

            # Subscribe to the node
            handle = await subscription.subscribe_data_change(a_node, test.data_change_notification)
            print(f"✅ Subscribed to node with handle: {handle}")

            # Wait a bit to see if we get the current value immediately
            print("⏳ Waiting 3 seconds for initial value notification...")
            await asyncio.sleep(3)

            if test.updates_received > 0:
                print(f"✅ Received {test.updates_received} initial notification(s)")
            else:
                print("⚠️  No initial value notification received")

            print("\n" + "="*50)
            print("Now publishing new values to test live updates...")
            print("="*50)

            # Reset counter for the live test
            initial_count = test.updates_received

            # The test script will wait while external updates are published
            print("📢 Please publish new values to 'float/a' topic from another terminal")
            print("   Example: curl -X POST http://localhost:4000/graphql -H \"Content-Type: application/json\" -d '{\"query\":\"mutation { publish(input: {topic: \\\"float/a\\\", payload: \\\"99.9\\\", qos: 0}) { success } }\"}'")
            print("\n⏳ Waiting 15 seconds for live updates...")

            await asyncio.sleep(15)

            live_updates = test.updates_received - initial_count
            if live_updates > 0:
                print(f"✅ Received {live_updates} live update(s)!")
                print(f"📊 All values received: {test.values_received}")
            else:
                print("❌ No live updates received")

            # Cleanup
            await subscription.unsubscribe(handle)
            await subscription.delete()
            print("✅ Unsubscribed and cleaned up")

            # Summary
            print(f"\n📊 SUMMARY:")
            print(f"   Total notifications: {test.updates_received}")
            print(f"   Values received: {test.values_received}")

        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_live_subscription())