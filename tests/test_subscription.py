#!/usr/bin/env python3

import asyncio
import pytest
from asyncua import Client
import logging

# Enable detailed logging
logging.basicConfig(level=logging.INFO)

async def test_subscription():
    url = "opc.tcp://localhost:4840/server"

    async with Client(url=url) as client:
        print("Connected to OPC UA server")

        # Get Objects folder
        objects = await client.get_root_node().get_child("0:Objects")
        monster_folder = await objects.get_child("2:MonsterMQ")

        # Get the float/a node that was just created
        try:
            float_folder = await monster_folder.get_child("2:float")
            a_node = await float_folder.get_child("2:a")

            print(f"Found node: {a_node}")
            print(f"NodeId: {a_node.nodeid}")

            # Read current value
            current_value = await a_node.read_value()
            print(f"Current value: {current_value}")

            # Create subscription
            subscription = await client.create_subscription(500, None)
            print("Created subscription")

            # Subscribe to the node
            def data_change_notification(node, val, data):
                print(f"Data change notification - Node: {node}, Value: {val}, Data: {data}")

            handle = await subscription.subscribe_data_change(a_node, data_change_notification)
            print(f"Subscribed to node with handle: {handle}")

            # Wait for potential notifications
            print("Waiting 10 seconds for subscription notifications...")
            await asyncio.sleep(10)

            # Unsubscribe
            await subscription.unsubscribe(handle)
            await subscription.delete()
            print("Unsubscribed and cleaned up")

        except Exception as e:
            print(f"Error: {e}")
            return

if __name__ == "__main__":
    asyncio.run(test_subscription())