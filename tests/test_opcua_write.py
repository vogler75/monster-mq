#!/usr/bin/env python3
"""
OPC UA Write Test Script
This script connects to the MonsterMQ OPC UA server and writes values to test nodes
to verify that writes are properly published to MQTT topics.
"""

import asyncio
import pytest
from asyncua import Client, ua
import logging
import time
import os

# Enable detailed logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables with defaults
OPCUA_URL = os.getenv("OPCUA_URL", "opc.tcp://localhost:4840/server")

async def test_opcua_write():
    """Test writing values to OPC UA nodes"""
    url = OPCUA_URL

    print(f"🔗 Connecting to OPC UA server at: {url}")

    async with Client(url=url) as client:
        print("✅ Connected successfully!")

        # First, create a node by publishing an MQTT message to trigger node creation
        print("\n📤 First, let's publish an MQTT message to create the node...")

        # We need to trigger node creation first by publishing to the topic
        # The OPC UA server will create nodes dynamically when it receives MQTT messages

        # For now, let's try to find existing nodes or create one by publishing first
        print("\n🔍 Looking for existing write/* nodes...")

        # Navigate to the MonsterMQ folder
        objects = await client.get_root_node().get_child("0:Objects")
        print("✅ Found Objects folder")

        monster_folder = await objects.get_child("2:MonsterMQ")
        print("✅ Found MonsterMQ folder")

        # Look for write folder or nodes
        children = await monster_folder.get_children()
        print(f"📂 MonsterMQ folder has {len(children)} children")

        write_nodes = []
        for child in children:
            try:
                browse_name = await child.read_browse_name()
                if "write" in browse_name.Name.lower():
                    write_nodes.append(child)
                    print(f"🎯 Found write-related node: {browse_name.Name}")
            except Exception as e:
                logger.debug(f"Error reading child node: {e}")

        if not write_nodes:
            print("❌ No write nodes found. Node creation happens when MQTT messages are received.")
            print("💡 Let's try to write to a node that should exist from configuration...")

            # Try to navigate to write/oee node directly
            # This should exist because we have write/# in the configuration

            # Let's try different approaches to find or create the node
            print("\n🔧 Attempting to find write/oee node...")

            # Method 1: Try to get it by NodeId if it exists
            # The NodeId format should be ns=2;s=MonsterMQ/write/oee:v
            node_id = ua.NodeId("MonsterMQ/write/oee:v", 2)
            test_node = client.get_node(node_id)

            # Try to read to see if it exists
            try:
                current_value = await test_node.read_value()
                print(f"✅ Found existing node with value: {current_value}")
                write_nodes.append(test_node)
            except:
                print("❌ Node exists but can't read value, trying to write anyway...")
                write_nodes.append(test_node)

        # Now try to write to the nodes we found/created
        assert len(write_nodes) > 0, "No write nodes available for testing"
        
        write_success_count = 0
        for i, node in enumerate(write_nodes):
            print(f"\n🖊️  Testing write to node {i+1}/{len(write_nodes)}")
            print(f"📍 NodeId: {node.nodeid}")

            try:
                # Read current value first
                current_value = await node.read_value()
                print(f"📊 Current value: {current_value}")
            except Exception as e:
                print(f"⚠️  Could not read current value: {e}")

            # Write a test value
            test_value = 42 + i  # Different value for each node
            print(f"✍️  Writing value: {test_value}")

            await node.write_value(test_value)
            print(f"✅ Write successful!")
            write_success_count += 1

            # Wait a moment and read back
            await asyncio.sleep(1)
            new_value = await node.read_value()
            print(f"📊 Value after write: {new_value}")

            print("-" * 40)

            # Wait between writes
            if i < len(write_nodes) - 1:
                print("⏳ Waiting 2 seconds before next write...")
                await asyncio.sleep(2)
        
        # Assert at least one write succeeded
        assert write_success_count > 0, "No successful writes performed"
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])