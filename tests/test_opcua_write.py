#!/usr/bin/env python3
"""
OPC UA Write Tests
Tests for writing values to OPC UA nodes in the MonsterMQ broker.
"""

import asyncio
import pytest
from asyncua import Client, ua
import logging
import os

# Enable detailed logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables with defaults
OPCUA_URL = os.getenv("OPCUA_URL", "opc.tcp://localhost:4841/server")

pytestmark = pytest.mark.asyncio


async def test_opcua_write_direct():
    """Test writing directly to a known OPC UA node (write/oee)"""
    url = OPCUA_URL

    print(f"Connecting to OPC UA server at: {url}")

    async with Client(url=url) as client:
        print("Connected successfully!")

        # Access the write/oee variable node directly
        # NodeId format: ns=2;s=opcua/server/write/oee:v
        node_id = ua.NodeId("opcua/server/write/oee:v", 2)
        node = client.get_node(node_id)

        print(f"Attempting to write to NodeId: {node_id}")

        try:
            # Read current value first
            current_value = await node.read_value()
            print(f"Current value: {current_value}")
        except Exception as e:
            print(f"Could not read current value: {e}")

        # Write a test value
        test_value = "999"
        print(f"Writing value: {test_value}")

        await node.write_value(test_value)
        print(f"Write successful!")

        # Wait and read back
        await asyncio.sleep(1)
        new_value = await node.read_value()
        print(f"Value after write: {new_value}")
        
        # Verify the write succeeded
        assert new_value is not None, "Failed to read value after write"
        assert str(new_value) == test_value, f"Expected {test_value}, got {new_value}"


async def test_opcua_write_browse_and_write():
    """Test browsing for write/* nodes and writing to them"""
    url = OPCUA_URL

    print(f"Connecting to OPC UA server at: {url}")

    async with Client(url=url) as client:
        print("Connected successfully!")

        # Navigate to the opcua/server folder by NodeId (since browse name contains slash)
        objects = client.get_objects_node()
        print("Found Objects folder")

        # Get opcua/server folder by direct NodeId
        opcua_folder = client.get_node(ua.NodeId("opcua/server:o", 2))
        print("Found opcua/server folder")

        # Look for write folder and browse its children to get variable nodes
        write_folder = client.get_node(ua.NodeId("opcua/server/write:o", 2))
        print("Found write folder")
        
        write_children = await write_folder.get_children()
        print(f"write folder has {len(write_children)} variable nodes")

        write_nodes = []
        for child in write_children:
            try:
                node_class = await child.read_node_class()
                if node_class == ua.NodeClass.Variable:
                    browse_name = await child.read_browse_name()
                    write_nodes.append(child)
                    print(f"Found variable node: {browse_name.Name}")
            except Exception as e:
                logger.debug(f"Error reading child node: {e}")

        if not write_nodes:
            print("No write variable nodes found by browsing.")
            print("Trying direct NodeId access to write/oee node...")

            # Try to get it by NodeId
            node_id = ua.NodeId("opcua/server/write/oee:v", 2)
            test_node = client.get_node(node_id)

            try:
                current_value = await test_node.read_value()
                print(f"Found existing node with value: {current_value}")
                write_nodes.append(test_node)
            except:
                print("Could not access node, may not exist yet")
                write_nodes.append(test_node)

        # Write to the nodes we found
        assert len(write_nodes) > 0, "No write nodes available for testing"
        
        write_success_count = 0
        for i, node in enumerate(write_nodes):
            print(f"\nTesting write to node {i+1}/{len(write_nodes)}")
            print(f"NodeId: {node.nodeid}")

            try:
                current_value = await node.read_value()
                print(f"Current value: {current_value}")
            except Exception as e:
                print(f"Could not read current value: {e}")

            # Write a test value
            test_value = str(42 + i)
            print(f"Writing value: {test_value}")

            await node.write_value(test_value)
            print(f"Write successful!")
            write_success_count += 1

            # Read back
            await asyncio.sleep(1)
            new_value = await node.read_value()
            print(f"Value after write: {new_value}")

            if i < len(write_nodes) - 1:
                await asyncio.sleep(1)
        
        assert write_success_count > 0, "No successful writes performed"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])