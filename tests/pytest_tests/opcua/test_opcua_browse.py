#!/usr/bin/env python3
"""
OPC UA Client script to test MonsterMQ broker OPC UA server
This script will connect to the OPC UA server and browse the Objects folder
to find and examine the MonsterMQ node.
"""

import asyncio
import pytest
from asyncua import Client, ua
import logging
import sys
import os

# Mark all tests as async
pytestmark = pytest.mark.asyncio

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables with defaults
OPCUA_URL = os.getenv("OPCUA_URL", "opc.tcp://localhost:4841/server")

async def browse_node(client, node, indent=0):
    """Recursively browse a node and its children"""
    try:
        # Get node attributes
        node_class = await node.read_node_class()
        browse_name = await node.read_browse_name()
        display_name = await node.read_display_name()
        node_id = node.nodeid

        # Print node information
        prefix = "  " * indent
        print(f"{prefix}Node: {display_name.Text}")
        print(f"{prefix}  - NodeId: {node_id}")
        print(f"{prefix}  - BrowseName: {browse_name.Name}")
        print(f"{prefix}  - NodeClass: {node_class}")

        # If it's a Variable node, try to read its value
        if node_class == ua.NodeClass.Variable:
            try:
                value = await node.read_value()
                data_type = await node.read_data_type()
                print(f"{prefix}  - Value: {value}")
                print(f"{prefix}  - DataType: {data_type}")
            except Exception as e:
                print(f"{prefix}  - Value: <Error reading value: {e}>")

        # Get references to child nodes
        try:
            children = await node.get_children()
            if children and indent < 3:  # Limit depth to avoid infinite recursion
                print(f"{prefix}  - Children ({len(children)}):")
                for child in children:
                    await browse_node(client, child, indent + 1)
        except Exception as e:
            print(f"{prefix}  - Error browsing children: {e}")

    except Exception as e:
        print(f"{prefix}Error reading node: {e}")

async def test_opcua_server():
    """Main test function"""
    # Connect to the MonsterMQ OPC UA server
    url = OPCUA_URL

    print(f"Connecting to OPC UA server at: {url}")

    try:
        async with Client(url=url) as client:
            print("Connected successfully!")

            # Get server information
            try:
                # Get application description instead
                app_desc = await client.get_application_uri()
                print(f"Server Application URI: {app_desc}")
            except Exception as e:
                print(f"Error getting server info: {e}")

            print("\n" + "="*50)
            print("BROWSING OBJECTS FOLDER")
            print("="*50)

            # Get the Objects folder
            objects_node = client.get_objects_node()
            print(f"Objects folder NodeId: {objects_node.nodeid}")

            # Browse Objects folder children with better error handling
            try:
                children = await objects_node.get_children()
                print(f"\nObjects folder has {len(children)} children:")

                monster_mq_node = None
                for child in children:
                    try:
                        browse_name = await child.read_browse_name()
                        display_name = await child.read_display_name()
                        print(f"  - {display_name.Text} (BrowseName: {browse_name.Name}, NodeId: {child.nodeid})")

                        # Look for MonsterMQ node
                        if "MonsterMQ" in browse_name.Name or "MonsterMQ" in display_name.Text:
                            monster_mq_node = child
                            print(f"    *** FOUND MonsterMQ node! ***")
                    except Exception as e:
                        print(f"  - Error reading child node {child.nodeid}: {e}")
            except Exception as e:
                print(f"Error browsing Objects folder children: {e}")

            print("\n" + "="*50)
            print("DETAILED BROWSING")
            print("="*50)

            if monster_mq_node:
                print(f"\nDetailed examination of MonsterMQ node:")
                await browse_node(client, monster_mq_node, 0)
            else:
                print(f"\nMonsterMQ node not found! Let's examine all children in detail:")
                for child in children:
                    await browse_node(client, child, 0)
                    print("-" * 30)

            # Try to find MonsterMQ node by NodeId if we know the namespace
            print("\n" + "="*50)
            print("SEARCHING BY NAMESPACE")
            print("="*50)

            try:
                # Get namespace array
                namespace_array = await client.get_namespace_array()
                print(f"Available namespaces:")
                for i, ns in enumerate(namespace_array):
                    print(f"  {i}: {ns}")

                # Look for our namespace
                monster_ns_index = None
                for i, ns in enumerate(namespace_array):
                    if "MonsterMQ" in ns or "monster" in ns.lower():
                        monster_ns_index = i
                        print(f"  *** Found MonsterMQ namespace at index {i}: {ns}")
                        break

                if monster_ns_index:
                    # Try to get MonsterMQ node by NodeId
                    monster_node_id = ua.NodeId("MonsterMQ", monster_ns_index)
                    try:
                        monster_node = client.get_node(monster_node_id)
                        print(f"\nTrying to access MonsterMQ node by NodeId: {monster_node_id}")
                        display_name = await monster_node.read_display_name()
                        print(f"Success! Display name: {display_name.Text}")
                        await browse_node(client, monster_node, 0)
                    except Exception as e:
                        print(f"Error accessing MonsterMQ node by NodeId: {e}")

            except Exception as e:
                print(f"Error examining namespaces: {e}")

    except Exception as e:
        print(f"Failed to connect to OPC UA server: {e}")
        print("Make sure the MonsterMQ broker with OPC UA server is running on localhost:4840")
        sys.exit(1)

if __name__ == "__main__":
    print("MonsterMQ OPC UA Server Test Client")
    print("=" * 40)
    asyncio.run(test_opcua_server())