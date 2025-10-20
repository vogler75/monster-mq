#!/usr/bin/env python3
"""
Debug direct node access to MonsterMQ node
"""

import asyncio
from asyncua import Client, ua
import logging
import os

# Configure logging
logging.basicConfig(level=logging.WARNING)

# Configuration from environment variables with defaults
OPCUA_URL = os.getenv("OPCUA_URL", "opc.tcp://localhost:4840/server")

async def debug_test():
    url = OPCUA_URL
    print(f"Connecting to: {url}")

    try:
        async with Client(url=url) as client:
            print("✓ Connected!")

            # Based on logs: NodeId{ns=2, id=MonsterMQ}
            node_id = ua.NodeId("MonsterMQ", 2)
            print(f"Trying to access node: {node_id}")

            try:
                monster_node = client.get_node(node_id)
                print("✓ Got node object")

                # Try to read basic attributes
                display_name = await monster_node.read_display_name()
                print(f"✓ Display Name: {display_name.Text}")

                browse_name = await monster_node.read_browse_name()
                print(f"✓ Browse Name: {browse_name.Name}")

                node_class = await monster_node.read_node_class()
                print(f"✓ Node Class: {node_class}")

                # Try to browse children
                children = await monster_node.get_children()
                print(f"✓ Children: {len(children)}")

                for child in children:
                    child_name = await child.read_display_name()
                    print(f"  - Child: {child_name.Text}")

            except Exception as e:
                print(f"✗ Error accessing node: {e}")

            # Also test by browsing Objects folder specifically
            print("\n=== BROWSING OBJECTS FOLDER REFERENCES ===")
            objects_node = client.get_objects_node()

            # Get raw references instead of children
            try:
                # Use browse instead of get_children for more detailed info
                browse_result = await objects_node.browse()
                print(f"Objects folder has {len(browse_result)} references:")

                for ref in browse_result:
                    print(f"  - Reference to: {ref.target_node}")
                    print(f"    - Display Name: {ref.display_name}")
                    print(f"    - Browse Name: {ref.browse_name}")
                    print(f"    - Node Class: {ref.node_class}")
                    print(f"    - Type Definition: {ref.type_definition}")

            except Exception as e:
                print(f"Error browsing Objects folder: {e}")

    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    asyncio.run(debug_test())