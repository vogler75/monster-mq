#!/usr/bin/env python3
"""
Simple OPC UA test to check MonsterMQ namespaces and nodes
"""

import asyncio
from asyncua import Client, ua
import logging

# Configure logging
logging.basicConfig(level=logging.WARNING)  # Reduce log noise

async def simple_test():
    url = "opc.tcp://localhost:4840/server"
    print(f"Connecting to: {url}")

    try:
        async with Client(url=url) as client:
            print("✓ Connected successfully!")

            # Check namespaces
            print("\n=== NAMESPACES ===")
            namespaces = await client.get_namespace_array()
            for i, ns in enumerate(namespaces):
                print(f"  {i}: {ns}")
                if "MonsterMQ" in ns:
                    print(f"    *** Found MonsterMQ namespace at index {i} ***")

            # Check Objects folder children
            print("\n=== OBJECTS FOLDER CHILDREN ===")
            objects_node = client.get_objects_node()
            children = await objects_node.get_children()
            print(f"Objects folder has {len(children)} children:")

            for child in children:
                try:
                    browse_name = await child.read_browse_name()
                    display_name = await child.read_display_name()
                    print(f"  - {display_name.Text} (BrowseName: {browse_name.Name}, NodeId: {child.nodeid})")

                    if "MonsterMQ" in str(browse_name.Name) or "MonsterMQ" in str(display_name.Text):
                        print(f"    *** FOUND MonsterMQ node! ***")
                except Exception as e:
                    print(f"  - Error reading child: {e}")

            # Try to find MonsterMQ node by direct NodeId
            print("\n=== DIRECT NODE ACCESS ===")
            monster_ns_index = None
            for i, ns in enumerate(namespaces):
                if "MonsterMQ" in ns:
                    monster_ns_index = i
                    break

            # Try both MonsterMQ namespaces
            monster_namespaces = []
            for i, ns in enumerate(namespaces):
                if "MonsterMQ" in ns:
                    monster_namespaces.append(i)

            for ns_idx in monster_namespaces:
                print(f"\nTrying namespace {ns_idx} ({namespaces[ns_idx]}):")
                # Try different NodeId patterns
                test_node_ids = [
                    ua.NodeId("MonsterMQ", ns_idx),
                    ua.NodeId(1, ns_idx),  # numeric
                    ua.NodeId("ROOT_FOLDER", ns_idx),
                ]

                for node_id in test_node_ids:
                    try:
                        test_node = client.get_node(node_id)
                        display_name = await test_node.read_display_name()
                        browse_name = await test_node.read_browse_name()
                        print(f"✓ Found node {node_id}: {display_name.Text} ({browse_name.Name})")
                    except Exception as e:
                        print(f"✗ Node {node_id} not found: {e}")

            if not monster_namespaces:
                print("No MonsterMQ namespace found")

    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    asyncio.run(simple_test())