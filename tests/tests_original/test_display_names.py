#!/usr/bin/env python3

import asyncio
from asyncua import Client
import logging
import os

# Enable detailed logging
logging.basicConfig(level=logging.INFO)

# Configuration from environment variables with defaults
OPCUA_URL = os.getenv("OPCUA_URL", "opc.tcp://localhost:4840/server")

async def test_display_names():
    url = OPCUA_URL

    async with Client(url=url) as client:
        print("Connected to OPC UA server")

        # Get root folder
        root = client.get_root_node()
        print(f"Root: {root}")

        # Get Objects folder
        objects = await root.get_child("0:Objects")
        print(f"Objects: {objects}")

        # Get MonsterMQ folder
        try:
            monster_folder = await objects.get_child("2:MonsterMQ")
            print(f"MonsterMQ folder: {monster_folder}")

            # Get display name of MonsterMQ folder
            monster_display_name = await monster_folder.read_display_name()
            print(f"MonsterMQ display name: {monster_display_name.Text}")

            # Browse children of MonsterMQ folder
            print("\nBrowsing MonsterMQ folder...")
            children = await monster_folder.get_children()
            for child in children:
                child_display_name = await child.read_display_name()
                child_browse_name = await child.read_browse_name()
                node_id = child.nodeid
                print(f"  Child: {child_display_name.Text} (Browse: {child_browse_name.Name}, NodeId: {node_id})")

                # If this is a folder, browse its children too
                try:
                    grandchildren = await child.get_children()
                    for grandchild in grandchildren:
                        gc_display_name = await grandchild.read_display_name()
                        gc_browse_name = await grandchild.read_browse_name()
                        gc_node_id = grandchild.nodeid
                        print(f"    Grandchild: {gc_display_name.Text} (Browse: {gc_browse_name.Name}, NodeId: {gc_node_id})")
                except:
                    # Not a folder or no children
                    pass

        except Exception as e:
            print(f"Error accessing MonsterMQ folder: {e}")
            return

        print("\nDisplay name test completed!")

if __name__ == "__main__":
    asyncio.run(test_display_names())