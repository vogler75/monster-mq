#!/usr/bin/env python3
"""
Simple OPC UA connection test
"""
import asyncio
from asyncua import Client

async def test_connection():
    url = "opc.tcp://localhost:4841/server"
    print(f"Connecting to {url}...")
    
    try:
        client = Client(url=url, timeout=10)
        await client.connect()
        print("✓ Connected!")
        
        # Get root node
        root = client.get_root_node()
        print(f"Root node: {root}")
        
        # Browse Objects folder
        objects = client.get_objects_node()
        print(f"Objects node: {objects}")
        children = await objects.get_children()
        print(f"Objects children: {len(children)}")
        for child in children:
            name = await child.read_browse_name()
            print(f"  - {name.Name}")
        
        await client.disconnect()
        print("✓ Disconnected")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_connection())
