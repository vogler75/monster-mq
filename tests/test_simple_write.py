#!/usr/bin/env python3
"""
Simple OPC UA Write Test Script
This script directly writes to the write/oee variable node.
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
OPCUA_URL = os.getenv("OPCUA_URL", "opc.tcp://localhost:4840/server")

async def test_simple_write():
    """Test writing to the write/oee variable node directly"""
    url = OPCUA_URL

    print(f"🔗 Connecting to OPC UA server at: {url}")

    async with Client(url=url) as client:
        print("✅ Connected successfully!")

        # Try to access the write/oee variable node directly
        # Based on the logs, it should have NodeId: ns=2;s=MonsterMQ/write/oee:v
        node_id = ua.NodeId("MonsterMQ/write/oee:v", 2)
        node = client.get_node(node_id)

        print(f"📍 Attempting to write to NodeId: {node_id}")

        try:
            # Try to read current value first
            current_value = await node.read_value()
            print(f"📊 Current value: {current_value}")
        except Exception as e:
            print(f"⚠️  Could not read current value: {e}")

        # Write a test value (as string since the node was created from a string MQTT message)
        test_value = "999"
        print(f"✍️  Writing value: {test_value}")

        await node.write_value(test_value)
        print(f"✅ Write successful!")

        # Wait a moment and read back
        await asyncio.sleep(1)
        new_value = await node.read_value()
        print(f"📊 Value after write: {new_value}")
        
        # Verify the write succeeded
        assert new_value is not None, "Failed to read value after write"

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])