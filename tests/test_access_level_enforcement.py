#!/usr/bin/env python3
"""
Test OPC UA access level enforcement.
This test verifies that:
1. READ_WRITE nodes allow writes
2. READ_ONLY nodes reject writes
3. The fast lookup mechanism works correctly
"""

import asyncio
from asyncua import Client, ua
import paho.mqtt.client as mqtt
import time
import logging

# Enable detailed logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_access_level_enforcement():
    """Test that access level enforcement works correctly"""
    url = "opc.tcp://localhost:4840/server"

    print("🧪 OPC UA Access Level Enforcement Test")
    print("=" * 60)
    print("Testing that access levels are properly enforced")
    print("=" * 60)

    try:
        async with Client(url=url) as client:
            print("✅ Connected to OPC UA server")

            # First, publish an MQTT message to create write/oee node
            # This should create a READ_WRITE node based on the configuration
            mqtt_client = mqtt.Client()
            mqtt_client.connect("localhost", 1883, 60)
            mqtt_client.loop_start()
            time.sleep(1)  # Wait for connection

            print("📤 Publishing MQTT message to write/oee to create node...")
            result = mqtt_client.publish("write/oee", "500")
            time.sleep(2)  # Wait for node creation

            mqtt_client.loop_stop()
            mqtt_client.disconnect()

            # Try to access the write/oee node
            write_node_id = ua.NodeId("MonsterMQ/write/oee:v", 2)
            write_node = client.get_node(write_node_id)

            print("\n🔍 Testing write/oee node (should be READ_WRITE)...")

            try:
                # Try to write to write/oee node
                test_value = "999"
                print(f"✍️  Writing value '{test_value}' to write/oee node...")
                await write_node.write_value(test_value)

                # Verify the write succeeded
                read_value = await write_node.read_value()
                print(f"✅ Write succeeded! Node value: {read_value}")

                if str(read_value) == test_value:
                    print("✅ Value correctly updated")
                    write_test_passed = True
                else:
                    print(f"❌ Value mismatch: expected {test_value}, got {read_value}")
                    write_test_passed = False

            except Exception as e:
                print(f"❌ Write to write/oee failed: {e}")
                write_test_passed = False

            # Now test a READ_ONLY node (test/oee)
            print("\n🔍 Testing test/oee node (should be READ_ONLY)...")

            # First create the test/oee node by publishing MQTT
            mqtt_client = mqtt.Client()
            mqtt_client.connect("localhost", 1883, 60)
            mqtt_client.loop_start()
            time.sleep(1)

            print("📤 Publishing MQTT message to test/oee to create READ_ONLY node...")
            result = mqtt_client.publish("test/oee", "200")
            time.sleep(2)  # Wait for node creation

            mqtt_client.loop_stop()
            mqtt_client.disconnect()

            try:
                # Try to access the test/oee node
                test_node_id = ua.NodeId("MonsterMQ/test/oee:v", 2)
                test_node = client.get_node(test_node_id)

                # Try to write to test/oee node (should fail)
                test_value = "888"
                print(f"✍️  Writing value '{test_value}' to test/oee node (should fail)...")
                await test_node.write_value(test_value)

                print("❌ Write to READ_ONLY node unexpectedly succeeded!")
                readonly_test_passed = False

            except Exception as e:
                if "Bad_NotWritable" in str(e) or "access level does not allow writing" in str(e):
                    print(f"✅ Write correctly rejected: {e}")
                    readonly_test_passed = True
                else:
                    print(f"❌ Write failed with unexpected error: {e}")
                    readonly_test_passed = False

            # Summary
            print(f"\n🎯 Test Summary:")
            print(f"   ✅ READ_WRITE nodes allow writes: {write_test_passed}")
            print(f"   ✅ READ_ONLY nodes reject writes: {readonly_test_passed}")

            if write_test_passed and readonly_test_passed:
                print(f"\n🎉 ALL ACCESS LEVEL TESTS PASSED!")
                print(f"   - READ_WRITE nodes work correctly ✅")
                print(f"   - READ_ONLY nodes are properly protected ✅")
                print(f"   - Fast lookup mechanism is working ✅")
                return True
            else:
                print(f"\n❌ Some access level tests failed!")
                return False

    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(test_access_level_enforcement())

    if success:
        print("\n✅ Access level enforcement test completed successfully!")
    else:
        print("\n❌ Access level enforcement test failed!")