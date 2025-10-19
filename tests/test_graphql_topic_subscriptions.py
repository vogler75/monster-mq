#!/usr/bin/env python3
"""
Test script for MonsterMQ GraphQL Topic Subscriptions

This script connects to the MonsterMQ GraphQL WebSocket endpoint and subscribes
to real-time MQTT message updates from multiple topic filters simultaneously.

Demonstrates the multiTopicUpdates subscription which allows efficient monitoring
of different topic categories in a single subscription connection.

Requirements:
    pip install websockets

Usage:
    # Subscribe to multiple device metrics topics
    python test_graphql_topic_subscriptions.py --filters "device/+/metrics" "device/+/status" "device/+/error"

    # Monitor all sensor data with wildcard
    python test_graphql_topic_subscriptions.py --filters "sensor/#"

    # Monitor system topics only
    python test_graphql_topic_subscriptions.py --filters "\$SYS/#"

    # Monitor multiple independent topic hierarchies
    python test_graphql_topic_subscriptions.py --filters "temperature/+" "humidity/+" "pressure/+"

    # Subscribe to single topic (equivalent to topicUpdates)
    python test_graphql_topic_subscriptions.py --filters "sensor/temperature"

    # Custom URL and format
    python test_graphql_topic_subscriptions.py --url "ws://broker.example.com:4000/graphqlws" --format BINARY --filters "camera/snapshot"
"""

import asyncio
import json
import sys
import argparse
import websockets
import base64
from datetime import datetime
from typing import Optional, List


class GraphQLTopicSubscriptionClient:
    """Client for subscribing to MonsterMQ MQTT topic updates via GraphQL WebSocket"""

    def __init__(self, url="ws://localhost:4000/graphqlws"):
        self.url = url
        self.websocket = None
        self.subscription_id = "1"
        self.message_count = 0

    async def connect(self):
        """Establish WebSocket connection"""
        print(f"Connecting to {self.url}...")
        self.websocket = await websockets.connect(
            self.url,
            subprotocols=["graphql-transport-ws"]
        )
        print("✓ Connected to GraphQL WebSocket endpoint")

    async def initialize(self):
        """Initialize the GraphQL WebSocket connection"""
        # Send connection_init message (graphql-ws protocol)
        init_message = {
            "type": "connection_init",
            "payload": {}
        }
        await self.websocket.send(json.dumps(init_message))

        # Wait for connection_ack
        response = await self.websocket.recv()
        msg = json.loads(response)

        if msg.get("type") == "connection_ack":
            print("✓ Connection acknowledged by server")
        else:
            print(f"⚠ Unexpected response: {msg}")

    async def subscribe(self, topic_filters: List[str], data_format: str = "JSON"):
        """
        Subscribe to topic updates with multiple filters

        Args:
            topic_filters: List of MQTT topic filters to monitor
                         Each filter supports MQTT wildcards:
                         - "+" for single-level wildcard (e.g., "sensor/+/temperature")
                         - "#" for multi-level wildcard (e.g., "device/#")
            data_format: Data format for payloads ("JSON" or "BINARY")
        """

        # Build GraphQL subscription query
        query = """
        subscription MultiTopicUpdates($filters: [String!]!, $format: DataFormat) {
            multiTopicUpdates(topicFilters: $filters, format: $format) {
                topic
                payload
                format
                timestamp
                qos
                retained
                clientId
            }
        }
        """

        # Build variables object
        variables = {
            "filters": topic_filters,
            "format": data_format
        }

        # Send subscribe message
        subscribe_message = {
            "id": self.subscription_id,
            "type": "subscribe",
            "payload": {
                "query": query,
                "variables": variables
            }
        }

        await self.websocket.send(json.dumps(subscribe_message))

        # Print subscription info
        print("\n" + "="*90)
        print("📡 Subscription started")
        print("="*90)
        print(f"Format:        {data_format}")
        print(f"Topic Filters: {len(topic_filters)} filter(s)")
        for i, tf in enumerate(topic_filters, 1):
            print(f"  {i}. {tf}")
        print("="*90 + "\n")
        print("Waiting for messages... (Press Ctrl+C to stop)\n")

    async def listen(self):
        """Listen for incoming topic update messages"""
        try:
            async for message in self.websocket:
                msg = json.loads(message)

                if msg.get("type") == "next":
                    # Extract topic update from the message
                    topic_update = msg.get("payload", {}).get("data", {}).get("multiTopicUpdates")
                    if topic_update:
                        self.message_count += 1
                        self.print_topic_update(topic_update)

                elif msg.get("type") == "error":
                    print(f"\n❌ Error: {msg.get('payload')}\n")

                elif msg.get("type") == "complete":
                    print(f"\n✓ Subscription completed (received {self.message_count} messages)\n")
                    break

        except websockets.exceptions.ConnectionClosed:
            print(f"\n⚠ Connection closed by server (received {self.message_count} messages)\n")
        except KeyboardInterrupt:
            print(f"\n\n⏹ Stopping subscription... (received {self.message_count} messages)\n")
            await self.unsubscribe()

    def print_topic_update(self, update: dict):
        """Pretty print a topic update message"""
        topic = update.get("topic", "?")
        payload = update.get("payload", "")
        data_format = update.get("format", "UNKNOWN")
        timestamp = update.get("timestamp", 0)
        qos = update.get("qos", 0)
        retained = update.get("retained", False)
        client_id = update.get("clientId")

        # Format timestamp (milliseconds since epoch to readable format)
        try:
            if isinstance(timestamp, (int, float)) and timestamp > 0:
                dt = datetime.fromtimestamp(timestamp / 1000.0)
                time_str = dt.strftime("%H:%M:%S.%f")[:-3]
            else:
                time_str = str(timestamp)
        except:
            time_str = str(timestamp)

        # Color codes
        colors = {
            "JSON": "\033[32m",     # Green
            "BINARY": "\033[33m",   # Yellow
        }
        reset = "\033[0m"
        color = colors.get(data_format, "")

        # Build flags
        flags = []
        if retained:
            flags.append("R")  # Retained
        flags_str = f"[{','.join(flags)}]" if flags else ""

        # Format payload (handle different data formats)
        payload_display = self.format_payload(payload, data_format)

        # Build main message line
        print(f"{color}[{time_str}]{reset} {topic} (QoS:{qos}, Fmt:{data_format}) {flags_str}")

        # Print payload
        print(f"  └─ Payload: {payload_display}")

        # Print client ID if available
        if client_id:
            print(f"  └─ Client: {client_id}")

        print()  # Empty line between messages

    def format_payload(self, payload: str, data_format: str) -> str:
        """Format payload based on data type"""
        if not payload:
            return "[empty]"

        try:
            if data_format == "BINARY":
                # Try to decode base64
                try:
                    decoded = base64.b64decode(payload)
                    # Try to display as text if possible
                    return f"[BINARY: {len(decoded)} bytes] {decoded[:50].decode('utf-8', errors='ignore')}"
                except:
                    return f"[BINARY: {payload[:50]}...]"
            elif data_format == "JSON":
                # Try to pretty-print JSON
                try:
                    data = json.loads(payload)
                    # If it's a large object, show first 100 chars
                    json_str = json.dumps(data, separators=(',', ':'))
                    if len(json_str) > 100:
                        return json_str[:100] + "..."
                    return json_str
                except:
                    # Not valid JSON, show as-is
                    if len(payload) > 100:
                        return payload[:100] + "..."
                    return payload
            else:
                # Default format
                if len(payload) > 100:
                    return payload[:100] + "..."
                return payload
        except:
            return payload[:100] + "..." if len(payload) > 100 else payload

    async def unsubscribe(self):
        """Unsubscribe from topic updates"""
        if self.websocket:
            complete_message = {
                "id": self.subscription_id,
                "type": "complete"
            }
            await self.websocket.send(json.dumps(complete_message))
            await self.websocket.close()
            print(f"✓ Disconnected (received {self.message_count} messages)\n")


async def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Subscribe to MonsterMQ MQTT topics via GraphQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Subscribe to multiple device metrics topics
  %(prog)s --filters "device/+/metrics" "device/+/status" "device/+/error"

  # Monitor all sensor data with wildcard
  %(prog)s --filters "sensor/#"

  # Monitor system topics
  %(prog)s --filters "$SYS/#"

  # Monitor multiple independent topic hierarchies
  %(prog)s --filters "temperature/+" "humidity/+" "pressure/+"

  # Subscribe to single topic
  %(prog)s --filters "sensor/temperature"

  # Use binary format for image/binary data
  %(prog)s --format BINARY --filters "camera/snapshot"

  # Monitor all topics
  %(prog)s --filters "#"
        """
    )

    parser.add_argument(
        "--url",
        default="ws://localhost:4000/graphqlws",
        help="GraphQL WebSocket URL (default: ws://localhost:4000/graphqlws)"
    )
    parser.add_argument(
        "--filters",
        nargs="+",
        default=["#"],
        help="MQTT topic filters to monitor (supports + and # wildcards). Default: # (all topics)"
    )
    parser.add_argument(
        "--format",
        choices=["JSON", "BINARY"],
        default="JSON",
        help="Data format for payloads (default: JSON)"
    )

    args = parser.parse_args()

    print("\n" + "="*90)
    print("MonsterMQ GraphQL Topic Subscription Client")
    print("="*90)
    print(f"Endpoint: {args.url}")
    print(f"Format:   {args.format}")
    print(f"Filters:  {', '.join(args.filters)}")
    print("="*90 + "\n")

    client = GraphQLTopicSubscriptionClient(args.url)

    try:
        await client.connect()
        await client.initialize()
        await client.subscribe(
            topic_filters=args.filters,
            data_format=args.format
        )
        await client.listen()

    except websockets.exceptions.WebSocketException as e:
        print(f"\n❌ WebSocket error: {e}\n")
        print("Make sure the MonsterMQ broker is running with GraphQL enabled.")
        print("Expected endpoint: ws://localhost:4000/graphqlws\n")
        print("Configuration in config.yaml:")
        print("  GraphQL:")
        print("    Enabled: true")
        print("    Port: 4000        # Or your configured port")
        print("    Path: /graphql    # Or your configured path\n")
        sys.exit(1)

    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⏹ Stopped by user\n")
        sys.exit(0)
