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

Keyboard commands (no Enter needed):
    - 's' - Subscribe to topics (re-subscribe if previously unsubscribed)
    - 'u' - Unsubscribe (keep connection open, can re-subscribe with 's')
    - 'q' - Quit program

Note: WebSocket connection remains open after unsubscribe, allowing re-subscription.
"""

import asyncio
import json
import sys
import argparse
import websockets
import base64
import threading
import select
import os
import tty
import termios
from datetime import datetime
from typing import Optional, List

# Configuration from environment variables with defaults
GRAPHQL_WS_URL = os.getenv("GRAPHQL_WS_URL", "ws://localhost:4000/graphqlws")

class GraphQLTopicSubscriptionClient:
    """Client for subscribing to MonsterMQ MQTT topic updates via GraphQL WebSocket"""

    def __init__(self, url=None):
        url = url or GRAPHQL_WS_URL
        self.url = url
        self.websocket = None
        self.subscription_id = "1"
        self.message_count = 0
        self.unsubscribe_event = asyncio.Event()  # Signal to unsubscribe
        self.subscribe_event = asyncio.Event()  # Signal to subscribe again
        self.exit_event = asyncio.Event()  # Signal to exit
        self.input_thread = None
        self.is_subscribed = False  # Track subscription state
        self.topic_filters = []  # Store topic filters for re-subscription
        self.data_format = "JSON"  # Store format for re-subscription
        self.original_settings = None  # Store original terminal settings

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
        print("Waiting for messages...\n")
        print("  's' - Subscribe to topics")
        print("  'u' - Unsubscribe (keep connection open)")
        print("  'q' - Quit program\n")

    def _set_raw_mode(self):
        """Set terminal to cbreak mode for keystroke input (keeps line handling intact)"""
        if sys.platform == 'win32':
            return
        try:
            fd = sys.stdin.fileno()
            self.original_settings = termios.tcgetattr(fd)
            # Use cbreak mode: single char input without Enter, but keeps line discipline
            new_settings = list(self.original_settings)
            new_settings[3] &= ~(termios.ICANON | termios.ECHO)  # Disable canonical mode and echo
            new_settings[6][termios.VMIN] = 0   # Non-blocking
            new_settings[6][termios.VTIME] = 0  # No timeout
            termios.tcsetattr(fd, termios.TCSADRAIN, new_settings)
        except:
            pass

    def _restore_terminal(self):
        """Restore terminal to original settings"""
        if sys.platform == 'win32' or self.original_settings is None:
            return
        try:
            fd = sys.stdin.fileno()
            termios.tcsetattr(fd, termios.TCSADRAIN, self.original_settings)
        except:
            pass

    def _input_listener(self, loop):
        """Background thread that listens for keyboard input"""
        if sys.platform == 'win32':
            import msvcrt
            try:
                while True:
                    if msvcrt.kbhit():
                        key = msvcrt.getch().decode().lower()
                        self._handle_key(key, loop)
                    threading.Event().wait(0.05)
            except:
                pass
        else:
            # Unix/Linux/Mac - use tty raw mode for non-blocking input
            self._set_raw_mode()
            try:
                while True:
                    # Check if stdin has data available (non-blocking)
                    ready, _, _ = select.select([sys.stdin], [], [], 0.1)
                    if ready:
                        key = sys.stdin.read(1).lower()
                        if key:
                            self._handle_key(key, loop)
            except:
                pass
            finally:
                self._restore_terminal()

    def _handle_key(self, key, loop):
        """Handle keyboard input"""
        if key == 's':
            if not self.is_subscribed:
                print("📡 Subscribing...", flush=True)
                asyncio.run_coroutine_threadsafe(
                    self._trigger_subscribe(),
                    loop
                )
        elif key == 'u':
            if self.is_subscribed:
                print("⏹ Unsubscribing...", flush=True)
                asyncio.run_coroutine_threadsafe(
                    self._trigger_unsubscribe(),
                    loop
                )
        elif key == 'q':
            print("⏹ Exiting...", flush=True)
            asyncio.run_coroutine_threadsafe(
                self._trigger_exit(),
                loop
            )

    async def _trigger_subscribe(self):
        """Trigger subscribe from another thread"""
        self.subscribe_event.set()

    async def _trigger_unsubscribe(self):
        """Trigger unsubscribe from another thread"""
        self.unsubscribe_event.set()

    async def _trigger_exit(self):
        """Trigger exit from another thread"""
        self.exit_event.set()

    async def listen(self):
        """Listen for incoming topic update messages"""
        # Store topic filters for re-subscription
        self.topic_filters = self.topic_filters if self.topic_filters else ["#"]

        # Start background thread for keyboard input
        loop = asyncio.get_event_loop()
        self.input_thread = threading.Thread(target=self._input_listener, args=(loop,), daemon=True)
        self.input_thread.start()

        try:
            while True:
                # Check if exit was requested
                if self.exit_event.is_set():
                    print(f"\n✓ Exiting... (received {self.message_count} messages total)\n", flush=True)
                    break

                # Check if unsubscribe was requested
                if self.unsubscribe_event.is_set() and self.is_subscribed:
                    self.unsubscribe_event.clear()
                    self.is_subscribed = False
                    print(f"\n✓ Unsubscribed!\n", flush=True)
                    await self.unsubscribe()
                    continue

                # Check if subscribe was requested
                if self.subscribe_event.is_set() and not self.is_subscribed:
                    self.subscribe_event.clear()
                    self.is_subscribed = True
                    print(f"\n✓ Subscribed!\n", flush=True)
                    await self.subscribe(self.topic_filters, self.data_format)
                    continue

                try:
                    # Wait for message with timeout
                    message = await asyncio.wait_for(self.websocket.recv(), timeout=0.5)
                    msg = json.loads(message)

                    if msg.get("type") == "next":
                        # Extract topic update from the message
                        topic_update = msg.get("payload", {}).get("data", {}).get("multiTopicUpdates")
                        if topic_update and self.is_subscribed:
                            self.message_count += 1
                            self.print_topic_update(topic_update)

                    elif msg.get("type") == "error":
                        print(f"\n❌ Error: {msg.get('payload')}\n", flush=True)

                    elif msg.get("type") == "complete":
                        if self.is_subscribed:
                            self.is_subscribed = False
                            print(f"\n✓ Subscription ended\n", flush=True)

                except asyncio.TimeoutError:
                    continue

        except websockets.exceptions.ConnectionClosed:
            print(f"\n⚠ Connection closed by server (received {self.message_count} messages)\n", flush=True)
        except KeyboardInterrupt:
            print(f"\n⏹ Stopping... (received {self.message_count} messages)\n", flush=True)
            if self.is_subscribed:
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
        print(f"{color}[{time_str}]{reset} {topic} (QoS:{qos}, Fmt:{data_format}) {flags_str}", flush=True)

        # Print payload
        print(f"  └─ Payload: {payload_display}", flush=True)

        # Print client ID if available
        if client_id:
            print(f"  └─ Client: {client_id}", flush=True)

        print(flush=True)  # Empty line between messages

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
        """Unsubscribe from topic updates (keeps connection open)"""
        if self.websocket:
            complete_message = {
                "id": self.subscription_id,
                "type": "complete"
            }
            await self.websocket.send(json.dumps(complete_message))
            # DO NOT CLOSE - keep connection open for re-subscription


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
        default=GRAPHQL_WS_URL,
        help=f"GraphQL WebSocket URL (default: {GRAPHQL_WS_URL})"
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
    # Store filters and format for potential re-subscription
    client.topic_filters = args.filters
    client.data_format = args.format

    try:
        await client.connect()
        await client.initialize()
        client.is_subscribed = True
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

    finally:
        # Ensure terminal is restored
        if 'client' in locals():
            client._restore_terminal()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⏹ Stopped by user\n")
    finally:
        # Restore terminal to cooked mode on any exit
        if sys.platform != 'win32':
            try:
                import subprocess
                subprocess.run(['stty', 'sane'], check=False)
            except:
                pass
