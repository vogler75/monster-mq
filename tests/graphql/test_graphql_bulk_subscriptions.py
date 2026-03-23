#!/usr/bin/env python3
"""
Test script for MonsterMQ GraphQL Bulk Topic Subscriptions

This script connects to the MonsterMQ GraphQL WebSocket endpoint and subscribes
to bulk message collections from MQTT topics.

The topicUpdatesBulk subscription collects multiple message updates and emits
them as a batch, reducing network overhead for high-frequency message streams.

Requirements:
    pip install websockets

Usage:
    # Collect 100 messages or wait 2000ms, whichever comes first
    python test_graphql_bulk_subscriptions.py --filters "opcua/plc01/tags/#" --timeout 2000 --max-size 100

    # Fast collection with smaller batches (500ms or 10 messages)
    python test_graphql_bulk_subscriptions.py --filters "sensor/#" --timeout 500 --max-size 10

    # Large batches for efficient transfer (5000ms or 1000 messages)
    python test_graphql_bulk_subscriptions.py --filters "device/#" --timeout 5000 --max-size 1000

    # Multiple topic filters with bulk collection
    python test_graphql_bulk_subscriptions.py --filters "temperature/+" "humidity/+" --timeout 1000 --max-size 50

Keyboard commands (no Enter needed):
    - 's' - Subscribe to bulk topic updates
    - 'u' - Unsubscribe (keep connection open)
    - 'q' - Quit program
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
import subprocess
import pytest
from datetime import datetime
from typing import List

# Configuration from environment variables with defaults
GRAPHQL_WS_URL = os.getenv("GRAPHQL_WS_URL", "ws://localhost:4000/graphqlws")

class GraphQLBulkSubscriptionClient:
    """Client for subscribing to MonsterMQ MQTT bulk topic updates via GraphQL WebSocket"""

    def __init__(self, url=None):
        url = url or GRAPHQL_WS_URL
        self.url = url
        self.websocket = None
        self.subscription_id = "1"
        self.batch_count = 0
        self.total_messages = 0
        self.unsubscribe_event = asyncio.Event()
        self.subscribe_event = asyncio.Event()
        self.exit_event = asyncio.Event()
        self.input_thread = None
        self.is_subscribed = False
        self.topic_filters = []
        self.data_format = "JSON"
        self.timeout_ms = 1000
        self.max_size = 100
        self.original_settings = None

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
        init_message = {
            "type": "connection_init",
            "payload": {}
        }
        await self.websocket.send(json.dumps(init_message))

        response = await self.websocket.recv()
        msg = json.loads(response)

        if msg.get("type") == "connection_ack":
            print("✓ Connection acknowledged by server")
        else:
            print(f"⚠ Unexpected response: {msg}")

    async def subscribe(self, topic_filters: List[str], data_format: str = "JSON",
                       timeout_ms: int = 1000, max_size: int = 100):
        """
        Subscribe to bulk topic updates with configurable batching

        Args:
            topic_filters: List of MQTT topic filters to monitor
            data_format: Data format for payloads ("JSON" or "BINARY")
            timeout_ms: Timeout in milliseconds for batch collection
            max_size: Maximum messages per batch
        """

        query = """\
        subscription BulkTopicUpdates($filters: [String!]!, $format: DataFormat, $timeoutMs: Int!, $maxSize: Int!) {
            topicUpdatesBulk(topicFilters: $filters, format: $format, timeoutMs: $timeoutMs, maxSize: $maxSize) {
                updates {
                    topic
                    payload
                    format
                    timestamp
                    qos
                    retained
                    clientId
                }
                count
                timestamp
            }
        }
        """

        variables = {
            "filters": topic_filters,
            "format": data_format,
            "timeoutMs": timeout_ms,
            "maxSize": max_size
        }

        subscribe_message = {
            "id": self.subscription_id,
            "type": "subscribe",
            "payload": {
                "query": query,
                "variables": variables
            }
        }

        await self.websocket.send(json.dumps(subscribe_message))

        print("\n" + "="*90)
        print("📦 Bulk Subscription started")
        print("="*90)
        print(f"Format:        {data_format}")
        print(f"Timeout:       {timeout_ms}ms")
        print(f"Max Size:      {max_size} messages")
        print(f"Topic Filters: {len(topic_filters)} filter(s)")
        for i, tf in enumerate(topic_filters, 1):
            print(f"  {i}. {tf}")
        print("="*90 + "\n")
        print("Waiting for message batches...\n")
        print("  's' - Subscribe to bulk updates")
        print("  'u' - Unsubscribe (keep connection open)")
        print("  'q' - Quit program\n")

    def _set_raw_mode(self):
        """Set terminal to cbreak mode for keystroke input"""
        if sys.platform == 'win32':
            return
        try:
            fd = sys.stdin.fileno()
            self.original_settings = termios.tcgetattr(fd)
            new_settings = list(self.original_settings)
            new_settings[3] &= ~(termios.ICANON | termios.ECHO)
            new_settings[6][termios.VMIN] = 0
            new_settings[6][termios.VTIME] = 0
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
        """Background thread for keyboard input"""
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
            self._set_raw_mode()
            try:
                while True:
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
                print("📦 Subscribing...", flush=True)
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
        """Listen for incoming bulk update messages"""
        self.topic_filters = self.topic_filters if self.topic_filters else ["#"]

        loop = asyncio.get_event_loop()
        self.input_thread = threading.Thread(target=self._input_listener, args=(loop,), daemon=True)
        self.input_thread.start()

        try:
            while True:
                if self.exit_event.is_set():
                    print(f"\n✓ Exiting... (received {self.batch_count} batches, {self.total_messages} total messages)\n", flush=True)
                    break

                if self.unsubscribe_event.is_set() and self.is_subscribed:
                    self.unsubscribe_event.clear()
                    self.is_subscribed = False
                    print(f"\n✓ Unsubscribed!\n", flush=True)
                    await self.unsubscribe()
                    continue

                if self.subscribe_event.is_set() and not self.is_subscribed:
                    self.subscribe_event.clear()
                    self.is_subscribed = True
                    print(f"\n✓ Subscribed!\n", flush=True)
                    await self.subscribe(
                        self.topic_filters,
                        self.data_format,
                        self.timeout_ms,
                        self.max_size
                    )
                    continue

                try:
                    message = await asyncio.wait_for(self.websocket.recv(), timeout=0.5)
                    msg = json.loads(message)

                    if msg.get("type") == "next":
                        bulk_update = msg.get("payload", {}).get("data", {}).get("topicUpdatesBulk")
                        if bulk_update and self.is_subscribed:
                            self.batch_count += 1
                            self.total_messages += bulk_update.get("count", 0)
                            self.print_bulk_update(bulk_update)

                    elif msg.get("type") == "error":
                        print(f"\n❌ Error: {msg.get('payload')}\n", flush=True)

                    elif msg.get("type") == "complete":
                        if self.is_subscribed:
                            self.is_subscribed = False
                            print(f"\n✓ Subscription ended\n", flush=True)

                except asyncio.TimeoutError:
                    continue

        except websockets.exceptions.ConnectionClosed:
            print(f"\n⚠ Connection closed by server (received {self.batch_count} batches)\n", flush=True)
        except KeyboardInterrupt:
            print(f"\n⏹ Stopping... (received {self.batch_count} batches)\n", flush=True)
            if self.is_subscribed:
                await self.unsubscribe()

    def print_bulk_update(self, bulk: dict):
        """Pretty print a bulk update message"""
        count = bulk.get("count", 0)
        timestamp = bulk.get("timestamp", 0)
        updates = bulk.get("updates", [])

        # Format timestamp
        try:
            if isinstance(timestamp, (int, float)) and timestamp > 0:
                dt = datetime.fromtimestamp(timestamp / 1000.0)
                time_str = dt.strftime("%H:%M:%S.%f")[:-3]
            else:
                time_str = str(timestamp)
        except:
            time_str = str(timestamp)

        # Print batch header
        print(f"\n📦 Batch #{self.batch_count} - {count} messages [{time_str}]")
        print("=" * 90)

        # Print each message in the batch
        for i, update in enumerate(updates, 1):
            topic = update.get("topic", "?")
            payload = update.get("payload", "")
            fmt = update.get("format", "UNKNOWN")
            qos = update.get("qos", 0)
            retained = update.get("retained", False)
            msg_timestamp = update.get("timestamp", 0)
            client_id = update.get("clientId")

            # Format message timestamp
            try:
                if isinstance(msg_timestamp, (int, float)) and msg_timestamp > 0:
                    dt = datetime.fromtimestamp(msg_timestamp / 1000.0)
                    msg_time_str = dt.strftime("%H:%M:%S.%f")[:-3]
                else:
                    msg_time_str = str(msg_timestamp)
            except:
                msg_time_str = str(msg_timestamp)

            # Format payload
            if not payload:
                payload_display = "[empty]"
            elif len(payload) > 60:
                payload_display = payload[:60] + "..."
            else:
                payload_display = payload

            # Print message
            flags = "[R]" if retained else ""
            print(f"  {i}. [{msg_time_str}] {topic} (QoS:{qos}, Fmt:{fmt}) {flags}")
            print(f"     └─ {payload_display}")
            if client_id:
                print(f"     └─ Client: {client_id}")

        print("=" * 90)

    async def unsubscribe(self):
        """Unsubscribe from topic updates (keeps connection open)"""
        if self.websocket:
            complete_message = {
                "id": self.subscription_id,
                "type": "complete"
            }
            await self.websocket.send(json.dumps(complete_message))


async def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Subscribe to MonsterMQ MQTT bulk topic updates via GraphQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  # Collect up to 100 messages or wait 2000ms
  %(prog)s --filters "opcua/plc01/tags/#" --timeout 2000 --max-size 100

  # Fast collection with smaller batches
  %(prog)s --filters "sensor/#" --timeout 500 --max-size 10

  # Multiple topic filters
  %(prog)s --filters "temperature/+" "humidity/+" "pressure/+" --timeout 1000 --max-size 50

  # Monitor all topics with large batches
  %(prog)s --filters "#" --timeout 5000 --max-size 1000
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
        help="MQTT topic filters to monitor (default: #)"
    )
    parser.add_argument(
        "--format",
        choices=["JSON", "BINARY"],
        default="JSON",
        help="Data format (default: JSON)"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=1000,
        help="Batch timeout in milliseconds (default: 1000)"
    )
    parser.add_argument(
        "--max-size",
        type=int,
        default=100,
        help="Maximum messages per batch (default: 100)"
    )

    args = parser.parse_args()

    print("\n" + "="*90)
    print("MonsterMQ GraphQL Bulk Topic Subscription Client")
    print("="*90)
    print(f"Endpoint:      {args.url}")
    print(f"Format:        {args.format}")
    print(f"Timeout:       {args.timeout}ms")
    print(f"Max Size:      {args.max_size} messages")
    print(f"Filters:       {', '.join(args.filters)}")
    print("="*90 + "\n")

    client = GraphQLBulkSubscriptionClient(args.url)
    client.topic_filters = args.filters
    client.data_format = args.format
    client.timeout_ms = args.timeout
    client.max_size = args.max_size

    try:
        await client.connect()
        await client.initialize()
        client.is_subscribed = True
        await client.subscribe(
            topic_filters=args.filters,
            data_format=args.format,
            timeout_ms=args.timeout,
            max_size=args.max_size
        )
        await client.listen()

    except websockets.exceptions.WebSocketException as e:
        print(f"\n❌ WebSocket error: {e}\n")
        print("Make sure the MonsterMQ broker is running with GraphQL enabled.")
        print("Expected endpoint: ws://localhost:4000/graphqlws\n")
        sys.exit(1)

    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        if 'client' in locals():
            client._restore_terminal()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⏹ Stopped by user\n")
    finally:
        # Restore terminal on any exit
        if sys.platform != 'win32':
            try:
                subprocess.run(['stty', 'sane'], check=False)
            except:
                pass
