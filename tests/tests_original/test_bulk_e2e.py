#!/usr/bin/env python3
"""
End-to-End test for MonsterMQ GraphQL Bulk Topic Subscriptions

This script demonstrates bulk subscriptions by:
1. Publishing test messages to MQTT topics
2. Subscribing to bulk topic updates simultaneously
3. Showing how messages are collected and batched

This helps verify that the bulk subscription feature works correctly,
collecting multiple rapid updates into single batches.

Requirements:
    pip install websockets requests

Usage:
    # Test with default settings (1000ms timeout, 100 message max)
    python test_bulk_e2e.py

    # Test with custom broker endpoint
    python test_bulk_e2e.py --url ws://mybroker:4000/graphqlws

    # Test with fast batching (collect quickly)
    python test_bulk_e2e.py --timeout 500 --max-size 10

    # Test with slower batching (wait longer or collect more)
    python test_bulk_e2e.py --timeout 3000 --max-size 200
"""

import asyncio
import json
import sys
import argparse
import websockets
import requests
import time
import os
from datetime import datetime

# Configuration from environment variables with defaults
GRAPHQL_WS_URL = os.getenv("GRAPHQL_WS_URL", "ws://localhost:4000/graphqlws")
GRAPHQL_URL = os.getenv("GRAPHQL_URL", "http://localhost:4000/graphql")

class BulkSubscriptionTester:
    """Test bulk subscriptions with simultaneous message publishing"""

    def __init__(self, url=None, http_url=None,
                 timeout_ms=1000, max_size=100):
        url = url or GRAPHQL_WS_URL
        http_url = http_url or GRAPHQL_URL
        self.ws_url = url
        self.http_url = http_url
        self.timeout_ms = timeout_ms
        self.max_size = max_size
        self.websocket = None
        self.batch_count = 0
        self.total_messages = 0
        self.subscription_id = "1"

    async def connect(self):
        """Connect to GraphQL WebSocket endpoint"""
        print(f"🔗 Connecting to {self.ws_url}...")
        self.websocket = await websockets.connect(
            self.ws_url,
            subprotocols=["graphql-transport-ws"]
        )
        print("✓ Connected")

    async def initialize(self):
        """Initialize WebSocket connection"""
        init_msg = {"type": "connection_init", "payload": {}}
        await self.websocket.send(json.dumps(init_msg))
        response = json.loads(await self.websocket.recv())
        if response.get("type") == "connection_ack":
            print("✓ Connection initialized")
        else:
            raise Exception(f"Failed to initialize: {response}")

    async def subscribe_to_bulk(self, topic_filters):
        """Subscribe to bulk topic updates"""
        query = """\
        subscription BulkTest($filters: [String!]!, $timeoutMs: Int!, $maxSize: Int!) {
            topicUpdatesBulk(topicFilters: $filters, timeoutMs: $timeoutMs, maxSize: $maxSize) {
                updates {
                    topic
                    payload
                    timestamp
                    clientId
                }
                count
                timestamp
            }
        }
        """

        msg = {
            "id": self.subscription_id,
            "type": "subscribe",
            "payload": {
                "query": query,
                "variables": {
                    "filters": topic_filters,
                    "timeoutMs": self.timeout_ms,
                    "maxSize": self.max_size
                }
            }
        }

        await self.websocket.send(json.dumps(msg))
        print(f"\n📦 Subscribed to bulk updates:")
        print(f"   Topic filters: {topic_filters}")
        print(f"   Timeout: {self.timeout_ms}ms")
        print(f"   Max size: {self.max_size} messages\n")

    def publish_message(self, topic, payload):
        """Publish a message via HTTP GraphQL mutation"""
        mutation = """\
        mutation PublishMessage($input: PublishInput!) {
            publish(input: $input) {
                success
                topic
                timestamp
            }
        }
        """

        variables = {
            "input": {
                "topic": topic,
                "payload": payload,
                "qos": 0,
                "retained": False,
                "format": "JSON"
            }
        }

        try:
            response = requests.post(
                self.http_url,
                json={"query": mutation, "variables": variables},
                headers={"Content-Type": "application/json"},
                timeout=5
            )
            if response.status_code == 200:
                result = response.json()
                if "errors" not in result and result.get("data", {}).get("publish", {}).get("success"):
                    return True
        except Exception as e:
            print(f"❌ Publish error: {e}")
        return False

    async def listen_for_batches(self, duration_seconds=30):
        """Listen for bulk update batches"""
        print(f"📡 Listening for {duration_seconds} seconds...\n")
        print("=" * 90)

        start_time = time.time()
        try:
            while time.time() - start_time < duration_seconds:
                try:
                    message = await asyncio.wait_for(self.websocket.recv(), timeout=1.0)
                    msg = json.loads(message)

                    if msg.get("type") == "next":
                        bulk = msg.get("payload", {}).get("data", {}).get("topicUpdatesBulk")
                        if bulk:
                            self.print_batch(bulk)

                    elif msg.get("type") == "error":
                        print(f"❌ Subscription error: {msg.get('payload')}")

                except asyncio.TimeoutError:
                    continue

        except Exception as e:
            print(f"❌ Listen error: {e}")

    def print_batch(self, bulk):
        """Print batch information"""
        self.batch_count += 1
        count = bulk.get("count", 0)
        self.total_messages += count
        timestamp = bulk.get("timestamp", 0)

        # Format timestamp
        try:
            if isinstance(timestamp, (int, float)) and timestamp > 0:
                dt = datetime.fromtimestamp(timestamp / 1000.0)
                time_str = dt.strftime("%H:%M:%S.%f")[:-3]
            else:
                time_str = "?"
        except:
            time_str = "?"

        print(f"\n📦 Batch #{self.batch_count} [{time_str}] - {count} messages:")
        print("-" * 90)

        updates = bulk.get("updates", [])
        for i, update in enumerate(updates, 1):
            topic = update.get("topic", "?")
            payload = update.get("payload", "")[:50]
            msg_timestamp = update.get("timestamp", 0)

            try:
                if isinstance(msg_timestamp, (int, float)) and msg_timestamp > 0:
                    dt = datetime.fromtimestamp(msg_timestamp / 1000.0)
                    msg_time = dt.strftime("%H:%M:%S.%f")[:-3]
                else:
                    msg_time = "?"
            except:
                msg_time = "?"

            print(f"  {i:2d}. [{msg_time}] {topic:40s} → {payload}")

        print("-" * 90)

    async def run_test(self, test_duration=30, publish_interval=0.05, num_messages=None):
        """Run the full test"""
        try:
            await self.connect()
            await self.initialize()
            await self.subscribe_to_bulk(["test/bulk/+"])

            # Start publishing messages in background
            print("📤 Publishing test messages every 50ms...")
            publish_task = asyncio.create_task(
                self.publisher_loop(num_messages, publish_interval)
            )

            # Listen for batches
            await self.listen_for_batches(test_duration)

            # Cancel publisher
            publish_task.cancel()
            try:
                await publish_task
            except asyncio.CancelledError:
                pass

        except Exception as e:
            print(f"❌ Test error: {e}")
            import traceback
            traceback.print_exc()

        # Print summary
        print("\n" + "=" * 90)
        print(f"✓ Test Summary:")
        print(f"  Total batches received: {self.batch_count}")
        print(f"  Total messages received: {self.total_messages}")
        if self.batch_count > 0:
            avg_per_batch = self.total_messages / self.batch_count
            print(f"  Average messages per batch: {avg_per_batch:.1f}")
        print("=" * 90 + "\n")

    async def publisher_loop(self, num_messages, interval):
        """Publish test messages at regular intervals"""
        count = 0
        topics = ["test/bulk/sensor1", "test/bulk/sensor2", "test/bulk/sensor3"]

        try:
            while num_messages is None or count < num_messages:
                topic = topics[count % len(topics)]
                payload = json.dumps({
                    "value": 20 + (count % 10),
                    "timestamp": datetime.now().isoformat(),
                    "sequence": count
                })

                if self.publish_message(topic, payload):
                    count += 1
                    if count % 10 == 0:
                        print(f"  Published {count} messages")

                await asyncio.sleep(interval)

        except asyncio.CancelledError:
            print(f"\n✓ Publisher stopped after {count} messages")


async def main():
    """Main test function"""
    parser = argparse.ArgumentParser(
        description="End-to-end test for GraphQL bulk topic subscriptions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  # Basic test with defaults
  %(prog)s

  # Test with custom endpoints
  %(prog)s --ws ws://mybroker:4000/graphqlws --http http://mybroker:4000/graphql

  # Test with fast batching (collect quickly)
  %(prog)s --timeout 500 --max-size 10

  # Test with slow batching (wait longer)
  %(prog)s --timeout 3000 --max-size 200

  # Publish specific number of messages
  %(prog)s --num-messages 500 --duration 60
        """
    )

    parser.add_argument(
        "--ws",
        dest="ws_url",
        default=GRAPHQL_WS_URL,
        help=f"GraphQL WebSocket URL (default: {GRAPHQL_WS_URL})"
    )
    parser.add_argument(
        "--http",
        dest="http_url",
        default=GRAPHQL_URL,
        help=f"GraphQL HTTP URL (default: {GRAPHQL_URL})"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=1000,
        help="Batch timeout in ms (default: 1000)"
    )
    parser.add_argument(
        "--max-size",
        type=int,
        default=100,
        help="Max messages per batch (default: 100)"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=30,
        help="Test duration in seconds (default: 30)"
    )
    parser.add_argument(
        "--num-messages",
        type=int,
        default=None,
        help="Number of messages to publish (default: unlimited for duration)"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.05,
        help="Message publish interval in seconds (default: 0.05 = 20 msg/sec)"
    )

    args = parser.parse_args()

    print("\n" + "=" * 90)
    print("MonsterMQ GraphQL Bulk Subscriptions - End-to-End Test")
    print("=" * 90)
    print(f"WebSocket URL:     {args.ws_url}")
    print(f"HTTP URL:          {args.http_url}")
    print(f"Timeout:           {args.timeout}ms")
    print(f"Max Size:          {args.max_size} messages")
    print(f"Test Duration:     {args.duration}s")
    print(f"Publish Interval:  {args.interval}s ({1/args.interval:.0f} msg/sec)")
    if args.num_messages:
        print(f"Num Messages:      {args.num_messages}")
    print("=" * 90 + "\n")

    tester = BulkSubscriptionTester(
        url=args.ws_url,
        http_url=args.http_url,
        timeout_ms=args.timeout,
        max_size=args.max_size
    )

    await tester.run_test(
        test_duration=args.duration,
        publish_interval=args.interval,
        num_messages=args.num_messages
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹ Test interrupted by user\n")
        sys.exit(0)
