#!/usr/bin/env python3
"""
Test script for publishing messages via MonsterMQ GraphQL

This helper script publishes test messages to MonsterMQ MQTT topics via GraphQL mutations.
Use this in conjunction with the subscription test scripts to verify end-to-end functionality.

Requirements:
    pip install requests

Usage:
    # Publish a single JSON message
    python test_graphql_publisher.py --topic "test/message" --payload '{"value": 42}'

    # Publish multiple messages with delay
    python test_graphql_publisher.py --topic "sensor/temperature" --payload '23.5' --repeat 5 --delay 1

    # Publish with custom settings
    python test_graphql_publisher.py --topic "device/status" --payload "online" --qos 1 --retained

    # Publish to multiple topics
    python test_graphql_publisher.py --topics "temp/room1" "temp/room2" "temp/room3" --payload '22.5'

    # Publish binary data (base64 encoded)
    python test_graphql_publisher.py --topic "camera/snapshot" --payload "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJ" --format BINARY
"""

import requests
import json
import time
import sys
import os
import argparse
from datetime import datetime

# Configuration from environment variables with defaults
GRAPHQL_URL = os.getenv("GRAPHQL_URL", "http://localhost:4000/graphql")

class GraphQLPublisher:
    """Client for publishing messages to MonsterMQ via GraphQL"""

    def __init__(self, url=None):
        url = url or GRAPHQL_URL
        self.url = url
        self.headers = {
            "Content-Type": "application/json"
        }
        self.message_count = 0

    def publish(self, topic: str, payload: str, qos: int = 0, retained: bool = False,
                data_format: str = "JSON") -> bool:
        """
        Publish a single message via GraphQL mutation

        Args:
            topic: MQTT topic to publish to
            payload: Message payload
            qos: Quality of Service (0, 1, or 2)
            retained: Whether to retain the message
            data_format: Data format ("JSON" or "BINARY")

        Returns:
            True if successful, False otherwise
        """

        mutation = """
        mutation PublishMessage($input: PublishInput!) {
            publish(input: $input) {
                success
                topic
                timestamp
                error
            }
        }
        """

        variables = {
            "input": {
                "topic": topic,
                "payload": payload,
                "qos": qos,
                "retained": retained,
                "format": data_format
            }
        }

        payload_json = {
            "query": mutation,
            "variables": variables
        }

        try:
            response = requests.post(self.url, json=payload_json, headers=self.headers)
            response.raise_for_status()

            result = response.json()

            # Check for GraphQL errors
            if "errors" in result:
                print(f"❌ GraphQL Error: {result['errors']}")
                return False

            # Check mutation result
            data = result.get("data", {}).get("publish", {})
            if data.get("success"):
                timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                print(f"✓ [{timestamp}] Published to '{topic}' (QoS:{qos}, Retained:{retained})")
                print(f"  └─ Payload: {payload[:100]}{'...' if len(payload) > 100 else ''}")
                self.message_count += 1
                return True
            else:
                print(f"❌ Publish failed: {data.get('error', 'Unknown error')}")
                return False

        except requests.exceptions.RequestException as e:
            print(f"❌ HTTP Error: {e}")
            return False
        except json.JSONDecodeError:
            print(f"❌ Invalid JSON response: {response.text[:200]}")
            return False
        except Exception as e:
            print(f"❌ Error: {e}")
            return False

    def publish_batch(self, messages: list) -> int:
        """
        Publish multiple messages

        Args:
            messages: List of dicts with keys: topic, payload, qos, retained, format

        Returns:
            Number of successfully published messages
        """

        success_count = 0
        for msg in messages:
            if self.publish(
                topic=msg.get("topic"),
                payload=msg.get("payload", ""),
                qos=msg.get("qos", 0),
                retained=msg.get("retained", False),
                data_format=msg.get("format", "JSON")
            ):
                success_count += 1
            time.sleep(0.1)  # Small delay between publishes

        return success_count


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Publish messages to MonsterMQ via GraphQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Publish a single message
  %(prog)s --topic "test/message" --payload "Hello MQTT"

  # Publish with QoS and retained flag
  %(prog)s --topic "device/status" --payload "online" --qos 1 --retained

  # Publish JSON data
  %(prog)s --topic "sensor/temp" --payload '{"value": 23.5, "unit": "celsius"}'

  # Publish multiple times with delay
  %(prog)s --topic "heartbeat" --payload "ping" --repeat 3 --delay 2

  # Publish to multiple topics (same payload)
  %(prog)s --topics "device/1/temp" "device/2/temp" "device/3/temp" --payload "25"

  # Publish binary data
  %(prog)s --topic "camera/snapshot" --payload "base64data..." --format BINARY

  # Monitor mode: publish test messages continuously (useful with subscription tests)
  %(prog)s --monitor --interval 2
        """
    )

    parser.add_argument(
        "--url",
        default="http://localhost:4000/graphql",
        help="GraphQL HTTP URL (default: http://localhost:4000/graphql)"
    )
    parser.add_argument(
        "--topic",
        help="Topic to publish to (single topic)"
    )
    parser.add_argument(
        "--topics",
        nargs="+",
        help="Multiple topics to publish to (same payload)"
    )
    parser.add_argument(
        "--payload",
        default="{}",
        help="Message payload (default: empty JSON object)"
    )
    parser.add_argument(
        "--qos",
        type=int,
        choices=[0, 1, 2],
        default=0,
        help="Quality of Service level (default: 0)"
    )
    parser.add_argument(
        "--retained",
        action="store_true",
        help="Retain message on broker"
    )
    parser.add_argument(
        "--format",
        choices=["JSON", "BINARY"],
        default="JSON",
        help="Data format (default: JSON)"
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Number of times to publish (default: 1)"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0,
        help="Delay between publishes in seconds (default: 0)"
    )
    parser.add_argument(
        "--monitor",
        action="store_true",
        help="Monitor mode: continuously publish test messages"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2,
        help="Interval for monitor mode in seconds (default: 2)"
    )

    args = parser.parse_args()

    print("\n" + "="*80)
    print("MonsterMQ GraphQL Message Publisher")
    print("="*80)
    print(f"Endpoint: {args.url}")
    print("="*80 + "\n")

    publisher = GraphQLPublisher(args.url)

    try:
        # Monitor mode
        if args.monitor:
            print("📡 Monitor mode: publishing test messages continuously")
            print("   (Press Ctrl+C to stop)\n")
            topic_counter = 0
            while True:
                topic = f"test/monitor/message_{topic_counter}"
                payload = json.dumps({
                    "timestamp": datetime.now().isoformat(),
                    "message_number": topic_counter,
                    "status": "ok"
                })
                publisher.publish(topic, payload)
                topic_counter += 1
                time.sleep(args.interval)

        # Single topic with repeats
        elif args.topic:
            print(f"Publishing to topic: {args.topic}\n")
            for i in range(args.repeat):
                success = publisher.publish(
                    topic=args.topic,
                    payload=args.payload,
                    qos=args.qos,
                    retained=args.retained,
                    data_format=args.format
                )
                if i < args.repeat - 1 and args.delay > 0:
                    time.sleep(args.delay)

        # Multiple topics
        elif args.topics:
            print(f"Publishing to {len(args.topics)} topic(s)\n")
            for topic in args.topics:
                publisher.publish(
                    topic=topic,
                    payload=args.payload,
                    qos=args.qos,
                    retained=args.retained,
                    data_format=args.format
                )
                time.sleep(0.1)

        else:
            parser.print_help()
            print("\nError: Either --topic, --topics, or --monitor must be specified\n")
            sys.exit(1)

        print(f"\n✓ Published {publisher.message_count} message(s)\n")

    except websockets.exceptions.WebSocketException as e:
        print(f"\n❌ WebSocket error: {e}\n")
        print("Make sure the MonsterMQ broker is running with GraphQL enabled.\n")
        sys.exit(1)

    except KeyboardInterrupt:
        print(f"\n\n⏹ Stopped by user")
        print(f"✓ Published {publisher.message_count} message(s)\n")
        sys.exit(0)

    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
