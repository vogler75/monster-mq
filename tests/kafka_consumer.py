#!/usr/bin/env python3
import argparse
from kafka import KafkaConsumer

def main():
    parser = argparse.ArgumentParser(description="Simple Kafka Consumer for testing")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers (comma-separated)")
    parser.add_argument("--topic", default="test-topic", help="Name of the Kafka topic to subscribe to")
    parser.add_argument("--group-id", default=None, help="Kafka consumer group ID (defaults to None, running in fan-out/no-group mode)")
    parser.add_argument("--auto-offset-reset", default="latest", choices=["earliest", "latest"], help="Where to start reading if no offset is committed")
    parser.add_argument("--api-version", default=None, help="Kafka API version (e.g. '2.5.0'). Solves compatibility issues with modern brokers.")
    args = parser.parse_args()

    api_ver = None
    if args.api_version:
        try:
            api_ver = tuple(map(int, args.api_version.split(".")))
        except ValueError:
            print(f"Warning: Invalid API version format '{args.api_version}'. Defaulting to auto-negotiate.")

    print(f"Connecting to Kafka bootstrap servers: {args.bootstrap_servers}")
    group_str = f"Group ID: {args.group_id}" if args.group_id else "No Group (Fan-out mode)"
    print(f"Subscribing to topic: {args.topic} ({group_str})")

    # Initialize consumer
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers.split(","),
        group_id=args.group_id,
        auto_offset_reset=args.auto_offset_reset,
        enable_auto_commit=False if args.group_id else True,
        api_version=api_ver,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: v.decode("utf-8") if v else None
    )

    # Force a metadata refresh to verify connection
    try:
        partitions = consumer.partitions_for_topic(args.topic)
        if partitions is not None:
            group_log = f"using group '{args.group_id}'" if args.group_id else "in fan-out mode (no group)"
            print(f"Successfully connected to Kafka {group_log}! Topic '{args.topic}' has partitions: {list(partitions)}")
        else:
            print(f"Successfully connected to Kafka! Topic '{args.topic}' partition metadata could not be fetched (topic might not exist yet).")
    except Exception as e:
        print(f"Warning: Connection verification failed: {e}")

    print("Consumer started. Waiting for messages...")

    was_connected = True
    try:
        while True:
            # Poll for messages (wait up to 1 second)
            message_pack = consumer.poll(timeout_ms=1000)
            
            # Check connection status
            is_connected = consumer.bootstrap_connected()
            if is_connected != was_connected:
                if not is_connected:
                    print("Warning: Lost connection to Kafka broker! Attempting to reconnect...")
                else:
                    print("Success: Reconnected to Kafka broker!")
                was_connected = is_connected

            # Process any retrieved messages
            if message_pack:
                for tp, messages in message_pack.items():
                    for message in messages:
                        print(f"Received message:")
                        print(f"  Topic:     {message.topic}")
                        print(f"  Partition: {message.partition}")
                        print(f"  Offset:    {message.offset}")
                        print(f"  Key:       {message.key}")
                        print(f"  Value:     {message.value}")
                        print("-" * 50)
                
                # Commit offsets manually after processing the batch
                if args.group_id:
                    try:
                        consumer.commit()
                        print("Offsets committed successfully.")
                    except Exception as e:
                        print(f"Warning: Failed to commit offsets: {e}")
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()
        print("Consumer closed.")

if __name__ == "__main__":
    main()
