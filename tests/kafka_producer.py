#!/usr/bin/env python3
import argparse
import time
import json
import random
from kafka import KafkaProducer

def main():
    parser = argparse.ArgumentParser(description="Simple Kafka Producer for testing")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers (comma-separated)")
    parser.add_argument("--topic", default="test-topic", help="Name of the Kafka topic to publish to")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval in seconds between published messages")
    parser.add_argument("--max-keys", type=int, default=10, help="The 'n' value for key generation (keys 1...n)")
    parser.add_argument("--key-base", default="", help="Base key name prefix (e.g., 'Original/PV/')")
    parser.add_argument("--count", type=int, default=0, help="Number of messages to publish (0 for infinite)")
    parser.add_argument("--api-version", default=None, help="Kafka API version (e.g. '2.5.0'). Solves compatibility issues with modern brokers.")
    parser.add_argument("--username", default=None, help="SASL username for authentication")
    parser.add_argument("--password", default=None, help="SASL password for authentication")
    parser.add_argument("--security-protocol", default=None, choices=["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"], help="Security protocol (defaults to SASL_PLAINTEXT if credentials are provided)")
    args = parser.parse_args()

    api_ver = None
    if args.api_version:
        try:
            api_ver = tuple(map(int, args.api_version.split(".")))
        except ValueError:
            print(f"Warning: Invalid API version format '{args.api_version}'. Defaulting to auto-negotiate.")

    # Dynamically build producer configuration
    kafka_kwargs = {
        "bootstrap_servers": args.bootstrap_servers.split(","),
        "api_version": api_ver,
        "key_serializer": lambda k: str(k).encode("utf-8"),
        "value_serializer": lambda v: json.dumps(v).encode("utf-8")
    }

    if args.username and args.password:
        protocol = args.security_protocol or "SASL_PLAINTEXT"
        print(f"Using SASL Authentication ({protocol}) with user: {args.username}")
        kafka_kwargs.update({
            "security_protocol": protocol,
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": args.username,
            "sasl_plain_password": args.password,
        })
    elif args.security_protocol:
        kafka_kwargs["security_protocol"] = args.security_protocol

    print(f"Connecting to Kafka bootstrap servers: {args.bootstrap_servers}")
    # Initialize producer
    producer = KafkaProducer(**kafka_kwargs)

    # Force a metadata refresh to verify connection
    try:
        # Fetching partitions forces a metadata refresh and checks connection
        partitions = producer.partitions_for(args.topic)
        print(f"Successfully connected to Kafka! Topic '{args.topic}' partitions found: {list(partitions) if partitions else 'None (auto-creating)'}")
    except Exception as e:
        print(f"Warning: Connection verification failed: {e}")

    print(f"Publishing to topic: {args.topic}")
    key_pattern = f"{args.key_base}test-<1...{args.max_keys}>"
    print(f"Rotating keys matching pattern: {key_pattern}")
    print(f"Publish interval: {args.interval}s")

    sent_count = 0
    key_counter = 1

    try:
        while True:
            # Generate a payload with some random/incrementing value and a millisecond timestamp
            payload = {
                "sequence": sent_count + 1,
                "value": round(random.uniform(10.0, 100.0), 2),
                "timestamp": int(time.time() * 1000)
            }
            
            # Format key as <key-base>test-<1...n>
            key_str = f"{args.key_base}test-{key_counter}"
            
            try:
                # Send message asynchronously
                future = producer.send(args.topic, key=key_str, value=payload)
                
                # Block or wait for receipt (optional, but good for logging)
                record_metadata = future.get(timeout=10)
                
                print(f"[{sent_count + 1}] Sent key={key_str} payload={payload} (partition={record_metadata.partition}, offset={record_metadata.offset})")
            except Exception as e:
                print(f"[{sent_count + 1}] Error publishing message: {e}")
            
            sent_count += 1
            
            # Increment key rotating from 1...n
            key_counter = (key_counter % args.max_keys) + 1

            if args.count > 0 and sent_count >= args.count:
                print(f"Finished sending {args.count} messages.")
                break

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed.")

if __name__ == "__main__":
    main()
