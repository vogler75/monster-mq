#!/usr/bin/env python3
"""
MQTT Producer - Publishes messages with increasing sequence numbers.

Publishes 10 messages in rapid succession, then waits 100ms, and repeats.
This simulates high-frequency message production to test persistent session handling.
"""

import paho.mqtt.client as mqtt
import time
import sys
import threading
import os
import argparse
import yaml

# Load configuration from config.yaml
def load_config():
    """Load configuration from config.yaml file."""
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"[Producer] Warning: config.yaml not found at {config_path}, using defaults")
        return {}
    except Exception as e:
        print(f"[Producer] Warning: Error loading config.yaml: {e}, using defaults")
        return {}

# Load configuration
config = load_config()

# MQTT Configuration from config file
BROKER_HOST = config.get('broker', {}).get('host', 'localhost')
BROKER_PORT = config.get('broker', {}).get('port', 1883)
BROKER_KEEPALIVE = config.get('broker', {}).get('keepalive', 60)
TOPIC = config.get('topic', 'test/sequence')

# Default Message Configuration from config file
DEFAULT_QOS = config.get('qos', 1)
DEFAULT_BURST_COUNT = config.get('producer', {}).get('burst_count', 1)
DEFAULT_BURST_DELAY = config.get('producer', {}).get('burst_delay', 0.1)
DEFAULT_CLIENT_ID = config.get('producer', {}).get('client_id', 'producer_test')

# State file for persistence from config file
STATE_FILE_NAME = config.get('producer', {}).get('state_file', '.producer_state')
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), STATE_FILE_NAME)

def load_sequence():
    """Load the last sequence number from state file."""
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                return int(f.read().strip())
    except (ValueError, IOError) as e:
        print(f"[Producer] Warning: Could not load state file: {e}")
    return 0

def save_sequence(sequence):
    """Save the current sequence number to state file."""
    try:
        with open(STATE_FILE, "w") as f:
            f.write(str(sequence))
    except IOError as e:
        print(f"[Producer] Warning: Could not save state file: {e}")

# Connection state
connected_event = threading.Event()
connection_failed = False

def on_connect(client, userdata, flags, rc):
    global connection_failed
    if rc == 0:
        print(f"[Producer] Connected to broker at {BROKER_HOST}:{BROKER_PORT}")
        print(f"[Producer] Publishing to topic: {TOPIC}")
        print(f"[Producer] QoS level: {userdata['qos']}")
        print(f"[Producer] Configuration: {userdata['burst_count']} messages per burst, {userdata['burst_delay']*1000}ms between bursts")
        connected_event.set()  # Signal that connection is established
    else:
        print(f"[Producer] Connection failed with code {rc}")
        connection_failed = True
        connected_event.set()

def on_publish(client, userdata, mid):
    # Optional: Track published messages
    pass

def main():
    global connection_failed

    parser = argparse.ArgumentParser(description='MQTT Producer with configurable QoS and burst settings')
    parser.add_argument('--qos', type=int, choices=[0, 1, 2], default=DEFAULT_QOS,
                        help=f'QoS level (0, 1, or 2, default: {DEFAULT_QOS})')
    parser.add_argument('--burst-count', type=int, default=DEFAULT_BURST_COUNT,
                        help=f'Number of messages per burst (default: {DEFAULT_BURST_COUNT})')
    parser.add_argument('--burst-delay', type=float, default=DEFAULT_BURST_DELAY,
                        help=f'Delay between bursts in seconds (default: {DEFAULT_BURST_DELAY})')
    args = parser.parse_args()

    qos = args.qos
    burst_count = args.burst_count
    burst_delay = args.burst_delay

    userdata = {
        'qos': qos,
        'burst_count': burst_count,
        'burst_delay': burst_delay
    }

    client = mqtt.Client(client_id=DEFAULT_CLIENT_ID, clean_session=True, userdata=userdata)
    client.on_connect = on_connect
    client.on_publish = on_publish

    try:
        client.connect(BROKER_HOST, BROKER_PORT, keepalive=BROKER_KEEPALIVE)
        client.loop_start()

        # Wait for CONNACK
        print(f"[Producer] Waiting for CONNACK...")
        connected_event.wait(timeout=5)

        if connection_failed:
            print(f"[Producer] Failed to connect, exiting")
            sys.exit(1)

        if not connected_event.is_set():
            print(f"[Producer] Connection timeout, exiting")
            sys.exit(1)

        sequence = load_sequence()
        if sequence > 0:
            print(f"[Producer] Resuming from sequence {sequence}")
        print(f"[Producer] Starting message production...")

        while True:
            # Send burst of messages
            for _ in range(burst_count):
                payload = str(sequence)
                result = client.publish(TOPIC, payload, qos=qos)
                print(f"[Producer] Published: {sequence} (mid={result.mid})")
                sequence += 1

            # Save state after each burst
            save_sequence(sequence)

            # Wait before next burst
            time.sleep(burst_delay)

    except KeyboardInterrupt:
        if 'sequence' in locals():
            save_sequence(sequence)
            print(f"\n[Producer] Stopping... Total messages sent: {sequence}, state saved")
        else:
            print(f"\n[Producer] Stopping...")
    except Exception as e:
        if 'sequence' in locals():
            save_sequence(sequence)
            print(f"[Producer] Error: {e}, state saved")
        else:
            print(f"[Producer] Error: {e}")
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()
