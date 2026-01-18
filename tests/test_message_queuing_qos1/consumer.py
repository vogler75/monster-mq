#!/usr/bin/env python3
"""
MQTT Consumer - Persistent session subscriber that detects message gaps.

This script subscribes with a persistent session (clean_session=False) and
verifies that incoming sequence numbers are consecutive. Any gaps indicate
message loss or delivery issues.

The last received sequence number is persisted to disk, so the consumer can
resume from the correct position after restarts.

Usage:
    python consumer.py              # Start or resume persistent session
    python consumer.py --reset      # Reset session and start from 0
"""

import paho.mqtt.client as mqtt
import sys
import time
import argparse
import threading
import os
import yaml

# Load configuration from config.yaml
def load_config():
    """Load configuration from config.yaml file."""
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        print(f"[Consumer] Warning: config.yaml not found at {config_path}, using defaults")
        return {}
    except Exception as e:
        print(f"[Consumer] Warning: Error loading config.yaml: {e}, using defaults")
        return {}

# Load configuration
config = load_config()

# MQTT Configuration from config file
BROKER_HOST = config.get('broker', {}).get('host', 'localhost')
BROKER_PORT = config.get('broker', {}).get('port', 1883)
BROKER_KEEPALIVE = config.get('broker', {}).get('keepalive', 60)
CLIENT_ID = config.get('consumer', {}).get('client_id', 'consumer_test_persistent')
TOPIC = config.get('topic', 'test/sequence')
DEFAULT_QOS = config.get('qos', 1)

# State file from config
STATE_FILE_NAME = config.get('consumer', {}).get('state_file', '.consumer_state')
STATE_FILE = os.path.join(os.path.dirname(__file__), STATE_FILE_NAME)

# State tracking
expected_sequence = 0
received_count = 0
gap_count = 0
last_received = None

# Connection state
connected_event = threading.Event()
subscribed_event = threading.Event()
connection_failed = False

def on_connect(client, userdata, flags, rc):
    global connection_failed
    if rc == 0:
        session_present = flags.get('session present', False)
        qos = userdata['qos']
        print(f"[Consumer] Connected to broker at {BROKER_HOST}:{BROKER_PORT}")
        print(f"[Consumer] Client ID: {CLIENT_ID}")
        print(f"[Consumer] QoS level: {qos}")
        print(f"[Consumer] Session present: {session_present}")
        connected_event.set()

        if session_present:
            # Persistent session exists - subscriptions are already active
            print(f"[Consumer] Resuming persistent session - expecting sequence from {expected_sequence}")
            print(f"[Consumer] Using existing subscriptions (not re-subscribing)")
            subscribed_event.set()  # Signal that we're ready (no SUBACK expected)
        else:
            # New session - need to subscribe
            print(f"[Consumer] New session - starting from 0")
            result, mid = client.subscribe(TOPIC, qos=qos)
            if result == mqtt.MQTT_ERR_SUCCESS:
                print(f"[Consumer] Subscribe request sent for topic: {TOPIC} with QoS {qos}")
            else:
                print(f"[Consumer] Subscribe request failed with code {result}")
    else:
        print(f"[Consumer] Connection failed with code {rc}")
        connection_failed = True
        connected_event.set()

def on_subscribe(client, userdata, mid, granted_qos):
    print(f"[Consumer] Subscription confirmed with QoS: {granted_qos}")
    subscribed_event.set()

def save_state():
    """Save the current expected sequence to disk"""
    try:
        with open(STATE_FILE, 'w') as f:
            f.write(str(expected_sequence))
    except Exception as e:
        print(f"[Consumer] Warning: Failed to save state: {e}")

def load_state():
    """Load the last expected sequence from disk"""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                seq = int(f.read().strip())
                print(f"[Consumer] Loaded state from disk: expecting sequence {seq}")
                return seq
        except Exception as e:
            print(f"[Consumer] Warning: Failed to load state: {e}")
    return 0

def on_message(client, userdata, msg):
    global expected_sequence, received_count, gap_count, last_received

    try:
        sequence = int(msg.payload.decode())
        received_count += 1
        last_received = time.time()

        if sequence == expected_sequence:
            # Expected sequence number - all good
            # Use carriage return to overwrite the same line
            print(f"\r[Consumer] ✓ Received: {sequence} (expected {expected_sequence})    ", end='', flush=True)
            expected_sequence = sequence + 1
            save_state()  # Persist state after each message
        elif sequence > expected_sequence:
            # Gap detected - messages were lost or not delivered
            # Print with newline to preserve error message
            gap_size = sequence - expected_sequence
            gap_count += 1
            print(f"\n[Consumer] ⚠ WARNING: Gap detected! Received {sequence}, expected {expected_sequence}")
            print(f"[Consumer] ⚠ Missing {gap_size} message(s): {expected_sequence} to {sequence-1}")
            expected_sequence = sequence + 1
            save_state()  # Persist state even after gap
        else:
            # Received older message (duplicate or out of order)
            # Print with newline to preserve error message
            print(f"\n[Consumer] ⚠ WARNING: Out of order! Received {sequence}, expected {expected_sequence}")
            print(f"[Consumer] ⚠ Duplicate or delayed message")
            # Don't update expected_sequence or save state for old messages

    except ValueError as e:
        print(f"\n[Consumer] Error parsing message: {e}")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        print(f"[Consumer] Unexpected disconnection (code {rc})")
    else:
        print(f"[Consumer] Disconnected")

def print_stats():
    """Print statistics about message reception"""
    # Add extra newline to clear the carriage-return line
    print("\n\n" + "="*60)
    print(f"[Consumer] Statistics:")
    print(f"  Total messages received: {received_count}")
    print(f"  Next expected sequence: {expected_sequence}")
    print(f"  Gaps detected: {gap_count}")
    if gap_count > 0:
        print(f"  ⚠ WARNING: {gap_count} gap(s) detected - possible message loss!")
    else:
        print(f"  ✓ No gaps detected - all messages received in order")
    print("="*60 + "\n")

def main():
    global expected_sequence, connection_failed

    parser = argparse.ArgumentParser(description='MQTT persistent session consumer with gap detection')
    parser.add_argument('--reset', action='store_true', help='Reset session (clean_session=True) and delete state file')
    parser.add_argument('--start-from', type=int, help='Override expected starting sequence number')
    parser.add_argument('--qos', type=int, choices=[0, 1, 2], default=DEFAULT_QOS,
                        help=f'QoS level (0, 1, or 2, default: {DEFAULT_QOS})')
    args = parser.parse_args()

    qos = args.qos
    userdata = {'qos': qos}

    # Create client with persistent session (clean_session=False) unless --reset
    clean_session = args.reset
    if clean_session:
        print(f"[Consumer] Resetting session (clean_session=True)")
        # Delete state file on reset
        if os.path.exists(STATE_FILE):
            os.remove(STATE_FILE)
            print(f"[Consumer] Deleted state file")
        expected_sequence = 0
    else:
        # Load state from disk if not resetting
        expected_sequence = load_state()

    # Allow manual override of starting sequence
    if args.start_from is not None:
        expected_sequence = args.start_from
        print(f"[Consumer] Manually overriding sequence to start from {expected_sequence}")
        save_state()

    client = mqtt.Client(client_id=CLIENT_ID, clean_session=clean_session, userdata=userdata)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.on_subscribe = on_subscribe

    try:
        client.connect(BROKER_HOST, BROKER_PORT, keepalive=BROKER_KEEPALIVE)
        client.loop_start()

        # Wait for CONNACK
        print(f"[Consumer] Waiting for CONNACK...")
        connected_event.wait(timeout=5)

        if connection_failed:
            print(f"[Consumer] Failed to connect, exiting")
            sys.exit(1)

        if not connected_event.is_set():
            print(f"[Consumer] Connection timeout, exiting")
            sys.exit(1)

        # Wait for SUBACK
        print(f"[Consumer] Waiting for SUBACK...")
        # subscribed_event.wait(timeout=5)

        if not subscribed_event.is_set():
            print(f"[Consumer] Subscription timeout, continuing anyway")

        print(f"[Consumer] Running... Press Ctrl+C to stop and see statistics")
        print(f"[Consumer] You can disconnect and reconnect to test persistent session handling")

        # Run until interrupted
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print(f"\n[Consumer] Stopping...")
        print_stats()
    except Exception as e:
        print(f"[Consumer] Error: {e}")
        print_stats()
    finally:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()
