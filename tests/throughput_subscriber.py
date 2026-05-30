#!/usr/bin/env python3
"""
MonsterMQ Live Throughput (Value Changes Per Second) Subscriber
Connects to the broker, subscribes with QoS 1, and prints real-time message rates.
"""

import os
import sys
import time
import argparse
import threading
from datetime import datetime
import paho.mqtt.client as mqtt

# Default configuration (will fall back to env or arguments)
MQTT_HOST = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "Test")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "Test")
DEFAULT_TOPIC = "#"

class ThroughputTracker:
    def __init__(self):
        self.total_count = 0
        self.interval_count = 0
        self.lock = threading.Lock()
        self.start_time = None
        self.last_report_time = None
        self.history = []

    def record_message(self):
        with self.lock:
            if self.start_time is None:
                self.start_time = time.time()
                self.last_report_time = self.start_time
            self.total_count += 1
            self.interval_count += 1

    def report_loop(self, stop_event):
        print("\n" + "=" * 80)
        print(f" Live Throughput Monitor Started")
        print("=" * 80)
        print(f"  Format: [Elapsed] | Live Rate (msgs/s) | Running Average | Total Received")
        print("-" * 80)

        while not stop_event.is_set():
            time.sleep(1.0)
            with self.lock:
                if self.start_time is None:
                    # No messages received yet
                    print("  Waiting for messages...                                 ", end="\r", flush=True)
                    continue

                now = time.time()
                elapsed = now - self.start_time
                interval_elapsed = now - self.last_report_time

                if interval_elapsed > 0:
                    current_rate = self.interval_count / interval_elapsed
                else:
                    current_rate = 0.0

                overall_rate = self.total_count / elapsed if elapsed > 0 else 0.0
                self.history.append(current_rate)

                # Format human readable elapsed time
                mins, secs = divmod(int(elapsed), 60)
                hours, mins = divmod(mins, 60)
                time_str = f"{hours:02d}:{mins:02d}:{secs:02d}"

                # Print stats
                print(
                    f"  [{time_str}] | Live: {current_rate:8.2f} msg/s | Avg: {overall_rate:8.2f} msg/s | Total: {self.total_count:,}",
                    end="\r",
                    flush=True
                )

                self.interval_count = 0
                self.last_report_time = now

def main():
    parser = argparse.ArgumentParser(description="Monitor real-time MQTT message rate (QoS 1)")
    parser.add_argument("--host", default=MQTT_HOST, help=f"MQTT Broker host (default: {MQTT_HOST})")
    parser.add_argument("--port", type=int, default=MQTT_PORT, help=f"MQTT Broker port (default: {MQTT_PORT})")
    parser.add_argument("--user", default=MQTT_USERNAME, help="MQTT Username")
    parser.add_argument("--password", default=MQTT_PASSWORD, help="MQTT Password")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help=f"Topic to subscribe to (default: '{DEFAULT_TOPIC}')")
    parser.add_argument("--qos", type=int, choices=[0, 1, 2], default=1, help="QoS level (default: 1)")
    args = parser.parse_args()

    tracker = ThroughputTracker()
    stop_event = threading.Event()

    # Create Paho MQTT client
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"throughput_monitor_{int(time.time())}"
    )

    if args.user:
        client.username_pw_set(args.user, args.password)

    def on_connect(c, userdata, flags, reason_code, properties=None):
        rc = reason_code.value if hasattr(reason_code, 'value') else reason_code
        if rc == 0:
            print(f"[Success] Connected to broker at {args.host}:{args.port}")
            print(f"[Subscribing] Topic: '{args.topic}' with QoS: {args.qos}")
            c.subscribe(args.topic, qos=args.qos)
        else:
            print(f"[Error] Connection failed with reason code: {rc}")
            sys.exit(1)

    def on_message(c, userdata, msg):
        tracker.record_message()

    client.on_connect = on_connect
    client.on_message = on_message

    print(f"Connecting to MQTT Broker at {args.host}:{args.port}...")
    try:
        client.connect(args.host, args.port, keepalive=60)
    except Exception as e:
        print(f"[Error] Failed to connect: {e}")
        sys.exit(1)

    # Start report thread
    report_thread = threading.Thread(target=tracker.report_loop, args=(stop_event,), daemon=True)
    report_thread.start()

    # Start loop
    client.loop_start()

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n\nStopping monitor...")
        stop_event.set()
        client.loop_stop()
        client.disconnect()
        
        # Summary report
        if tracker.start_time:
            total_elapsed = time.time() - tracker.start_time
            avg_rate = tracker.total_count / total_elapsed if total_elapsed > 0 else 0
            max_rate = max(tracker.history) if tracker.history else 0
            print("=" * 80)
            print(" Live Throughput Summary")
            print("=" * 80)
            print(f"  Total Duration:     {total_elapsed:.2f} seconds")
            print(f"  Total Messages:     {tracker.total_count:,}")
            print(f"  Average Throughput: {avg_rate:.2f} messages/sec")
            print(f"  Peak Throughput:    {max_rate:.2f} messages/sec")
            print("=" * 80)
        else:
            print("No messages received.")

if __name__ == "__main__":
    main()
