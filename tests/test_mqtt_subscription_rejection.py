#!/usr/bin/env python3
"""
Test per-subscription rejection behavior:
- Root wildcard '#' should be rejected with FAILURE (0x80) when AllowRootWildcardSubscription=false
- Unauthorized topics by ACL should be rejected with FAILURE without disconnecting the client

Prerequisites:
- Broker running locally on 1883
- Configuration sets AllowRootWildcardSubscription: false
- (Optional) ACL rules that deny subscription to a specific test topic (e.g., 'secret/#') for the test user

This script:
1. Connects an MQTT client
2. Attempts to subscribe to multiple topics in one SUBSCRIBE packet:
   a) 'test/allowed' (expected success)
   b) '#' (expected rejection if root wildcard disabled)
   c) 'secret/data' (expected rejection if ACL denies)
3. Prints returned granted QoS list and interprets results.
4. Verifies the client remains connected after rejections.

Interpretation of results:
- Granted QoS values are integers 0,1,2 or 128 (0x80). Paho represents FAILURE as 128.
"""

import sys
import time
from typing import List

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("paho-mqtt not installed. Install with: pip install paho-mqtt")
    sys.exit(1)

BROKER_HOST = "localhost"
BROKER_PORT = 1883
KEEPALIVE = 30

# Topics under test
ALLOWED_TOPIC = "test/allowed"
ROOT_WILDCARD = "#"
ACL_DENIED_TOPIC = "secret/data"  # Adjust to a topic you know is denied, or leave if no ACL -> expect success

results = {
    "connected": False,
    "rc": None,
    "granted": None,
    "disconnect_flag": False,
}


def on_connect(client, userdata, flags, rc):
    results["connected"] = True
    results["rc"] = rc
    print(f"[on_connect] Connected rc={rc} flags={flags}")


def on_subscribe(client, userdata, mid, granted_qos: List[int]):
    results["granted"] = granted_qos
    print(f"[on_subscribe] mid={mid} granted={granted_qos}")


def on_disconnect(client, userdata, rc):
    results["disconnect_flag"] = True
    print(f"[on_disconnect] rc={rc}")


def main():
    client = mqtt.Client()
    client.username_pw_set("Test", "Test")
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_disconnect = on_disconnect

    print("Connecting to broker as user 'Test'...")
    client.connect(BROKER_HOST, BROKER_PORT, KEEPALIVE)
    client.loop_start()

    # Wait for connect
    for _ in range(50):
        if results["connected"]:
            break
        time.sleep(0.1)
    if not results["connected"]:
        print("ERROR: Did not connect to broker")
        return 1

    print("Sending multi-topic SUBSCRIBE...")
    # Single subscribe call with multiple topics
    # Each entry: (topic, qos requested)
    (rc, mid) = client.subscribe([(ALLOWED_TOPIC, 0), (ROOT_WILDCARD, 0), (ACL_DENIED_TOPIC, 0)])
    if rc != mqtt.MQTT_ERR_SUCCESS:
        print(f"ERROR: subscribe() returned rc={rc}")
        return 1

    # Wait for SUBACK
    for _ in range(50):
        if results["granted"] is not None:
            break
        time.sleep(0.1)

    client.loop_stop()

    if results["granted"] is None:
        print("ERROR: Did not receive SUBACK")
        return 1

    granted = results["granted"]
    topics = [ALLOWED_TOPIC, ROOT_WILDCARD, ACL_DENIED_TOPIC]
    print("Results:")
    for t, g in zip(topics, granted):
        status = "ACCEPTED" if g in (0,1,2) else ("REJECTED" if g == 128 else f"UNKNOWN({g})")
        print(f"  Topic '{t}': {status} (granted={g})")

    # Validate expectations (soft asserts; just print)
    print("Validation:")
    if granted[0] == 128:
        print(f"  Unexpected: Allowed topic '{ALLOWED_TOPIC}' was rejected")
    else:
        print(f"  OK: Allowed topic '{ALLOWED_TOPIC}' accepted with QoS {granted[0]}")

    if granted[1] == 128:
        print("  OK: Root wildcard rejected as expected (AllowRootWildcardSubscription=false)")
    else:
        print("  NOTE: Root wildcard accepted (config may allow it)")

    if granted[2] == 128:
        print("  ACL: 'secret/data' rejected (ACL enforcement working)\n")
    else:
        print("  ACL: 'secret/data' accepted (either ACL allows it or ACL disabled)")

    if results["disconnect_flag"]:
        print("ERROR: Client was disconnected unexpectedly after subscription attempts")
        return 1
    else:
        print("OK: Client remained connected after rejected subscriptions")

    client.disconnect()
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
