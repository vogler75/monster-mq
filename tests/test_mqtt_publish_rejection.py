#!/usr/bin/env python3
"""
Test MQTT publish rejection / handling for unauthorized topics.

Scenarios covered:
1. Publish to an allowed topic with QoS 1 -> Expect PUBACK (on_publish callback).
2. Publish to a forbidden topic (by ACL) with QoS 1 -> Expect either:
   a) Immediate disconnect (if broker configured to disconnect on unauthorized publish), OR
   b) No PUBACK within timeout while connection stays open (silent drop) if configured NOT to disconnect.

Usage:
- Start broker locally (host=localhost, port=1883)
- Ensure user/ACL configuration so that FORBIDDEN_TOPIC is not allowed for the chosen user.
- Optionally adjust DISCONNECT_TIMEOUT / PUBACK_TIMEOUT below.
- Run: python3 tests/test_mqtt_publish_rejection.py

Notes:
- The script infers behavior; it does not assert exit codes strictly. It prints diagnostics so it can be adapted easily.
- If user management is disabled, both publishes will likely succeed (no rejection possible) and the script reports that.
"""

import sys
import time
from typing import Optional

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("paho-mqtt not installed. Install with: pip install paho-mqtt")
    sys.exit(1)

BROKER_HOST = "localhost"
BROKER_PORT = 1883
KEEPALIVE = 30

# Adjust these to a known allowed / forbidden pair under your ACL policy
ALLOWED_TOPIC = "test/allowed/publish"
FORBIDDEN_TOPIC = "secret/publish"  # Ensure ACL denies this; otherwise it will be accepted

# Credentials fixed for test user
USERNAME: Optional[str] = "Test"
PASSWORD: Optional[str] = "Test"

# Timeouts (seconds)
PUBACK_TIMEOUT = 3.0   # Wait this long for PUBACK for QoS1 allowed publish
FORBIDDEN_WAIT = 3.0   # Wait this long to detect silent drop

state = {
    "connected": False,
    "disconnect_flag": False,
    "allowed_puback": False,
    "forbidden_puback": False,
    "current_mid_allowed": None,
    "current_mid_forbidden": None,
}

def on_connect(client, userdata, flags, rc):
    state["connected"] = True
    print(f"[on_connect] rc={rc} flags={flags}")


def on_disconnect(client, userdata, rc):
    state["disconnect_flag"] = True
    print(f"[on_disconnect] rc={rc}")


def on_publish(client, userdata, mid):
    if mid == state.get("current_mid_allowed"):
        state["allowed_puback"] = True
        print(f"[on_publish] PUBACK for allowed mid={mid}")
    elif mid == state.get("current_mid_forbidden"):
        state["forbidden_puback"] = True
        print(f"[on_publish] PUBACK for forbidden mid={mid} (unexpected if ACL is enforced)")
    else:
        print(f"[on_publish] PUBACK mid={mid} (untracked)")


def wait_for(condition_key: str, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if state.get(condition_key):
            return True
        time.sleep(0.05)
    return False


def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_publish = on_publish

    if USERNAME is not None:
        client.username_pw_set(USERNAME, PASSWORD or "")

    print("Connecting to broker...")
    client.connect(BROKER_HOST, BROKER_PORT, KEEPALIVE)
    client.loop_start()

    # Wait for connect
    for _ in range(50):
        if state["connected"]:
            break
        time.sleep(0.1)
    if not state["connected"]:
        print("ERROR: Could not connect to broker")
        client.loop_stop()
        return 1

    # 1. Allowed publish (QoS 1)
    print(f"Publishing allowed topic '{ALLOWED_TOPIC}' (QoS1)...")
    (rc, mid_allowed) = client.publish(ALLOWED_TOPIC, payload="42", qos=1, retain=False)
    if rc != mqtt.MQTT_ERR_SUCCESS:
        print(f"ERROR: publish() returned rc={rc} for allowed topic")
    state["current_mid_allowed"] = mid_allowed

    if wait_for("allowed_puback", PUBACK_TIMEOUT):
        print("OK: Received PUBACK for allowed publish")
    else:
        print("WARN: No PUBACK for allowed publish (unexpected)")

    if state["disconnect_flag"]:
        print("ERROR: Disconnected after allowed publish; aborting test")
        client.loop_stop()
        return 1

    # 2. Forbidden publish (QoS 1)
    print(f"Publishing forbidden topic '{FORBIDDEN_TOPIC}' (QoS1)...")
    (rc2, mid_forbidden) = client.publish(FORBIDDEN_TOPIC, payload="99", qos=1, retain=False)
    if rc2 != mqtt.MQTT_ERR_SUCCESS:
        print(f"ERROR: publish() returned rc={rc2} for forbidden topic (may indicate immediate rejection)")
    state["current_mid_forbidden"] = mid_forbidden

    # Wait to see outcome
    time.sleep(0.2)  # Small delay to allow immediate disconnect

    if state["disconnect_flag"]:
        print("INFO: Client disconnected after forbidden publish -> Broker likely configured to disconnect on unauthorized publish")
    else:
        # If not disconnected, check for PUBACK or silent drop
        if wait_for("forbidden_puback", FORBIDDEN_WAIT):
            print("WARN: Received PUBACK for forbidden topic (ACL may allow it or ACL disabled)")
        else:
            print("OK: No PUBACK for forbidden topic and still connected -> Silent drop behavior confirmed")

    if not state["disconnect_flag"]:
        print("Disconnecting cleanly...")
        client.disconnect()
        time.sleep(0.2)

    client.loop_stop()

    print("\nSummary:")
    print(f"  Allowed publish PUBACK: {state['allowed_puback']}")
    print(f"  Forbidden publish PUBACK: {state['forbidden_puback']}")
    print(f"  Disconnected after forbidden publish: {state['disconnect_flag']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
