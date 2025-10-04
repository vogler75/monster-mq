#!/usr/bin/env python3
"""
Test that an unauthorized publish causes broker disconnect when
UserManagement.DisconnectOnUnauthorized=true.

Prerequisites:
- Broker running on localhost:1883
- User management enabled
- User "Test" with password "Test"
- ACL allows publish to ALLOWED_TOPIC but denies publish to FORBIDDEN_TOPIC
- Configuration sets UserManagement.DisconnectOnUnauthorized: true

Behavior Verified:
1. Allowed QoS1 publish receives PUBACK and connection stays open.
2. Forbidden QoS1 publish triggers broker-initiated disconnect within timeout.

Exit Codes:
 0 = Success (disconnect observed after forbidden publish)
 1 = Could not connect
 2 = No PUBACK for allowed publish
 3 = Did not disconnect after forbidden publish

You can tweak topics by environment variables:
  ALLOWED_TOPIC (default: test/allowed/publish)
  FORBIDDEN_TOPIC (default: secret/publish)
  BROKER_HOST (default: localhost)
  BROKER_PORT (default: 1883)

"""
import os
import sys
import time
from typing import Optional

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("paho-mqtt not installed. Install with: pip install paho-mqtt")
    sys.exit(1)

BROKER_HOST = os.environ.get("BROKER_HOST", "localhost")
BROKER_PORT = int(os.environ.get("BROKER_PORT", "1883"))
KEEPALIVE = 30

ALLOWED_TOPIC = os.environ.get("ALLOWED_TOPIC", "test/allowed/publish")
FORBIDDEN_TOPIC = os.environ.get("FORBIDDEN_TOPIC", "secret/publish")

USERNAME: str = "Test"
PASSWORD: str = "Test"

PUBACK_TIMEOUT = 4.0
DISCONNECT_TIMEOUT = 4.0

state = {
    "connected": False,
    "disconnect_flag": False,
    "allowed_puback": False,
    "forbidden_puback": False,
    "mid_allowed": None,
    "mid_forbidden": None,
}

def on_connect(client, userdata, flags, rc):
    state["connected"] = True
    print(f"[on_connect] rc={rc} flags={flags}")


def on_disconnect(client, userdata, rc):
    state["disconnect_flag"] = True
    print(f"[on_disconnect] rc={rc}")


def on_publish(client, userdata, mid):
    if mid == state["mid_allowed"]:
        state["allowed_puback"] = True
        print(f"[on_publish] PUBACK for allowed mid={mid}")
    elif mid == state["mid_forbidden"]:
        state["forbidden_puback"] = True
        print(f"[on_publish] PUBACK for forbidden mid={mid} (unexpected under disconnect-on-unauthorized policy)")
    else:
        print(f"[on_publish] PUBACK mid={mid} (untracked)")


def wait_for(condition_func, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition_func():
            return True
        time.sleep(0.05)
    return False


def main() -> int:
    print("Connecting to broker expecting disconnect on unauthorized publish...")
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_publish = on_publish
    client.username_pw_set(USERNAME, PASSWORD)

    try:
        client.connect(BROKER_HOST, BROKER_PORT, KEEPALIVE)
    except Exception as e:
        print(f"ERROR: Connection failed: {e}")
        return 1

    client.loop_start()

    if not wait_for(lambda: state["connected"], 5.0):
        print("ERROR: Did not connect within timeout")
        client.loop_stop()
        return 1

    # Allowed publish
    print(f"Publishing allowed topic '{ALLOWED_TOPIC}' (QoS1)...")
    rc_allowed, mid_allowed = client.publish(ALLOWED_TOPIC, payload="ALLOWED", qos=1, retain=False)
    state["mid_allowed"] = mid_allowed
    if rc_allowed != mqtt.MQTT_ERR_SUCCESS:
        print(f"WARN: publish() returned rc={rc_allowed} for allowed topic")

    if not wait_for(lambda: state["allowed_puback"], PUBACK_TIMEOUT):
        print("ERROR: No PUBACK for allowed publish")
        client.loop_stop()
        return 2

    if state["disconnect_flag"]:
        print("ERROR: Disconnected after allowed publish (unexpected)")
        client.loop_stop()
        return 2

    # Forbidden publish
    print(f"Publishing forbidden topic '{FORBIDDEN_TOPIC}' (QoS1)... expecting disconnect")
    rc_forbidden, mid_forbidden = client.publish(FORBIDDEN_TOPIC, payload="FORBIDDEN", qos=1, retain=False)
    state["mid_forbidden"] = mid_forbidden
    if rc_forbidden != mqtt.MQTT_ERR_SUCCESS:
        print(f"NOTE: publish() rc={rc_forbidden} for forbidden topic (may still proceed)")

    # Expect broker to disconnect us quickly
    if not wait_for(lambda: state["disconnect_flag"], DISCONNECT_TIMEOUT):
        # If we got a PUBACK instead, report specifically
        if state["forbidden_puback"]:
            print("ERROR: Forbidden publish received PUBACK instead of disconnect (ACL may allow topic or policy misconfigured)")
        else:
            print("ERROR: Did not disconnect after forbidden publish within timeout")
        client.loop_stop()
        return 3

    print("OK: Broker disconnected client after forbidden publish as expected")
    client.loop_stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
