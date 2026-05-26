#!/usr/bin/env python3
"""
MonsterMQ Slow/Non-Responsive Subscriber Simulation.
Intercepts and delays QoS 1 PUBACKs to showcase in-flight metrics on the dashboard.
"""

import os
import time
import uuid
import threading
import sys
import paho.mqtt.client as mqtt

# Load connection settings from env/defaults
MQTT_HOST = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "Test")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "Test")

# State tracking for intercepted PUBACKs
delayed_pubacks = []
should_delay_puback = True


def print_banner(title):
    print("=" * 80)
    print(f" {title}")
    print("=" * 80)


def main():
    global should_delay_puback

    print_banner("MonsterMQ Slow/Non-Responsive Subscriber Simulation")
    print(f"Target Broker: {MQTT_HOST}:{MQTT_PORT}")
    print("This script will connect, intercept QoS 1 PUBACKs, publish 10 QoS 1 messages,")
    print("let you inspect the active in-flight count on the dashboard, and then release them.")
    print()

    client_id = "non_responsive_client"
    connack_event = threading.Event()
    suback_event = threading.Event()
    connection_rc = [None]

    def on_connect(c, userdata, flags, reason_code, properties=None):
        rc = reason_code.value if hasattr(reason_code, 'value') else reason_code
        connection_rc[0] = rc
        if rc == 0:
            connack_event.set()
        else:
            print(f"[SUBSCRIBER] Connection refused with return code: {rc}")
            connack_event.set()

    def on_subscribe(c, userdata, mid, reason_codes, properties=None):
        print(f"[SUBSCRIBER] Subscribed to topic filter successfully.")
        suback_event.set()

    def on_message(c, userdata, msg):
        print(f"[SUBSCRIBER] Received message: '{msg.payload.decode()}' (mid={msg.mid})")

    # Connect Subscriber
    credentials_to_try = [
        (MQTT_USERNAME, MQTT_PASSWORD),
        (os.getenv("MQTT_ADMIN_USER", "Admin"), os.getenv("MQTT_ADMIN_PASS", "Admin")),
        ("Admin", "Admin"),
        ("Test", "Test"),
        ("", "")  # Anonymous connection
    ]

    slow_client = None
    connected = False
    original_send_puback = None

    for user, password in credentials_to_try:
        connack_event.clear()
        connection_rc[0] = None

        slow_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=mqtt.MQTTv311
        )
        slow_client.on_connect = on_connect
        slow_client.on_subscribe = on_subscribe
        slow_client.on_message = on_message

        if user:
            slow_client.username_pw_set(user, password)

        # Monkeypatch PUBACK
        original_send_puback = slow_client._send_puback

        def custom_send_puback(mid):
            if should_delay_puback:
                print(f"[SUBSCRIBER] >>> INTERCEPTED & DELAYED PUBACK for message ID: {mid}")
                delayed_pubacks.append(mid)
                return mqtt.MQTT_ERR_SUCCESS
            else:
                return original_send_puback(mid)

        slow_client._send_puback = custom_send_puback

        user_label = user if user else "[Anonymous]"
        print(f"Connecting subscriber client '{client_id}' using user '{user_label}'...")
        try:
            slow_client.connect(MQTT_HOST, MQTT_PORT)
            slow_client.loop_start()

            if connack_event.wait(3.0) and connection_rc[0] == 0:
                print(f"[SUBSCRIBER] Connected successfully as '{user_label}'!")
                connected = True
                break
            else:
                slow_client.loop_stop()
                slow_client.disconnect()
        except Exception as e:
            print(f"[SUBSCRIBER] Connection error with user '{user_label}': {e}")

    if not connected:
        print("[ERROR] Could not connect subscriber to the broker. Exiting.")
        sys.exit(1)

    # Subscribe to test/# topic filter
    slow_client.subscribe("test/#", qos=1)
    if not suback_event.wait(5.0):
        print("[ERROR] Subscription timed out. Exiting.")
        slow_client.loop_stop()
        slow_client.disconnect()
        sys.exit(1)

    time.sleep(0.5)

    # Connect a temporary Publisher client to publish 10 QoS 1 messages
    pub_id = f"temp_publisher_{uuid.uuid4().hex[:6]}"
    pub_client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=pub_id,
        protocol=mqtt.MQTTv311
    )
    # Use same credentials that successfully connected the subscriber
    if user:
        pub_client.username_pw_set(user, password)

    try:
        print(f"\nConnecting publisher '{pub_id}'...")
        pub_client.connect(MQTT_HOST, MQTT_PORT)
        pub_client.loop_start()

        print("\nPublishing 10 QoS 1 messages to 'test/test'...")
        for i in range(10):
            payload = f"In-Flight Demonstration Message #{i + 1}"
            pub_client.publish("test/test", payload, qos=1)
            time.sleep(0.1)

        print("[PUBLISHER] Finished publishing. Disconnecting publisher...")
        time.sleep(0.5)
        pub_client.loop_stop()
        pub_client.disconnect()
    except Exception as e:
        print(f"[PUBLISHER] Failed to publish messages: {e}")

    # Step 1: Wait while in-flight is high
    print("\n" + "#" * 80)
    print(" STAGE 1: IN-FLIGHT ACCUMULATION (SND QUEUE SATURATED)")
    print(" " + "#" * 80)
    print("The client has received the messages but has NOT acknowledged them.")
    print("Please check your MonsterMQ dashboard active sessions:")
    print(" -> The 'In-Flight (Snd/Rcv)' column for 'non_responsive_client' should show: 10 / 0")
    print(" -> Click 'View Details' to check the detailed metric cards and properties.")
    print()
    
    wait_seconds = 15
    for remaining in range(wait_seconds, 0, -1):
        print(f"  [WAIT] Delaying PUBACKs. {remaining} seconds remaining... (Delayed count: {len(delayed_pubacks)}/10)", end="\r")
        time.sleep(1)
    print("\n")

    # Step 2: Release PUBACKs
    print("\n" + "#" * 80)
    print(" STAGE 2: RELEASING DELAYED PUBACKS (DRAINING IN-FLIGHT QUEUE)")
    print(" " + "#" * 80)
    print(f"Releasing {len(delayed_pubacks)} intercepted PUBACKs back to the broker...")
    
    should_delay_puback = False
    for mid in delayed_pubacks:
        print(f"[SUBSCRIBER] <<< Releasing delayed PUBACK for message ID: {mid}")
        original_send_puback(mid)
        time.sleep(0.1)  # small gap to see dynamic drain if dashboard updates fast

    print("\nAll delayed PUBACKs sent.")
    print("Please check your dashboard:")
    print(" -> The 'In-Flight (Snd/Rcv)' column for 'non_responsive_client' should now have drained to: 0 / 0")
    print()

    # Step 3: Keep connected so user can inspect quiet state
    quiet_seconds = 10
    for remaining in range(quiet_seconds, 0, -1):
        print(f"  [WAIT] Keeping client connected. {remaining} seconds remaining before disconnect...", end="\r")
        time.sleep(1)
    print("\n")

    # Cleanup and exit
    print("Disconnecting subscriber and terminating simulation...")
    slow_client.loop_stop()
    slow_client.disconnect()
    print_banner("Simulation Finished Successfully!")


if __name__ == "__main__":
    main()
