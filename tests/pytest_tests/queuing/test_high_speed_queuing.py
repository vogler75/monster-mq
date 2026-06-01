#!/usr/bin/env python3
import json
import threading
import time
import uuid
import paho.mqtt.client as mqtt
import pytest

def _make_client(client_id, username, password, clean_session=True):
    """Create an MQTT v3.1.1 client with threading events for sync."""
    c = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id,
        protocol=mqtt.MQTTv311,
        clean_session=clean_session,
    )
    if username:
        c.username_pw_set(username, password)

    c._connack = threading.Event()
    c._suback = threading.Event()

    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            client._connack.set()

    def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        client._suback.set()

    c.on_connect = on_connect
    c.on_subscribe = on_subscribe
    return c

def _connect(client, host, port, timeout=45.0):
    """Connect and wait for CONNACK."""
    client._connack.clear()
    client.connect(host, port, keepalive=60)
    client.loop_start()
    assert client._connack.wait(timeout), f"CONNACK not received within {timeout}s"

def _subscribe(client, topic, qos, timeout=10.0):
    """Subscribe and wait for SUBACK."""
    client._suback.clear()
    client.subscribe(topic, qos=qos)
    assert client._suback.wait(timeout), f"SUBACK not received within {timeout}s"

def _disconnect(client):
    """Cleanly disconnect and stop the network loop."""
    client.loop_stop()
    client.disconnect()
    time.sleep(0.1)

@pytest.mark.timeout(300)
def test_high_speed_queuing(cfg):
    """
    High-speed queuing test for QoS 1 messages under extreme loads.
    1. sub_offline connects (clean_session=False), subscribes, and disconnects.
    2. sub_online connects (clean_session=False), subscribes, and stays online.
    3. pub connects, publishes 50,000 QoS 1 messages, and disconnects.
    4. sub_offline reconnects.
    5. Verify that both sub_offline and sub_online receive all 50,000 messages.
    """
    uid = uuid.uuid4().hex[:8]
    topic = f"test/highspeed/{uid}"
    
    sub_offline_id = f"sub_offline_{uid}"
    sub_online_id = f"sub_online_{uid}"
    pub_id = f"pub_highspeed_{uid}"
    
    sub_offline_received = []
    sub_online_received = []
    
    offline_lock = threading.Lock()
    online_lock = threading.Lock()
    
    import traceback
    
    def on_offline_message(client, userdata, msg):
        try:
            data = json.loads(msg.payload)
            with offline_lock:
                sub_offline_received.append(data["seq"])
        except Exception as e:
            print(f"[CALLBACK ERROR] offline subscriber: {e}")
            traceback.print_exc()

    def on_online_message(client, userdata, msg):
        try:
            data = json.loads(msg.payload)
            with online_lock:
                sub_online_received.append(data["seq"])
        except Exception as e:
            print(f"[CALLBACK ERROR] online subscriber: {e}")
            traceback.print_exc()

    # -- Step 1: Connect sub_offline, subscribe, disconnect --
    print(f"\n[TEST] Setting up offline subscriber persistent session {sub_offline_id}")
    sub_offline = _make_client(sub_offline_id, cfg["username"], cfg["password"], clean_session=False)
    sub_offline.on_message = on_offline_message
    _connect(sub_offline, cfg["sub_host"], cfg["sub_port"])
    _subscribe(sub_offline, topic, qos=1)
    _disconnect(sub_offline)
    print("[TEST] sub_offline disconnected. Session is now active on broker.")

    # -- Step 2: Connect sub_online, subscribe, keep online --
    print(f"[TEST] Setting up online subscriber persistent session {sub_online_id}")
    sub_online = _make_client(sub_online_id, cfg["username"], cfg["password"], clean_session=False)
    sub_online.on_message = on_online_message
    _connect(sub_online, cfg["sub_host"], cfg["sub_port"])
    _subscribe(sub_online, topic, qos=1)
    print("[TEST] sub_online connected and subscribed.")

    # -- Step 3: Connect publisher, send 50,000 QoS 1 messages --
    total_messages = 50000
    batch_size = 1000
    
    print(f"[TEST] Starting publisher {pub_id} to publish {total_messages} messages")
    pub = _make_client(pub_id, cfg["username"], cfg["password"], clean_session=True)
    _connect(pub, cfg["pub_host"], cfg["pub_port"])
    
    start_time = time.time()
    last_info = None
    
    for seq in range(total_messages):
        payload = json.dumps({"seq": seq, "ts": time.time()})
        last_info = pub.publish(topic, payload, qos=1)
        
        # Every batch_size messages, wait for confirmation of the last publish to control flow rate
        if (seq + 1) % batch_size == 0:
            last_info.wait_for_publish(timeout=10.0)
            elapsed = time.time() - start_time
            rate = (seq + 1) / elapsed if elapsed > 0 else 0
            with online_lock:
                online_count = len(sub_online_received)
            print(f"[PUB] Sent {seq + 1}/{total_messages} (Rate: {rate:.1f} msg/s) | sub_online received: {online_count}")
            
    # Wait for the very last message to be published
    if last_info:
        last_info.wait_for_publish(timeout=20.0)
        
    pub_elapsed = time.time() - start_time
    overall_rate = total_messages / pub_elapsed
    print(f"[TEST] Publisher finished in {pub_elapsed:.2f} seconds (Overall Rate: {overall_rate:.1f} msg/s)")
    _disconnect(pub)

    # -- Step 4: Reconnect sub_offline and drain the broker queue --
    print(f"[TEST] Reconnecting sub_offline {sub_offline_id} to drain 50,000 messages...")
    sub_offline = _make_client(sub_offline_id, cfg["username"], cfg["password"], clean_session=False)
    sub_offline.on_message = on_offline_message
    _connect(sub_offline, cfg["sub_host"], cfg["sub_port"])
    
    # Wait for both subscribers to receive all messages
    drain_timeout = 120.0
    deadline = time.time() + drain_timeout
    success = False
    
    print(f"[TEST] Waiting up to {drain_timeout}s for all 50,000 messages to be received by both clients...")
    
    while time.time() < deadline:
        with offline_lock:
            offline_count = len(sub_offline_received)
        with online_lock:
            online_count = len(sub_online_received)
            
        print(f"[DRAIN STATUS] sub_offline: {offline_count}/{total_messages} | sub_online: {online_count}/{total_messages}")
        
        if offline_count >= total_messages and online_count >= total_messages:
            success = True
            break
            
        time.sleep(2.0)
        
    with offline_lock:
        final_offline_count = len(sub_offline_received)
        offline_unique = sorted(set(sub_offline_received))
    with online_lock:
        final_online_count = len(sub_online_received)
        online_unique = sorted(set(sub_online_received))
        
    print(f"[TEST] Final Counts -> sub_offline: {final_offline_count} ({len(offline_unique)} unique) | sub_online: {final_online_count} ({len(online_unique)} unique)")
    
    # -- Step 5: Clean up persistent sessions --
    print("[TEST] Cleaning up persistent sessions...")
    _disconnect(sub_offline)
    _disconnect(sub_online)
    
    # Connect with clean_session=True to purge sessions
    cleanup_offline = _make_client(sub_offline_id, cfg["username"], cfg["password"], clean_session=True)
    _connect(cleanup_offline, cfg["sub_host"], cfg["sub_port"])
    _disconnect(cleanup_offline)
    
    cleanup_online = _make_client(sub_online_id, cfg["username"], cfg["password"], clean_session=True)
    _connect(cleanup_online, cfg["sub_host"], cfg["sub_port"])
    _disconnect(cleanup_online)
    
    print("[TEST] Cleanup completed.")

    # -- Step 6: Assertions --
    assert success, f"Timed out draining messages after {drain_timeout}s. sub_offline: {final_offline_count}, sub_online: {final_online_count}"
    assert len(offline_unique) == total_messages, f"sub_offline unique count is {len(offline_unique)}, expected {total_messages}"
    assert len(online_unique) == total_messages, f"sub_online unique count is {len(online_unique)}, expected {total_messages}"
    
    expected_set = set(range(total_messages))
    offline_missing = expected_set - set(offline_unique)
    online_missing = expected_set - set(online_unique)
    
    assert not offline_missing, f"sub_offline is missing {len(offline_missing)} messages. First 10 missing: {sorted(list(offline_missing))[:10]}"
    assert not online_missing, f"sub_online is missing {len(online_missing)} messages. First 10 missing: {sorted(list(online_missing))[:10]}"
    
    print(f"[PASS] Successfully verified full 50,000 messages delivery with QoS 1 on both subscribers!")
