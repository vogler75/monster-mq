#!/usr/bin/env python3
"""
Queued message delivery test for persistent sessions.

Tests that QoS 1/2 messages published while a subscriber is offline
are delivered when the subscriber reconnects, with no gaps in the
sequence.

Usage:
    # Basic test (QoS 1, 10 msg/s, default broker)
    pytest test_queuing.py -v -s

    # QoS 2, 100 msg/s
    pytest test_queuing.py -v -s --qos 2 --rate 100

    # Clustering: different hosts for publisher and subscriber
    pytest test_queuing.py -v -s --pub-host broker1 --sub-host broker2

    # Multiple subscribers
    pytest test_queuing.py -v -s --subscribers 3

    # All rates
    pytest test_queuing.py -v -s -k "rate"

    # Custom disconnect duration
    pytest test_queuing.py -v -s --disconnect-seconds 10

    # No disconnects (pure latency measurement)
    pytest test_queuing.py -v -s --no-disconnect
"""

import json
import statistics
import threading
import time
import uuid

import paho.mqtt.client as mqtt
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client(client_id, username, password, clean_session=True):
    """Create an MQTTv311 client with threading events for synchronization."""
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


def _connect(client, host, port, timeout=5.0):
    """Connect and wait for CONNACK."""
    client._connack.clear()
    client.connect(host, port, keepalive=60)
    client.loop_start()
    assert client._connack.wait(timeout), f"CONNACK not received within {timeout}s"


def _subscribe(client, topic, qos, timeout=5.0):
    """Subscribe and wait for SUBACK."""
    client._suback.clear()
    client.subscribe(topic, qos=qos)
    assert client._suback.wait(timeout), f"SUBACK not received within {timeout}s"


def _disconnect(client):
    """Cleanly disconnect and stop the network loop."""
    client.loop_stop()
    client.disconnect()
    time.sleep(0.2)


# ---------------------------------------------------------------------------
# Core test logic
# ---------------------------------------------------------------------------

class SubscriberState:
    """Per-subscriber state for tracking received messages and latency."""
    def __init__(self, index, client_id):
        self.index = index
        self.client_id = client_id
        self.received_sequences = []
        self.latencies = []  # (seq, latency_ms) tuples
        self.lock = threading.Lock()
        self.first_received = threading.Event()
        self.client = None
        self.online = False

    def on_message(self, client, userdata, msg):
        recv_ts = time.time()
        try:
            data = json.loads(msg.payload)
            with self.lock:
                self.received_sequences.append(data["seq"])
                if "ts" in data:
                    self.latencies.append((data["seq"], (recv_ts - data["ts"]) * 1000.0))
                if not self.first_received.is_set():
                    self.first_received.set()
        except (json.JSONDecodeError, KeyError):
            pass

    @property
    def tag(self):
        return f"[SUB-{self.index}]"


def _run_queuing_test(cfg, rate, label=""):
    """
    1. Subscribers connect with clean_session=False, subscribe, wait for
       CONNACK+SUBACK before publisher starts.
    2. Publisher publishes until all subscribers confirm receipt of the first
       message — only then does the "real" sequence begin.
    3. Subscribers disconnect; publisher keeps going at *rate* msg/s.
    4. Subscribers reconnect (persistent session) and drain the queue.
    5. Verify the sequence from first-received to last-published has no gaps
       for each subscriber independently.
    """
    qos = cfg["qos"]
    disconnect_seconds = cfg["disconnect_seconds"]
    num_subscribers = cfg.get("subscribers", 1)
    uid = uuid.uuid4().hex[:8]
    topic = f"test/queuing/{uid}"
    pub_client_id = f"pub_queuing_{uid}"

    # -- Phase 1: subscribers connect + subscribe -----------------------------
    print(f"\n{'='*60}")
    print(f"[{label}] QoS={qos}  rate={rate} msg/s  disconnect={disconnect_seconds}s  subscribers={num_subscribers}")
    print(f"{'='*60}")

    subs = []
    for i in range(num_subscribers):
        sub_client_id = f"sub_queuing_{uid}_{i}"
        state = SubscriberState(i, sub_client_id)
        client = _make_client(sub_client_id, cfg["username"], cfg["password"],
                              clean_session=False)
        client.on_message = state.on_message
        _connect(client, cfg["sub_host"], cfg["sub_port"])
        _subscribe(client, topic, qos)
        state.client = client
        state.online = True
        subs.append(state)
        print(f"{state.tag} Connected and subscribed to {topic} (QoS {qos}, persistent session)")

    # -- Phase 2: publisher connects AFTER subscribers are ready --------------
    pub = _make_client(pub_client_id, cfg["username"], cfg["password"],
                       clean_session=True)
    _connect(pub, cfg["pub_host"], cfg["pub_port"])

    interval = 1.0 / rate
    seq = 0

    # Publish until all subscribers confirm receipt of at least one message.
    print(f"[PUB] Sending messages until all subscribers confirm receipt ...")
    deadline = time.time() + 15.0
    while time.time() < deadline:
        if all(s.first_received.is_set() for s in subs):
            break
        pub.publish(topic, json.dumps({"seq": seq, "ts": time.time()}), qos=qos)
        seq += 1
        time.sleep(interval)
    for s in subs:
        assert s.first_received.is_set(), f"{s.tag} never received any message within 15s"

    # Record which sequence number was first received across all subscribers
    first_seq = max(s.received_sequences[0] for s in subs)
    print(f"[PUB] All subscribers confirmed. Baseline seq={first_seq}")

    # Continue warmup for a bit to build confidence
    warmup_extra = max(3, rate // 2)
    for _ in range(warmup_extra):
        pub.publish(topic, json.dumps({"seq": seq, "ts": time.time()}), qos=qos)
        seq += 1
        time.sleep(interval)

    time.sleep(0.5)
    for s in subs:
        with s.lock:
            warmup_received = len(s.received_sequences)
        print(f"{s.tag} Received {warmup_received} messages while online")
    print(f"[PUB] Publishing at {rate} msg/s")

    # -- Phase 3: publish continuously while subscribers cycle on/off ---------
    no_disconnect = cfg.get("no_disconnect", False)
    total_phase3 = rate * disconnect_seconds
    quarter = max(1, total_phase3 // 4)
    reconnect_pause = 1.0  # seconds offline between cycles

    phase3_start_seq = seq
    msgs_since_reconnect = 0
    cycle = 0

    if no_disconnect:
        print(f"[PUB] Publishing {total_phase3} messages; subscribers stay connected (--no-disconnect)")
    else:
        print(f"[PUB] Publishing {total_phase3} messages; subscribers disconnect every {quarter} msgs")

    for i in range(total_phase3):
        pub.publish(topic, json.dumps({"seq": seq, "ts": time.time()}), qos=qos)
        seq += 1
        msgs_since_reconnect += 1
        time.sleep(interval)

        # Every 25% of messages: disconnect all subscribers, wait, reconnect
        if not no_disconnect and msgs_since_reconnect >= quarter:
            any_online = any(s.online for s in subs)
            if any_online:
                cycle += 1
                for s in subs:
                    if s.online:
                        print(f"{s.tag} Cycle {cycle}: disconnecting at seq {seq}")
                        _disconnect(s.client)
                        s.online = False
                msgs_since_reconnect = 0

                # Keep publishing during the offline pause
                pause_end = time.time() + reconnect_pause
                while time.time() < pause_end:
                    pub.publish(topic, json.dumps({"seq": seq, "ts": time.time()}), qos=qos)
                    seq += 1
                    time.sleep(interval)

                # Reconnect all
                for s in subs:
                    print(f"{s.tag} Cycle {cycle}: reconnecting at seq {seq}")
                    client = _make_client(s.client_id, cfg["username"], cfg["password"],
                                          clean_session=False)
                    client.on_message = s.on_message
                    _connect(client, cfg["sub_host"], cfg["sub_port"])
                    s.client = client
                    s.online = True

    phase3_published = seq - phase3_start_seq
    print(f"[PUB] Phase 3 done: published {phase3_published} messages with "
          f"{cycle} disconnect cycles")

    # Small buffer so broker finishes persisting / delivering
    time.sleep(0.5)

    # Ensure all subscribers are connected for final drain
    for s in subs:
        if not s.online:
            client = _make_client(s.client_id, cfg["username"], cfg["password"],
                                  clean_session=False)
            client.on_message = s.on_message
            _connect(client, cfg["sub_host"], cfg["sub_port"])
            s.client = client
            s.online = True

    # Publish tail messages to confirm stream is alive after all cycles
    tail_count = max(5, rate)
    for _ in range(tail_count):
        pub.publish(topic, json.dumps({"seq": seq, "ts": time.time()}), qos=qos)
        seq += 1
        time.sleep(interval)

    last_seq = seq  # exclusive upper bound
    expected_count = last_seq - first_seq
    print(f"[PUB] Total published: seq {first_seq}..{last_seq - 1} "
          f"({expected_count} messages in validated range)")

    # Wait for queued + tail messages to arrive on all subscribers
    drain_timeout = max(15, disconnect_seconds * 3)
    deadline = time.time() + drain_timeout
    while time.time() < deadline:
        all_done = True
        for s in subs:
            with s.lock:
                count = sum(1 for sq in s.received_sequences if first_seq <= sq < last_seq)
                if count < expected_count:
                    all_done = False
                    break
        if all_done:
            break
        time.sleep(0.2)

    time.sleep(1.0)

    # -- Phase 5: cleanup -----------------------------------------------------
    _disconnect(pub)
    for s in subs:
        _disconnect(s.client)

    # Clean up persistent sessions
    for s in subs:
        cleanup = _make_client(s.client_id, cfg["username"], cfg["password"],
                               clean_session=True)
        _connect(cleanup, cfg["sub_host"], cfg["sub_port"])
        _disconnect(cleanup)

    # -- Phase 6: verify sequence integrity per subscriber --------------------
    all_passed = True
    for s in subs:
        with s.lock:
            relevant = [sq for sq in s.received_sequences if first_seq <= sq < last_seq]
            unique = sorted(set(relevant))
            lat_values = [lat for (sq, lat) in s.latencies if first_seq <= sq < last_seq]

        # Latency statistics
        if lat_values:
            lat_values.sort()
            n = len(lat_values)
            avg = statistics.mean(lat_values)
            med = statistics.median(lat_values)
            p95 = lat_values[int(n * 0.95)] if n > 1 else lat_values[0]
            p99 = lat_values[int(n * 0.99)] if n > 1 else lat_values[0]
            print(f"\n{s.tag} [LATENCY] {n} samples")
            print(f"  Min:  {lat_values[0]:.2f} ms")
            print(f"  Max:  {lat_values[-1]:.2f} ms")
            print(f"  Avg:  {avg:.2f} ms")
            print(f"  Med:  {med:.2f} ms")
            print(f"  P95:  {p95:.2f} ms")
            print(f"  P99:  {p99:.2f} ms")

        print(f"\n{s.tag} [RESULT] Received {len(relevant)} messages ({len(unique)} unique) "
              f"in range [{first_seq}..{last_seq - 1}]")

        if len(unique) == 0:
            pytest.fail(f"{s.tag} No messages received at all")

        expected = set(range(first_seq, last_seq))
        missing = sorted(expected - set(unique))
        duplicates = len(relevant) - len(unique)

        if missing:
            sample = missing[:20]
            print(f"{s.tag} [FAIL] Missing {len(missing)} messages: {sample}"
                  f"{'...' if len(missing) > 20 else ''}")
            all_passed = False

        if duplicates > 0:
            print(f"{s.tag} [INFO] {duplicates} duplicate(s) received (expected with QoS >= 1)")

        print(f"{s.tag} [RESULT] {len(unique)}/{expected_count} unique messages received")

        if qos == 2 and duplicates > 0:
            print(f"{s.tag} [FAIL] QoS 2 should have no duplicates, got {duplicates}")
            all_passed = False

    # Final assertions after reporting all subscribers
    for s in subs:
        with s.lock:
            relevant = [sq for sq in s.received_sequences if first_seq <= sq < last_seq]
            unique = sorted(set(relevant))
        missing = sorted(set(range(first_seq, last_seq)) - set(unique))
        duplicates = len(relevant) - len(unique)

        assert len(missing) == 0, (
            f"{s.tag} Missing {len(missing)}/{expected_count} messages. "
            f"First missing: {missing[:10]}"
        )
        if qos == 2:
            assert duplicates == 0, f"{s.tag} QoS 2 should have no duplicates, got {duplicates}"

    print(f"\n[PASS] All {expected_count} messages received successfully by all {num_subscribers} subscriber(s)")


# ---------------------------------------------------------------------------
# Parametrized tests at different rates
# ---------------------------------------------------------------------------

@pytest.mark.timeout(180)
@pytest.mark.parametrize("rate", [1, 10, 100], ids=["rate_1", "rate_10", "rate_100"])
def test_queuing_at_rate(cfg, rate, request):
    """Test queued message delivery at various publish rates."""
    cli_rate = request.config.getoption("--rate")
    if cli_rate is not None and rate != cli_rate:
        pytest.skip(f"Skipping rate={rate}, CLI requested --rate={cli_rate}")
    _run_queuing_test(cfg, rate, label=f"rate_{rate}")
