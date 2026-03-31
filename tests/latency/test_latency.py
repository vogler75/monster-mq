#!/usr/bin/env python3
"""
MQTT publish-subscribe latency measurement.

Publishes messages at a fixed interval and measures the time between
publish and receive on the subscriber side. Both publisher and subscriber
stay connected for the entire test — no disconnect/reconnect cycles.

Usage:
    # Defaults: QoS 1, 100ms interval, 10s duration
    pytest test_latency.py -v -s

    # QoS 0, 10ms interval, 30 seconds
    pytest test_latency.py -v -s --qos 0 --interval-ms 10 --duration 30

    # QoS 2, 500ms interval
    pytest test_latency.py -v -s --qos 2 --interval-ms 500
"""

import json
import statistics
import threading
import time
import uuid

import paho.mqtt.client as mqtt
import pytest


def _make_client(client_id, username, password):
    c = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id,
        protocol=mqtt.MQTTv311,
        clean_session=True,
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
    client._connack.clear()
    client.connect(host, port, keepalive=60)
    client.loop_start()
    assert client._connack.wait(timeout), f"CONNACK not received within {timeout}s"


def _subscribe(client, topic, qos, timeout=5.0):
    client._suback.clear()
    client.subscribe(topic, qos=qos)
    assert client._suback.wait(timeout), f"SUBACK not received within {timeout}s"


def _disconnect(client):
    client.loop_stop()
    client.disconnect()


@pytest.mark.timeout(120)
def test_latency(cfg):
    """Measure pub-sub latency over a sustained publish stream."""
    qos = cfg["qos"]
    interval_s = cfg["interval_ms"] / 1000.0
    duration = cfg["duration"]
    host = cfg["host"]
    port = cfg["port"]

    uid = uuid.uuid4().hex[:8]
    topic = f"test/latency/{uid}"

    latencies_ms = []
    received_seqs = []
    lock = threading.Lock()
    first_received = threading.Event()

    def on_message(client, userdata, msg):
        recv_ts = time.time()
        try:
            data = json.loads(msg.payload)
            lat = (recv_ts - data["ts"]) * 1000.0
            with lock:
                latencies_ms.append(lat)
                if "seq" in data:
                    received_seqs.append(data["seq"])
                if not first_received.is_set():
                    first_received.set()
        except (json.JSONDecodeError, KeyError):
            pass

    # -- Connect subscriber first -----------------------------------------------
    sub = _make_client(f"lat_sub_{uid}", cfg["username"], cfg["password"])
    sub.on_message = on_message
    _connect(sub, host, port)
    _subscribe(sub, topic, qos)

    # -- Connect publisher -------------------------------------------------------
    pub = _make_client(f"lat_pub_{uid}", cfg["username"], cfg["password"])
    _connect(pub, host, port)

    print(f"\n{'='*60}")
    print(f"Latency test: QoS={qos}  interval={cfg['interval_ms']}ms  duration={duration}s")
    print(f"{'='*60}")

    # -- Wait for first message to confirm path is active -----------------------
    deadline = time.time() + 10.0
    while not first_received.is_set() and time.time() < deadline:
        pub.publish(topic, json.dumps({"ts": time.time(), "warmup": True}), qos=qos)
        time.sleep(interval_s)
    assert first_received.is_set(), "Subscriber never received a message within 10s"

    # Reset after warmup
    with lock:
        latencies_ms.clear()

    # -- Publish for the configured duration ------------------------------------
    end_time = time.time() + duration
    seq = 0
    while time.time() < end_time:
        pub.publish(topic, json.dumps({"ts": time.time(), "seq": seq}), qos=qos)
        seq += 1
        time.sleep(interval_s)
    count = seq

    # Wait for remaining messages to arrive
    time.sleep(1.0)

    # -- Cleanup -----------------------------------------------------------------
    _disconnect(pub)
    _disconnect(sub)

    # -- Results -----------------------------------------------------------------
    with lock:
        lats = list(latencies_ms)

    print(f"\n[INFO] Published {count} messages, received {len(lats)}")

    assert len(lats) > 0, "No messages received"

    lats.sort()
    n = len(lats)
    avg = statistics.mean(lats)
    med = statistics.median(lats)
    stdev = statistics.stdev(lats) if n > 1 else 0.0
    p95 = lats[int(n * 0.95)] if n > 1 else lats[0]
    p99 = lats[int(n * 0.99)] if n > 1 else lats[0]

    print(f"\n[LATENCY] {n} samples")
    print(f"  Min:    {lats[0]:.2f} ms")
    print(f"  Max:    {lats[-1]:.2f} ms")
    print(f"  Avg:    {avg:.2f} ms")
    print(f"  Median: {med:.2f} ms")
    print(f"  StdDev: {stdev:.2f} ms")
    print(f"  P95:    {p95:.2f} ms")
    print(f"  P99:    {p99:.2f} ms")

    loss = count - n
    if loss > 0:
        print(f"  Loss:   {loss}/{count} ({loss/count*100:.1f}%)")
    else:
        print(f"  Loss:   0/{count} (0.0%)")

    # -- Sequence validation -----------------------------------------------------
    with lock:
        seqs = list(received_seqs)

    if seqs:
        out_of_order = 0
        duplicates = 0
        gaps = 0
        prev = seqs[0]
        seen = {prev}
        for s in seqs[1:]:
            if s in seen:
                duplicates += 1
            else:
                seen.add(s)
                if s != prev + 1:
                    if s < prev:
                        out_of_order += 1
                    else:
                        gaps += s - prev - 1
                prev = s

        expected = set(range(count))
        missing = sorted(expected - seen)

        print(f"\n[SEQUENCE] {len(seqs)} messages checked")
        print(f"  Missing:      {len(missing)}")
        print(f"  Duplicates:   {duplicates}")
        print(f"  Out-of-order: {out_of_order}")
        if missing:
            sample = missing[:20]
            print(f"  First missing: {sample}{'...' if len(missing) > 20 else ''}")
