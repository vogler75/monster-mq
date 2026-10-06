import base64
import os
import tempfile
import time
import pytest
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
from .conftest import find_free_port, start_edge_node, start_main_node


class MessageSink:
    def __init__(self):
        self.messages = []

    def on_message(self, client, userdata, msg):
        self.messages.append({
            "topic": msg.topic,
            "payload": msg.payload.decode("utf-8"),
            "qos": msg.qos,
            "retain": msg.retain
        })


def make_mqtt_client(client_id, port):
    client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt.MQTTv5)
    sink = MessageSink()
    client.on_message = sink.on_message
    client.connect("127.0.0.1", port)
    client.loop_start()
    return client, sink


def test_qos_forwarding_edge_to_main(edge_main_pair):
    edge, main = edge_main_pair
    c_edge, _ = make_mqtt_client("pub-edge", edge.mqtt_port)
    c_main, sink_main = make_mqtt_client("sub-main", main.mqtt_port)

    c_main.subscribe("test/edge_to_main/#", qos=2)
    time.sleep(0.5)

    for qos in [0, 1, 2]:
        c_edge.publish(f"test/edge_to_main/q{qos}", f"hello-q{qos}", qos=qos)

    # Wait for all 3 messages
    start = time.time()
    while time.time() - start < 5.0 and len(sink_main.messages) < 3:
        time.sleep(0.1)

    c_edge.loop_stop()
    c_main.loop_stop()

    assert len(sink_main.messages) == 3
    payloads = {m["topic"]: m["payload"] for m in sink_main.messages}
    assert payloads["test/edge_to_main/q0"] == "hello-q0"
    assert payloads["test/edge_to_main/q1"] == "hello-q1"
    assert payloads["test/edge_to_main/q2"] == "hello-q2"


def test_qos_forwarding_main_to_edge(edge_main_pair):
    edge, main = edge_main_pair
    c_main, _ = make_mqtt_client("pub-main", main.mqtt_port)
    c_edge, sink_edge = make_mqtt_client("sub-edge", edge.mqtt_port)

    c_edge.subscribe("test/main_to_edge/#", qos=2)
    time.sleep(0.5)

    for qos in [0, 1, 2]:
        c_main.publish(f"test/main_to_edge/q{qos}", f"from-main-q{qos}", qos=qos)

    # Wait for all 3 messages
    start = time.time()
    while time.time() - start < 5.0 and len(sink_edge.messages) < 3:
        time.sleep(0.1)

    c_main.loop_stop()
    c_edge.loop_stop()

    assert len(sink_edge.messages) == 3
    payloads = {m["topic"]: m["payload"] for m in sink_edge.messages}
    assert payloads["test/main_to_edge/q0"] == "from-main-q0"
    assert payloads["test/main_to_edge/q1"] == "from-main-q1"
    assert payloads["test/main_to_edge/q2"] == "from-main-q2"


def test_retained_and_delete_bidirectional(edge_main_pair):
    edge, main = edge_main_pair
    c_edge, _ = make_mqtt_client("pub-edge-ret", edge.mqtt_port)
    c_main, _ = make_mqtt_client("pub-main-ret", main.mqtt_port)

    # 1. Edge publishes retained message
    c_edge.publish("test/retained/from_edge", "edge-retained-val", qos=1, retain=True)
    time.sleep(1.0)

    # Late subscriber on Main connects and subscribes
    c_main_late, sink_main_late = make_mqtt_client("sub-main-late", main.mqtt_port)
    c_main_late.subscribe("test/retained/from_edge", qos=1)

    start = time.time()
    while time.time() - start < 3.0 and len(sink_main_late.messages) == 0:
        time.sleep(0.1)

    assert len(sink_main_late.messages) == 1
    assert sink_main_late.messages[0]["payload"] == "edge-retained-val"
    assert sink_main_late.messages[0]["retain"] is True
    c_main_late.loop_stop()

    # Edge publishes retained delete (empty payload)
    c_edge.publish("test/retained/from_edge", "", qos=1, retain=True)
    time.sleep(1.0)

    # Second late subscriber on Main should receive nothing
    c_main_late2, sink_main_late2 = make_mqtt_client("sub-main-late2", main.mqtt_port)
    c_main_late2.subscribe("test/retained/from_edge", qos=1)
    time.sleep(0.5)
    c_main_late2.loop_stop()
    assert len(sink_main_late2.messages) == 0

    # 2. Main publishes retained message
    c_main.publish("test/retained/from_main", "main-retained-val", qos=1, retain=True)
    time.sleep(1.0)

    # Late subscriber on Edge connects and subscribes
    c_edge_late, sink_edge_late = make_mqtt_client("sub-edge-late", edge.mqtt_port)
    c_edge_late.subscribe("test/retained/from_main", qos=1)

    start = time.time()
    while time.time() - start < 3.0 and len(sink_edge_late.messages) == 0:
        time.sleep(0.1)

    assert len(sink_edge_late.messages) == 1
    assert sink_edge_late.messages[0]["payload"] == "main-retained-val"
    assert sink_edge_late.messages[0]["retain"] is True
    c_edge_late.loop_stop()

    # Main publishes retained delete
    c_main.publish("test/retained/from_main", "", qos=1, retain=True)
    time.sleep(1.0)

    c_edge_late2, sink_edge_late2 = make_mqtt_client("sub-edge-late2", edge.mqtt_port)
    c_edge_late2.subscribe("test/retained/from_main", qos=1)
    time.sleep(0.5)
    c_edge_late2.loop_stop()
    assert len(sink_edge_late2.messages) == 0

    c_edge.loop_stop()
    c_main.loop_stop()


def test_loop_prevention(edge_main_pair):
    edge, main = edge_main_pair
    c_edge, sink_edge = make_mqtt_client("loop-edge", edge.mqtt_port)
    c_edge.subscribe("test/loop/#", qos=1)
    time.sleep(0.5)

    # Publish on Edge
    c_edge.publish("test/loop/one", "no-echo", qos=1)

    # Wait for the local message
    start = time.time()
    while time.time() - start < 2.0 and len(sink_edge.messages) == 0:
        time.sleep(0.1)

    assert len(sink_edge.messages) == 1

    # Wait further: replica reaching Main should NOT be echoed back to Edge
    time.sleep(1.5)
    c_edge.loop_stop()
    assert len(sink_edge.messages) == 1, "Message was echoed back through PeerLink loop!"


def test_status_and_resync_endpoint(edge_main_pair):
    edge, main = edge_main_pair
    status_edge = edge.get_status()
    status_main = main.get_status()

    assert status_edge["nodeId"] == "edge-a"
    assert status_main["nodeId"] == "main-b"
    assert status_edge["enabled"] is True
    assert status_main["enabled"] is True

    # Test resync endpoint on Main
    import requests
    res = requests.post(f"http://127.0.0.1:{main.peer_port}/peerlink/v1/resync?source=edge-a")
    assert res.status_code in [200, 204]


def test_cross_shared_secrets_tls(edge_binary):
    """Test full TLS 1.3 shared-secret mutual authentication between Go Edge and Kotlin broker."""
    work_dir = tempfile.mkdtemp(prefix="pl_sec_")
    secret = base64.b64encode(b"01234567890123456789012345678901").decode("ascii")

    edge_mqtt = find_free_port()
    edge_peer = find_free_port()
    main_mqtt = find_free_port()
    main_peer = find_free_port()

    edge = start_edge_node(edge_binary, "sec-edge", edge_mqtt, edge_peer, "sec-main", main_peer, work_dir, shared_secrets=[secret], tls=True)
    main = start_main_node("sec-main", main_mqtt, main_peer, "sec-edge", edge_peer, work_dir, shared_secrets=[secret], tls=True)

    try:
        # Wait until both peers reach STREAMING
        start = time.time()
        connected = False
        while time.time() - start < 12.0:
            try:
                status_main = main.get_status()
                src = next((s for s in status_main.get("sources", []) if s.get("nodeId") == "sec-edge"), None)
                if src and src.get("state") == "STREAMING":
                    connected = True
                    break
            except Exception:
                pass
            time.sleep(0.3)

        assert connected, "TLS Shared-Secret link between Edge and Main failed to reach STREAMING"

        # Verify messages flow over the TLS+Secret authenticated link
        c_edge, _ = make_mqtt_client("pub-sec-edge", edge.mqtt_port)
        c_main, sink_main = make_mqtt_client("sub-sec-main", main.mqtt_port)
        c_main.subscribe("test/sec/#", qos=1)
        time.sleep(0.5)

        c_edge.publish("test/sec/secret_msg", "encrypted_payload", qos=1)

        start = time.time()
        while time.time() - start < 5.0 and len(sink_main.messages) == 0:
            time.sleep(0.1)

        c_edge.loop_stop()
        c_main.loop_stop()

        assert len(sink_main.messages) == 1
        assert sink_main.messages[0]["payload"] == "encrypted_payload"

    finally:
        edge.stop()
        main.stop()
