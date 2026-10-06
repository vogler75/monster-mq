import os
import socket
import subprocess
import tempfile
import time
import pytest
import yaml
import requests
import paho.mqtt.client as mqtt

EDGE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../edge"))
MAIN_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
BROKER_DIR = os.path.join(MAIN_DIR, "broker")

EDGE_BIN = os.path.join(EDGE_DIR, "bin/monstermq-edge-darwin-arm64")
if not os.path.exists(EDGE_BIN):
    EDGE_BIN = os.path.join(EDGE_DIR, "bin/monstermq-edge")


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def wait_port_open(port, host="127.0.0.1", timeout=15.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except (OSError, ConnectionRefusedError):
            time.sleep(0.1)
    return False


@pytest.fixture(scope="session")
def edge_binary():
    assert os.path.exists(EDGE_BIN), f"Edge binary not found at {EDGE_BIN}"
    return EDGE_BIN


class BrokerProcess:
    def __init__(self, name, proc, work_dir, mqtt_port, peer_port):
        self.name = name
        self.proc = proc
        self.work_dir = work_dir
        self.mqtt_port = mqtt_port
        self.peer_port = peer_port

    def stop(self):
        if self.proc:
            try:
                self.proc.terminate()
                self.proc.wait(timeout=3)
            except Exception:
                try:
                    self.proc.kill()
                except Exception:
                    pass
            self.proc = None

    def get_status(self):
        resp = requests.get(f"http://127.0.0.1:{self.peer_port}/peerlink/v1/status", timeout=2)
        assert resp.status_code == 200
        return resp.json()


def start_edge_node(edge_bin, node_id, mqtt_port, peer_port, remote_node_id, remote_peer_port, work_dir, shared_secrets=None, tls=False):
    cfg = {
        "NodeId": node_id,
        "TCP": {"Enabled": True, "Port": mqtt_port},
        "WS": {"Enabled": False},
        "GraphQL": {"Enabled": False},
        "Metrics": {"Enabled": False},
        "SQLite": {"Path": os.path.join(work_dir, f"{node_id}.db")},
        "DefaultStoreType": "SQLITE",
        "PeerLink": {
            "Enabled": True,
            "AllowUnauthenticatedPeers": not bool(shared_secrets),
            "Listener": {
                "Address": "127.0.0.1",
                "Port": peer_port,
                "AllowedNetworks": ["127.0.0.1/32"],
                "AllowPlaintext": not tls,
            },
            "Log": {
                "MaxBytes": 16777216,
                "DrainOnShutdownMs": 1000,
            },
            "Peers": [
                {
                    "NodeId": remote_node_id,
                    "Address": f"127.0.0.1:{remote_peer_port}",
                    "Serve": True,
                }
            ],
        },
    }
    if tls:
        cfg["PeerLink"]["Tls"] = {
            "Enabled": True,
            "AutoGenerate": True,
            "ClientAuth": "NONE",
            "InsecureSkipVerify": True,
        }
    if shared_secrets:
        cfg["PeerLink"]["SharedSecrets"] = shared_secrets

    cfg_path = os.path.join(work_dir, f"{node_id}.yaml")
    with open(cfg_path, "w") as f:
        yaml.dump(cfg, f)

    cmd = [edge_bin, "-config", cfg_path]
    log_file = open(os.path.join(work_dir, f"{node_id}.log"), "w")
    proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
    assert wait_port_open(mqtt_port), f"Edge {node_id} failed to open MQTT port {mqtt_port}"
    assert wait_port_open(peer_port), f"Edge {node_id} failed to open PeerLink port {peer_port}"
    return BrokerProcess(node_id, proc, work_dir, mqtt_port, peer_port)


def start_main_node(node_id, mqtt_port, peer_port, remote_node_id, remote_peer_port, work_dir, shared_secrets=None, tls=False):
    db_dir = os.path.join(work_dir, f"{node_id}_sqlite")
    os.makedirs(db_dir, exist_ok=True)
    cfg = {
        "NodeId": node_id,
        "TCP": mqtt_port,
        "WS": 0,
        "SQLite": {
            "Path": db_dir,
            "EnableWAL": True,
        },
        "DefaultStoreType": "SQLITE",
        "GraphQL": {"Enabled": False, "Port": 0},
        "MCP": {"Enabled": False, "Port": 0},
        "Metrics": {"Enabled": False},
        "UserManagement": {"Enabled": False},
        "QueuedMessagesEnabled": True,
        "PeerLink": {
            "Enabled": True,
            "AllowUnauthenticatedPeers": not bool(shared_secrets),
            "Listener": {
                "Address": "127.0.0.1",
                "Port": peer_port,
                "AllowedNetworks": ["127.0.0.1/32"],
                "AllowPlaintext": not tls,
            },
            "Log": {
                "MaxBytes": 16777216,
                "DrainOnShutdownMs": 1000,
            },
            "Peers": [
                {
                    "NodeId": remote_node_id,
                    "Address": f"127.0.0.1:{remote_peer_port}",
                    "Serve": True,
                }
            ],
        },
    }
    if tls:
        cfg["PeerLink"]["Tls"] = {
            "Enabled": True,
            "AutoGenerate": True,
            "ClientAuth": "NONE",
        }
        cfg["PeerLink"]["Peers"][0]["Tls"] = {
            "Enabled": True,
            "InsecureSkipVerify": True,
        }
    if shared_secrets:
        cfg["PeerLink"]["SharedSecrets"] = shared_secrets

    cfg_path = os.path.join(work_dir, f"{node_id}.yaml")
    with open(cfg_path, "w") as f:
        yaml.dump(cfg, f)

    cmd = [
        "java",
        "-cp",
        "target/classes:target/dependencies/*",
        "at.rocworks.MonsterKt",
        "-config",
        cfg_path,
    ]
    log_file = open(os.path.join(work_dir, f"{node_id}.log"), "w")
    proc = subprocess.Popen(cmd, cwd=BROKER_DIR, stdout=log_file, stderr=subprocess.STDOUT)
    assert wait_port_open(mqtt_port, timeout=20.0), f"Main {node_id} failed to open MQTT port {mqtt_port}"
    assert wait_port_open(peer_port, timeout=20.0), f"Main {node_id} failed to open PeerLink port {peer_port}"
    return BrokerProcess(node_id, proc, work_dir, mqtt_port, peer_port)


@pytest.fixture
def edge_main_pair(edge_binary):
    """Starts a connected pair of Go Edge broker and Kotlin Main broker."""
    work_dir = tempfile.mkdtemp(prefix="pl_test_")
    edge_mqtt = find_free_port()
    edge_peer = find_free_port()
    main_mqtt = find_free_port()
    main_peer = find_free_port()

    edge = start_edge_node(edge_binary, "edge-a", edge_mqtt, edge_peer, "main-b", main_peer, work_dir)
    main = start_main_node("main-b", main_mqtt, main_peer, "edge-a", edge_peer, work_dir)

    # Wait until both peers establish PeerLink connection (streaming)
    start = time.time()
    connected = False
    while time.time() - start < 10.0:
        try:
            status_edge = edge.get_status()
            status_main = main.get_status()
            edge_src = next((s for s in status_edge.get("sources", []) if s.get("nodeId") == "main-b"), None)
            main_src = next((s for s in status_main.get("sources", []) if s.get("nodeId") == "edge-a"), None)
            if edge_src and edge_src.get("state") == "STREAMING" and main_src and main_src.get("state") == "STREAMING":
                connected = True
                break
        except Exception:
            pass
        time.sleep(0.2)

    assert connected, "Edge and Main did not reach STREAMING state within 10s"

    yield edge, main

    edge.stop()
    main.stop()
