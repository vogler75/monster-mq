"""
MQTT client bridge with mutual TLS (issue #111).

Starts a local Mosquitto that requires a client certificate, creates MQTT-Client
bridges on MonsterMQ through GraphQL and checks that messages flow in both
directions only when the bridge presents its certificate.

Requirements:
  - MonsterMQ running on this machine (it reads the generated certificate files
    from the local temp directory), GraphQL at GRAPHQL_URL and MQTT at
    MQTT_BROKER/MQTT_PORT.
  - A `mosquitto` binary on PATH (or MOSQUITTO_BIN).
  - The Python `cryptography` package.

The tests are skipped when any of these is missing.
"""
import datetime
import ipaddress
import os
import shutil
import socket
import ssl
import subprocess
import threading
import time
import uuid

import pytest
import requests
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion

pytestmark = [pytest.mark.mtls, pytest.mark.integration]

GRAPHQL_URL = os.getenv("GRAPHQL_URL", "http://localhost:4000/graphql")
BROKER_HOST = os.getenv("MQTT_BROKER", "localhost")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
USERNAME = os.getenv("MQTT_USERNAME", "Test")
PASSWORD = os.getenv("MQTT_PASSWORD", "Test")
MOSQUITTO_BIN = os.getenv("MOSQUITTO_BIN") or shutil.which("mosquitto")


# --- certificates ---

def _write_certs(directory):
    crypto = pytest.importorskip("cryptography")  # noqa: F841
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    now = datetime.datetime.now(datetime.timezone.utc)

    def issue(cn, key, issuer_name, issuer_key, ca=False, san=None):
        builder = (x509.CertificateBuilder()
                   .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)]))
                   .issuer_name(issuer_name)
                   .public_key(key.public_key())
                   .serial_number(x509.random_serial_number())
                   .not_valid_before(now - datetime.timedelta(minutes=5))
                   .not_valid_after(now + datetime.timedelta(days=1))
                   .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True))
        if san:
            builder = builder.add_extension(x509.SubjectAlternativeName(san), critical=False)
        return builder.sign(issuer_key, hashes.SHA256())

    def write_key(name, key, password=None):
        encryption = (serialization.BestAvailableEncryption(password.encode()) if password
                      else serialization.NoEncryption())
        path = os.path.join(directory, name)
        with open(path, "wb") as f:
            f.write(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, encryption))
        return path

    def write_cert(name, cert):
        path = os.path.join(directory, name)
        with open(path, "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))
        return path

    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "bridge-test-ca")])
    ca = issue("bridge-test-ca", ca_key, ca_name, ca_key, ca=True)
    server_key = ec.generate_private_key(ec.SECP256R1())
    server = issue("localhost", server_key, ca_name, ca_key,
                   san=[x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))])
    client_key = ec.generate_private_key(ec.SECP256R1())
    client = issue("bridge-client", client_key, ca_name, ca_key)

    return {
        "ca": write_cert("ca.pem", ca),
        "server_cert": write_cert("server.pem", server),
        "server_key": write_key("server.key", server_key),
        "client_cert": write_cert("client.pem", client),
        "client_key": write_key("client.key", client_key),
        "client_key_encrypted": write_key("client-enc.key", client_key, password="bridge-secret"),
    }


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# --- GraphQL ---

def _graphql(query, variables=None, headers=None):
    response = requests.post(GRAPHQL_URL, json={"query": query, "variables": variables or {}},
                             headers=headers or {}, timeout=10)
    response.raise_for_status()
    return response.json()


@pytest.fixture(scope="module")
def auth_headers():
    try:
        _graphql("{ __typename }")
    except Exception:
        pytest.skip("GraphQL endpoint not reachable at " + GRAPHQL_URL)
    for username, password in ((os.getenv("GRAPHQL_USERNAME"), os.getenv("GRAPHQL_PASSWORD")),
                               ("Admin", "Admin"), (USERNAME, PASSWORD)):
        if not username or not password:
            continue
        result = _graphql("mutation($u: String!, $p: String!) { login(username: $u, password: $p) { token } }",
                          {"u": username, "p": password})
        token = ((result.get("data") or {}).get("login") or {}).get("token")
        if token:
            return {"Authorization": "Bearer " + token}
    return {}


@pytest.fixture(scope="module")
def tls_support(auth_headers):
    result = _graphql('{ __type(name: "MqttClientConnectionConfigInput") { inputFields { name } } }',
                      headers=auth_headers)
    fields = {f["name"] for f in (((result.get("data") or {}).get("__type") or {}).get("inputFields") or [])}
    if "tlsClientCertPath" not in fields:
        pytest.skip("broker does not support MQTT client TLS options yet")


# --- remote broker ---

@pytest.fixture(scope="module")
def remote_broker(tmp_path_factory):
    if not MOSQUITTO_BIN:
        pytest.skip("mosquitto binary not found (set MOSQUITTO_BIN)")
    directory = str(tmp_path_factory.mktemp("bridge-mtls"))
    certs = _write_certs(directory)
    port = _free_port()
    conf = os.path.join(directory, "mosquitto.conf")
    with open(conf, "w") as f:
        f.write("\n".join([
            "per_listener_settings false",
            "allow_anonymous true",
            "listener {} 127.0.0.1".format(port),
            "cafile " + certs["ca"],
            "certfile " + certs["server_cert"],
            "keyfile " + certs["server_key"],
            "require_certificate true",
            "",
        ]))
    proc = subprocess.Popen([MOSQUITTO_BIN, "-c", conf], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            break
        except OSError:
            if proc.poll() is not None:
                pytest.fail("mosquitto exited: " + proc.stdout.read().decode(errors="replace"))
            time.sleep(0.2)
    yield {"port": port, **certs}
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


# --- MQTT helpers ---

class Collector:
    def __init__(self, client, topic):
        self.messages = []
        self.event = threading.Event()
        subscribed = threading.Event()
        client.on_message = self._on_message
        client.on_subscribe = lambda *args: subscribed.set()
        client.subscribe(topic, qos=1)
        assert subscribed.wait(5), "subscribe to {} not acknowledged".format(topic)

    def _on_message(self, client, userdata, msg):
        self.messages.append((msg.topic, msg.payload.decode(errors="replace")))
        self.event.set()

    def wait_for(self, payload, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if any(p == payload for _, p in self.messages):
                return True
            self.event.wait(0.2)
            self.event.clear()
        return False


def _connect(client, host, port):
    connected = threading.Event()
    client.on_connect = lambda c, u, f, rc, props=None: connected.set() if rc == 0 else None
    client.connect(host, port)
    client.loop_start()
    assert connected.wait(10), "could not connect to {}:{}".format(host, port)
    return client


@pytest.fixture
def local_client():
    client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="bridge-mtls-local-" + uuid.uuid4().hex[:6])
    if USERNAME:
        client.username_pw_set(USERNAME, PASSWORD)
    yield _connect(client, BROKER_HOST, BROKER_PORT)
    client.loop_stop()
    client.disconnect()


@pytest.fixture
def remote_client(remote_broker):
    client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="bridge-mtls-remote-" + uuid.uuid4().hex[:6])
    client.tls_set(ca_certs=remote_broker["ca"], certfile=remote_broker["client_cert"],
                   keyfile=remote_broker["client_key"], cert_reqs=ssl.CERT_REQUIRED)
    yield _connect(client, "localhost", remote_broker["port"])
    client.loop_stop()
    client.disconnect()


# --- bridge lifecycle ---

@pytest.fixture
def bridge(auth_headers, tls_support):
    created = []

    def create(config, prefix):
        name = "bridge-mtls-" + uuid.uuid4().hex[:8]
        result = _graphql(
            """mutation($input: MqttClientInput!) {
                 mqttClient { create(input: $input) { success errors } }
               }""",
            {"input": {"name": name, "namespace": "test/" + name, "nodeId": "*", "enabled": True, "config": config}},
            headers=auth_headers)
        payload = ((result.get("data") or {}).get("mqttClient") or {}).get("create") or {}
        assert payload.get("success"), "create failed: {}".format(result)
        created.append(name)
        for address in (
            {"mode": "SUBSCRIBE", "remoteTopic": prefix + "/down/#", "localTopic": prefix + "/down", "qos": 1},
            {"mode": "PUBLISH", "remoteTopic": prefix + "/up", "localTopic": prefix + "/up/#", "qos": 1},
        ):
            result = _graphql(
                """mutation($name: String!, $input: MqttClientAddressInput!) {
                     mqttClient { addAddress(deviceName: $name, input: $input) { success errors } }
                   }""",
                {"name": name, "input": address}, headers=auth_headers)
            added = ((result.get("data") or {}).get("mqttClient") or {}).get("addAddress") or {}
            assert added.get("success"), "addAddress failed: {}".format(result)
        return name

    yield create

    for name in created:
        _graphql("mutation($n: String!) { mqttClient { delete(name: $n) } }", {"n": name}, headers=auth_headers)


def _tls_config(remote_broker, **overrides):
    config = {
        "brokerUrl": "ssl://127.0.0.1:{}".format(remote_broker["port"]),
        "clientId": "monstermq-" + uuid.uuid4().hex[:8],
        "tlsCaCertPath": remote_broker["ca"],
        # The URL uses the IP address; verify the certificate against its DNS name instead
        "tlsServerName": "localhost",
        "tlsClientCertPath": remote_broker["client_cert"],
        "tlsClientKeyPath": remote_broker["client_key_encrypted"],
        "tlsClientKeyPassword": "bridge-secret",
    }
    config.update(overrides)
    return {k: v for k, v in config.items() if v is not None}


def _assert_flow(local_client, remote_client, prefix, expect):
    remote_in = Collector(remote_client, prefix + "/up/#")
    local_in = Collector(local_client, prefix + "/down/#")

    # The bridge connects asynchronously; keep publishing until it forwards or the time is up.
    deadline = time.time() + (20 if expect else 6)
    up_ok = down_ok = False
    n = 0
    while time.time() < deadline and not (up_ok and down_ok):
        n += 1
        if not up_ok:
            local_client.publish(prefix + "/up/sensor", "up-{}".format(n), qos=1)
            up_ok = remote_in.wait_for("up-{}".format(n), 1)
        if not down_ok:
            remote_client.publish(prefix + "/down/cmd", "down-{}".format(n), qos=1)
            down_ok = local_in.wait_for("down-{}".format(n), 1)

    if expect:
        assert up_ok, "local -> remote message was not bridged"
        assert down_ok, "remote -> local message was not bridged"
    else:
        assert not up_ok and not down_ok, "bridge without client certificate must not forward messages"


def test_bridge_with_client_certificate_forwards_both_ways(remote_broker, bridge, local_client, remote_client):
    prefix = "bridgemtls/" + uuid.uuid4().hex[:8]
    bridge(_tls_config(remote_broker), prefix)
    _assert_flow(local_client, remote_client, prefix, expect=True)


def test_bridge_without_client_certificate_is_rejected(remote_broker, bridge, local_client, remote_client):
    prefix = "bridgemtls/" + uuid.uuid4().hex[:8]
    bridge(_tls_config(remote_broker, tlsClientCertPath=None, tlsClientKeyPath=None, tlsClientKeyPassword=None), prefix)
    _assert_flow(local_client, remote_client, prefix, expect=False)


def test_invalid_tls_config_is_rejected(auth_headers, tls_support):
    result = _graphql(
        """mutation($input: MqttClientInput!) { mqttClient { create(input: $input) { success errors } } }""",
        {"input": {"name": "bridge-mtls-invalid-" + uuid.uuid4().hex[:6], "namespace": "test/invalid", "nodeId": "*",
                   "config": {"brokerUrl": "tcp://localhost:1883", "tlsClientCertPath": "/tmp/client.pem"}}},
        headers=auth_headers)
    payload = result["data"]["mqttClient"]["create"]
    assert payload["success"] is False
    joined = " ".join(payload["errors"])
    assert "ssl://" in joined and "tlsClientKeyPath" in joined, joined
