#!/usr/bin/env python3
"""Client certificate (mutual TLS) authentication tests.

Covers SSL.UseIdentityAsUsername, where the certificate Common Name is used as
the authenticated username instead of a password.
"""
import os
import threading
import time
import uuid

import paho.mqtt.client as mqtt
import pytest
import requests

pytestmark = pytest.mark.mtls

GRAPHQL_URL = os.getenv("GRAPHQL_URL", "http://localhost:4000/graphql")
ADMIN_USER = os.getenv("MQTT_ADMIN_USERNAME", "Admin")
ADMIN_PASSWORD = os.getenv("MQTT_ADMIN_PASSWORD", "Admin")
# "1" when the broker under test has SSL.AutoCreateUser enabled, "0" when disabled.
AUTO_CREATE = os.getenv("MTLS_AUTO_CREATE_USER", "")


def _make_client(client_id, mtls_config, with_certificate=True, username=None, password=None):
    """Build an MQTT 3.1.1 client, optionally presenting a client certificate."""
    unique_id = "{}_{}".format(client_id, uuid.uuid4().hex[:8])
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, unique_id, protocol=mqtt.MQTTv311)

    if with_certificate:
        c.tls_set(ca_certs=mtls_config["ca_cert"],
                  certfile=mtls_config["client_cert"],
                  keyfile=mtls_config["client_key"])
    else:
        c.tls_set(ca_certs=mtls_config["ca_cert"])
    if mtls_config["insecure"]:
        c.tls_insecure_set(True)

    if username is not None:
        c.username_pw_set(username, password)

    c._connack_event = threading.Event()
    c._suback_event = threading.Event()
    c._messages = []
    c._rc = None

    def on_connect(client, userdata, flags, rc, properties=None):
        client._rc = rc
        client._connack_event.set()

    def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        client._suback_event.set()

    def on_message(client, userdata, msg):
        client._messages.append((msg.topic, msg.payload.decode()))

    c.on_connect = on_connect
    c.on_subscribe = on_subscribe
    c.on_message = on_message
    return c


def _connect(client, mtls_config, timeout=10.0):
    """Connect and wait for CONNACK. Returns the reason code, or None on timeout."""
    client.connect_async(mtls_config["host"], mtls_config["port"], keepalive=30)
    client.loop_start()
    if not client._connack_event.wait(timeout=timeout):
        return None
    return client._rc


def _close(client):
    client.loop_stop()
    try:
        client.disconnect()
    except Exception:
        pass


def _graphql(query, variables=None, headers=None):
    return requests.post(GRAPHQL_URL,
                         json={"query": query, "variables": variables or {}},
                         headers=headers or {},
                         timeout=10).json()


def _admin_headers():
    """Log in as admin, or skip when GraphQL/user management is not reachable."""
    try:
        result = _graphql(
            "mutation($u: String!, $p: String!) { login(username: $u, password: $p) { success token } }",
            {"u": ADMIN_USER, "p": ADMIN_PASSWORD},
        )
    except requests.RequestException:
        pytest.skip("GraphQL endpoint not reachable at " + GRAPHQL_URL)
    token = ((result.get("data") or {}).get("login") or {}).get("token")
    if not token:
        pytest.skip("Could not log in as admin for user management")
    return {"Authorization": "Bearer " + token}


def _delete_user(username, headers):
    result = _graphql(
        "mutation($u: String!) { user { deleteUser(username: $u) { success message } } }",
        {"u": username},
        headers,
    )
    return ((result.get("data") or {}).get("user") or {}).get("deleteUser") or {}


def _get_user(username, headers):
    result = _graphql(
        "query($u: String) { users(username: $u) { username enabled canSubscribe canPublish isAdmin } }",
        {"u": username},
        headers,
    )
    users = (result.get("data") or {}).get("users") or []
    return users[0] if users else None


def _ensure_user(username, headers):
    """Create the account for a certificate Common Name if it does not exist yet."""
    if _get_user(username, headers) is not None:
        return True
    result = _graphql(
        "mutation($u: String!, $p: String!) { user { createUser(input: {username: $u, password: $p}) "
        "{ success message } } }",
        {"u": username, "p": "cert-" + uuid.uuid4().hex},
        headers,
    )
    created = ((result.get("data") or {}).get("user") or {}).get("createUser") or {}
    return bool(created.get("success"))


def _set_user_enabled(username, enabled, headers):
    result = _graphql(
        "mutation($u: String!, $e: Boolean!) { user { updateUser(input: {username: $u, enabled: $e}) "
        "{ success message } } }",
        {"u": username, "e": enabled},
        headers,
    )
    return ((result.get("data") or {}).get("user") or {}).get("updateUser") or {}


@pytest.fixture(autouse=True)
def account_exists(request, mtls_config):
    """With AutoCreateUser disabled the account must be pre-provisioned, so create it here.

    Skipped for the test that deliberately removes the account.
    """
    if AUTO_CREATE == "0" and request.node.name != "test_unknown_common_name_rejected":
        _ensure_user(mtls_config["common_name"], _admin_headers())
    yield


def test_connect_with_client_certificate(mtls_config):
    """A client presenting a valid certificate connects without username or password."""
    client = _make_client("cert_connect", mtls_config)
    try:
        rc = _connect(client, mtls_config)
        assert rc is not None, "no CONNACK received"
        assert rc == 0, "expected CONNACK 0, got {}".format(rc)
    finally:
        _close(client)


def test_publish_and_subscribe_with_client_certificate(mtls_config):
    """A certificate-authenticated client can actually publish and subscribe."""
    topic = "test/mtls/{}".format(uuid.uuid4().hex[:8])
    payload = "hello over mtls"

    subscriber = _make_client("cert_sub", mtls_config)
    publisher = _make_client("cert_pub", mtls_config)
    try:
        assert _connect(subscriber, mtls_config) == 0
        subscriber.subscribe(topic, qos=1)
        assert subscriber._suback_event.wait(timeout=5.0), "no SUBACK received"

        assert _connect(publisher, mtls_config) == 0
        publisher.publish(topic, payload, qos=1)

        deadline = time.time() + 5.0
        while time.time() < deadline and not subscriber._messages:
            time.sleep(0.1)

        assert subscriber._messages, "certificate-authenticated client received no message"
        assert subscriber._messages[0] == (topic, payload)
    finally:
        _close(subscriber)
        _close(publisher)


def test_disabled_user_is_rejected(mtls_config):
    """Disabling the account for a certificate Common Name blocks that client."""
    headers = _admin_headers()
    username = mtls_config["common_name"]

    # Make sure the account exists by connecting once.
    warmup = _make_client("cert_warmup", mtls_config)
    try:
        assert _connect(warmup, mtls_config) == 0
    finally:
        _close(warmup)

    result = _set_user_enabled(username, False, headers)
    if not result.get("success"):
        pytest.skip("Could not disable user {}: {}".format(username, result.get("message")))

    try:
        client = _make_client("cert_disabled", mtls_config)
        try:
            rc = _connect(client, mtls_config, timeout=10.0)
            assert rc != 0, "disabled user {} was allowed to connect (rc={})".format(username, rc)
        finally:
            _close(client)
    finally:
        _set_user_enabled(username, True, headers)


def test_password_client_without_certificate(mtls_config):
    """With ClientAuth REQUEST, a client with no certificate still uses username and password."""
    client = _make_client("password_client", mtls_config,
                          with_certificate=False,
                          username=ADMIN_USER, password=ADMIN_PASSWORD)
    try:
        rc = _connect(client, mtls_config)
        if rc is None:
            pytest.skip("no CONNACK - broker may be configured with ClientAuth REQUIRED")
        assert rc == 0, "expected CONNACK 0 for username/password client, got {}".format(rc)
    finally:
        _close(client)


@pytest.mark.skipif(AUTO_CREATE != "1",
                    reason="needs a broker with SSL.AutoCreateUser enabled (set MTLS_AUTO_CREATE_USER=1)")
def test_account_created_on_first_connect(mtls_config):
    """With AutoCreateUser enabled, a certificate without an account gets one, without admin rights."""
    headers = _admin_headers()
    username = mtls_config["common_name"]

    _delete_user(username, headers)
    assert _get_user(username, headers) is None, "account should not exist before the test"

    client = _make_client("cert_autocreate", mtls_config)
    try:
        assert _connect(client, mtls_config) == 0, "client with a valid certificate could not connect"
    finally:
        _close(client)

    created = _get_user(username, headers)
    assert created is not None, "no account was created for the certificate Common Name"
    assert created["enabled"] is True
    assert created["isAdmin"] is False, "auto-created accounts must not be administrators"


@pytest.mark.skipif(AUTO_CREATE != "0",
                    reason="needs a broker with SSL.AutoCreateUser disabled (set MTLS_AUTO_CREATE_USER=0)")
def test_unknown_common_name_rejected(mtls_config):
    """With AutoCreateUser disabled, a certificate without an account is rejected."""
    headers = _admin_headers()
    username = mtls_config["common_name"]

    result = _delete_user(username, headers)
    if not result.get("success") and _get_user(username, headers) is not None:
        pytest.skip("Could not remove account {} for the test".format(username))

    try:
        client = _make_client("cert_unknown", mtls_config)
        try:
            rc = _connect(client, mtls_config, timeout=10.0)
            assert rc != 0, "certificate with no account was allowed to connect (rc={})".format(rc)
        finally:
            _close(client)
    finally:
        _ensure_user(username, headers)
