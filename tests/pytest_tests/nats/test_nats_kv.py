"""
NATS Key-Value tests against MonsterMQ's native NATS port.

Buckets are archive groups with a last-value store (bucket "Default" = archive group "Default"),
keys are MQTT topics with "." as separator. Requires the broker to run with `NATS: 4222`.

Environment: NATS_URL (default nats://localhost:4222), GRAPHQL_URL, MQTT_BROKER, MQTT_PORT, SKIP_NATS=1.
"""
import asyncio
import os
import socket
import time
import uuid
from urllib.parse import urlparse

import paho.mqtt.client as mqtt
import pytest
import requests
from paho.mqtt.client import CallbackAPIVersion

nats = pytest.importorskip("nats")
from nats.js.errors import BadRequestError, BucketNotFoundError, KeyNotFoundError, KeyWrongLastSequenceError  # noqa: E402

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
GRAPHQL_URL = os.getenv("GRAPHQL_URL", "http://localhost:4000/graphql")
MQTT_HOST = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
BUCKET = "Default"


def _nats_reachable() -> bool:
    url = urlparse(NATS_URL)
    try:
        with socket.create_connection((url.hostname, url.port or 4222), timeout=2):
            return True
    except OSError:
        return False


pytestmark = [
    pytest.mark.skipif(os.getenv("SKIP_NATS", "0") == "1", reason="NATS tests skipped"),
    pytest.mark.skipif(not _nats_reachable(), reason=f"NATS port not reachable at {NATS_URL}"),
]


@pytest.fixture(scope="module", autouse=True)
def default_archive_group():
    """The Default archive group (MEMORY last-value store) must be running to serve as bucket."""
    try:
        requests.post(
            GRAPHQL_URL,
            json={"query": 'mutation { archiveGroup { enable(name: "Default") { success } } }'},
            timeout=5,
        )
    except Exception:
        pass
    time.sleep(1)


@pytest.fixture
def prefix():
    """Unique key prefix per test, e.g. kvtest.<id> (MQTT topic kvtest/<id>)."""
    return f"kvtest.{uuid.uuid4().hex[:8]}"


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, timeout=20))


async def _kv():
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    return nc, await js.key_value(BUCKET)


def _mqtt_client(subscribe=None):
    client = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=f"kvtest-{uuid.uuid4().hex[:8]}")
    if subscribe:
        client.on_connect = lambda c, userdata, flags, reason_code, properties: c.subscribe(subscribe)
    client.connect(MQTT_HOST, MQTT_PORT)
    client.loop_start()
    return client


def test_bucket_listed():
    async def scenario():
        nc = await nats.connect(NATS_URL)
        try:
            streams = await nc.jetstream().streams_info()
            assert f"KV_{BUCKET}" in [stream.config.name for stream in streams]
        finally:
            await nc.close()

    run(scenario())


def test_unknown_bucket():
    async def scenario():
        nc = await nats.connect(NATS_URL)
        try:
            with pytest.raises(BucketNotFoundError):
                await nc.jetstream().key_value("NoSuchArchiveGroup")
        finally:
            await nc.close()

    run(scenario())


def test_put_get_delete(prefix):
    async def scenario():
        nc, kv = await _kv()
        try:
            key = f"{prefix}.temp"
            revision = await kv.put(key, b"21.5")
            entry = await kv.get(key)
            assert entry.value == b"21.5"
            assert entry.revision == revision

            await kv.put(key, b"22.0")
            assert (await kv.get(key)).value == b"22.0"

            await kv.delete(key)
            with pytest.raises(KeyNotFoundError):
                await kv.get(key)
        finally:
            await nc.close()

    run(scenario())


def test_create_only_if_absent(prefix):
    async def scenario():
        nc, kv = await _kv()
        try:
            key = f"{prefix}.once"
            await kv.create(key, b"1")
            with pytest.raises((KeyWrongLastSequenceError, BadRequestError)):
                await kv.create(key, b"2")
            assert (await kv.get(key)).value == b"1"
        finally:
            await nc.close()

    run(scenario())


def test_kv_put_is_retained_mqtt_message(prefix):
    async def put():
        nc, kv = await _kv()
        try:
            await kv.put(f"{prefix}.speed", b"1500")
        finally:
            await nc.close()

    run(put())

    received = []
    client = _mqtt_client(subscribe=prefix.replace(".", "/") + "/#")
    try:
        client.on_message = lambda c, u, msg: received.append((msg.topic, msg.payload, msg.retain))
        deadline = time.time() + 5
        while not received and time.time() < deadline:
            time.sleep(0.1)
    finally:
        client.loop_stop()
        client.disconnect()
    assert received == [(prefix.replace(".", "/") + "/speed", b"1500", True)]


def test_mqtt_publish_visible_in_kv(prefix):
    client = _mqtt_client()
    try:
        client.publish(prefix.replace(".", "/") + "/level", b"42", qos=1, retain=True).wait_for_publish(5)
    finally:
        client.loop_stop()
        client.disconnect()
    time.sleep(0.5)

    async def scenario():
        nc, kv = await _kv()
        try:
            assert (await kv.get(f"{prefix}.level")).value == b"42"
        finally:
            await nc.close()

    run(scenario())


def test_keys_and_watch(prefix):
    async def scenario():
        nc, kv = await _kv()
        try:
            await kv.put(f"{prefix}.a", b"1")
            await kv.put(f"{prefix}.b", b"2")
            # nats-py applies key filters client-side as substring matches
            assert sorted(await kv.keys(filters=[prefix])) == [f"{prefix}.a", f"{prefix}.b"]

            watcher = await kv.watch(f"{prefix}.>")
            initial = {}
            while True:
                entry = await watcher.updates(timeout=5)
                if entry is None:  # end of initial values
                    break
                initial[entry.key] = entry.value
            assert initial == {f"{prefix}.a": b"1", f"{prefix}.b": b"2"}

            await kv.put(f"{prefix}.a", b"3")
            entry = await watcher.updates(timeout=5)
            assert (entry.key, entry.value) == (f"{prefix}.a", b"3")

            await kv.delete(f"{prefix}.b")
            entry = await watcher.updates(timeout=5)
            assert (entry.key, entry.operation) == (f"{prefix}.b", "DEL")
            await watcher.stop()
        finally:
            await nc.close()

    run(scenario())


def test_request_reply_between_nats_clients(prefix):
    """Reply-to is carried through the broker, so core NATS request/reply works."""
    async def scenario():
        responder = await nats.connect(NATS_URL)
        requester = await nats.connect(NATS_URL)
        try:
            async def handler(msg):
                await msg.respond(b"pong:" + msg.data)

            await responder.subscribe(f"{prefix}.svc", cb=handler)
            await responder.flush()
            await asyncio.sleep(0.2)
            reply = await requester.request(f"{prefix}.svc", b"ping", timeout=5)
            assert reply.data == b"pong:ping"
        finally:
            await requester.close()
            await responder.close()

    run(scenario())


def test_headers_pass_through(prefix):
    async def scenario():
        publisher = await nats.connect(NATS_URL)
        subscriber = await nats.connect(NATS_URL)
        try:
            sub = await subscriber.subscribe(f"{prefix}.hdr")
            await subscriber.flush()
            await asyncio.sleep(0.2)
            await publisher.publish(f"{prefix}.hdr", b"x", headers={"Source": "test"})
            msg = await sub.next_msg(timeout=5)
            assert msg.headers == {"Source": "test"}
        finally:
            await publisher.close()
            await subscriber.close()

    run(scenario())
