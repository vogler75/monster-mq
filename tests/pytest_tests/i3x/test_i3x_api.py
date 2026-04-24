#!/usr/bin/env python3
"""
Integration tests for the MonsterMQ i3X v1 API (mounted at /i3x/v1).

Covers:
- GET /info (unauthenticated discovery endpoint)
- v1 response envelope ({success, result} / {success, results[]} / {success:false, error})
- Namespaces
- Object types (synthetic)
- Relationship types
- Objects (root, parentId, typeElementId, metadata)
- POST /objects/list and /objects/related
- POST /objects/value with maxDepth
- PUT /objects/{id}/value and round-trip read
- POST /objects/history
- Subscription lifecycle: create, register, publish, sync, delete

Configuration via environment variables:
    I3X_URL         Base URL for the i3X v1 API (default: http://localhost:3002/i3x/v1)
    MQTT_BROKER     MQTT broker host (default: localhost)
    MQTT_PORT       MQTT broker port (default: 1883)
    MQTT_USERNAME   MQTT username (default: Admin)
    MQTT_PASSWORD   MQTT password (default: Admin)
    GRAPHQL_URL     GraphQL endpoint for login (default: http://localhost:4000/graphql)
"""

import json
import os
import time
import uuid

import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
import pytest
import requests


I3X_URL = os.getenv("I3X_URL", "http://localhost:3002/i3x/v1")
GRAPHQL_URL = os.getenv("GRAPHQL_URL", "http://localhost:4000/graphql")
REST_API_URL = os.getenv("REST_API_URL", "http://localhost:4000/api/v1")
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "Admin")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "Admin")

REQUEST_TIMEOUT = 10


# ---------------------------------------------------------------------------
# Auth + fixtures
# ---------------------------------------------------------------------------


def _login_token():
    """Try REST login, then GraphQL login. Returns None when auth is disabled."""
    try:
        resp = requests.post(
            f"{REST_API_URL}/login",
            json={"username": MQTT_USERNAME, "password": MQTT_PASSWORD},
            timeout=5,
        )
        if resp.status_code == 200:
            token = resp.json().get("token")
            if token:
                return token
    except Exception:
        pass
    try:
        resp = requests.post(
            GRAPHQL_URL,
            json={
                "query": 'mutation { login(username: "%s", password: "%s") { token } }'
                % (MQTT_USERNAME, MQTT_PASSWORD)
            },
            timeout=5,
        )
        token = resp.json().get("data", {}).get("login", {}).get("token")
        if token:
            return token
    except Exception:
        pass
    return None


@pytest.fixture(scope="module")
def auth_headers():
    token = _login_token()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


@pytest.fixture(scope="module")
def server_reachable():
    """Skip the whole module if the i3X v1 server is unreachable."""
    try:
        resp = requests.get(f"{I3X_URL}/info", timeout=3)
    except Exception as e:
        pytest.skip(f"i3X v1 API not reachable at {I3X_URL}: {e}")
    if resp.status_code >= 500:
        pytest.skip(f"i3X v1 API server error: HTTP {resp.status_code}")
    return True


@pytest.fixture
def publish_retained():
    """Publish retained messages and clean them up after the test."""
    published = []

    def _publish(topic, payload):
        client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            protocol=mqtt.MQTTv5,
        )
        if MQTT_USERNAME:
            client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        client.connect(MQTT_BROKER, MQTT_PORT)
        client.loop_start()
        info = client.publish(topic, payload, qos=1, retain=True)
        info.wait_for_publish(timeout=5)
        time.sleep(0.2)  # let broker persist
        client.loop_stop()
        client.disconnect()
        published.append(topic)

    yield _publish

    # Clear retained
    if published:
        client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            protocol=mqtt.MQTTv5,
        )
        if MQTT_USERNAME:
            client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        try:
            client.connect(MQTT_BROKER, MQTT_PORT)
            client.loop_start()
            for t in published:
                client.publish(t, "", qos=1, retain=True).wait_for_publish(timeout=3)
            time.sleep(0.2)
            client.loop_stop()
            client.disconnect()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# /info
# ---------------------------------------------------------------------------


class TestInfo:
    def test_info_is_unauthenticated(self, server_reachable):
        resp = requests.get(f"{I3X_URL}/info", timeout=REQUEST_TIMEOUT)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["success"] is True
        result = body["result"]
        assert result["specVersion"].startswith("1.")
        assert result["serverName"]
        assert result["serverVersion"]
        caps = result["capabilities"]
        assert caps["query"]["history"] is True
        assert caps["update"]["current"] is True
        assert caps["subscribe"]["stream"] is True


# ---------------------------------------------------------------------------
# Namespaces, ObjectTypes, RelationshipTypes
# ---------------------------------------------------------------------------


class TestDiscovery:
    def test_namespaces_shape(self, auth_headers, server_reachable):
        resp = requests.get(f"{I3X_URL}/namespaces", headers=auth_headers, timeout=REQUEST_TIMEOUT)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["success"] is True
        assert isinstance(body["result"], list)
        assert any(ns["uri"] == "http://i3x.dev/base" for ns in body["result"])
        for ns in body["result"]:
            assert "uri" in ns and "displayName" in ns

    def test_objecttypes_include_synthetics(self, auth_headers, server_reachable):
        resp = requests.get(f"{I3X_URL}/objecttypes", headers=auth_headers, timeout=REQUEST_TIMEOUT)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["success"] is True
        ids = {t["elementId"] for t in body["result"]}
        for expected in ("TopicFolder", "JsonObject", "JsonArray", "Number", "String", "Boolean", "Binary"):
            assert expected in ids, f"missing synthetic ObjectType {expected}"
        for t in body["result"]:
            assert t["namespaceUri"]
            assert t["sourceTypeId"] == t["elementId"]

    def test_objecttypes_filter_by_namespace(self, auth_headers, server_reachable):
        resp = requests.get(
            f"{I3X_URL}/objecttypes",
            headers=auth_headers,
            params={"namespaceUri": "http://i3x.dev/base"},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        for t in body["result"]:
            assert t["namespaceUri"] == "http://i3x.dev/base"

    def test_objecttypes_query_bulk_not_found(self, auth_headers, server_reachable):
        resp = requests.post(
            f"{I3X_URL}/objecttypes/query",
            headers=auth_headers,
            json={"elementIds": ["TopicFolder", "ThisDoesNotExist"]},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        body = resp.json()
        # Bulk envelope: top-level success=false when any item fails.
        assert body["success"] is False
        assert len(body["results"]) == 2
        assert body["results"][0]["success"] is True
        assert body["results"][0]["elementId"] == "TopicFolder"
        assert body["results"][1]["success"] is False
        assert body["results"][1]["error"]["code"] == 404

    def test_relationshiptypes_built_in(self, auth_headers, server_reachable):
        resp = requests.get(
            f"{I3X_URL}/relationshiptypes", headers=auth_headers, timeout=REQUEST_TIMEOUT
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        ids = {t["elementId"]: t for t in body["result"]}
        for expected in ("HasParent", "HasChildren", "HasComponent", "ComponentOf"):
            assert expected in ids
        assert ids["HasParent"]["reverseOf"] == "HasChildren"
        assert ids["HasChildren"]["reverseOf"] == "HasParent"
        assert ids["HasComponent"]["reverseOf"] == "ComponentOf"


# ---------------------------------------------------------------------------
# Objects
# ---------------------------------------------------------------------------


class TestObjects:
    def test_root_objects_present(self, auth_headers, publish_retained, server_reachable):
        unique = f"i3xtest_{uuid.uuid4().hex[:6]}"
        publish_retained(f"{unique}/line1/motor1", json.dumps({"speed": 1500, "enabled": True}))
        time.sleep(0.3)

        resp = requests.get(
            f"{I3X_URL}/objects",
            headers=auth_headers,
            params={"root": "true"},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        root_ids = [o["elementId"] for o in body["result"]]
        assert unique in root_ids

        # parentId is null for roots
        root = next(o for o in body["result"] if o["elementId"] == unique)
        assert root["parentId"] is None

    def test_parent_id_query_returns_direct_children(
        self, auth_headers, publish_retained, server_reachable
    ):
        unique = f"i3xtest_{uuid.uuid4().hex[:6]}"
        publish_retained(f"{unique}/line1/motor1/speed", "1500")
        publish_retained(f"{unique}/line1/motor2/speed", "2000")
        time.sleep(0.3)

        resp = requests.get(
            f"{I3X_URL}/objects",
            headers=auth_headers,
            params={"parentId": f"{unique}/line1"},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        ids = {o["elementId"] for o in body["result"]}
        assert f"{unique}/line1/motor1" in ids
        assert f"{unique}/line1/motor2" in ids
        # No grandchildren
        assert f"{unique}/line1/motor1/speed" not in ids

    def test_objects_list_bulk(self, auth_headers, publish_retained, server_reachable):
        unique = f"i3xtest_{uuid.uuid4().hex[:6]}"
        publish_retained(f"{unique}/leaf", "42")
        time.sleep(0.3)

        resp = requests.post(
            f"{I3X_URL}/objects/list",
            headers=auth_headers,
            json={
                "elementIds": [f"{unique}/leaf", f"{unique}/missing"],
                "includeMetadata": True,
            },
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is False  # one item missing
        assert len(body["results"]) == 2
        ok_item = body["results"][0]
        assert ok_item["success"] is True
        assert ok_item["result"]["elementId"] == f"{unique}/leaf"
        assert ok_item["result"]["metadata"]["typeNamespaceUri"]
        assert body["results"][1]["success"] is False
        assert body["results"][1]["error"]["code"] == 404

    def test_objects_related_has_children(self, auth_headers, publish_retained, server_reachable):
        unique = f"i3xtest_{uuid.uuid4().hex[:6]}"
        publish_retained(f"{unique}/a/b", "1")
        publish_retained(f"{unique}/a/c", "2")
        time.sleep(0.3)

        resp = requests.post(
            f"{I3X_URL}/objects/related",
            headers=auth_headers,
            json={
                "elementIds": [f"{unique}/a"],
                "relationshipType": "HasChildren",
            },
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        results = body["results"][0]["result"]
        assert {r["object"]["elementId"] for r in results} == {
            f"{unique}/a/b",
            f"{unique}/a/c",
        }
        for r in results:
            assert r["sourceRelationship"] == "HasChildren"


# ---------------------------------------------------------------------------
# Values
# ---------------------------------------------------------------------------


class TestValues:
    def test_primitive_value(self, auth_headers, publish_retained, server_reachable):
        unique = f"i3xtest_{uuid.uuid4().hex[:6]}"
        topic = f"{unique}/speed"
        publish_retained(topic, "1500")
        time.sleep(0.3)

        resp = requests.post(
            f"{I3X_URL}/objects/value",
            headers=auth_headers,
            json={"elementIds": [topic], "maxDepth": 1},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        r = body["results"][0]
        assert r["success"] is True
        assert r["elementId"] == topic
        assert r["result"]["quality"] == "Good"
        # Broker decodes numeric strings to numbers
        assert r["result"]["value"] in (1500, "1500", 1500.0)
        assert r["result"]["isComposition"] is False

    def test_json_composition_expansion(self, auth_headers, publish_retained, server_reachable):
        unique = f"i3xtest_{uuid.uuid4().hex[:6]}"
        topic = f"{unique}/motor"
        publish_retained(topic, json.dumps({"speed": 1500, "enabled": True}))
        time.sleep(0.3)

        resp = requests.post(
            f"{I3X_URL}/objects/value",
            headers=auth_headers,
            json={"elementIds": [topic], "maxDepth": 2},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        body = resp.json()
        r = body["results"][0]
        assert r["success"] is True
        result = r["result"]
        assert result["isComposition"] is True
        assert result["value"] == {"speed": 1500, "enabled": True}
        assert "components" in result
        comp_ids = set(result["components"].keys())
        assert f"{topic}/speed" in comp_ids
        assert f"{topic}/enabled" in comp_ids

    def test_put_value_roundtrip(self, auth_headers, publish_retained, server_reachable):
        unique = f"i3xtest_{uuid.uuid4().hex[:6]}"
        topic = f"{unique}/writable"
        # Bootstrap the topic so cleanup works.
        publish_retained(topic, "0")

        resp = requests.put(
            f"{I3X_URL}/objects/{topic}/value",
            headers=auth_headers,
            data=json.dumps({"value": 42}),
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["result"]["status"] == "published"

        time.sleep(0.3)
        read = requests.post(
            f"{I3X_URL}/objects/value",
            headers=auth_headers,
            json={"elementIds": [topic]},
            timeout=REQUEST_TIMEOUT,
        ).json()
        assert read["success"] is True
        assert read["results"][0]["result"]["value"] in (42, "42", 42.0)


# ---------------------------------------------------------------------------
# History
# ---------------------------------------------------------------------------


class TestHistory:
    def test_history_missing_element(self, auth_headers, server_reachable):
        resp = requests.post(
            f"{I3X_URL}/objects/history",
            headers=auth_headers,
            json={
                "elementIds": ["this/does/not/exist/anywhere"],
                "maxValues": 10,
            },
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200
        body = resp.json()
        # Top-level success=false because the single item failed.
        assert body["success"] is False
        assert body["results"][0]["success"] is False
        assert body["results"][0]["error"]["code"] == 404


# ---------------------------------------------------------------------------
# Subscriptions
# ---------------------------------------------------------------------------


class TestSubscriptions:
    def test_create_register_sync_delete(
        self, auth_headers, publish_retained, server_reachable
    ):
        unique = f"i3xtest_{uuid.uuid4().hex[:6]}"
        topic = f"{unique}/sub/value"
        # Ensure the topic exists before registering.
        publish_retained(topic, "1")
        time.sleep(0.2)

        client_id = f"pytest-{uuid.uuid4().hex[:6]}"
        resp = requests.post(
            f"{I3X_URL}/subscriptions",
            headers=auth_headers,
            json={"clientId": client_id, "displayName": "pytest-sub"},
            timeout=REQUEST_TIMEOUT,
        )
        assert resp.status_code == 200, resp.text
        created = resp.json()
        assert created["success"] is True
        sub_id = created["result"]["subscriptionId"]
        assert sub_id

        try:
            reg = requests.post(
                f"{I3X_URL}/subscriptions/register",
                headers=auth_headers,
                json={
                    "clientId": client_id,
                    "subscriptionId": sub_id,
                    "elementIds": [topic],
                    "maxDepth": 1,
                },
                timeout=REQUEST_TIMEOUT,
            )
            assert reg.status_code == 200
            assert reg.json()["success"] is True

            # Publish an update and poll sync.
            mqtt_client = mqtt.Client(
                callback_api_version=CallbackAPIVersion.VERSION2,
                protocol=mqtt.MQTTv5,
            )
            if MQTT_USERNAME:
                mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
            mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
            mqtt_client.loop_start()
            mqtt_client.publish(topic, "2", qos=1, retain=True).wait_for_publish(timeout=3)
            mqtt_client.publish(topic, "3", qos=1, retain=True).wait_for_publish(timeout=3)
            mqtt_client.loop_stop()
            mqtt_client.disconnect()

            # Give the listener a moment to enqueue.
            updates = []
            deadline = time.time() + 5
            while time.time() < deadline and not updates:
                time.sleep(0.2)
                sync = requests.post(
                    f"{I3X_URL}/subscriptions/sync",
                    headers=auth_headers,
                    json={"clientId": client_id, "subscriptionId": sub_id},
                    timeout=REQUEST_TIMEOUT,
                ).json()
                assert sync["success"] is True
                updates = sync["result"]["updates"]

            assert updates, "No updates received via /sync within 5s"
            for u in updates:
                assert u["elementId"] == topic
                assert "sequenceNumber" in u
                assert u["quality"] in ("Good", "GoodNoData", "Bad", "Uncertain")

            # Ack with the highest sequence number; the queue should be empty now.
            max_seq = max(u["sequenceNumber"] for u in updates)
            sync_after = requests.post(
                f"{I3X_URL}/subscriptions/sync",
                headers=auth_headers,
                json={
                    "clientId": client_id,
                    "subscriptionId": sub_id,
                    "lastSequenceNumber": max_seq,
                },
                timeout=REQUEST_TIMEOUT,
            ).json()
            assert sync_after["success"] is True
            assert sync_after["result"]["updates"] == []
        finally:
            # Clean up.
            requests.post(
                f"{I3X_URL}/subscriptions/delete",
                headers=auth_headers,
                json={"clientId": client_id, "subscriptionIds": [sub_id]},
                timeout=REQUEST_TIMEOUT,
            )


# ---------------------------------------------------------------------------
# Envelope shape regression tests
# ---------------------------------------------------------------------------


class TestEnvelope:
    def test_single_result_envelope(self, auth_headers, server_reachable):
        resp = requests.get(f"{I3X_URL}/namespaces", headers=auth_headers, timeout=REQUEST_TIMEOUT)
        body = resp.json()
        assert set(body.keys()) == {"success", "result"}

    def test_bulk_envelope_top_level_success_false_on_item_failure(
        self, auth_headers, server_reachable
    ):
        resp = requests.post(
            f"{I3X_URL}/objects/list",
            headers=auth_headers,
            json={"elementIds": ["does/not/exist/ever"]},
            timeout=REQUEST_TIMEOUT,
        )
        body = resp.json()
        assert set(body.keys()) == {"success", "results"}
        assert body["success"] is False
        assert body["results"][0]["success"] is False
        assert body["results"][0]["error"]["code"] == 404
