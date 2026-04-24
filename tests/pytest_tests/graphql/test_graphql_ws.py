#!/usr/bin/env python3
"""
Pytest integration tests for the MonsterMQ GraphQL WebSocket interface.
"""

import asyncio
import json
import os
import time
import uuid

import pytest
import requests

websockets = pytest.importorskip("websockets")


pytestmark = [pytest.mark.graphql, pytest.mark.external, pytest.mark.integration]

GRAPHQL_URL = os.getenv("GRAPHQL_URL", "http://localhost:4000/graphql")
GRAPHQL_WS_URL = os.getenv("GRAPHQL_WS_URL", "ws://localhost:4000/graphqlws")
GRAPHQL_USERNAME = os.getenv("GRAPHQL_USERNAME", os.getenv("MQTT_USERNAME", "Admin"))
GRAPHQL_PASSWORD = os.getenv("GRAPHQL_PASSWORD", os.getenv("MQTT_PASSWORD", "Admin"))
REQUEST_TIMEOUT = 10


def _broker_available() -> bool:
    try:
        response = requests.post(
            GRAPHQL_URL,
            json={"query": "{ __typename }"},
            timeout=2,
        )
        return response.status_code == 200
    except Exception:
        return False


@pytest.fixture(scope="module", autouse=True)
def require_graphql():
    if not _broker_available():
        pytest.skip(f"GraphQL endpoint not reachable at {GRAPHQL_URL}")


def _credential_candidates():
    candidates = [
        (os.getenv("GRAPHQL_USERNAME"), os.getenv("GRAPHQL_PASSWORD")),
        (os.getenv("MQTT_USERNAME"), os.getenv("MQTT_PASSWORD")),
        (GRAPHQL_USERNAME, GRAPHQL_PASSWORD),
        ("Admin", "Admin"),
        ("Test", "Test"),
    ]
    seen = set()
    for username, password in candidates:
        if not username or not password:
            continue
        key = (username, password)
        if key in seen:
            continue
        seen.add(key)
        yield key


def _auth_headers():
    for username, password in _credential_candidates():
        response = requests.post(
            GRAPHQL_URL,
            json={
                "query": """
                    mutation Login($username: String!, $password: String!) {
                        login(username: $username, password: $password) {
                            token
                        }
                    }
                """,
                "variables": {"username": username, "password": password},
            },
            headers={"Content-Type": "application/json"},
            timeout=REQUEST_TIMEOUT,
        )
        if response.status_code != 200:
            continue
        token = response.json().get("data", {}).get("login", {}).get("token")
        if token:
            return {"Authorization": f"Bearer {token}"}
    return {}


def _graphql(query, variables=None, headers=None):
    request_headers = {"Content-Type": "application/json"}
    if headers:
        request_headers.update(headers)
    response = requests.post(
        GRAPHQL_URL,
        json={"query": query, "variables": variables or {}},
        headers=request_headers,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    result = response.json()
    assert "errors" not in result, f"GraphQL errors: {result['errors']}"
    return result["data"]


def _publish(topic, payload):
    data = _graphql(
        """
        mutation Publish($input: PublishInput!) {
            publish(input: $input) {
                success
                topic
                error
            }
        }
        """,
        {
            "input": {
                "topic": topic,
                "payload": payload,
                "format": "JSON",
                "qos": 0,
                "retained": False,
            }
        },
        headers=_auth_headers(),
    )
    result = data["publish"]
    assert result["success"] is True, result.get("error")
    return result


async def _connect_ws():
    websocket = await websockets.connect(
        GRAPHQL_WS_URL,
        subprotocols=["graphql-transport-ws"],
    )
    await websocket.send(json.dumps({"type": "connection_init", "payload": {}}))
    response = json.loads(await asyncio.wait_for(websocket.recv(), timeout=5))
    assert response.get("type") == "connection_ack", response
    return websocket


async def _receive_next(websocket, data_key, timeout=6):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        remaining = max(0.1, deadline - time.monotonic())
        message = json.loads(await asyncio.wait_for(websocket.recv(), timeout=remaining))
        if message.get("type") == "next":
            payload = message.get("payload", {})
            if "errors" in payload:
                raise AssertionError(f"Subscription errors: {payload['errors']}")
            data = payload.get("data", {})
            if data.get(data_key) is not None:
                return data[data_key]
        if message.get("type") == "error":
            raise AssertionError(f"Subscription error: {message.get('payload')}")
    raise AssertionError(f"No {data_key} subscription payload received within {timeout}s")


def test_graphql_websocket_connection_ack():
    async def run():
        websocket = await _connect_ws()
        await websocket.close()

    asyncio.run(run())


def test_topic_updates_subscription_receives_published_message():
    async def run():
        topic = f"test/graphql/ws/topic/{uuid.uuid4().hex}"
        payload = f"ws-topic-{uuid.uuid4().hex}"
        websocket = await _connect_ws()
        try:
            await websocket.send(
                json.dumps(
                    {
                        "id": "topic-updates",
                        "type": "subscribe",
                        "payload": {
                            "query": """
                                subscription TopicUpdates($filters: [String!]!) {
                                    topicUpdates(topicFilters: $filters, format: JSON) {
                                        topic
                                        payload
                                        format
                                        qos
                                        retained
                                    }
                                }
                            """,
                            "variables": {"filters": [topic]},
                        },
                    }
                )
            )
            await asyncio.sleep(0.2)
            _publish(topic, payload)
            update = await _receive_next(websocket, "topicUpdates")
            assert update["topic"] == topic
            assert update["payload"] == payload
            assert update["format"] == "JSON"
            assert update["qos"] == 0
            assert update["retained"] is False
        finally:
            await websocket.close()

    asyncio.run(run())


def test_topic_updates_bulk_subscription_batches_published_messages():
    async def run():
        run_id = uuid.uuid4().hex
        topic_filter = f"test/graphql/ws/bulk/{run_id}/#"
        topics = [f"test/graphql/ws/bulk/{run_id}/{index}" for index in range(3)]
        websocket = await _connect_ws()
        try:
            await websocket.send(
                json.dumps(
                    {
                        "id": "bulk-updates",
                        "type": "subscribe",
                        "payload": {
                            "query": """
                                subscription BulkUpdates($filters: [String!]!) {
                                    topicUpdatesBulk(
                                        topicFilters: $filters,
                                        format: JSON,
                                        timeoutMs: 500,
                                        maxSize: 3
                                    ) {
                                        count
                                        updates {
                                            topic
                                            payload
                                            format
                                        }
                                    }
                                }
                            """,
                            "variables": {"filters": [topic_filter]},
                        },
                    }
                )
            )
            await asyncio.sleep(0.2)
            for index, topic in enumerate(topics):
                _publish(topic, f"ws-bulk-{index}")

            batch = await _receive_next(websocket, "topicUpdatesBulk", timeout=8)
            received = {update["topic"]: update for update in batch["updates"]}
            assert batch["count"] == len(batch["updates"])
            assert set(topics).issubset(received.keys())
            for index, topic in enumerate(topics):
                assert received[topic]["payload"] == f"ws-bulk-{index}"
                assert received[topic]["format"] == "JSON"
        finally:
            await websocket.close()

    asyncio.run(run())


def test_system_logs_subscription_accepts_subscribe_request():
    async def run():
        websocket = await _connect_ws()
        try:
            await websocket.send(
                json.dumps(
                    {
                        "id": "system-logs",
                        "type": "subscribe",
                        "payload": {
                            "query": """
                                subscription SystemLogs {
                                    systemLogs {
                                        timestamp
                                        level
                                        logger
                                        message
                                        node
                                    }
                                }
                            """,
                            "variables": {},
                        },
                    }
                )
            )

            try:
                message = json.loads(await asyncio.wait_for(websocket.recv(), timeout=1))
            except asyncio.TimeoutError:
                return

            assert message.get("type") != "error", message
            if message.get("type") == "next":
                payload = message.get("payload", {})
                assert "errors" not in payload, payload
        finally:
            await websocket.close()

    asyncio.run(run())
