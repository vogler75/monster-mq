#!/usr/bin/env python3
"""
Pytest integration tests for the MonsterMQ GraphQL HTTP interface.
"""

import os
import time
import uuid

import pytest
import requests


pytestmark = [pytest.mark.graphql, pytest.mark.external, pytest.mark.integration]

GRAPHQL_URL = os.getenv("GRAPHQL_URL", "http://localhost:4000/graphql")
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


@pytest.fixture(scope="module")
def auth_headers():
    for username, password in _credential_candidates():
        result = _graphql(
            """
            mutation Login($username: String!, $password: String!) {
                login(username: $username, password: $password) {
                    success
                    token
                }
            }
            """,
            {"username": username, "password": password},
            allow_errors=True,
        )
        login = result.get("data", {}).get("login") or {}
        token = login.get("token")
        if token:
            return {"Authorization": f"Bearer {token}"}
    return {}


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


def _graphql(query, variables=None, headers=None, allow_errors=False):
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
    if not allow_errors:
        assert "errors" not in result, f"GraphQL errors: {result['errors']}"
    return result


def _publish(topic, payload, headers=None, retained=False):
    result = _graphql(
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
                "retained": retained,
            }
        },
        headers=headers,
    )
    return result["data"]["publish"]


def test_graphql_typename_smoke():
    result = _graphql("{ __typename }")
    assert result["data"]["__typename"] == "Query"


def test_graphql_schema_exposes_core_roots():
    result = _graphql(
        """
        {
            __schema {
                queryType { name }
                mutationType { name }
                subscriptionType { name }
            }
        }
        """
    )
    schema = result["data"]["__schema"]
    assert schema["queryType"]["name"] == "Query"
    assert schema["mutationType"]["name"] == "Mutation"
    assert schema["subscriptionType"]["name"] == "Subscription"


def test_login_mutation_has_stable_shape():
    result = _graphql(
        """
        mutation Login($username: String!, $password: String!) {
            login(username: $username, password: $password) {
                success
                token
                message
                username
                isAdmin
            }
        }
        """,
        {"username": GRAPHQL_USERNAME, "password": GRAPHQL_PASSWORD},
        allow_errors=True,
    )
    assert "errors" not in result, f"GraphQL login errors: {result['errors']}"
    login = result["data"]["login"]
    assert isinstance(login["success"], bool)
    assert isinstance(login["isAdmin"], bool)
    assert set(login.keys()) == {"success", "token", "message", "username", "isAdmin"}


def test_publish_rejects_wildcard_topic(auth_headers):
    result = _publish(
        f"test/graphql/http/{uuid.uuid4().hex}/+",
        "wildcard-rejected",
        headers=auth_headers,
    )
    assert result["success"] is False
    assert "wildcard" in result["error"].lower()


def test_publish_and_read_retained_message(auth_headers):
    topic = f"test/graphql/http/retained/{uuid.uuid4().hex}"
    payload = f"retained-{uuid.uuid4().hex}"

    publish = _publish(topic, payload, headers=auth_headers, retained=True)
    assert publish["success"] is True, publish.get("error")
    assert publish["topic"] == topic

    query = """
    query Retained($topic: String!) {
        retainedMessage(topic: $topic, format: JSON) {
            topic
            payload
            qos
            retainedFormat: format
        }
    }
    """

    retained = None
    for _ in range(20):
        result = _graphql(query, {"topic": topic}, headers=auth_headers)
        retained = result["data"]["retainedMessage"]
        if retained and retained["payload"] == payload:
            break
        time.sleep(0.25)

    assert retained is not None, "Retained message was not returned"
    assert retained["topic"] == topic
    assert retained["payload"] == payload
    assert retained["qos"] == 0
    assert retained["retainedFormat"] == "JSON"


def test_publish_batch_returns_one_result_per_input(auth_headers):
    run_id = uuid.uuid4().hex
    inputs = [
        {
            "topic": f"test/graphql/http/batch/{run_id}/{index}",
            "payload": f"batch-{index}",
            "format": "JSON",
            "qos": 0,
            "retained": False,
        }
        for index in range(3)
    ]

    result = _graphql(
        """
        mutation PublishBatch($inputs: [PublishInput!]!) {
            publishBatch(inputs: $inputs) {
                success
                topic
                error
            }
        }
        """,
        {"inputs": inputs},
        headers=auth_headers,
    )

    results = result["data"]["publishBatch"]
    assert len(results) == len(inputs)
    assert all(item["success"] for item in results), results
    assert [item["topic"] for item in results] == [item["topic"] for item in inputs]


def test_current_value_and_archive_queries_are_well_formed(auth_headers):
    topic = f"test/graphql/http/current/{uuid.uuid4().hex}"
    payload = f"current-{uuid.uuid4().hex}"
    publish = _publish(topic, payload, headers=auth_headers)
    assert publish["success"] is True, publish.get("error")

    result = _graphql(
        """
        query CurrentAndHistory($topic: String!) {
            currentValue(topic: $topic, format: JSON) {
                topic
                payload
                format
            }
            archivedMessages(topicFilter: $topic, format: JSON, limit: 10) {
                topic
                payload
                format
            }
        }
        """,
        {"topic": topic},
        headers=auth_headers,
        allow_errors=True,
    )

    assert "data" in result
    if "errors" in result:
        messages = " ".join(error.get("message", "") for error in result["errors"])
        assert "archive" in messages.lower() or "lastvalue" in messages.lower()
        return

    current = result["data"]["currentValue"]
    if current is not None:
        assert current["topic"] == topic
        assert current["format"] == "JSON"

    archived = result["data"]["archivedMessages"]
    assert isinstance(archived, list)
