#!/usr/bin/env python3
"""
Integration tests for the MonsterMQ enabledFeatures GraphQL API.

Verifies that:
- broker { enabledFeatures } returns a valid list of known feature names
- brokers { enabledFeatures } is consistent with the single-node query
- Features explicitly disabled in config are absent from the response
- Features explicitly enabled in config are present in the response

Optional environment variables for config-specific assertions:
    EXPECTED_FEATURES   Comma-separated list of features expected to be enabled,
                        e.g. "MqttClient,Kafka". If set, tests assert exact match.
    DISABLED_FEATURES   Comma-separated list of features expected to be disabled.

Default test run (no env vars) validates structure only and works with any config.

Example with the config-no-features.yaml (MqttClient=true, all others false):
    EXPECTED_FEATURES=MqttClient pytest graphql/test_features.py -v
"""

import os
import pytest
import requests

GRAPHQL_URL = os.getenv("GRAPHQL_URL", "http://localhost:4000/graphql")


def _broker_available() -> bool:
    """Quick check whether the GraphQL endpoint is reachable."""
    try:
        requests.post(GRAPHQL_URL, json={"query": "{ __typename }"}, timeout=2)
        return True
    except Exception:
        return False


@pytest.fixture(scope="module", autouse=True)
def require_broker():
    """Skip all tests in this module if the broker is not reachable."""
    if not _broker_available():
        pytest.skip(f"Broker GraphQL endpoint not reachable at {GRAPHQL_URL}")

# All feature names recognised by MonsterMQ
ALL_KNOWN_FEATURES = {
    "OpcUa", "OpcUaServer", "MqttClient", "Kafka", "Nats", "Telegram",
    "WinCCOa", "WinCCUa", "Plc4x", "Neo4j", "JdbcLogger",
    "SparkplugB", "FlowEngine", "Agents",
}


def _graphql(query: str) -> dict:
    response = requests.post(
        GRAPHQL_URL,
        json={"query": query},
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    response.raise_for_status()
    result = response.json()
    assert "errors" not in result, f"GraphQL errors: {result['errors']}"
    return result["data"]


# ---------------------------------------------------------------------------
# broker { enabledFeatures } — single node
# ---------------------------------------------------------------------------

def test_broker_enabled_features_is_list():
    """enabledFeatures must be a list (never null)."""
    data = _graphql("{ broker { enabledFeatures } }")
    features = data["broker"]["enabledFeatures"]
    assert isinstance(features, list), "enabledFeatures should be a list"


def test_broker_enabled_features_are_known_names():
    """Every returned feature name must be a recognised MonsterMQ feature."""
    data = _graphql("{ broker { enabledFeatures } }")
    features = data["broker"]["enabledFeatures"]
    unknown = set(features) - ALL_KNOWN_FEATURES
    assert not unknown, f"Unknown feature names returned: {unknown}"


def test_broker_enabled_features_no_duplicates():
    """Feature list must not contain duplicate entries."""
    data = _graphql("{ broker { enabledFeatures } }")
    features = data["broker"]["enabledFeatures"]
    assert len(features) == len(set(features)), f"Duplicate features in response: {features}"


def test_broker_enabled_features_expected(request):
    """
    If EXPECTED_FEATURES env var is set, assert the returned set matches exactly.
    Skipped when EXPECTED_FEATURES is not set.
    """
    raw = os.getenv("EXPECTED_FEATURES", "").strip()
    if not raw:
        pytest.skip("EXPECTED_FEATURES not set — skipping exact-match assertion")

    expected = {f.strip() for f in raw.split(",") if f.strip()}
    data = _graphql("{ broker { enabledFeatures } }")
    actual = set(data["broker"]["enabledFeatures"])
    assert actual == expected, (
        f"Feature mismatch.\n  Expected: {sorted(expected)}\n  Actual:   {sorted(actual)}"
    )


def test_broker_disabled_features_absent():
    """
    If DISABLED_FEATURES env var is set, assert those features are NOT in the response.
    Skipped when DISABLED_FEATURES is not set.
    """
    raw = os.getenv("DISABLED_FEATURES", "").strip()
    if not raw:
        pytest.skip("DISABLED_FEATURES not set — skipping absent-feature assertion")

    disabled = {f.strip() for f in raw.split(",") if f.strip()}
    data = _graphql("{ broker { enabledFeatures } }")
    actual = set(data["broker"]["enabledFeatures"])
    unexpected = disabled & actual
    assert not unexpected, (
        f"Features expected to be disabled but are present: {unexpected}"
    )


# ---------------------------------------------------------------------------
# brokers { enabledFeatures } — cluster list
# ---------------------------------------------------------------------------

def test_brokers_query_returns_list():
    """brokers query must return a non-empty list."""
    data = _graphql("{ brokers { nodeId enabledFeatures } }")
    assert isinstance(data["brokers"], list)
    assert len(data["brokers"]) >= 1, "brokers list should have at least one entry"


def test_brokers_enabled_features_are_known_names():
    """Every feature name on every cluster node must be a recognised feature."""
    data = _graphql("{ brokers { nodeId enabledFeatures } }")
    for node in data["brokers"]:
        unknown = set(node["enabledFeatures"]) - ALL_KNOWN_FEATURES
        assert not unknown, (
            f"Node {node['nodeId']} returned unknown feature names: {unknown}"
        )


def test_broker_and_brokers_consistent():
    """
    The single-node broker query and the brokers list query must return the same
    enabledFeatures for isCurrent=true node.
    """
    single = _graphql("{ broker { nodeId enabledFeatures } }")
    cluster = _graphql("{ brokers { nodeId isCurrent enabledFeatures } }")

    current_node = next(
        (n for n in cluster["brokers"] if n["isCurrent"]), None
    )
    assert current_node is not None, "No isCurrent=true node found in brokers list"

    assert single["broker"]["nodeId"] == current_node["nodeId"], (
        "broker and brokers(isCurrent) nodeId mismatch"
    )
    assert set(single["broker"]["enabledFeatures"]) == set(current_node["enabledFeatures"]), (
        f"enabledFeatures mismatch between broker and brokers(isCurrent):\n"
        f"  broker:   {sorted(single['broker']['enabledFeatures'])}\n"
        f"  brokers:  {sorted(current_node['enabledFeatures'])}"
    )
