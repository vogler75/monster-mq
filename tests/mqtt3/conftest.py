"""
Conftest for MQTT v3 tests.
These tests use admin/public credentials by default (configurable via env vars).
"""
import os
import pytest


@pytest.fixture
def broker_config():
    """Broker connection configuration for MQTT v3 tests."""
    return {
        "host": os.getenv("MQTT_BROKER", "localhost"),
        "port": int(os.getenv("MQTT_PORT", "1883")),
        "username": os.getenv("MQTT_USERNAME", "admin"),
        "password": os.getenv("MQTT_PASSWORD", "public"),
    }
