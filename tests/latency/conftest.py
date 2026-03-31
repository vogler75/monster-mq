"""Pytest configuration for latency tests."""
import os
import pytest


def pytest_addoption(parser):
    parser.addoption("--host", default=os.getenv("MQTT_BROKER", "localhost"),
                     help="Broker host (default: localhost)")
    parser.addoption("--port", default=int(os.getenv("MQTT_PORT", "1883")), type=int,
                     help="Broker port (default: 1883)")
    parser.addoption("--qos", default=1, type=int, choices=[0, 1, 2],
                     help="QoS level (0, 1, or 2, default: 1)")
    parser.addoption("--interval-ms", default=100, type=int,
                     help="Publish interval in milliseconds (default: 100)")
    parser.addoption("--duration", default=10, type=int,
                     help="Test duration in seconds (default: 10)")
    parser.addoption("--username", default=os.getenv("MQTT_USERNAME", "Test"),
                     help="MQTT username")
    parser.addoption("--password", default=os.getenv("MQTT_PASSWORD", "Test"),
                     help="MQTT password")


@pytest.fixture
def cfg(request):
    """Collect all CLI options into a single config dict."""
    return {
        "host": request.config.getoption("--host"),
        "port": request.config.getoption("--port"),
        "qos": request.config.getoption("--qos"),
        "interval_ms": request.config.getoption("--interval-ms"),
        "duration": request.config.getoption("--duration"),
        "username": request.config.getoption("--username"),
        "password": request.config.getoption("--password"),
    }
