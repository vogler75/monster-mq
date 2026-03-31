"""Pytest configuration for queuing tests."""
import os
import pytest


def pytest_addoption(parser):
    parser.addoption("--pub-host", default=os.getenv("MQTT_BROKER", "localhost"),
                     help="Publisher broker host (default: localhost)")
    parser.addoption("--pub-port", default=int(os.getenv("MQTT_PORT", "1883")), type=int,
                     help="Publisher broker port (default: 1883)")
    parser.addoption("--sub-host", default=None,
                     help="Subscriber broker host (defaults to --pub-host)")
    parser.addoption("--sub-port", default=None, type=int,
                     help="Subscriber broker port (defaults to --pub-port)")
    parser.addoption("--qos", default=1, type=int, choices=[1, 2],
                     help="QoS level (1 or 2, default: 1)")
    parser.addoption("--rate", default=None, type=int,
                     help="Publish rate in msg/s (overrides parametrized rates)")
    parser.addoption("--disconnect-seconds", default=5, type=int,
                     help="Seconds the subscriber stays disconnected (default: 5)")
    parser.addoption("--no-disconnect", action="store_true", default=False,
                     help="Keep subscriber connected the entire time (no disconnect cycles)")
    parser.addoption("--subscribers", default=3, type=int,
                     help="Number of concurrent subscribers (default: 3)")
    parser.addoption("--username", default=os.getenv("MQTT_USERNAME", "Test"),
                     help="MQTT username")
    parser.addoption("--password", default=os.getenv("MQTT_PASSWORD", "Test"),
                     help="MQTT password")


@pytest.fixture
def cfg(request):
    """Collect all CLI options into a single config dict."""
    pub_host = request.config.getoption("--pub-host")
    pub_port = request.config.getoption("--pub-port")
    return {
        "pub_host": pub_host,
        "pub_port": pub_port,
        "sub_host": request.config.getoption("--sub-host") or pub_host,
        "sub_port": request.config.getoption("--sub-port") or pub_port,
        "qos": request.config.getoption("--qos"),
        "disconnect_seconds": request.config.getoption("--disconnect-seconds"),
        "no_disconnect": request.config.getoption("--no-disconnect"),
        "subscribers": request.config.getoption("--subscribers"),
        "username": request.config.getoption("--username"),
        "password": request.config.getoption("--password"),
    }
