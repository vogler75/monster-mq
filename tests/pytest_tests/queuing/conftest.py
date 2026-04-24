"""Pytest configuration for queuing tests."""
import os
import pytest


def _addoption(parser, *args, **kwargs):
    try:
        parser.addoption(*args, **kwargs)
    except ValueError as exc:
        if "already added" not in str(exc):
            raise


def pytest_addoption(parser):
    _addoption(parser, "--pub-host", default=os.getenv("MQTT_BROKER", "localhost"),
               help="Publisher broker host (default: localhost)")
    _addoption(parser, "--pub-port", default=int(os.getenv("MQTT_PORT", "1883")), type=int,
               help="Publisher broker port (default: 1883)")
    _addoption(parser, "--sub-host", default=None,
               help="Subscriber broker host (defaults to --pub-host)")
    _addoption(parser, "--sub-port", default=None, type=int,
               help="Subscriber broker port (defaults to --pub-port)")
    _addoption(parser, "--qos", default=1, type=int, choices=[1, 2],
               help="QoS level (1 or 2, default: 1)")
    _addoption(parser, "--rate", default=None, type=int,
               help="Publish rate in msg/s (overrides parametrized rates)")
    _addoption(parser, "--disconnect-seconds", default=5, type=int,
               help="Seconds the subscriber stays disconnected (default: 5)")
    _addoption(parser, "--no-disconnect", action="store_true", default=False,
               help="Keep subscriber connected the entire time (no disconnect cycles)")
    _addoption(parser, "--subscribers", default=3, type=int,
               help="Number of concurrent subscribers (default: 3)")
    _addoption(parser, "--username", default=os.getenv("MQTT_USERNAME", "Test"),
               help="MQTT username")
    _addoption(parser, "--password", default=os.getenv("MQTT_PASSWORD", "Test"),
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
