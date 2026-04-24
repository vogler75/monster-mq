"""OPC UA pytest collection helpers."""

import importlib.util


def pytest_ignore_collect(collection_path, config):
    if collection_path.name.startswith("test_") and importlib.util.find_spec("asyncua") is None:
        return True
    return False
