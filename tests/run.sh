#!/usr/bin/env bash
# Run Monster-MQ integration tests with pytest.
# Loads environment variables from .env if it exists.
# Copy .env-example to .env and adjust values for your environment.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

print_help() {
    cat <<EOF
Usage: ./run.sh [OPTIONS] [PATH [::TEST_FUNCTION]]

Options:
  -a    Run all tests
  -h    Show this help message

Path examples:
  pytest_tests/mqtt3/                                          run one directory
  pytest_tests/mqtt3/test_basic_pubsub.py                     run one file
  pytest_tests/mqtt3/test_basic_pubsub.py::test_basic_pubsub_qos0  run one test

Available test groups (directories):
  pytest_tests/mqtt3/       MQTT v3.1.1 tests
  pytest_tests/mqtt5/       MQTT v5 tests
  pytest_tests/graphql/     GraphQL API tests
  pytest_tests/rest/        REST API tests
  pytest_tests/opcua/       OPC UA tests
  pytest_tests/database/    Database backend tests
  pytest_tests/flow/        Flow engine tests
  pytest_tests/queuing/     Message queuing tests
  pytest_tests/latency/     Latency tests
  pytest_tests/i3x/         i3X API tests

Skip test groups via .env or inline env vars:
  SKIP_GRAPHQL=1 ./run.sh -a
  SKIP_MQTT3=1 SKIP_MQTT5=1 ./run.sh -a

Environment variables (set in .env or inline):
  MQTT_BROKER       Broker hostname          (default: localhost)
  MQTT_PORT         Broker port              (default: 1883)
  MQTT_USERNAME     MQTT username            (default: Test)
  MQTT_PASSWORD     MQTT password            (default: Test)
  MQTT_ADMIN_USER   Admin username           (default: Admin)
  MQTT_ADMIN_PASS   Admin password           (default: Admin)
  GRAPHQL_URL       GraphQL endpoint         (default: http://localhost:4000/graphql)
  SKIP_MQTT3        Skip mqtt3 tests         (0/1, default: 0)
  SKIP_MQTT5        Skip mqtt5 tests         (0/1, default: 0)
  SKIP_GRAPHQL      Skip graphql tests       (0/1, default: 0)
  SKIP_REST         Skip rest tests          (0/1, default: 0)
  SKIP_OPCUA        Skip opcua tests         (0/1, default: 0)
  SKIP_DATABASE     Skip database tests      (0/1, default: 0)
  SKIP_FLOW         Skip flow tests          (0/1, default: 0)
  SKIP_QUEUING      Skip queuing tests       (0/1, default: 0)
  SKIP_LATENCY      Skip latency tests       (0/1, default: 0)
  SKIP_I3X          Skip i3x tests           (0/1, default: 0)
EOF
}

# Load .env if present
if [ -f "$SCRIPT_DIR/.env" ]; then
    set -a
    source "$SCRIPT_DIR/.env"
    set +a
fi

# No arguments: print help
if [ $# -eq 0 ]; then
    print_help
    exit 0
fi

case "$1" in
    -h)
        print_help
        exit 0
        ;;
    -a)
        exec pytest
        ;;
    *)
        exec pytest "$@"
        ;;
esac
