# Monster-MQ Pytest Test Suite

## Setup

1. **Create a virtual environment and install dependencies:**
   ```bash
   cd tests
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure the environment:**
   ```bash
   cp .env-example .env
   # edit .env to match your broker
   ```

3. **Ensure the broker is running:**
   ```bash
   cd ../broker
   ./run.sh
   ```

## Running Tests

Use `run.sh` which automatically loads `.env`:

```bash
./run.sh -h                                    # show help
./run.sh -a                                    # run all tests
./run.sh pytest_tests/mqtt3/                   # run one directory
./run.sh pytest_tests/mqtt3/test_basic_pubsub.py                        # run one file
./run.sh pytest_tests/mqtt3/test_basic_pubsub.py::test_basic_pubsub_qos0  # run one test
```

### Skip test groups

Set `SKIP_<GROUP>=1` in `.env` or inline:

```bash
SKIP_GRAPHQL=1 ./run.sh -a
SKIP_MQTT3=1 SKIP_MQTT5=1 ./run.sh -a
```

Available groups: `MQTT3`, `MQTT5`, `GRAPHQL`, `REST`, `OPCUA`, `DATABASE`, `FLOW`, `QUEUING`, `LATENCY`, `I3X`.

### Run tests by marker

```bash
./run.sh -m mqtt5
./run.sh -m "mqtt5 and not slow"
./run.sh -m subscription_options
```

### Useful pytest flags

```bash
./run.sh -a -x            # stop on first failure
./run.sh -a --maxfail=3   # stop after 3 failures
./run.sh -a -s            # show print statements
./run.sh -a --lf          # rerun only last failed tests
./run.sh -a -n auto       # parallel execution (requires pytest-xdist)
```

### Generate reports

```bash
./run.sh -a --junit-xml=results.xml     # JUnit XML (for CI/CD)
./run.sh -a --html=report.html          # HTML report (requires pytest-html)
```

## Test Organization

```
pytest_tests/
  mqtt3/        MQTT v3.1.1 tests
  mqtt5/        MQTT v5 tests
  graphql/      GraphQL HTTP and WebSocket tests
  rest/         REST API tests
  opcua/        OPC UA tests
  database/     Database backend tests
  flow/         Flow engine end-to-end tests
  queuing/      Persistent queue tests
  latency/      Latency tests
  i3x/          i3X API tests

other_tests/
  latency-rs/   Rust-based latency test tool
  queuing/      Standalone queue producer/consumer scripts
```

## Shared Fixtures (conftest.py)

| Fixture            | Description                                      |
|--------------------|--------------------------------------------------|
| `broker_config`    | Broker connection settings from env vars         |
| `mqtt_client`      | Unconfigured MQTT v5 client                      |
| `connected_client` | Connected MQTT v5 client with loop started       |
| `clean_topic`      | Auto-clears retained messages after the test     |
| `message_collector`| Helper for collecting and waiting for messages   |

## CI/CD Integration

```yaml
- name: Run tests
  run: |
    cd tests
    pip install -r requirements.txt
    ./run.sh -a --junit-xml=results.xml
```
