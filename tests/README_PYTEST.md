# Monster-MQ Pytest Test Suite

## Setup

1. **Install dependencies:**
   ```bash
   cd tests
   pip install -r requirements.txt
   ```

2. **Ensure broker is running:**
   ```bash
   cd ../broker
   ./run.sh  # or run.bat on Windows
   ```

## Running Tests

### Run all tests:
```bash
pytest
```

### Run specific test file:
```bash
pytest test_mqtt5_rap_pytest.py
```

### Run tests by marker:
```bash
pytest -m mqtt5                    # All MQTT v5 tests
pytest -m subscription_options     # Subscription option tests only
pytest -m "mqtt5 and not slow"     # MQTT v5 tests excluding slow ones
```

### Run tests with keyword matching:
```bash
pytest -k "rap"                    # Tests with "rap" in name
pytest -k "retain"                 # Tests with "retain" in name
```

### Parallel execution (faster):
```bash
pytest -n auto                     # Use all CPU cores
pytest -n 4                        # Use 4 workers
```

### Verbose output:
```bash
pytest -v                          # Verbose
pytest -vv                         # Extra verbose
pytest -s                          # Show print statements
```

### Generate reports:
```bash
pytest --html=report.html          # HTML report
pytest --cov=. --cov-report=html   # Coverage report
pytest --junit-xml=results.xml     # JUnit XML (for CI/CD)
```

### Stop on first failure:
```bash
pytest -x                          # Stop on first failure
pytest --maxfail=3                 # Stop after 3 failures
```

## Test Organization

### Fixtures (conftest.py)
- `broker_config` - Broker connection settings
- `mqtt_client` - Unconfigured MQTT v5 client
- `connected_client` - Connected client with loop started
- `clean_topic` - Auto-cleanup for retained messages
- `message_collector` - Helper for collecting/waiting for messages

### Test Files
- `test_mqtt5_rap_pytest.py` - Retain As Published (example pytest conversion)
- `test_mqtt5_*.py` - Original test files (can be run standalone or with pytest)

## Example Test Structure

```python
import pytest

@pytest.mark.mqtt5
@pytest.mark.subscription_options
def test_my_feature(connected_client, message_collector, clean_topic):
    topic = clean_topic("test/my/topic")
    
    # Setup callbacks
    connected_client.on_message = message_collector.on_message
    
    # Subscribe
    connected_client.subscribe(topic, qos=1)
    
    # Publish
    connected_client.publish(topic, "test_message", qos=1)
    
    # Wait and verify
    assert message_collector.wait_for_messages(1)
    assert message_collector.messages[0]['payload'] == "test_message"
```

## Comparing: Old vs New

### Old way (standalone script):
```bash
python test_mqtt5_retain_as_published.py
```
- Manual test discovery
- Repetitive setup code
- All tests run even if one fails
- No parallel execution

### New way (pytest):
```bash
pytest test_mqtt5_rap_pytest.py -v
```
- Automatic test discovery
- Shared fixtures (less code)
- Stop on failure with `-x`
- Parallel execution with `-n auto`
- Better reporting

## CI/CD Integration

```yaml
# Example GitHub Actions workflow
- name: Run tests
  run: |
    cd tests
    pip install -r requirements.txt
    pytest --junit-xml=results.xml --html=report.html
```

## Tips

- Use `-v` for verbose output to see individual test names
- Use `--tb=short` for concise error messages
- Use `-k` for quick test filtering during development
- Use `--lf` to rerun only last failed tests
- Use `--sw` to stepwise run (stop at first failure, resume from there)
