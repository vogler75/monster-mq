# Original Test Files (Backup)

This directory contains the original standalone Python test files before pytest conversion.

## Purpose

These files serve as:
- **Fallback** if pytest conversion has issues
- **Reference** for test logic and structure
- **Archive** of original test implementation

## Files

All test files can be run standalone:
```bash
python test_mqtt5_connection.py
python test_mqtt5_retain_as_published.py
# etc.
```

## New Pytest Tests

The pytest-converted versions are in the parent `tests/` directory and can be run with:
```bash
pytest                                    # Run all tests
pytest test_mqtt5_connection_pytest.py   # Run specific test
pytest -m mqtt5                          # Run by marker
```

## Date Archived

January 30, 2026

## Note

These original tests are fully functional and can still be used if needed.
They were backed up during the pytest conversion to maintain a safety net.
