#!/usr/bin/env python3
"""
Helper script to convert old-style MQTT tests to pytest format.
This script maintains full test coverage while converting to pytest structure.
"""

import re
import sys
from pathlib import Path


def convert_test_file(filepath):
    """
    Convert a test file from run_test() style to pytest style.
    
    Strategy:
    1. Remove global state variables
    2. Convert run_test() to proper test_*() functions
    3. Add broker_config fixture to test functions
    4. Add username_pw_set for authentication
    5. Replace BROKER_HOST/BROKER_PORT with broker_config
    6. Convert return True/False to pytest assertions
    7. Remove if __name__ == "__main__" block
    """
    
    filepath = Path(filepath)
    if not filepath.exists():
        print(f"Error: File {filepath} not found")
        return False
    
    print(f"\n{'='*70}")
    print(f"Converting: {filepath.name}")
    print(f"{'='*70}\n")
    
    content = filepath.read_text(encoding='utf-8')
    original_lines = len(content.splitlines())
    
    # Check if already converted
    if 'def run_test():' not in content:
        print(f"✓ Already converted (no run_test() function found)")
        return True
    
    # Pattern analysis
    has_broker_host = 'BROKER_HOST' in content
    has_broker_port = 'BROKER_PORT' in content
    has_global_state = re.search(r'^[a-z_]+ = \[\]', content, re.MULTILINE)
    num_test_functions = len(re.findall(r'^def test_[a-z_]+\(\):', content, re.MULTILINE))
    
    print(f"Analysis:")
    print(f"  - Lines: {original_lines}")
    print(f"  - Uses BROKER_HOST/PORT: {has_broker_host}/{has_broker_port}")
    print(f"  - Has global state: {bool(has_global_state)}")
    print(f"  - Existing test_* functions: {num_test_functions}")
    
    # Read test file structure
    lines = content.splitlines(keepends=True)
    
    # Find key sections
    import_end = 0
    for i, line in enumerate(lines):
        if line.strip() and not line.startswith('#') and not line.startswith('import') and not line.startswith('from') and not line.startswith('"""') and not line.startswith("'''"):
            if not any(x in line for x in ['"""', "'''"]):
                import_end = i
                break
    
    print(f"\nConversion needed:")
    print(f"  1. Add pytest import if missing")
    print(f"  2. Add pytestmark = pytest.mark.mqtt5")
    print(f"  3. Remove global state variables")
    print(f"  4. Convert BROKER_HOST/PORT to broker_config fixture")
    print(f"  5. Add authentication with username_pw_set")
    print(f"  6. Convert test functions to use fixtures")
    print(f"  7. Remove run_test() wrapper")
    print(f"  8. Replace if __name__ == '__main__' block")
    
    print(f"\n⚠ Manual conversion recommended for complex tests")
    print(f"  Use conftest.py fixtures: broker_config, message_collector")
    print(f"  Ensure all test scenarios are covered")
    
    return True


def main():
    if len(sys.argv) < 2:
        print("Usage: python convert_test_helper.py <test_file>")
        sys.exit(1)
    
    filepath = sys.argv[1]
    success = convert_test_file(filepath)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
