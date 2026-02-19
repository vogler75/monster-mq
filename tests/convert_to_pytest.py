#!/usr/bin/env python3
"""
Batch convert Monster-MQ tests to pytest format.

This script converts standalone test files to pytest-compatible format
while maintaining backward compatibility (tests can still run standalone).
"""
import re
import os
from pathlib import Path


PYTEST_HEADER = '''"""
{docstring}
Can be run standalone or with pytest.
"""
import time
import pytest
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.subscribeoptions import SubscribeOptions


pytestmark = pytest.mark.mqtt5
'''

PYTEST_MAIN = '''

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
'''


def convert_test_file(filepath):
    """Convert a test file to pytest format."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Skip if already converted
    if 'pytest.mark.mqtt5' in content or 'pytest.main' in content:
        print(f"  Skipping {filepath.name} (already pytest)")
        return False
    
    # Extract docstring
    docstring_match = re.search(r'"""(.*?)"""', content, re.DOTALL)
    docstring = docstring_match.group(1).strip() if docstring_match else "MQTT v5 Test"
    
    # Replace if __name__ == "__main__" block with pytest.main
    if 'if __name__ == "__main__":' in content:
        # Find the main block
        main_block_start = content.find('if __name__ == "__main__":')
        if main_block_start != -1:
            # Remove everything from main block onwards
            content = content[:main_block_start].rstrip()
            content += PYTEST_MAIN
    
    # Add pytest import if not present
    if 'import pytest' not in content:
        # Find first import section
        import_pos = content.find('import ')
        if import_pos != -1:
            # Insert pytest import after other imports
            lines = content.split('\n')
            for i, line in enumerate(lines):
                if line.startswith('import ') or line.startswith('from '):
                    continue
                else:
                    # Insert pytest imports before this line
                    lines.insert(i, 'import pytest')
                    lines.insert(i+1, '')
                    break
            content = '\n'.join(lines)
    
    # Add pytestmark if not present
    if 'pytestmark' not in content:
        # Add after imports
        lines = content.split('\n')
        for i, line in enumerate(lines):
            if line.startswith('#') or line.strip() == '' or line.startswith('"""'):
                continue
            if not (line.startswith('import ') or line.startswith('from ')):
                lines.insert(i, '\npytestmark = pytest.mark.mqtt5\n')
                break
        content = '\n'.join(lines)
    
    return content


def main():
    """Convert all MQTT5 test files."""
    test_dir = Path(__file__).parent
    
    mqtt5_tests = list(test_dir.glob('test_mqtt5_*.py'))
    mqtt5_tests = [t for t in mqtt5_tests if 'pytest' not in t.name and 'rap_simple' not in t.name]
    
    print(f"Found {len(mqtt5_tests)} MQTT5 tests to convert")
    print()
    
    converted = 0
    skipped = 0
    
    for test_file in mqtt5_tests:
        print(f"Converting: {test_file.name}")
        result = convert_test_file(test_file)
        
        if result:
            # Backup original
            backup_path = test_file.with_suffix('.py.bak')
            test_file.rename(backup_path)
            
            # Write converted
            with open(test_file, 'w', encoding='utf-8') as f:
                f.write(result)
            
            print(f"  ✓ Converted (backup: {backup_path.name})")
            converted += 1
        else:
            skipped += 1
    
    print()
    print(f"Conversion complete:")
    print(f"  Converted: {converted}")
    print(f"  Skipped: {skipped}")
    print()
    print("Run tests with: pytest -v")


if __name__ == "__main__":
    main()
