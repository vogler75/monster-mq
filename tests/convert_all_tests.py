#!/usr/bin/env python3
"""
Batch convert all test files to pytest format.
Converts tests from standalone script style to pytest-compatible format.
"""
import os
import re
from pathlib import Path

# Map of old test files to new pytest files
TESTS_TO_CONVERT = [
    # MQTT5 tests
    "test_mqtt5_flow_control.py",
    "test_mqtt5_message_expiry.py",
    "test_mqtt5_no_local.py",
    "test_mqtt5_payload_format.py",
    "test_mqtt5_properties.py",
    "test_mqtt5_reason_codes.py",
    "test_mqtt5_request_response.py",
    "test_mqtt5_retain_as_published.py",
    "test_mqtt5_retain_handling.py",
    "test_mqtt5_server_properties.py",
    "test_mqtt5_topic_alias.py",
    "test_mqtt5_will_delay.py",
    
    # MQTT3 tests
    "test_access_level_enforcement.py",
    "test_basic_pubsub.py",
    "test_basic_retained.py",
    "test_bulk_e2e.py",
    "test_display_names.py",
    "test_live_messaging.py",
    "test_live_subscription.py",
    "test_mqtt_publish.py",
    "test_mqtt_publish_bulk_retained.py",
    "test_mqtt_publish_rejection.py",
    "test_mqtt_subscription_rejection.py",
    "test_node_ids.py",
    "test_publish.py",
    "test_simple.py",
    "test_simple_pubsub.py",
    "test_simple_write.py",
    "test_subscription.py",
    
    # GraphQL tests
    "test_graphql_bulk_subscriptions.py",
    "test_graphql_publisher.py",
    "test_graphql_system_logs.py",
    "test_graphql_topic_subscriptions.py",
    
    # OPC-UA tests
    "test_opcua_client.py",
    "test_opcua_subscription.py",
    "test_opcua_subscription_fixed.py",
    "test_opcua_write.py",
]


def convert_test_to_pytest(source_path, dest_path):
    """Convert a test file to pytest format."""
    with open(source_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if already pytest-compatible (has @pytest.mark or def test_)
    if '@pytest.mark' in content or re.search(r'\ndef test_\w+\([^)]*\):', content):
        print(f"  ✓ Already pytest-compatible: {os.path.basename(source_path)}")
        # Just copy it over
        with open(dest_path, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    
    # Add pytest import if not present
    if 'import pytest' not in content:
        # Find the imports section and add pytest
        import_match = re.search(r'(import\s+\w+.*?\n)+', content)
        if import_match:
            imports_end = import_match.end()
            content = content[:imports_end] + 'import pytest\n' + content[imports_end:]
    
    # Add pytestmark if MQTT5 test
    if 'mqtt5' in str(source_path).lower() and 'pytestmark' not in content:
        # Add after imports
        import_section_end = content.find('\n\n', content.find('import'))
        if import_section_end > 0:
            content = (content[:import_section_end] + 
                      '\n\npytestmark = pytest.mark.mqtt5\n' + 
                      content[import_section_end:])
    
    # Convert main() execution pattern to pytest
    # Pattern: if __name__ == "__main__": sys.exit(main())
    content = re.sub(
        r'if __name__ == "__main__":\s+sys\.exit\((\w+)\(\)\)',
        r'if __name__ == "__main__":\n    pytest.main([__file__, "-v"])',
        content
    )
    
    # Convert test classes to functions if they're unittest style
    # This is complex and test-specific, so we'll handle case by case
    
    # Write converted content
    with open(dest_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"  → Converted: {os.path.basename(source_path)}")
    return True


def main():
    """Convert all tests."""
    original_dir = Path("tests_original")
    tests_dir = Path(".")
    
    if not original_dir.exists():
        print("ERROR: tests_original directory not found")
        return 1
    
    print("Converting tests to pytest format...")
    print("=" * 70)
    
    converted = 0
    skipped = 0
    errors = 0
    
    for test_file in TESTS_TO_CONVERT:
        source = original_dir / test_file
        dest = tests_dir / test_file
        
        if not source.exists():
            print(f"  ✗ Not found: {test_file}")
            skipped += 1
            continue
        
        try:
            if convert_test_to_pytest(source, dest):
                converted += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"  ✗ Error converting {test_file}: {e}")
            errors += 1
    
    print("=" * 70)
    print(f"Conversion complete:")
    print(f"  Converted: {converted}")
    print(f"  Skipped:   {skipped}")
    print(f"  Errors:    {errors}")
    print()
    print("Run tests with: pytest -v")
    
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
