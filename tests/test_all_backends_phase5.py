#!/usr/bin/env python3
"""
Test Phase 5 (Message Expiry) across all 4 database backends:
- SQLite (port 1883)
- PostgreSQL (port 1884)
- MongoDB (port 1885)
- CrateDB (port 1886)
"""

import sys
import subprocess
import time

def test_backend(name, port):
    """Test a single backend"""
    print(f"\n{'='*70}")
    print(f"Testing {name} backend on port {port}")
    print(f"{'='*70}")
    
    try:
        result = subprocess.run(
            [sys.executable, "test_mqtt5_phase5_retained_expiry.py", "--port", str(port)],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        # Check if all tests passed
        if "Total: 3/3 tests passed" in result.stdout:
            print(f"✅ {name}: ALL TESTS PASSED")
            return True
        else:
            print(f"❌ {name}: TESTS FAILED")
            print(result.stdout)
            if result.stderr:
                print("STDERR:", result.stderr)
            return False
    except subprocess.TimeoutExpired:
        print(f"❌ {name}: TEST TIMEOUT")
        return False
    except Exception as e:
        print(f"❌ {name}: ERROR - {e}")
        return False

def main():
    backends = [
        ("SQLite", 1883),
        ("PostgreSQL", 1884),
        ("MongoDB", 1885),
        ("CrateDB", 1886)
    ]
    
    results = {}
    
    print("Phase 5 (Message Expiry) - Multi-Backend Validation")
    print("="*70)
    
    for name, port in backends:
        results[name] = test_backend(name, port)
        time.sleep(2)  # Brief pause between backends
    
    # Summary
    print(f"\n\n{'='*70}")
    print("FINAL SUMMARY")
    print(f"{'='*70}")
    
    for name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{name:15} {status}")
    
    passed_count = sum(1 for p in results.values() if p)
    total_count = len(results)
    
    print(f"\nTotal: {passed_count}/{total_count} backends passed")
    print(f"{'='*70}\n")
    
    return 0 if passed_count == total_count else 1

if __name__ == "__main__":
    sys.exit(main())
