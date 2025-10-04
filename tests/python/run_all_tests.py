#!/usr/bin/env python3
"""
Master test runner for DriftDB Python test suite.
Runs all test categories and provides a summary report.
"""

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime

# Colors for terminal output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'
BOLD = '\033[1m'

def run_test_category(category_name, test_dir):
    """Run tests in a specific category."""
    print(f"\n{BLUE}{BOLD}Running {category_name} Tests{RESET}")
    print("=" * 60)

    test_files = list(Path(test_dir).glob("test_*.py"))
    if not test_files:
        print(f"{YELLOW}No tests found in {test_dir}{RESET}")
        return 0, 0, []

    passed = 0
    failed = 0
    failed_tests = []

    for test_file in sorted(test_files):
        test_name = test_file.stem
        print(f"  Running {test_name}...", end=" ")

        try:
            result = subprocess.run(
                [sys.executable, str(test_file)],
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode == 0:
                print(f"{GREEN}✓ PASSED{RESET}")
                passed += 1
            else:
                print(f"{RED}✗ FAILED{RESET}")
                failed += 1
                failed_tests.append((test_name, result.stderr or result.stdout))
        except subprocess.TimeoutExpired:
            print(f"{RED}✗ TIMEOUT{RESET}")
            failed += 1
            failed_tests.append((test_name, "Test timed out after 30 seconds"))
        except Exception as e:
            print(f"{RED}✗ ERROR{RESET}")
            failed += 1
            failed_tests.append((test_name, str(e)))

    print(f"\n  {category_name} Results: {GREEN}{passed} passed{RESET}, {RED}{failed} failed{RESET}")
    return passed, failed, failed_tests

def main():
    """Main test runner."""
    print(f"{BOLD}DriftDB Python Test Suite{RESET}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Get test directory
    script_dir = Path(__file__).parent

    # Categories to test
    categories = [
        ("Unit", script_dir / "unit"),
        ("Integration", script_dir / "integration"),
        ("Security", script_dir / "security"),
        ("Performance", script_dir / "performance"),
    ]

    total_passed = 0
    total_failed = 0
    all_failed_tests = []

    # Run each category
    for category_name, test_dir in categories:
        if test_dir.exists():
            passed, failed, failed_tests = run_test_category(category_name, test_dir)
            total_passed += passed
            total_failed += failed
            if failed_tests:
                all_failed_tests.append((category_name, failed_tests))

    # Print summary
    print("\n" + "=" * 60)
    print(f"{BOLD}TEST SUMMARY{RESET}")
    print("=" * 60)
    print(f"Total Tests Run: {total_passed + total_failed}")
    print(f"{GREEN}Passed: {total_passed}{RESET}")
    print(f"{RED}Failed: {total_failed}{RESET}")

    if total_failed > 0:
        success_rate = (total_passed / (total_passed + total_failed)) * 100
        print(f"Success Rate: {success_rate:.1f}%")
    else:
        print(f"Success Rate: {GREEN}100%{RESET}")

    # Show failed test details
    if all_failed_tests:
        print(f"\n{RED}{BOLD}Failed Test Details:{RESET}")
        print("-" * 60)
        for category, failed_tests in all_failed_tests:
            print(f"\n{category} Tests:")
            for test_name, error in failed_tests:
                print(f"  • {test_name}")
                # Show first line of error
                error_lines = error.split('\n')
                if error_lines and error_lines[0]:
                    print(f"    {error_lines[0][:100]}")

    print("\n" + "=" * 60)
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Exit with appropriate code
    sys.exit(0 if total_failed == 0 else 1)

if __name__ == "__main__":
    # Check if server is running
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="driftdb",
            user="driftdb",
            password="driftdb"
        )
        conn.close()
    except:
        print(f"{RED}ERROR: Cannot connect to DriftDB server on localhost:5433{RESET}")
        print("Please ensure the server is running before running tests.")
        sys.exit(1)

    main()