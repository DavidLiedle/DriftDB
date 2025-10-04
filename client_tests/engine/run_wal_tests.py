#!/usr/bin/env python3
"""
WAL Test Runner for DriftDB

This script runs both the comprehensive WAL crash recovery tests
and the Engine-WAL integration tests.
"""

import sys
import subprocess
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_test_script(script_path: Path, script_name: str) -> bool:
    """Run a test script and return success status"""
    logger.info(f"\n{'=' * 80}")
    logger.info(f"RUNNING {script_name.upper()}")
    logger.info(f"{'=' * 80}")

    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=script_path.parent,
            capture_output=False,  # Let output go to console
            text=True
        )

        if result.returncode == 0:
            logger.info(f"✓ {script_name} completed successfully")
            return True
        else:
            logger.error(f"✗ {script_name} failed with exit code {result.returncode}")
            return False

    except Exception as e:
        logger.error(f"✗ Failed to run {script_name}: {e}")
        return False

def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Run all WAL tests for DriftDB")
    parser.add_argument("--test", choices=["recovery", "integration", "all"],
                       default="all", help="Which tests to run")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Get script directory
    script_dir = Path(__file__).parent

    # Define test scripts
    recovery_test = script_dir / "test_wal_recovery.py"
    integration_test = script_dir / "test_engine_wal_integration.py"

    # Check that test scripts exist
    if not recovery_test.exists():
        logger.error(f"Recovery test script not found: {recovery_test}")
        return False

    if not integration_test.exists():
        logger.error(f"Integration test script not found: {integration_test}")
        return False

    logger.info("Starting DriftDB WAL Test Suite")
    logger.info(f"Script directory: {script_dir}")

    # Track results
    results = {}

    # Run selected tests
    if args.test in ["recovery", "all"]:
        results["WAL Crash Recovery Tests"] = run_test_script(
            recovery_test, "WAL Crash Recovery Tests"
        )

    if args.test in ["integration", "all"]:
        results["Engine-WAL Integration Tests"] = run_test_script(
            integration_test, "Engine-WAL Integration Tests"
        )

    # Final summary
    logger.info(f"\n{'=' * 80}")
    logger.info("FINAL TEST SUITE SUMMARY")
    logger.info(f"{'=' * 80}")

    total_tests = len(results)
    passed_tests = sum(1 for success in results.values() if success)
    failed_tests = total_tests - passed_tests

    for test_name, success in results.items():
        status = "PASSED" if success else "FAILED"
        icon = "✓" if success else "✗"
        logger.info(f"{icon} {test_name}: {status}")

    logger.info(f"\nTotal Test Suites: {total_tests}")
    logger.info(f"Passed: {passed_tests}")
    logger.info(f"Failed: {failed_tests}")
    logger.info(f"Success Rate: {(passed_tests / total_tests) * 100:.1f}%")

    if failed_tests == 0:
        logger.info("\n🎉 ALL WAL TESTS PASSED! 🎉")
        logger.info("DriftDB WAL implementation is working correctly.")
    else:
        logger.error(f"\n⚠️  {failed_tests} TEST SUITE(S) FAILED")
        logger.error("Review the test output above for details.")

    return failed_tests == 0

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)