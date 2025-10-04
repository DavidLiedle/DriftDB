#!/usr/bin/env python3
"""
Master test runner for DriftDB test suite

Usage:
    python tests/run_all_tests.py              # Run all tests
    python tests/run_all_tests.py --unit       # Run unit tests only
    python tests/run_all_tests.py --sql        # Run SQL tests only
    python tests/run_all_tests.py --quick      # Run quick tests only (no slow/performance)
    python tests/run_all_tests.py --coverage   # Generate coverage report
"""
import sys
import os
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
import json

# Colors for output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'
BOLD = '\033[1m'

class TestRunner:
    def __init__(self):
        self.test_dir = Path(__file__).parent
        self.root_dir = self.test_dir.parent
        self.results = {}
        self.start_time = None
        self.end_time = None

    def print_header(self, text, color=BLUE):
        """Print a formatted header"""
        print(f"\n{color}{BOLD}{'=' * 70}")
        print(f"{text.center(70)}")
        print(f"{'=' * 70}{RESET}\n")

    def print_section(self, text):
        """Print a section header"""
        print(f"\n{BLUE}{BOLD}{text}")
        print(f"{'-' * len(text)}{RESET}")

    def run_command(self, cmd, description=""):
        """Run a command and capture output"""
        if description:
            print(f"Running: {description}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False
            )
            return result.returncode == 0, result.stdout, result.stderr
        except Exception as e:
            return False, "", str(e)

    def organize_legacy_tests(self):
        """Move legacy test files to appropriate directories"""
        self.print_section("Organizing Legacy Tests")

        # Create directory structure
        dirs = ['unit', 'integration', 'sql', 'performance', 'stress', 'utils']
        for dir_name in dirs:
            (self.test_dir / dir_name).mkdir(exist_ok=True)

        # Create __init__.py files
        for dir_name in dirs:
            init_file = self.test_dir / dir_name / "__init__.py"
            if not init_file.exists():
                init_file.write_text("")

        # Map old tests to new structure
        test_mapping = {
            'unit': [
                'test_basic_sql.py',
                'test_connection.py',
                'test_simple.py',
                'test_count_star.py',
                'test_simple_txn.py'
            ],
            'integration': [
                'test_temporal.py',
                'test_backup_restore.py',
                'test_engine_wal_integration.py',
                'test_wal_recovery.py',
                'test_transactions.py',
                'test_fixed_features.py',
                'test_new_features.py'
            ],
            'sql': [
                'test_advanced_sql.py',
                'test_distinct.py',
                'test_set_operations.py',
                'test_prepared_statements.py',
                'test_explain_plan.py',
                'test_postgres.py',
                'test_psql.py',
                'comprehensive_sql_test_suite.py'
            ],
            'performance': [
                'test_index_optimization.py',
                'test_time_travel_performance.py'
            ]
        }

        moved = []
        for category, files in test_mapping.items():
            for filename in files:
                src = self.root_dir / filename
                if src.exists():
                    dst = self.test_dir / category / filename
                    if not dst.exists():
                        # Copy instead of move to preserve originals
                        import shutil
                        shutil.copy2(src, dst)
                        moved.append((filename, category))
                        print(f"  ✓ {filename} -> tests/{category}/")

        if moved:
            print(f"\n{GREEN}Organized {len(moved)} test files{RESET}")
        else:
            print(f"{YELLOW}No test files to organize{RESET}")

    def run_category(self, category, args):
        """Run tests for a specific category"""
        category_dir = self.test_dir / category

        if not category_dir.exists() or not any(category_dir.glob("test_*.py")):
            print(f"{YELLOW}No tests found in {category}{RESET}")
            return True

        self.print_section(f"Running {category.upper()} Tests")

        # Build pytest command
        cmd = ["python", "-m", "pytest", str(category_dir)]

        # Add options
        if args.verbose:
            cmd.append("-v")
        if args.parallel:
            cmd.extend(["-n", "auto"])
        if args.coverage:
            cmd.extend(["--cov=driftdb", "--cov-report=html"])
        if args.quick:
            cmd.extend(["-m", "not slow and not performance"])

        # Run tests
        success, stdout, stderr = self.run_command(cmd, f"{category} tests")

        # Parse results
        if "passed" in stdout or "passed" in stderr:
            self.results[category] = "PASSED"
            print(f"{GREEN}✓ {category} tests passed{RESET}")
        else:
            self.results[category] = "FAILED"
            print(f"{RED}✗ {category} tests failed{RESET}")
            if args.verbose:
                print(stdout)
                print(stderr)

        return success

    def run_all_tests(self, args):
        """Run all test categories"""
        self.start_time = datetime.now()

        # Categories to run
        if args.category:
            categories = [args.category]
        else:
            categories = ['unit', 'integration', 'sql', 'performance', 'stress']

        # Skip slow categories in quick mode
        if args.quick:
            categories = [c for c in categories if c not in ['performance', 'stress']]

        # Run each category
        all_passed = True
        for category in categories:
            if not self.run_category(category, args):
                all_passed = False
                if args.fail_fast:
                    break

        self.end_time = datetime.now()
        return all_passed

    def print_summary(self):
        """Print test results summary"""
        self.print_header("TEST RESULTS SUMMARY")

        # Results table
        print(f"{'Category':<20} {'Result':<10}")
        print("-" * 30)

        for category, result in self.results.items():
            color = GREEN if result == "PASSED" else RED
            print(f"{category:<20} {color}{result:<10}{RESET}")

        # Timing
        if self.start_time and self.end_time:
            duration = self.end_time - self.start_time
            print(f"\nTotal time: {duration.total_seconds():.2f} seconds")

        # Overall result
        all_passed = all(r == "PASSED" for r in self.results.values())
        if all_passed:
            self.print_header("ALL TESTS PASSED", GREEN)
            return 0
        else:
            self.print_header("SOME TESTS FAILED", RED)
            return 1

    def check_dependencies(self):
        """Check if required dependencies are installed"""
        self.print_section("Checking Dependencies")

        # Check for pytest
        success, stdout, _ = self.run_command(["pytest", "--version"])
        if success:
            print(f"  ✓ pytest installed")
        else:
            print(f"  ✗ pytest not found. Install with: pip install -r tests/requirements.txt")
            return False

        # Check for DriftDB server binary
        server_binary = self.root_dir / "target" / "release" / "driftdb-server"
        if server_binary.exists():
            print(f"  ✓ DriftDB server binary found")
        else:
            print(f"  ✗ DriftDB server not built. Build with: cargo build --release")
            return False

        return True


def main():
    parser = argparse.ArgumentParser(description='DriftDB Test Runner')

    # Test selection
    parser.add_argument('--category', choices=['unit', 'integration', 'sql', 'performance', 'stress'],
                       help='Run specific test category')
    parser.add_argument('--unit', action='store_const', const='unit', dest='category',
                       help='Run unit tests only')
    parser.add_argument('--sql', action='store_const', const='sql', dest='category',
                       help='Run SQL tests only')
    parser.add_argument('--integration', action='store_const', const='integration', dest='category',
                       help='Run integration tests only')

    # Test options
    parser.add_argument('--quick', action='store_true',
                       help='Skip slow and performance tests')
    parser.add_argument('--coverage', action='store_true',
                       help='Generate coverage report')
    parser.add_argument('--parallel', action='store_true',
                       help='Run tests in parallel')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')
    parser.add_argument('--fail-fast', action='store_true',
                       help='Stop on first failure')
    parser.add_argument('--organize', action='store_true',
                       help='Organize legacy test files')

    args = parser.parse_args()

    runner = TestRunner()

    # Print header
    runner.print_header("DRIFTDB TEST SUITE")

    # Check dependencies
    if not runner.check_dependencies():
        return 1

    # Organize tests if requested
    if args.organize:
        runner.organize_legacy_tests()
        return 0

    # Run tests
    try:
        if runner.run_all_tests(args):
            return runner.print_summary()
        else:
            return runner.print_summary()
    except KeyboardInterrupt:
        print(f"\n{YELLOW}Test run interrupted{RESET}")
        return 1
    except Exception as e:
        print(f"\n{RED}Test runner error: {e}{RESET}")
        return 1


if __name__ == "__main__":
    sys.exit(main())