#!/usr/bin/env python3
"""
Test runner script for data source module tests.
Provides different test execution modes and reporting options.
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path


def run_command(cmd, description):
    """Run a command and handle errors."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(cmd, check=True, text=True)
        print(f"✅ {description} completed successfully")
        return result.returncode
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed with exit code {e.returncode}")
        return e.returncode


def main():
    parser = argparse.ArgumentParser(description="Run data source module tests")
    parser.add_argument(
        "--mode",
        choices=["unit", "integration", "all", "fast", "coverage"],
        default="all",
        help="Test mode to run"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="count",
        default=0,
        help="Increase verbosity (use -v, -vv, or -vvv)"
    )
    parser.add_argument(
        "--parallel", "-n",
        type=int,
        default=1,
        help="Number of parallel processes (requires pytest-xdist)"
    )
    parser.add_argument(
        "--html-report",
        action="store_true",
        help="Generate HTML test report"
    )
    parser.add_argument(
        "--json-report",
        action="store_true",
        help="Generate JSON test report"
    )
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Run performance benchmarks"
    )
    parser.add_argument(
        "--pattern",
        type=str,
        help="Run tests matching pattern (e.g., 'test_create*')"
    )
    parser.add_argument(
        "--failed-only",
        action="store_true",
        help="Run only previously failed tests"
    )
    
    args = parser.parse_args()
    
    # Base pytest command
    cmd = ["python", "-m", "pytest"]
    
    # Add verbosity
    if args.verbose >= 1:
        cmd.append("-v")
    if args.verbose >= 2:
        cmd.append("-s")  # Don't capture output
    if args.verbose >= 3:
        cmd.append("--tb=long")  # Long traceback format
    
    # Add parallel execution
    if args.parallel > 1:
        cmd.extend(["-n", str(args.parallel)])
    
    # Test selection based on mode
    if args.mode == "unit":
        cmd.extend(["-m", "not integration"])
        print("Running unit tests only...")
    elif args.mode == "integration":
        cmd.extend(["-m", "integration"])
        print("Running integration tests only...")
    elif args.mode == "fast":
        cmd.extend(["-m", "not slow and not integration"])
        print("Running fast tests only...")
    elif args.mode == "coverage":
        cmd.extend([
            "--cov=repositories.data_source",
            "--cov=services.data_source", 
            "--cov=routes.data_source",
            "--cov=schemas.data_source",
            "--cov-report=html",
            "--cov-report=term-missing",
            "--cov-fail-under=80"
        ])
        print("Running tests with coverage analysis...")
    else:
        print("Running all tests...")
    
    # Add pattern matching
    if args.pattern:
        cmd.extend(["-k", args.pattern])
    
    # Add failed-only
    if args.failed_only:
        cmd.append("--lf")
    
    # Add reporting
    if args.html_report:
        cmd.extend(["--html=reports/test_report.html", "--self-contained-html"])
        os.makedirs("reports", exist_ok=True)
    
    if args.json_report:
        cmd.extend(["--json-report", "--json-report-file=reports/test_report.json"])
        os.makedirs("reports", exist_ok=True)
    
    # Add benchmark
    if args.benchmark:
        cmd.append("--benchmark-only")
    
    # Add test discovery paths
    test_paths = [
        "tests/test_data_source/test_data_source_repository.py",
        "tests/test_data_source/test_data_source_service.py", 
        "tests/test_data_source/test_data_source_routes.py",
        "tests/test_data_source/test_data_source_schemas.py",
        "tests/test_data_source/test_data_source_integration.py"
    ]
    
    # Only add existing test files
    existing_paths = [path for path in test_paths if os.path.exists(path)]
    if existing_paths:
        cmd.extend(existing_paths)
    else:
        # Fallback to test directory
        cmd.append("tests/test_data_source/")
    
    # Run the tests
    return_code = run_command(cmd, f"Data Source Tests ({args.mode} mode)")
    
    # Print summary
    if return_code == 0:
        print("\n🎉 All tests passed!")
        if args.mode == "coverage":
            print("📊 Coverage report generated in htmlcov/index.html")
        if args.html_report:
            print("📄 HTML report generated in reports/test_report.html")
    else:
        print(f"\n💥 Tests failed with exit code {return_code}")
    
    return return_code


if __name__ == "__main__":
    sys.exit(main())