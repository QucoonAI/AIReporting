import sys
import subprocess
import argparse


def run_command(cmd, description):
    """Run a command and handle errors."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*60}")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.stdout:
        print("STDOUT:")
        print(result.stdout)
    
    if result.stderr:
        print("STDERR:")
        print(result.stderr)
    
    if result.returncode != 0:
        print(f"❌ {description} FAILED with exit code {result.returncode}")
        return False
    else:
        print(f"✅ {description} PASSED")
        return True


def main():
    parser = argparse.ArgumentParser(description="Run data_source module tests")
    parser.add_argument(
        "--mode", 
        choices=["unit", "integration", "e2e", "all"], 
        default="all",
        help="Test mode to run"
    )
    parser.add_argument(
        "--coverage", 
        action="store_true",
        help="Run tests with coverage reporting"
    )
    parser.add_argument(
        "--verbose", 
        action="store_true",
        help="Run tests in verbose mode"
    )
    parser.add_argument(
        "--html-report", 
        action="store_true",
        help="Generate HTML coverage report"
    )
    parser.add_argument(
        "--parallel", 
        action="store_true",
        help="Run tests in parallel"
    )
    
    args = parser.parse_args()
    
    # Base pytest command
    cmd = ["python", "-m", "pytest"]
    
    # Add verbosity
    if args.verbose:
        cmd.extend(["-v", "-s"])
    else:
        cmd.append("-q")
    
    # Add parallel execution
    if args.parallel:
        cmd.extend(["-n", "auto"])
    
    # Add coverage if requested
    if args.coverage:
        cmd.extend([
            "--cov=models.data_source",
            "--cov=repositories.data_source", 
            "--cov=services.data_source",
            "--cov=api.data_source",
            "--cov=schemas.data_source",
            "--cov-report=term-missing"
        ])
        
        if args.html_report:
            cmd.extend(["--cov-report=html:htmlcov"])
    
    # Add test markers based on mode
    if args.mode == "unit":
        cmd.extend(["-m", "not integration and not e2e"])
        cmd.extend(["tests/test_schemas.py", "tests/test_repository.py", "tests/test_service.py"])
    elif args.mode == "integration":
        cmd.extend(["-m", "integration"])
        cmd.append("tests/test_integration.py")
    elif args.mode == "e2e":
        cmd.extend(["-m", "e2e"])
        cmd.extend(["tests/test_api_endpoints.py", "tests/test_e2e.py"])
    else:  # all
        cmd.append("tests/")
    
    # Run the tests
    success = run_command(cmd, f"Data Source Tests ({args.mode})")
    
    if args.coverage and args.html_report:
        print(f"\n📊 Coverage report generated in htmlcov/index.html")
    
    if not success:
        sys.exit(1)
    
    print(f"\n🎉 All {args.mode} tests passed successfully!")


if __name__ == "__main__":
    main()