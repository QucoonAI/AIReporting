.PHONY: install test test-unit test-integration test-e2e test-all test-coverage test-performance clean lint format

# Installation
install:
	pip install -r requirements-test.txt

# Test commands
test: test-unit

test-unit:
	python run_tests.py --mode unit --verbose

test-integration:
	python run_tests.py --mode integration --verbose

test-e2e:
	python run_tests.py --mode e2e --verbose

test-all:
	python run_tests.py --mode all --verbose

test-coverage:
	python run_tests.py --mode all --coverage --html-report

test-performance:
	python -m pytest tests/test_performance.py -v -m slow

test-security:
	python -m pytest tests/test_security.py -v

test-parallel:
	python run_tests.py --mode all --parallel --verbose

# Code quality
lint:
	flake8 models/ repositories/ services/ api/ schemas/ tests/
	mypy models/ repositories/ services/ api/ schemas/

format:
	black models/ repositories/ services/ api/ schemas/ tests/
	isort models/ repositories/ services/ api/ schemas/ tests/

# Cleanup
clean:
	rm -rf __pycache__/
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete

# CI/CD pipeline
ci: install lint test-coverage
	@echo "✅ All CI checks passed!"
