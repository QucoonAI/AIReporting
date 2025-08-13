# Data Source Module Testing Guide

This guide covers the comprehensive test suite for the data source module, including unit tests, integration tests, and schema validation tests.

## Test Structure

```
tests/
├── conftest.py                      # Test configuration and fixtures
├── test_data_source_repository.py   # Repository layer unit tests
├── test_data_source_service.py      # Service layer unit tests  
├── test_data_source_routes.py       # Route/API integration tests
├── test_data_source_schemas.py      # Schema validation tests
├── test_data_source_integration.py  # Full integration tests
├── pytest.ini                       # Pytest configuration
├── requirements-test.txt             # Test dependencies
└── run_tests.py                     # Test runner script
```

## Test Categories

### 1. Unit Tests
- **Repository Tests**: Database operations, CRUD functionality, error handling
- **Service Tests**: Business logic, validation, error scenarios
- **Schema Tests**: Pydantic model validation, URL validation, field constraints

### 2. Integration Tests
- **Route Tests**: Full API endpoint testing with mocked dependencies
- **Database Integration**: Real database operations with test database
- **End-to-End**: Complete user workflows and data lifecycle

### 3. Performance Tests
- **Bulk Operations**: Testing with large datasets
- **Concurrent Access**: Multi-user scenarios
- **Response Times**: Ensuring acceptable performance

## Setup

### 1. Install Dependencies

```bash
# Install test dependencies
pip install -r requirements-test.txt

# Or install specific packages
pip install pytest pytest-asyncio pytest-mock httpx sqlmodel
```

### 2. Environment Setup

Create a test environment file `.env.test`:

```env
DATABASE_URL=sqlite:///./test.db
REDIS_URL=redis://localhost:6379/1
SECRET_KEY=test-secret-key
SENDGRID_AUTH_KEY=test-sendgrid-key
```

### 3. Test Database Setup

The tests use SQLite for simplicity, but you can configure PostgreSQL for more realistic testing:

```bash
# For PostgreSQL testing (optional)
createdb testdb
export DATABASE_URL="postgresql://user:password@localhost:5432/testdb"
```

## Running Tests

### Quick Start

```bash
# Run all tests
python run_tests.py

# Run specific test categories
python run_tests.py --mode unit
python run_tests.py --mode integration
python run_tests.py --mode fast

# Run with coverage
python run_tests.py --mode coverage
```

### Using Pytest Directly

```bash
# Run all tests
pytest

# Run specific test files
pytest tests/test_data_source_service.py

# Run tests with coverage
pytest --cov=services.data_source --cov=repositories.data_source

# Run only fast tests
pytest -m "not slow and not integration"

# Run with verbose output
pytest -v -s

# Run in parallel
pytest -n 4
```

### Test Markers

Tests are marked with the following categories:

```bash
# Unit tests only
pytest -m "not integration"

# Integration tests only  
pytest -m integration

# Fast tests only
pytest -m "not slow"

# Slow tests only
pytest -m slow
```

## Test Coverage

### Running Coverage Analysis

```bash
# Generate coverage report
python run_tests.py --mode coverage

# View HTML coverage report
open htmlcov/index.html
```

### Coverage Targets

- **Repository Layer**: > 95%
- **Service Layer**: > 90% 
- **Route Layer**: > 85%
- **Schema Layer**: > 95%

## Test Scenarios Covered

### Repository Layer
- ✅ CRUD operations (Create, Read, Update, Delete)
- ✅ Pagination and filtering
- ✅ Search functionality
- ✅ User isolation (users can only access their own data)
- ✅ Error handling and rollback scenarios
- ✅ Database constraint validation
- ✅ Concurrent access patterns

### Service Layer
- ✅ Business logic validation
- ✅ Duplicate name checking
- ✅ Error propagation and HTTP exception handling
- ✅ Data transformation and mapping
- ✅ Authorization and ownership verification

### Route Layer
- ✅ HTTP request/response handling
- ✅ Authentication and authorization
- ✅ Input validation and error responses
- ✅ Pagination parameter handling
- ✅ Content negotiation
- ✅ Error status codes

### Schema Layer
- ✅ Field validation (required fields, length limits, format validation)
- ✅ URL validation for different data source types
- ✅ Enum validation for data source types
- ✅ Custom validator logic
- ✅ Error message formatting
- ✅ Model serialization and deserialization

### Integration Tests
- ✅ Complete CRUD lifecycle workflows
- ✅ User isolation and security
- ✅ Concurrent operation handling
- ✅ Error recovery and rollback
- ✅ Performance with bulk operations
- ✅ Real database constraints and relationships

## Mock Strategy

### What We Mock
- **Redis Operations**: Session management, OTP storage
- **Email Services**: Verification and notification emails
- **External APIs**: Third-party integrations
- **Time-sensitive Operations**: Timestamps, expiration

### What We Don't Mock
- **Database Operations**: Use real database for integration tests
- **Schema Validation**: Test actual Pydantic validation
- **HTTP Request/Response**: Test real FastAPI routing
- **Business Logic**: Test actual service implementations

## Test Data Management

### Fixtures
- `test_user`: Standard test user with valid credentials
- `test_data_source`: Sample data source for testing
- `current_user_mock`: Mock authenticated user context
- `async_session_mock`: Mock database session for unit tests
- `sample_data_source_create`: Valid creation payload
- `invalid_data_source_create`: Invalid payload for validation testing

### Test Data Cleanup
- Integration tests use transaction rollback
- Unit tests use mocked dependencies
- Test database is recreated for each test session

## Debugging Failed Tests

### Common Issues and Solutions

1. **Database Connection Errors**
   ```bash
   # Check database URL
   echo $DATABASE_URL
   
   # Test connection
   python -c "from config.database import async_engine; print('DB OK')"
   ```

2. **Redis Connection Errors**
   ```bash
   # Check Redis connection
   redis-cli ping
   
   # Use Redis mock for testing
   pytest -k "not redis"
   ```

3. **Authentication Errors**
   ```bash
   # Run without auth (unit tests only)
   pytest tests/test_data_source_service.py
   ```

4. **Import Errors**
   ```bash
   # Check Python path
   export PYTHONPATH="${PYTHONPATH}:."
   ```

### Verbose Debugging

```bash
# Maximum verbosity
python run_tests.py --verbose 3

# Show print statements
pytest -s

# Debug specific test
pytest tests/test_data_source_service.py::TestDataSourceService::test_create_data_source_success -vvv -s
```

## Performance Testing

### Benchmarking

```bash
# Run performance benchmarks
python run_tests.py --benchmark

# Custom benchmark thresholds
pytest --benchmark-min-rounds=5 --benchmark-warmup=on
```

### Load Testing

```bash
# Install locust for load testing
pip install locust

# Run load tests (if implemented)
locust -f tests/load_test_data_sources.py --host=http://localhost:8000
```

## Continuous Integration

### GitHub Actions Example

```yaml
name: Test Data Source Module

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    services:
      postgres:
        image: postgres:13
        env:
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: testdb
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
      
      redis:
        image: redis:6
        options: >-
          --health-cmd "redis-cli ping"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    
    steps:
    - uses: actions/checkout@v2
    
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: 3.9
    
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install -r requirements-test.txt
    
    - name: Run tests
      run: python run_tests.py --mode coverage
      env:
        DATABASE_URL: postgresql://postgres:postgres@localhost:5432/testdb
        REDIS_URL: redis://localhost:6379/1
    
    - name: Upload coverage
      uses: codecov/codecov-action@v1
```

## Best Practices

### Writing New Tests

1. **Follow the AAA Pattern**: Arrange, Act, Assert
   ```python
   async def test_create_data_source():
       # Arrange
       user_id = 1
       create_request = DataSourceCreateRequest(...)
       
       # Act
       result = await service.create_data_source(user_id, create_request)
       
       # Assert
       assert result.data_source_name == "Test Source"
   ```

2. **Use Descriptive Test Names**
   ```python
   def test_create_data_source_with_duplicate_name_raises_http_exception():
       # Test implementation
   ```

3. **Test Edge Cases and Error Conditions**
   ```python
   def test_create_data_source_with_empty_name():
   def test_create_data_source_with_invalid_url():
   def test_create_data_source_with_database_error():
   ```

4. **Mock External Dependencies**
   ```python
   @patch('services.email.EmailService.send_notification')
   def test_operation_sends_notification(mock_email):
       # Test implementation
   ```

5. **Use Fixtures for Reusable Test Data**
   ```python
   @pytest.fixture
   def sample_user():
       return User(email="test@example.com", ...)
   ```

### Test Organization

- Group related tests in classes
- Use descriptive docstrings
- Separate unit, integration, and performance tests
- Keep tests independent and isolated
- Use parametrized tests for similar scenarios

### Performance Considerations

- Mock expensive operations in unit tests
- Use database transactions for rollback in integration tests
- Run slow tests separately from fast feedback loop
- Monitor test execution time and optimize bottlenecks

## Troubleshooting

### Test Database Issues

```bash
# Reset test database
rm -f test.db
python -c "from models import *; from config.database import engine; SQLModel.metadata.create_all(engine)"
```

### Dependency Issues

```bash
# Clean install
pip freeze | grep -E "(pytest|fastapi|sqlmodel)" | xargs pip uninstall -y
pip install -r requirements-test.txt
```

### Import Path Issues

```bash
# Add current directory to Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Or use pytest's import mode
pytest --import-mode=importlib
```

## Contributing

When adding new features to the data source module:

1. Write tests first (TDD approach)
2. Ensure all test categories are covered
3. Update test documentation
4. Verify coverage meets targets
5. Test edge cases and error conditions
6. Add integration tests for new endpoints
7. Update fixtures if needed

## Reporting Issues

When reporting test failures, include:

1. Full test command used
2. Complete error traceback
3. Environment details (Python version, OS, dependencies)
4. Database and Redis configuration
5. Any custom configuration or overrides

## Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [FastAPI Testing](https://fastapi.tiangolo.com/tutorial/testing/)
- [SQLModel Testing](https://sqlmodel.tiangolo.com/tutorial/fastapi/tests/)
- [Pydantic Validation Testing](https://pydantic-docs.helpmanual.io/usage/validators/)

---

For questions or issues with the test suite, please open an issue or contact the development team.