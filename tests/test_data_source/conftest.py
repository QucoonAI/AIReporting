import sys
import os
import asyncio
import pytest
import pytest_asyncio
from typing import AsyncGenerator, Dict, Any
from fastapi.testclient import TestClient
from sqlmodel import SQLModel, create_engine, Session
from sqlmodel.pool import StaticPool
from unittest.mock import AsyncMock, MagicMock

# Add the app directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'app'))

# Import your app and dependencies
from main import app  # Now imports from app/main.py
from config.database import get_session
from config.redis import redis_manager
from core.dependencies import get_current_user, get_data_source_service, get_data_source_repo
from services.redis import AsyncRedisService
from services.data_source import DataSourceService
from repositories.data_source import DataSourceRepository
from models.data_source import DataSource
from models.user import User, UserProfile
from schemas.enum import DataSourceType


# Test database setup
@pytest.fixture(name="engine")
def engine_fixture():
    """Create test database engine with in-memory SQLite."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


@pytest.fixture(name="session")
def session_fixture(engine):
    """Create test database session."""
    with Session(engine) as session:
        yield session


@pytest_asyncio.fixture
async def async_session_mock():
    """Mock async session for testing."""
    session = AsyncMock()
    session.add = MagicMock()
    session.delete = MagicMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.refresh = AsyncMock()
    session.get = AsyncMock()
    session.exec = AsyncMock()
    session.flush = AsyncMock()
    return session


@pytest.fixture
def redis_service_mock():
    """Mock Redis service for testing."""
    mock_redis = AsyncMock(spec=AsyncRedisService)
    mock_redis.verify_token = AsyncMock()
    mock_redis.generate_otp = AsyncMock(return_value="123456")
    mock_redis.store_otp = AsyncMock()
    mock_redis.verify_otp = AsyncMock()
    mock_redis.delete_otp = AsyncMock()
    return mock_redis


@pytest.fixture
def test_user():
    """Create a test user."""
    return User(
        user_id=1,
        user_email="test@example.com",
        user_first_name="Test",
        user_last_name="User",
        user_password="hashed_password",
        user_is_verified=True,
        user_is_active=True
    )


@pytest.fixture
def test_data_source():
    """Create a test data source."""
    return DataSource(
        data_source_id=1,
        data_source_user_id=1,
        data_source_name="Test CSV Source",
        data_source_type=DataSourceType.CSV,
        data_source_url="https://example.com/test.csv"
    )


@pytest.fixture
def current_user_mock():
    """Mock current user for authenticated routes."""
    return {
        "user_id": 1,
        "session_id": "test-session-id",
        "roles": ["user"],
        "session_data": {}
    }


@pytest_asyncio.fixture
async def data_source_repo_mock(async_session_mock):
    """Mock DataSourceRepository."""
    repo = DataSourceRepository(async_session_mock)
    return repo


@pytest_asyncio.fixture
async def data_source_service_mock(data_source_repo_mock):
    """Mock DataSourceService."""
    return DataSourceService(data_source_repo_mock)


@pytest.fixture
def client():
    """Create test client with dependency overrides."""
    def override_get_session():
        # This will be overridden in individual tests
        pass
    
    def override_get_current_user():
        return {
            "user_id": 1,
            "session_id": "test-session-id",
            "roles": ["user"],
            "session_data": {}
        }
    
    app.dependency_overrides[get_session] = override_get_session
    app.dependency_overrides[get_current_user] = override_get_current_user
    
    with TestClient(app) as client:
        yield client
    
    # Clean up overrides
    app.dependency_overrides.clear()


@pytest.fixture
def authenticated_client(client, current_user_mock):
    """Create authenticated test client."""
    def override_get_current_user():
        return current_user_mock
    
    app.dependency_overrides[get_current_user] = override_get_current_user
    return client


# Event loop fixture for async tests
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# Sample test data
@pytest.fixture
def sample_data_source_create():
    """Sample data for creating data source."""
    return {
        "data_source_name": "Test CSV Source",
        "data_source_type": "csv",
        "data_source_url": "https://example.com/test.csv"
    }


@pytest.fixture
def sample_data_source_update():
    """Sample data for updating data source."""
    return {
        "data_source_name": "Updated CSV Source",
        "data_source_type": "xlsx",
        "data_source_url": "https://example.com/updated.xlsx"
    }


@pytest.fixture
def invalid_data_source_create():
    """Invalid data for testing validation."""
    return {
        "data_source_name": "",  # Empty name
        "data_source_type": "invalid_type",
        "data_source_url": "not-a-url"
    }