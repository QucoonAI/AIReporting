import pytest
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from app.models.data_source import DataSource
from app.repositories.data_source import DataSourceRepository
from app.services.data_source import DataSourceService
from app.schemas.data_source import (
    DataSourceCreateRequest, DataSourceUpdateRequest,
)
from app.schemas.enum import DataSourceType


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_db_session():
    """Mock database session for testing."""
    session = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.refresh = AsyncMock()
    session.rollback = AsyncMock()
    session.get = AsyncMock()
    session.delete = AsyncMock()
    session.exec = AsyncMock()
    return session


@pytest.fixture
def sample_data_source():
    """Sample DataSource object for testing."""
    return DataSource(
        data_source_id=1,
        data_source_user_id=1,
        data_source_name="Test Data Source",
        data_source_type=DataSourceType.CSV,
        data_source_url="https://example.com/data.csv",
        data_source_created_at=datetime.now(timezone.utc),
        data_source_updated_at=datetime.now(timezone.utc)
    )


@pytest.fixture
def sample_create_request():
    """Sample DataSourceCreateRequest for testing."""
    return DataSourceCreateRequest(
        data_source_name="Test Data Source",
        data_source_type=DataSourceType.CSV,
        data_source_url="https://example.com/data.csv"
    )


@pytest.fixture
def sample_update_request():
    """Sample DataSourceUpdateRequest for testing."""
    return DataSourceUpdateRequest(
        data_source_name="Updated Data Source",
        data_source_type=DataSourceType.XLSX,
        data_source_url="https://example.com/updated_data.xlsx"
    )


@pytest.fixture
def mock_current_user():
    """Mock current user for testing."""
    return {"user_id": 1, "username": "testuser"}


@pytest.fixture
def data_source_repository(mock_db_session):
    """DataSourceRepository instance with mocked session."""
    return DataSourceRepository(mock_db_session)


@pytest.fixture
def data_source_service(data_source_repository):
    """DataSourceService instance with mocked repository."""
    return DataSourceService(data_source_repository)


