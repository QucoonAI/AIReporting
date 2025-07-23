import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime
from sqlmodel import select

from repositories.data_source import DataSourceRepository
from models.data_source import DataSource
from schemas.data_source import DataSourceUpdateRequest
from schemas.enum import DataSourceType


class TestDataSourceRepository:
    """Unit tests for DataSourceRepository."""

    @pytest_asyncio.fixture
    async def repository(self, async_session_mock):
        """Create repository instance with mocked session."""
        return DataSourceRepository(async_session_mock)

    @pytest_asyncio.async def test_create_data_source_success(self, repository, async_session_mock, test_data_source):
        """Test successful data source creation."""
        # Arrange
        test_data_source.data_source_id = None  # New data source
        async_session_mock.flush = AsyncMock()
        async_session_mock.commit = AsyncMock()
        async_session_mock.refresh = AsyncMock()

        # Act
        result = await repository.create_data_source(test_data_source)

        # Assert
        async_session_mock.add.assert_called_once_with(test_data_source)
        async_session_mock.flush.assert_called_once()
        async_session_mock.commit.assert_called_once()
        async_session_mock.refresh.assert_called_once_with(test_data_source)
        assert result == test_data_source

    @pytest_asyncio.async def test_create_data_source_failure(self, repository, async_session_mock, test_data_source):
        """Test data source creation failure with rollback."""
        # Arrange
        async_session_mock.commit = AsyncMock(side_effect=Exception("Database error"))
        async_session_mock.rollback = AsyncMock()

        # Act & Assert
        with pytest.raises(Exception, match="Database error"):
            await repository.create_data_source(test_data_source)
        
        async_session_mock.rollback.assert_called_once()

    @pytest_asyncio.async def test_get_data_source_by_id_found(self, repository, async_session_mock, test_data_source):
        """Test getting data source by ID when it exists."""
        # Arrange
        async_session_mock.get = AsyncMock(return_value=test_data_source)

        # Act
        result = await repository.get_data_source_by_id(1)

        # Assert
        async_session_mock.get.assert_called_once_with(DataSource, 1)
        assert result == test_data_source

    @pytest_asyncio.async def test_get_data_source_by_id_not_found(self, repository, async_session_mock):
        """Test getting data source by ID when it doesn't exist."""
        # Arrange
        async_session_mock.get = AsyncMock(return_value=None)

        # Act
        result = await repository.get_data_source_by_id(999)

        # Assert
        async_session_mock.get.assert_called_once_with(DataSource, 999)
        assert result is None

    @pytest_asyncio.async def test_get_data_source_by_name_found(self, repository, async_session_mock, test_data_source):
        """Test getting data source by name when it exists."""
        # Arrange
        mock_result = AsyncMock()
        mock_result.first = MagicMock(return_value=test_data_source)
        async_session_mock.exec = AsyncMock(return_value=mock_result)

        # Act
        result = await repository.get_data_source_by_name(1, "Test CSV Source")

        # Assert
        async_session_mock.exec.assert_called_once()
        mock_result.first.assert_called_once()
        assert result == test_data_source

    @pytest_asyncio.async def test_get_data_source_by_name_not_found(self, repository, async_session_mock):
        """Test getting data source by name when it doesn't exist."""
        # Arrange
        mock_result = AsyncMock()
        mock_result.first = MagicMock(return_value=None)
        async_session_mock.exec = AsyncMock(return_value=mock_result)

        # Act
        result = await repository.get_data_source_by_name(1, "Non-existent Source")

        # Assert
        assert result is None

    @pytest_asyncio.async def test_update_data_source_success(self, repository, async_session_mock, test_data_source):
        """Test successful data source update."""
        # Arrange
        update_data = DataSourceUpdateRequest(
            data_source_name="Updated Source",
            data_source_type=DataSourceType.XLSX,
            data_source_url="https://example.com/updated.xlsx"
        )
        async_session_mock.get = AsyncMock(return_value=test_data_source)
        async_session_mock.commit = AsyncMock()
        async_session_mock.refresh = AsyncMock()

        # Act
        result = await repository.update_data_source(1, update_data)

        # Assert
        async_session_mock.get.assert_called_once_with(DataSource, 1)
        assert result.data_source_name == "Updated Source"
        assert result.data_source_type == DataSourceType.XLSX
        assert result.data_source_url == "https://example.com/updated.xlsx"
        async_session_mock.commit.assert_called_once()

    @pytest_asyncio.async def test_update_data_source_not_found(self, repository, async_session_mock):
        """Test updating non-existent data source."""
        # Arrange
        async_session_mock.get = AsyncMock(return_value=None)
        update_data = DataSourceUpdateRequest(data_source_name="Updated")

        # Act & Assert
        with pytest.raises(ValueError, match="Data source with ID 999 not found"):
            await repository.update_data_source(999, update_data)

    @pytest_asyncio.async def test_delete_data_source_success(self, repository, async_session_mock, test_data_source):
        """Test successful data source deletion."""
        # Arrange
        async_session_mock.get = AsyncMock(return_value=test_data_source)
        async_session_mock.delete = AsyncMock()
        async_session_mock.commit = AsyncMock()

        # Act
        result = await repository.delete_data_source(1)

        # Assert
        async_session_mock.get.assert_called_once_with(DataSource, 1)
        async_session_mock.delete.assert_called_once_with(test_data_source)
        async_session_mock.commit.assert_called_once()
        assert result is True

    @pytest_asyncio.async def test_delete_data_source_not_found(self, repository, async_session_mock):
        """Test deleting non-existent data source."""
        # Arrange
        async_session_mock.get = AsyncMock(return_value=None)

        # Act
        result = await repository.delete_data_source(999)

        # Assert
        assert result is False

    @pytest_asyncio.async def test_get_user_data_sources(self, repository, async_session_mock):
        """Test getting all data sources for a user."""
        # Arrange
        mock_data_sources = [
            DataSource(data_source_id=1, data_source_user_id=1, data_source_name="Source 1", data_source_type=DataSourceType.CSV, data_source_url="url1"),
            DataSource(data_source_id=2, data_source_user_id=1, data_source_name="Source 2", data_source_type=DataSourceType.XLSX, data_source_url="url2")
        ]
        mock_result = AsyncMock()
        mock_result.all = MagicMock(return_value=mock_data_sources)
        async_session_mock.exec = AsyncMock(return_value=mock_result)

        # Act
        result = await repository.get_user_data_sources(1)

        # Assert
        async_session_mock.exec.assert_called_once()
        assert len(result) == 2
        assert result == mock_data_sources

    @pytest_asyncio.async def test_get_user_data_sources_with_filter(self, repository, async_session_mock):
        """Test getting filtered data sources for a user."""
        # Arrange
        mock_data_sources = [
            DataSource(data_source_id=1, data_source_user_id=1, data_source_name="CSV Source", data_source_type=DataSourceType.CSV, data_source_url="url1")
        ]
        mock_result = AsyncMock()
        mock_result.all = MagicMock(return_value=mock_data_sources)
        async_session_mock.exec = AsyncMock(return_value=mock_result)

        # Act
        result = await repository.get_user_data_sources(1, DataSourceType.CSV)

        # Assert
        async_session_mock.exec.assert_called_once()
        assert len(result) == 1
        assert result[0].data_source_type == DataSourceType.CSV

    @pytest_asyncio.async def test_get_user_data_sources_paginated(self, repository, async_session_mock):
        """Test paginated data source retrieval."""
        # Arrange
        mock_data_sources = [
            DataSource(data_source_id=1, data_source_user_id=1, data_source_name="Source 1", data_source_type=DataSourceType.CSV, data_source_url="url1")
        ]
        mock_data_result = AsyncMock()
        mock_data_result.all = MagicMock(return_value=mock_data_sources)
        
        mock_count_result = AsyncMock()
        mock_count_result.one = MagicMock(return_value=1)
        
        async_session_mock.exec = AsyncMock(side_effect=[mock_data_result, mock_count_result])

        # Act
        data_sources, total_count = await repository.get_user_data_sources_paginated(
            user_id=1,
            page=1,
            per_page=10
        )

        # Assert
        assert len(data_sources) == 1
        assert total_count == 1
        assert async_session_mock.exec.call_count == 2

    @pytest_asyncio.async def test_get_user_data_sources_paginated_with_search(self, repository, async_session_mock):
        """Test paginated data source retrieval with search."""
        # Arrange
        mock_data_sources = []
        mock_data_result = AsyncMock()
        mock_data_result.all = MagicMock(return_value=mock_data_sources)
        
        mock_count_result = AsyncMock()
        mock_count_result.one = MagicMock(return_value=0)
        
        async_session_mock.exec = AsyncMock(side_effect=[mock_data_result, mock_count_result])

        # Act
        data_sources, total_count = await repository.get_user_data_sources_paginated(
            user_id=1,
            page=1,
            per_page=10,
            search="test",
            data_source_type=DataSourceType.CSV,
            sort_by="data_source_name",
            sort_order="asc"
        )

        # Assert
        assert len(data_sources) == 0
        assert total_count == 0

    @pytest_asyncio.async def test_get_data_sources_list_admin(self, repository, async_session_mock):
        """Test admin function to get all data sources."""
        # Arrange
        mock_data_sources = [
            DataSource(data_source_id=1, data_source_user_id=1, data_source_name="Source 1", data_source_type=DataSourceType.CSV, data_source_url="url1"),
            DataSource(data_source_id=2, data_source_user_id=2, data_source_name="Source 2", data_source_type=DataSourceType.XLSX, data_source_url="url2")
        ]
        mock_data_result = AsyncMock()
        mock_data_result.all = MagicMock(return_value=mock_data_sources)
        
        mock_count_result = AsyncMock()
        mock_count_result.one = MagicMock(return_value=2)
        
        async_session_mock.exec = AsyncMock(side_effect=[mock_data_result, mock_count_result])

        # Act
        data_sources, total_count = await repository.get_data_sources_list(
            page=1,
            per_page=10
        )

        # Assert
        assert len(data_sources) == 2
        assert total_count == 2

    @pytest_asyncio.async def test_repository_error_handling(self, repository, async_session_mock):
        """Test repository error handling."""
        # Arrange
        async_session_mock.exec = AsyncMock(side_effect=Exception("Database connection error"))

        # Act & Assert
        with pytest.raises(Exception, match="Database connection error"):
            await repository.get_user_data_sources(1)