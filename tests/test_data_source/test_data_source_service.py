import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock
from fastapi import HTTPException

from services.data_source import DataSourceService
from repositories.data_source import DataSourceRepository
from schemas.data_source import DataSourceCreateRequest, DataSourceUpdateRequest
from schemas.enum import DataSourceType
from models.data_source import DataSource


class TestDataSourceService:
    """Unit tests for DataSourceService."""

    @pytest_asyncio.fixture
    async def mock_repo(self):
        """Create mock repository."""
        return AsyncMock(spec=DataSourceRepository)

    @pytest_asyncio.fixture
    async def service(self, mock_repo):
        """Create service instance with mock repository."""
        return DataSourceService(mock_repo)

    @pytest.mark.asyncio
    async def test_create_data_source_success(self, service, mock_repo, test_data_source):
        """Test successful data source creation."""
        # Arrange
        create_request = DataSourceCreateRequest(
            data_source_name="Test CSV Source",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/test.csv"
        )
        
        # Mock repository methods
        mock_repo.get_data_source_by_name = AsyncMock(return_value=None)
        mock_repo.create_data_source = AsyncMock(return_value=test_data_source)

        # Act
        result = await service.create_data_source(1, create_request)

        # Assert
        mock_repo.get_data_source_by_name.assert_called_once_with(1, "Test CSV Source")
        mock_repo.create_data_source.assert_called_once()
        
        # Check the created data source object
        created_ds_call = mock_repo.create_data_source.call_args[0][0]
        assert created_ds_call.data_source_user_id == 1
        assert created_ds_call.data_source_name == "Test CSV Source"
        assert created_ds_call.data_source_type == DataSourceType.CSV
        assert created_ds_call.data_source_url == "https://example.com/test.csv"
        
        assert result == test_data_source

    @pytest.mark.asyncio
    async def test_create_data_source_duplicate_name(self, service, mock_repo, test_data_source):
        """Test creating data source with duplicate name."""
        # Arrange
        create_request = DataSourceCreateRequest(
            data_source_name="Existing Source",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/test.csv"
        )
        
        # Mock existing data source
        mock_repo.get_data_source_by_name = AsyncMock(return_value=test_data_source)

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await service.create_data_source(1, create_request)
        
        assert exc_info.value.status_code == 400
        assert "already exists" in str(exc_info.value.detail)
        mock_repo.create_data_source.assert_not_called()

    @pytest.mark.asyncio
    async def test_create_data_source_repository_error(self, service, mock_repo):
        """Test data source creation with repository error."""
        # Arrange
        create_request = DataSourceCreateRequest(
            data_source_name="Test Source",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/test.csv"
        )
        
        mock_repo.get_data_source_by_name = AsyncMock(return_value=None)
        mock_repo.create_data_source = AsyncMock(side_effect=Exception("Database error"))

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await service.create_data_source(1, create_request)
        
        assert exc_info.value.status_code == 500
        assert "Failed to create data source" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_update_data_source_success(self, service, mock_repo, test_data_source):
        """Test successful data source update."""
        # Arrange
        update_request = DataSourceUpdateRequest(
            data_source_name="Updated Source",
            data_source_type=DataSourceType.XLSX,
            data_source_url="https://example.com/updated.xlsx"
        )
        
        updated_data_source = DataSource(
            data_source_id=1,
            data_source_user_id=1,
            data_source_name="Updated Source",
            data_source_type=DataSourceType.XLSX,
            data_source_url="https://example.com/updated.xlsx"
        )
        
        mock_repo.get_data_source_by_id = AsyncMock(return_value=test_data_source)
        mock_repo.get_data_source_by_name = AsyncMock(return_value=None)
        mock_repo.update_data_source = AsyncMock(return_value=updated_data_source)

        # Act
        result = await service.update_data_source(1, update_request)

        # Assert
        mock_repo.get_data_source_by_id.assert_called_once_with(1)
        mock_repo.get_data_source_by_name.assert_called_once_with(1, "Updated Source")
        mock_repo.update_data_source.assert_called_once_with(1, update_request)
        assert result == updated_data_source

    @pytest.mark.asyncio
    async def test_update_data_source_not_found(self, service, mock_repo):
        """Test updating non-existent data source."""
        # Arrange
        update_request = DataSourceUpdateRequest(data_source_name="Updated")
        mock_repo.get_data_source_by_id = AsyncMock(return_value=None)

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await service.update_data_source(999, update_request)
        
        assert exc_info.value.status_code == 404
        assert "not found" in str(exc_info.value.detail)
        mock_repo.update_data_source.assert_not_called()

    @pytest.mark.asyncio
    async def test_update_data_source_name_conflict(self, service, mock_repo, test_data_source):
        """Test updating data source with conflicting name."""
        # Arrange
        update_request = DataSourceUpdateRequest(data_source_name="Conflicting Name")
        
        conflicting_data_source = DataSource(
            data_source_id=2,
            data_source_user_id=1,
            data_source_name="Conflicting Name",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/conflict.csv"
        )
        
        mock_repo.get_data_source_by_id = AsyncMock(return_value=test_data_source)
        mock_repo.get_data_source_by_name = AsyncMock(return_value=conflicting_data_source)

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await service.update_data_source(1, update_request)
        
        assert exc_info.value.status_code == 400
        assert "already exists" in str(exc_info.value.detail)
        mock_repo.update_data_source.assert_not_called()

    @pytest.mark.asyncio
    async def test_update_data_source_same_name(self, service, mock_repo, test_data_source):
        """Test updating data source with same name (should not check for conflict)."""
        # Arrange
        update_request = DataSourceUpdateRequest(
            data_source_name="Test CSV Source",  # Same as existing name
            data_source_url="https://example.com/new-url.csv"
        )
        
        updated_data_source = DataSource(
            data_source_id=1,
            data_source_user_id=1,
            data_source_name="Test CSV Source",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/new-url.csv"
        )
        
        mock_repo.get_data_source_by_id = AsyncMock(return_value=test_data_source)
        mock_repo.update_data_source = AsyncMock(return_value=updated_data_source)

        # Act
        result = await service.update_data_source(1, update_request)

        # Assert
        mock_repo.get_data_source_by_name.assert_not_called()  # Should not check for name conflict
        mock_repo.update_data_source.assert_called_once_with(1, update_request)
        assert result == updated_data_source

    @pytest.mark.asyncio
    async def test_update_data_source_repository_error(self, service, mock_repo, test_data_source):
        """Test data source update with repository error."""
        # Arrange
        update_request = DataSourceUpdateRequest(data_source_name="Updated")
        
        mock_repo.get_data_source_by_id = AsyncMock(return_value=test_data_source)
        mock_repo.get_data_source_by_name = AsyncMock(return_value=None)
        mock_repo.update_data_source = AsyncMock(side_effect=Exception("Database error"))

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await service.update_data_source(1, update_request)
        
        assert exc_info.value.status_code == 500
        assert "Failed to update data source" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_delete_data_source_success(self, service, mock_repo, test_data_source):
        """Test successful data source deletion."""
        # Arrange
        mock_repo.get_data_source_by_id = AsyncMock(return_value=test_data_source)
        mock_repo.delete_data_source = AsyncMock(return_value=True)

        # Act
        result = await service.delete_data_source(1)

        # Assert
        mock_repo.get_data_source_by_id.assert_called_once_with(1)
        mock_repo.delete_data_source.assert_called_once_with(1)
        assert result == "Data source deleted successfully"

    @pytest.mark.asyncio
    async def test_delete_data_source_not_found(self, service, mock_repo):
        """Test deleting non-existent data source."""
        # Arrange
        mock_repo.get_data_source_by_id = AsyncMock(return_value=None)

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await service.delete_data_source(999)
        
        assert exc_info.value.status_code == 404
        assert "not found" in str(exc_info.value.detail)
        mock_repo.delete_data_source.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_data_source_repository_error(self, service, mock_repo, test_data_source):
        """Test data source deletion with repository error."""
        # Arrange
        mock_repo.get_data_source_by_id = AsyncMock(return_value=test_data_source)
        mock_repo.delete_data_source = AsyncMock(side_effect=Exception("Database error"))

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await service.delete_data_source(1)
        
        assert exc_info.value.status_code == 500
        assert "Failed to delete data source" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_service_handles_http_exceptions(self, service, mock_repo):
        """Test that service properly re-raises HTTPExceptions."""
        # Arrange
        create_request = DataSourceCreateRequest(
            data_source_name="Test",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/test.csv"
        )
        
        # Mock repository to raise HTTPException
        http_exception = HTTPException(status_code=400, detail="Validation error")
        mock_repo.get_data_source_by_name = AsyncMock(side_effect=http_exception)

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            await service.create_data_source(1, create_request)
        
        # Should re-raise the same HTTPException
        assert exc_info.value == http_exception