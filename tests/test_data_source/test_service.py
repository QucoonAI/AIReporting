import pytest
from unittest.mock import AsyncMock
from fastapi import HTTPException, status
from app.schemas.enum import DataSourceType
from app.models.data_source import DataSource


class TestDataSourceService:
    """Test cases for DataSourceService."""
    
    @pytest.mark.asyncio
    async def test_create_data_source_success(self, data_source_service, sample_create_request, sample_data_source):
        """Test successful data source creation."""
        # Setup
        data_source_service.data_source_repo.get_data_source_by_name = AsyncMock(return_value=None)
        data_source_service.data_source_repo.create_data_source = AsyncMock(return_value=sample_data_source)
        
        # Execute
        result = await data_source_service.create_data_source(1, sample_create_request)
        
        # Assert
        assert result == sample_data_source
        data_source_service.data_source_repo.get_data_source_by_name.assert_called_once_with(
            user_id=1, name=sample_create_request.data_source_name
        )
        data_source_service.data_source_repo.create_data_source.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_create_data_source_name_exists(self, data_source_service, sample_create_request, sample_data_source):
        """Test data source creation with existing name."""
        # Setup
        data_source_service.data_source_repo.get_data_source_by_name = AsyncMock(return_value=sample_data_source)
        
        # Execute & Assert
        with pytest.raises(HTTPException) as exc_info:
            await data_source_service.create_data_source(1, sample_create_request)
        
        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
        assert "already exists" in exc_info.value.detail
    
    @pytest.mark.asyncio
    async def test_create_data_source_repository_error(self, data_source_service, sample_create_request):
        """Test data source creation with repository error."""
        # Setup
        data_source_service.data_source_repo.get_data_source_by_name = AsyncMock(return_value=None)
        data_source_service.data_source_repo.create_data_source = AsyncMock(
            side_effect=Exception("Database error")
        )
        
        # Execute & Assert
        with pytest.raises(HTTPException) as exc_info:
            await data_source_service.create_data_source(1, sample_create_request)
        
        assert exc_info.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Failed to create data source" in exc_info.value.detail
    
    @pytest.mark.asyncio
    async def test_update_data_source_success(self, data_source_service, sample_update_request, sample_data_source):
        """Test successful data source update."""
        # Setup
        updated_data_source = DataSource(
            data_source_id=1,
            data_source_user_id=1,
            data_source_name="Updated Data Source",
            data_source_type=DataSourceType.XLSX,
            data_source_url="https://example.com/updated_data.xlsx",
            data_source_created_at=sample_data_source.data_source_created_at,
            data_source_updated_at=sample_data_source.data_source_updated_at
        )
        
        data_source_service.data_source_repo.get_data_source_by_id = AsyncMock(return_value=sample_data_source)
        data_source_service.data_source_repo.get_data_source_by_name = AsyncMock(return_value=None)
        data_source_service.data_source_repo.update_data_source = AsyncMock(return_value=updated_data_source)
        
        # Execute
        result = await data_source_service.update_data_source(1, sample_update_request)
        
        # Assert
        assert result == updated_data_source
        data_source_service.data_source_repo.update_data_source.assert_called_once_with(
            data_source_id=1, update_data=sample_update_request
        )
    
    @pytest.mark.asyncio
    async def test_update_data_source_not_found(self, data_source_service, sample_update_request):
        """Test updating non-existent data source."""
        # Setup
        data_source_service.data_source_repo.get_data_source_by_id = AsyncMock(return_value=None)
        
        # Execute & Assert
        with pytest.raises(HTTPException) as exc_info:
            await data_source_service.update_data_source(999, sample_update_request)
        
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
        assert "Data source not found" in exc_info.value.detail
    
    @pytest.mark.asyncio
    async def test_update_data_source_name_conflict(self, data_source_service, sample_update_request, sample_data_source):
        """Test updating data source with conflicting name."""
        # Setup
        conflicting_data_source = DataSource(
            data_source_id=2,
            data_source_user_id=1,
            data_source_name="Updated Data Source",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/other.csv",
            data_source_created_at=sample_data_source.data_source_created_at,
            data_source_updated_at=sample_data_source.data_source_updated_at
        )
        
        data_source_service.data_source_repo.get_data_source_by_id = AsyncMock(return_value=sample_data_source)
        data_source_service.data_source_repo.get_data_source_by_name = AsyncMock(return_value=conflicting_data_source)
        
        # Execute & Assert
        with pytest.raises(HTTPException) as exc_info:
            await data_source_service.update_data_source(1, sample_update_request)
        
        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
        assert "already exists" in exc_info.value.detail
    
    @pytest.mark.asyncio
    async def test_delete_data_source_success(self, data_source_service, sample_data_source):
        """Test successful data source deletion."""
        # Setup
        data_source_service.data_source_repo.get_data_source_by_id = AsyncMock(return_value=sample_data_source)
        data_source_service.data_source_repo.delete_data_source = AsyncMock(return_value=True)
        
        # Execute
        result = await data_source_service.delete_data_source(1)
        
        # Assert
        assert result == "Data source deleted successfully"
        data_source_service.data_source_repo.delete_data_source.assert_called_once_with(1)
    
    @pytest.mark.asyncio
    async def test_delete_data_source_not_found(self, data_source_service):
        """Test deleting non-existent data source."""
        # Setup
        data_source_service.data_source_repo.get_data_source_by_id = AsyncMock(return_value=None)
        
        # Execute & Assert
        with pytest.raises(HTTPException) as exc_info:
            await data_source_service.delete_data_source(999)
        
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
        assert "Data source not found" in exc_info.value.detail