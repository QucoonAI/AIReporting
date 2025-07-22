import pytest
from unittest.mock import AsyncMock
from fastapi import HTTPException, status
from sqlalchemy.exc import IntegrityError, OperationalError
from app.repositories.data_source import DataSourceRepository


class TestDataSourceErrorHandling:
    """Error handling test cases for DataSource operations."""
    
    @pytest.mark.asyncio
    async def test_database_connection_error(self, mock_db_session):
        """Test handling of database connection errors."""
        # Setup
        repo = DataSourceRepository(mock_db_session)
        mock_db_session.exec.side_effect = OperationalError("Connection lost", None, None)
        
        # Execute & Assert
        with pytest.raises(Exception) as exc_info:
            await repo.get_data_source_by_id(1)
        
        assert "Connection lost" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_unique_constraint_violation(self, mock_db_session, sample_data_source):
        """Test handling of unique constraint violations."""
        # Setup
        repo = DataSourceRepository(mock_db_session)
        mock_db_session.commit.side_effect = IntegrityError(
            "UNIQUE constraint failed", None, None
        )
        mock_db_session.rollback = AsyncMock()
        
        # Execute & Assert
        with pytest.raises(Exception) as exc_info:
            await repo.create_data_source(sample_data_source)
        
        mock_db_session.rollback.assert_called_once()
        assert "UNIQUE constraint failed" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_service_layer_error_propagation(self, data_source_service, sample_create_request):
        """Test error propagation from repository to service layer."""
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
    async def test_not_found_error_handling(self, data_source_service):
        """Test handling of not found errors."""
        # Setup
        data_source_service.data_source_repo.get_data_source_by_id = AsyncMock(return_value=None)
        
        # Execute & Assert
        with pytest.raises(HTTPException) as exc_info:
            await data_source_service.delete_data_source(999)
        
        assert exc_info.value.status_code == status.HTTP_404_NOT_FOUND
        assert "Data source not found" in exc_info.value.detail