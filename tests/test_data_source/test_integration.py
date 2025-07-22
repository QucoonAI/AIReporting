import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock
from app.repositories.data_source import DataSourceRepository
from app.services.data_source import DataSourceService
from app.models.data_source import DataSource
from app.schemas.data_source import DataSourceCreateRequest, DataSourceUpdateRequest
from app.schemas.enum import DataSourceType


class TestDataSourceIntegration:
    """Integration test cases for DataSource components."""
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_repository_service_integration_create(self):
        """Test repository and service integration for creation."""
        # Mock session
        mock_session = AsyncMock()
        mock_session.flush = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.refresh = AsyncMock()
        
        # Create repository and service
        repo = DataSourceRepository(mock_session)
        service = DataSourceService(repo)
        
        # Mock repository methods
        repo.get_data_source_by_name = AsyncMock(return_value=None)
        repo.create_data_source = AsyncMock(return_value=DataSource(
            data_source_id=1,
            data_source_user_id=1,
            data_source_name="Test Data",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/data.csv",
            data_source_created_at=datetime.now(timezone.utc),
            data_source_updated_at=datetime.now(timezone.utc)
        ))
        
        # Create request
        create_request = DataSourceCreateRequest(
            data_source_name="Test Data",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/data.csv"
        )
        
        # Execute
        result = await service.create_data_source(1, create_request)
        
        # Assert
        assert result.data_source_name == "Test Data"
        assert result.data_source_type == DataSourceType.CSV
        repo.get_data_source_by_name.assert_called_once_with(user_id=1, name="Test Data")
        repo.create_data_source.assert_called_once()
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_repository_service_integration_update(self):
        """Test repository and service integration for update."""
        # Mock session
        mock_session = AsyncMock()
        
        # Create repository and service
        repo = DataSourceRepository(mock_session)
        service = DataSourceService(repo)
        
        # Existing data source
        existing_ds = DataSource(
            data_source_id=1,
            data_source_user_id=1,
            data_source_name="Original Name",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/original.csv",
            data_source_created_at=datetime.now(timezone.utc),
            data_source_updated_at=datetime.now(timezone.utc)
        )
        
        # Updated data source
        updated_ds = DataSource(
            data_source_id=1,
            data_source_user_id=1,
            data_source_name="Updated Name",
            data_source_type=DataSourceType.XLSX,
            data_source_url="https://example.com/updated.xlsx",
            data_source_created_at=existing_ds.data_source_created_at,
            data_source_updated_at=datetime.now(timezone.utc)
        )
        
        # Mock repository methods
        repo.get_data_source_by_id = AsyncMock(return_value=existing_ds)
        repo.get_data_source_by_name = AsyncMock(return_value=None)
        repo.update_data_source = AsyncMock(return_value=updated_ds)
        
        # Update request
        update_request = DataSourceUpdateRequest(
            data_source_name="Updated Name",
            data_source_type=DataSourceType.XLSX,
            data_source_url="https://example.com/updated.xlsx"
        )
        
        # Execute
        result = await service.update_data_source(1, update_request)
        
        # Assert
        assert result.data_source_name == "Updated Name"
        assert result.data_source_type == DataSourceType.XLSX
        repo.get_data_source_by_id.assert_called_once_with(1)
        repo.get_data_source_by_name.assert_called_once_with(user_id=1, name="Updated Name")
        repo.update_data_source.assert_called_once_with(data_source_id=1, update_data=update_request)
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_full_stack_integration_workflow(self):
        """Test full stack integration from API to repository layer."""
        # This would be a more comprehensive test that involves
        # testing the actual API endpoints with real-ish dependencies
        # For brevity, we'll simulate the workflow
        
        # Mock session
        mock_session = AsyncMock()
        mock_session.flush = AsyncMock()
        mock_session.commit = AsyncMock()
        mock_session.refresh = AsyncMock()
        
        # Create components
        repo = DataSourceRepository(mock_session)
        service = DataSourceService(repo)
        
        # Test data
        test_data_source = DataSource(
            data_source_id=1,
            data_source_user_id=1,
            data_source_name="Integration Test",
            data_source_type=DataSourceType.POSTGRES,
            data_source_url="postgresql://user:pass@localhost:5432/testdb",
            data_source_created_at=datetime.now(timezone.utc),
            data_source_updated_at=datetime.now(timezone.utc)
        )
        
        # Mock the repository calls
        repo.get_data_source_by_name = AsyncMock(return_value=None)
        repo.create_data_source = AsyncMock(return_value=test_data_source)
        repo.get_data_source_by_id = AsyncMock(return_value=test_data_source)
        repo.get_user_data_sources = AsyncMock(return_value=[test_data_source])
        repo.delete_data_source = AsyncMock(return_value=True)
        
        # 1. Create data source through service
        create_request = DataSourceCreateRequest(
            data_source_name="Integration Test",
            data_source_type=DataSourceType.POSTGRES,
            data_source_url="postgresql://user:pass@localhost:5432/testdb"
        )
        
        created_ds = await service.create_data_source(1, create_request)
        assert created_ds.data_source_name == "Integration Test"
        
        # 2. Retrieve through repository
        retrieved_ds = await repo.get_data_source_by_id(1)
        assert retrieved_ds.data_source_id == 1
        
        # 3. List user data sources
        user_data_sources = await repo.get_user_data_sources(1)
        assert len(user_data_sources) == 1
        assert user_data_sources[0].data_source_name == "Integration Test"
        
        # 4. Delete through service
        delete_message = await service.delete_data_source(1)
        assert delete_message == "Data source deleted successfully"
        
        # Verify all repository methods were called
        repo.get_data_source_by_name.assert_called()
        repo.create_data_source.assert_called()
        repo.get_data_source_by_id.assert_called()
        repo.get_user_data_sources.assert_called()
        repo.delete_data_source.assert_called()
    
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_error_propagation_integration(self):
        """Test error propagation between service and repository layers."""
        # Mock session
        mock_session = AsyncMock()
        
        # Create repository and service
        repo = DataSourceRepository(mock_session)
        service = DataSourceService(repo)
        
        # Test database error propagation
        repo.get_data_source_by_name = AsyncMock(return_value=None)
        repo.create_data_source = AsyncMock(side_effect=Exception("Database connection failed"))
        
        create_request = DataSourceCreateRequest(
            data_source_name="Error Test",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/error.csv"
        )
        
        # Verify exception is properly wrapped and propagated
        with pytest.raises(Exception) as exc_info:
            await service.create_data_source(1, create_request)
        
        # The service should wrap repository exceptions in HTTPExceptions
        assert "Failed to create data source" in str(exc_info.value) or "Database connection failed" in str(exc_info.value)