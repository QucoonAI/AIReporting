import pytest
from unittest.mock import AsyncMock
from app.models.data_source import DataSource
from app.schemas.enum import DataSourceType
from app.schemas.data_source import DataSourceUpdateRequest


class TestDataSourceRepository:
    """Test cases for DataSourceRepository."""
    
    @pytest.mark.asyncio
    async def test_create_data_source_success(self, data_source_repository, sample_data_source):
        """Test successful data source creation."""
        # Setup
        data_source_repository.session.flush = AsyncMock()
        data_source_repository.session.commit = AsyncMock()
        data_source_repository.session.refresh = AsyncMock()
        
        # Execute
        result = await data_source_repository.create_data_source(sample_data_source)
        
        # Assert
        data_source_repository.session.add.assert_called_once_with(sample_data_source)
        data_source_repository.session.flush.assert_called_once()
        data_source_repository.session.commit.assert_called_once()
        data_source_repository.session.refresh.assert_called_once_with(sample_data_source)
        assert result == sample_data_source
    
    @pytest.mark.asyncio
    async def test_create_data_source_failure(self, data_source_repository, sample_data_source):
        """Test data source creation failure."""
        # Setup
        data_source_repository.session.flush.side_effect = Exception("Database error")
        data_source_repository.session.rollback = AsyncMock()
        
        # Execute & Assert
        with pytest.raises(Exception) as exc_info:
            await data_source_repository.create_data_source(sample_data_source)
        
        assert "Database error" in str(exc_info.value)
        data_source_repository.session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_data_source_by_id_found(self, data_source_repository, sample_data_source):
        """Test getting data source by ID when found."""
        # Setup
        data_source_repository.session.get = AsyncMock(return_value=sample_data_source)
        
        # Execute
        result = await data_source_repository.get_data_source_by_id(1)
        
        # Assert
        data_source_repository.session.get.assert_called_once_with(DataSource, 1)
        assert result == sample_data_source
    
    @pytest.mark.asyncio
    async def test_get_data_source_by_id_not_found(self, data_source_repository):
        """Test getting data source by ID when not found."""
        # Setup
        data_source_repository.session.get = AsyncMock(return_value=None)
        
        # Execute
        result = await data_source_repository.get_data_source_by_id(999)
        
        # Assert
        data_source_repository.session.get.assert_called_once_with(DataSource, 999)
        assert result is None
    
    @pytest.mark.asyncio
    async def test_get_data_source_by_name_found(self, data_source_repository, sample_data_source):
        """Test getting data source by name when found."""
        # Setup
        mock_result = AsyncMock()
        mock_result.first.return_value = sample_data_source
        data_source_repository.session.exec = AsyncMock(return_value=mock_result)
        
        # Execute
        result = await data_source_repository.get_data_source_by_name(1, "Test Data Source")
        
        # Assert
        data_source_repository.session.exec.assert_called_once()
        mock_result.first.assert_called_once()
        assert result == sample_data_source
    
    @pytest.mark.asyncio
    async def test_update_data_source_success(self, data_source_repository, sample_data_source):
        """Test successful data source update."""
        # Setup
        update_request = DataSourceUpdateRequest(
            data_source_name="Updated Name",
            data_source_type=DataSourceType.XLSX
        )
        data_source_repository.session.get = AsyncMock(return_value=sample_data_source)
        data_source_repository.session.commit = AsyncMock()
        data_source_repository.session.refresh = AsyncMock()
        
        # Execute
        result = await data_source_repository.update_data_source(1, update_request)
        
        # Assert
        assert result.data_source_name == "Updated Name"
        assert result.data_source_type == DataSourceType.XLSX
        data_source_repository.session.commit.assert_called_once()
        data_source_repository.session.refresh.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_update_data_source_not_found(self, data_source_repository):
        """Test updating non-existent data source."""
        # Setup
        update_request = DataSourceUpdateRequest(data_source_name="Updated Name")
        data_source_repository.session.get = AsyncMock(return_value=None)
        
        # Execute & Assert
        with pytest.raises(ValueError) as exc_info:
            await data_source_repository.update_data_source(999, update_request)
        
        assert "Data source with ID 999 not found" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_delete_data_source_success(self, data_source_repository, sample_data_source):
        """Test successful data source deletion."""
        # Setup
        data_source_repository.session.get = AsyncMock(return_value=sample_data_source)
        data_source_repository.session.delete = AsyncMock()
        data_source_repository.session.commit = AsyncMock()
        
        # Execute
        result = await data_source_repository.delete_data_source(1)
        
        # Assert
        assert result is True
        data_source_repository.session.delete.assert_called_once_with(sample_data_source)
        data_source_repository.session.commit.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_delete_data_source_not_found(self, data_source_repository):
        """Test deleting non-existent data source."""
        # Setup
        data_source_repository.session.get = AsyncMock(return_value=None)
        
        # Execute
        result = await data_source_repository.delete_data_source(999)
        
        # Assert
        assert result is False
    
    @pytest.mark.asyncio
    async def test_get_user_data_sources(self, data_source_repository, sample_data_source):
        """Test getting user data sources."""
        # Setup
        mock_result = AsyncMock()
        mock_result.all.return_value = [sample_data_source]
        data_source_repository.session.exec = AsyncMock(return_value=mock_result)
        
        # Execute
        result = await data_source_repository.get_user_data_sources(1)
        
        # Assert
        assert len(result) == 1
        assert result[0] == sample_data_source
        data_source_repository.session.exec.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_user_data_sources_with_type_filter(self, data_source_repository, sample_data_source):
        """Test getting user data sources with type filter."""
        # Setup
        mock_result = AsyncMock()
        mock_result.all.return_value = [sample_data_source]
        data_source_repository.session.exec = AsyncMock(return_value=mock_result)
        
        # Execute
        result = await data_source_repository.get_user_data_sources(1, DataSourceType.CSV)
        
        # Assert
        assert len(result) == 1
        assert result[0] == sample_data_source
        data_source_repository.session.exec.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_user_data_sources_paginated(self, data_source_repository, sample_data_source):
        """Test getting paginated user data sources."""
        # Setup
        mock_data_result = AsyncMock()
        mock_data_result.all.return_value = [sample_data_source]
        mock_count_result = AsyncMock()
        mock_count_result.one.return_value = 1
        
        data_source_repository.session.exec = AsyncMock(
            side_effect=[mock_data_result, mock_count_result]
        )
        
        # Execute
        data_sources, total_count = await data_source_repository.get_user_data_sources_paginated(
            user_id=1, page=1, per_page=10
        )
        
        # Assert
        assert len(data_sources) == 1
        assert data_sources[0] == sample_data_source
        assert total_count == 1
        assert data_source_repository.session.exec.call_count == 2