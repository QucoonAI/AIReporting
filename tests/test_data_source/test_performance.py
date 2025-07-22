import pytest
import time
from unittest.mock import AsyncMock
from app.repositories.data_source import DataSourceRepository


class TestDataSourcePerformance:
    """Performance test cases for DataSource operations."""
    
    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_bulk_create_performance(self, mock_db_session, performance_test_data):
        """Test performance of bulk data source creation."""
        # Setup
        repo = DataSourceRepository(mock_db_session)
        mock_db_session.flush = AsyncMock()
        mock_db_session.commit = AsyncMock()
        mock_db_session.refresh = AsyncMock()
        
        start_time = time.time()
        
        # Execute bulk creation
        for data_source in performance_test_data:
            await repo.create_data_source(data_source)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Assert performance criteria
        assert execution_time < 5.0, f"Bulk creation took {execution_time:.2f}s, expected < 5.0s"
        assert mock_db_session.commit.call_count == 100
    
    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_pagination_performance(self, mock_db_session, performance_test_data):
        """Test performance of paginated queries."""
        # Setup
        repo = DataSourceRepository(mock_db_session)
        
        # Mock paginated response
        mock_data_result = AsyncMock()
        mock_data_result.all.return_value = performance_test_data[:10]  # First page
        mock_count_result = AsyncMock()
        mock_count_result.one.return_value = 100
        
        mock_db_session.exec = AsyncMock(
            side_effect=[mock_data_result, mock_count_result]
        )
        
        start_time = time.time()
        
        # Execute paginated query
        data_sources, total_count = await repo.get_user_data_sources_paginated(
            user_id=1, page=1, per_page=10
        )
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Assert performance criteria
        assert execution_time < 1.0, f"Pagination took {execution_time:.2f}s, expected < 1.0s"
        assert len(data_sources) == 10
        assert total_count == 100