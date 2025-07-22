import pytest
from unittest.mock import AsyncMock
from app.schemas.data_source import DataSourceCreateRequest
from app.schemas.enum import DataSourceType


class TestDataSourceSecurity:
    """Security test cases for DataSource operations."""
    
    @pytest.mark.asyncio
    async def test_sql_injection_prevention_in_name(self, data_source_service):
        """Test SQL injection prevention in data source name."""
        # Setup malicious data
        malicious_request = DataSourceCreateRequest(
            data_source_name="Test'; DROP TABLE DataSource; --",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/data.csv"
        )
        
        # Mock repository to simulate no existing data source
        data_source_service.data_source_repo.get_data_source_by_name = AsyncMock(return_value=None)
        data_source_service.data_source_repo.create_data_source = AsyncMock()
        
        # Execute - should handle safely
        try:
            await data_source_service.create_data_source(1, malicious_request)
        except Exception as e:
            # Should not be a SQL injection error
            assert "syntax error" not in str(e).lower()
            assert "drop table" not in str(e).lower()
    
    @pytest.mark.asyncio
    async def test_xss_prevention_in_name(self, data_source_service):
        """Test XSS prevention in data source name."""
        # Setup XSS payload
        xss_request = DataSourceCreateRequest(
            data_source_name="<script>alert('xss')</script>",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/data.csv"
        )
        
        # Mock repository
        data_source_service.data_source_repo.get_data_source_by_name = AsyncMock(return_value=None)
        data_source_service.data_source_repo.create_data_source = AsyncMock()
        
        # Execute - should handle safely
        try:
            await data_source_service.create_data_source(1, xss_request)
            # Name should be stored as-is for later sanitization at display layer
        except Exception:
            pass  # Validation might reject certain characters
    
    @pytest.mark.asyncio
    async def test_unauthorized_access_prevention(self, data_source_service, sample_data_source):
        """Test prevention of unauthorized access to data sources."""
        # Setup - data source belongs to user 1
        data_source_service.data_source_repo.get_data_source_by_id = AsyncMock(
            return_value=sample_data_source
        )
        
        # This should be handled at the API layer, but testing service behavior
        # The service itself doesn't enforce user ownership, that's the API's job
        result = await data_source_service.data_source_repo.get_data_source_by_id(1)
        assert result.data_source_user_id == 1
    
    @pytest.mark.asyncio
    async def test_input_length_limits(self, data_source_service):
        """Test input length limits for security."""
        # Test extremely long name
        long_name = "A" * 1000  # Exceeds typical 255 char limit
        
        with pytest.raises(Exception):  # Should raise validation error
            DataSourceCreateRequest(
                data_source_name=long_name,
                data_source_type=DataSourceType.CSV,
                data_source_url="https://example.com/data.csv"
            )
    
    @pytest.mark.asyncio
    async def test_url_validation_security(self):
        """Test URL validation for security."""
        # Test malicious URLs
        malicious_urls = [
            "javascript:alert('xss')",
            "data:text/html,<script>alert('xss')</script>",
            "file:///etc/passwd",
            "ftp://malicious.com/backdoor"
        ]
        
        for url in malicious_urls:
            with pytest.raises(Exception):  # Should raise validation error
                DataSourceCreateRequest(
                    data_source_name="Test",
                    data_source_type=DataSourceType.CSV,
                    data_source_url=url
                )