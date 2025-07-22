import pytest
from pydantic import ValidationError
from app.schemas.data_source import (
    DataSourceCreateRequest, DataSourceUpdateRequest,
    DataSourceUrlValidator
)
from app.schemas.enum import DataSourceType


class TestDataSourceSchemas:
    """Test cases for DataSource schemas."""
    
    def test_data_source_create_request_valid(self):
        """Test valid DataSourceCreateRequest creation."""
        request = DataSourceCreateRequest(
            data_source_name="Test CSV",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/data.csv"
        )
        assert request.data_source_name == "Test CSV"
        assert request.data_source_type == DataSourceType.CSV
        assert str(request.data_source_url) == "https://example.com/data.csv"
    
    def test_data_source_create_request_empty_name(self):
        """Test DataSourceCreateRequest with empty name."""
        with pytest.raises(ValidationError) as exc_info:
            DataSourceCreateRequest(
                data_source_name="",
                data_source_type=DataSourceType.CSV,
                data_source_url="https://example.com/data.csv"
            )
        assert "Data source name cannot be empty" in str(exc_info.value)
    
    def test_data_source_create_request_whitespace_name(self):
        """Test DataSourceCreateRequest with whitespace-only name."""
        with pytest.raises(ValidationError) as exc_info:
            DataSourceCreateRequest(
                data_source_name="   ",
                data_source_type=DataSourceType.CSV,
                data_source_url="https://example.com/data.csv"
            )
        assert "Data source name cannot be empty" in str(exc_info.value)
    
    def test_data_source_create_request_postgres_url(self):
        """Test DataSourceCreateRequest with PostgreSQL URL."""
        request = DataSourceCreateRequest(
            data_source_name="Test Postgres",
            data_source_type=DataSourceType.POSTGRES,
            data_source_url="postgresql://user:password@localhost:5432/testdb"
        )
        assert request.data_source_type == DataSourceType.POSTGRES
    
    def test_data_source_create_request_invalid_postgres_url(self):
        """Test DataSourceCreateRequest with invalid PostgreSQL URL."""
        with pytest.raises(ValidationError) as exc_info:
            DataSourceCreateRequest(
                data_source_name="Test Postgres",
                data_source_type=DataSourceType.POSTGRES,
                data_source_url="invalid-postgres-url"
            )
        assert "Invalid PostgreSQL DSN" in str(exc_info.value)
    
    def test_data_source_update_request_partial(self):
        """Test DataSourceUpdateRequest with partial data."""
        request = DataSourceUpdateRequest(
            data_source_name="Updated Name"
        )
        assert request.data_source_name == "Updated Name"
        assert request.data_source_type is None
        assert request.data_source_url is None
    
    def test_data_source_url_validator_csv(self):
        """Test URL validator for CSV type."""
        url = DataSourceUrlValidator.validate_and_convert_url(
            DataSourceType.CSV, "https://example.com/data.csv"
        )
        assert str(url) == "https://example.com/data.csv"
    
    def test_data_source_url_validator_mssql(self):
        """Test URL validator for MSSQL type."""
        url = "Server=localhost;Database=testdb;Trusted_Connection=yes;"
        validated_url = DataSourceUrlValidator.validate_and_convert_url(
            DataSourceType.MSSQL, url
        )
        assert validated_url == url
    
    def test_data_source_url_validator_invalid_mssql(self):
        """Test URL validator for invalid MSSQL type."""
        with pytest.raises(ValueError) as exc_info:
            DataSourceUrlValidator.validate_and_convert_url(
                DataSourceType.MSSQL, "invalid-mssql-url"
            )
        assert "Invalid MSSQL connection string format" in str(exc_info.value)