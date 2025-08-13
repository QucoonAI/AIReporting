import pytest
from pydantic import ValidationError
from datetime import datetime

from schemas.data_source import (
    DataSourceCreateRequest, DataSourceUpdateRequest, DataSourceResponse,
    DataSourceCreateResponse, DataSourceUpdateResponse, DataSourceDeleteResponse,
    DataSourceListResponse, DataSourcePaginatedListResponse, PaginationMetadata,
    DataSourceUrlValidator
)
from schemas.enum import DataSourceType


class TestDataSourceSchemas:
    """Test data source schema validation."""

    def test_data_source_create_request_valid_csv(self):
        """Test valid CSV data source creation request."""
        data = {
            "data_source_name": "Test CSV",
            "data_source_type": "csv",
            "data_source_url": "https://example.com/data.csv"
        }
        
        schema = DataSourceCreateRequest(**data)
        assert schema.data_source_name == "Test CSV"
        assert schema.data_source_type == DataSourceType.CSV
        assert str(schema.data_source_url) == "https://example.com/data.csv"

    def test_data_source_create_request_valid_postgres(self):
        """Test valid PostgreSQL data source creation request."""
        data = {
            "data_source_name": "Test PostgreSQL",
            "data_source_type": "postgres",
            "data_source_url": "postgresql://user:password@localhost:5432/database"
        }
        
        schema = DataSourceCreateRequest(**data)
        assert schema.data_source_name == "Test PostgreSQL"
        assert schema.data_source_type == DataSourceType.POSTGRES
        assert str(schema.data_source_url) == "postgresql://user:password@localhost:5432/database"

    def test_data_source_create_request_valid_mysql(self):
        """Test valid MySQL data source creation request."""
        data = {
            "data_source_name": "Test MySQL",
            "data_source_type": "mysql",
            "data_source_url": "mysql://user:password@localhost:3306/database"
        }
        
        schema = DataSourceCreateRequest(**data)
        assert schema.data_source_name == "Test MySQL"
        assert schema.data_source_type == DataSourceType.MYSQL

    def test_data_source_create_request_valid_mongodb(self):
        """Test valid MongoDB data source creation request."""
        data = {
            "data_source_name": "Test MongoDB",
            "data_source_type": "mongodb",
            "data_source_url": "mongodb://user:password@localhost:27017/database"
        }
        
        schema = DataSourceCreateRequest(**data)
        assert schema.data_source_name == "Test MongoDB"
        assert schema.data_source_type == DataSourceType.MONGODB

    def test_data_source_create_request_valid_mssql(self):
        """Test valid MSSQL data source creation request."""
        data = {
            "data_source_name": "Test MSSQL",
            "data_source_type": "mssql",
            "data_source_url": "Server=localhost;Database=testdb;User Id=user;Password=password;"
        }
        
        schema = DataSourceCreateRequest(**data)
        assert schema.data_source_name == "Test MSSQL"
        assert schema.data_source_type == DataSourceType.MSSQL

    def test_data_source_create_request_valid_oracle(self):
        """Test valid Oracle data source creation request."""
        data = {
            "data_source_name": "Test Oracle",
            "data_source_type": "oracle",
            "data_source_url": "localhost:1521:XE"
        }
        
        schema = DataSourceCreateRequest(**data)
        assert schema.data_source_name == "Test Oracle"
        assert schema.data_source_type == DataSourceType.ORACLE

    def test_data_source_create_request_empty_name(self):
        """Test data source creation with empty name."""
        data = {
            "data_source_name": "",
            "data_source_type": "csv",
            "data_source_url": "https://example.com/data.csv"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            DataSourceCreateRequest(**data)
        
        errors = exc_info.value.errors()
        assert any("Data source name cannot be empty" in str(error) for error in errors)

    def test_data_source_create_request_whitespace_name(self):
        """Test data source creation with whitespace-only name."""
        data = {
            "data_source_name": "   ",
            "data_source_type": "csv",
            "data_source_url": "https://example.com/data.csv"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            DataSourceCreateRequest(**data)
        
        errors = exc_info.value.errors()
        assert any("Data source name cannot be empty" in str(error) for error in errors)

    def test_data_source_create_request_long_name(self):
        """Test data source creation with name exceeding max length."""
        data = {
            "data_source_name": "x" * 256,  # Exceeds 255 character limit
            "data_source_type": "csv",
            "data_source_url": "https://example.com/data.csv"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            DataSourceCreateRequest(**data)
        
        errors = exc_info.value.errors()
        assert any("ensure this value has at most 255 characters" in str(error) for error in errors)

    def test_data_source_create_request_invalid_type(self):
        """Test data source creation with invalid type."""
        data = {
            "data_source_name": "Test Source",
            "data_source_type": "invalid_type",
            "data_source_url": "https://example.com/data.csv"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            DataSourceCreateRequest(**data)
        
        errors = exc_info.value.errors()
        assert any("invalid_type" in str(error) for error in errors)

    def test_data_source_create_request_invalid_csv_url(self):
        """Test CSV data source with invalid URL."""
        data = {
            "data_source_name": "Test CSV",
            "data_source_type": "csv",
            "data_source_url": "not-a-url"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            DataSourceCreateRequest(**data)
        
        errors = exc_info.value.errors()
        assert any("Invalid file URL" in str(error) for error in errors)

    def test_data_source_create_request_invalid_postgres_url(self):
        """Test PostgreSQL data source with invalid URL."""
        data = {
            "data_source_name": "Test PostgreSQL",
            "data_source_type": "postgres",
            "data_source_url": "not-a-postgres-url"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            DataSourceCreateRequest(**data)
        
        errors = exc_info.value.errors()
        assert any("Invalid PostgreSQL DSN" in str(error) for error in errors)

    def test_data_source_create_request_invalid_mysql_url(self):
        """Test MySQL data source with invalid URL."""
        data = {
            "data_source_name": "Test MySQL",
            "data_source_type": "mysql",
            "data_source_url": "not-a-mysql-url"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            DataSourceCreateRequest(**data)
        
        errors = exc_info.value.errors()
        assert any("Invalid MySQL DSN" in str(error) for error in errors)

    def test_data_source_create_request_invalid_mongodb_url(self):
        """Test MongoDB data source with invalid URL."""
        data = {
            "data_source_name": "Test MongoDB",
            "data_source_type": "mongodb",
            "data_source_url": "not-a-mongodb-url"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            DataSourceCreateRequest(**data)
        
        errors = exc_info.value.errors()
        assert any("Invalid MongoDB DSN" in str(error) for error in errors)

    def test_data_source_create_request_invalid_mssql_url(self):
        """Test MSSQL data source with invalid URL."""
        data = {
            "data_source_name": "Test MSSQL",
            "data_source_type": "mssql",
            "data_source_url": "not-a-valid-connection-string"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            DataSourceCreateRequest(**data)
        
        errors = exc_info.value.errors()
        assert any("Invalid MSSQL connection string" in str(error) for error in errors)

    def test_data_source_create_request_invalid_oracle_url(self):
        """Test Oracle data source with invalid URL."""
        data = {
            "data_source_name": "Test Oracle",
            "data_source_type": "oracle",
            "data_source_url": "not-a-valid-oracle-string"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            DataSourceCreateRequest(**data)
        
        errors = exc_info.value.errors()
        assert any("Invalid Oracle connection string" in str(error) for error in errors)


class TestDataSourceUpdateRequest:
    """Test data source update request schema."""

    def test_data_source_update_request_valid_partial(self):
        """Test partial update with valid data."""
        data = {
            "data_source_name": "Updated Name"
        }
        
        schema = DataSourceUpdateRequest(**data)
        assert schema.data_source_name == "Updated Name"
        assert schema.data_source_type is None
        assert schema.data_source_url is None

    def test_data_source_update_request_valid_full(self):
        """Test full update with valid data."""
        data = {
            "data_source_name": "Updated CSV",
            "data_source_type": "xlsx",
            "data_source_url": "https://example.com/updated.xlsx"
        }
        
        schema = DataSourceUpdateRequest(**data)
        assert schema.data_source_name == "Updated CSV"
        assert schema.data_source_type == DataSourceType.XLSX
        assert str(schema.data_source_url) == "https://example.com/updated.xlsx"

    def test_data_source_update_request_empty_name(self):
        """Test update with empty name."""
        data = {
            "data_source_name": ""
        }
        
        with pytest.raises(ValidationError) as exc_info:
            DataSourceUpdateRequest(**data)
        
        errors = exc_info.value.errors()
        assert any("Data source name cannot be empty" in str(error) for error in errors)

    def test_data_source_update_request_no_url_validation_when_none(self):
        """Test that URL validation is skipped when URL is None."""
        data = {
            "data_source_name": "Updated Name",
            "data_source_type": "csv"
            # No URL provided
        }
        
        # Should not raise validation error
        schema = DataSourceUpdateRequest(**data)
        assert schema.data_source_name == "Updated Name"
        assert schema.data_source_type == DataSourceType.CSV
        assert schema.data_source_url is None

    def test_data_source_update_request_no_type_validation_when_none(self):
        """Test that type validation is skipped when type is None."""
        data = {
            "data_source_name": "Updated Name",
            "data_source_url": "https://example.com/test.csv"
            # No type provided
        }
        
        # Should not raise validation error
        schema = DataSourceUpdateRequest(**data)
        assert schema.data_source_name == "Updated Name"
        assert schema.data_source_type is None
        assert str(schema.data_source_url) == "https://example.com/test.csv"


class TestDataSourceResponse:
    """Test data source response schema."""

    def test_data_source_response_from_model(self):
        """Test creating response from model data."""
        model_data = {
            "data_source_id": 1,
            "data_source_user_id": 1,
            "data_source_name": "Test Source",
            "data_source_type": DataSourceType.CSV,
            "data_source_url": "https://example.com/test.csv",
            "data_source_created_at": datetime(2023, 1, 1, 12, 0, 0),
            "data_source_updated_at": datetime(2023, 1, 1, 12, 0, 0)
        }
        
        schema = DataSourceResponse(**model_data)
        assert schema.data_source_id == 1
        assert schema.data_source_user_id == 1
        assert schema.data_source_name == "Test Source"
        assert schema.data_source_type == DataSourceType.CSV
        assert str(schema.data_source_url) == "https://example.com/test.csv"
        assert schema.data_source_created_at == datetime(2023, 1, 1, 12, 0, 0)
        assert schema.data_source_updated_at == datetime(2023, 1, 1, 12, 0, 0)


class TestDataSourceResponseSchemas:
    """Test response wrapper schemas."""

    def test_data_source_create_response(self):
        """Test data source create response schema."""
        data_source_data = {
            "data_source_id": 1,
            "data_source_user_id": 1,
            "data_source_name": "Test Source",
            "data_source_type": DataSourceType.CSV,
            "data_source_url": "https://example.com/test.csv",
            "data_source_created_at": datetime(2023, 1, 1, 12, 0, 0),
            "data_source_updated_at": datetime(2023, 1, 1, 12, 0, 0)
        }
        
        response_data = {
            "message": "Data source created successfully",
            "data_source": DataSourceResponse(**data_source_data)
        }
        
        schema = DataSourceCreateResponse(**response_data)
        assert schema.message == "Data source created successfully"
        assert schema.data_source.data_source_id == 1

    def test_data_source_update_response(self):
        """Test data source update response schema."""
        data_source_data = {
            "data_source_id": 1,
            "data_source_user_id": 1,
            "data_source_name": "Updated Source",
            "data_source_type": DataSourceType.XLSX,
            "data_source_url": "https://example.com/updated.xlsx",
            "data_source_created_at": datetime(2023, 1, 1, 12, 0, 0),
            "data_source_updated_at": datetime(2023, 1, 2, 12, 0, 0)
        }
        
        response_data = {
            "message": "Data source updated successfully",
            "data_source": DataSourceResponse(**data_source_data)
        }
        
        schema = DataSourceUpdateResponse(**response_data)
        assert schema.message == "Data source updated successfully"
        assert schema.data_source.data_source_name == "Updated Source"

    def test_data_source_delete_response(self):
        """Test data source delete response schema."""
        response_data = {
            "message": "Data source deleted successfully"
        }
        
        schema = DataSourceDeleteResponse(**response_data)
        assert schema.message == "Data source deleted successfully"

    def test_data_source_list_response(self):
        """Test data source list response schema."""
        data_sources_data = [
            {
                "data_source_id": 1,
                "data_source_user_id": 1,
                "data_source_name": "Source 1",
                "data_source_type": DataSourceType.CSV,
                "data_source_url": "https://example.com/1.csv",
                "data_source_created_at": datetime(2023, 1, 1, 12, 0, 0),
                "data_source_updated_at": datetime(2023, 1, 1, 12, 0, 0)
            },
            {
                "data_source_id": 2,
                "data_source_user_id": 1,
                "data_source_name": "Source 2",
                "data_source_type": DataSourceType.XLSX,
                "data_source_url": "https://example.com/2.xlsx",
                "data_source_created_at": datetime(2023, 1, 2, 12, 0, 0),
                "data_source_updated_at": datetime(2023, 1, 2, 12, 0, 0)
            }
        ]
        
        response_data = {
            "message": "Data sources retrieved successfully",
            "data_sources": [DataSourceResponse(**ds) for ds in data_sources_data],
            "total_count": 2
        }
        
        schema = DataSourceListResponse(**response_data)
        assert schema.message == "Data sources retrieved successfully"
        assert len(schema.data_sources) == 2
        assert schema.total_count == 2

    def test_pagination_metadata(self):
        """Test pagination metadata schema."""
        pagination_data = {
            "page": 1,
            "per_page": 10,
            "total": 25,
            "total_pages": 3,
            "has_next": True,
            "has_prev": False
        }
        
        schema = PaginationMetadata(**pagination_data)
        assert schema.page == 1
        assert schema.per_page == 10
        assert schema.total == 25
        assert schema.total_pages == 3
        assert schema.has_next is True
        assert schema.has_prev is False

    def test_data_source_paginated_list_response(self):
        """Test paginated data source list response schema."""
        data_sources_data = [
            {
                "data_source_id": 1,
                "data_source_user_id": 1,
                "data_source_name": "Source 1",
                "data_source_type": DataSourceType.CSV,
                "data_source_url": "https://example.com/1.csv",
                "data_source_created_at": datetime(2023, 1, 1, 12, 0, 0),
                "data_source_updated_at": datetime(2023, 1, 1, 12, 0, 0)
            }
        ]
        
        pagination_data = {
            "page": 1,
            "per_page": 10,
            "total": 1,
            "total_pages": 1,
            "has_next": False,
            "has_prev": False
        }
        
        response_data = {
            "message": "Data sources retrieved successfully",
            "data_sources": [DataSourceResponse(**ds) for ds in data_sources_data],
            "pagination": PaginationMetadata(**pagination_data)
        }
        
        schema = DataSourcePaginatedListResponse(**response_data)
        assert schema.message == "Data sources retrieved successfully"
        assert len(schema.data_sources) == 1
        assert schema.pagination.total == 1


class TestDataSourceUrlValidator:
    """Test URL validator utility class."""

    def test_validate_name_valid(self):
        """Test name validation with valid name."""
        result = DataSourceUrlValidator.validate_name("Test Source")
        assert result == "Test Source"

    def test_validate_name_with_whitespace(self):
        """Test name validation with leading/trailing whitespace."""
        result = DataSourceUrlValidator.validate_name("  Test Source  ")
        assert result == "Test Source"

    def test_validate_name_empty_string(self):
        """Test name validation with empty string."""
        with pytest.raises(ValueError) as exc_info:
            DataSourceUrlValidator.validate_name("")
        
        assert "cannot be empty" in str(exc_info.value)

    def test_validate_name_whitespace_only(self):
        """Test name validation with whitespace-only string."""
        with pytest.raises(ValueError) as exc_info:
            DataSourceUrlValidator.validate_name("   ")
        
        assert "cannot be empty" in str(exc_info.value)

    def test_validate_name_none(self):
        """Test name validation with None."""
        result = DataSourceUrlValidator.validate_name(None)
        assert result is None

    def test_validate_and_convert_url_csv(self):
        """Test URL validation for CSV type."""
        url = "https://example.com/data.csv"
        result = DataSourceUrlValidator.validate_and_convert_url(DataSourceType.CSV, url)
        assert str(result) == url

    def test_validate_and_convert_url_postgres(self):
        """Test URL validation for PostgreSQL type."""
        url = "postgresql://user:password@localhost:5432/database"
        result = DataSourceUrlValidator.validate_and_convert_url(DataSourceType.POSTGRES, url)
        assert str(result) == url

    def test_validate_and_convert_url_mysql(self):
        """Test URL validation for MySQL type."""
        url = "mysql://user:password@localhost:3306/database"
        result = DataSourceUrlValidator.validate_and_convert_url(DataSourceType.MYSQL, url)
        assert str(result) == url

    def test_validate_and_convert_url_mongodb(self):
        """Test URL validation for MongoDB type."""
        url = "mongodb://user:password@localhost:27017/database"
        result = DataSourceUrlValidator.validate_and_convert_url(DataSourceType.MONGODB, url)
        assert str(result) == url

    def test_validate_and_convert_url_mssql_connection_string(self):
        """Test URL validation for MSSQL connection string format."""
        url = "Server=localhost;Database=testdb;User Id=user;Password=password;"
        result = DataSourceUrlValidator.validate_and_convert_url(DataSourceType.MSSQL, url)
        assert result == url

    def test_validate_and_convert_url_mssql_uri_format(self):
        """Test URL validation for MSSQL URI format."""
        url = "mssql://user:password@localhost:1433/database"
        result = DataSourceUrlValidator.validate_and_convert_url(DataSourceType.MSSQL, url)
        assert result == url

    def test_validate_and_convert_url_oracle_standard(self):
        """Test URL validation for Oracle standard format."""
        url = "localhost:1521:XE"
        result = DataSourceUrlValidator.validate_and_convert_url(DataSourceType.ORACLE, url)
        assert result == url

    def test_validate_and_convert_url_oracle_tns(self):
        """Test URL validation for Oracle TNS format."""
        url = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SID=XE)))"
        result = DataSourceUrlValidator.validate_and_convert_url(DataSourceType.ORACLE, url)
        assert result == url

    def test_validate_and_convert_url_invalid_csv(self):
        """Test URL validation for invalid CSV URL."""
        with pytest.raises(ValueError) as exc_info:
            DataSourceUrlValidator.validate_and_convert_url(DataSourceType.CSV, "not-a-url")
        
        assert "Invalid file URL" in str(exc_info.value)

    def test_validate_and_convert_url_invalid_postgres(self):
        """Test URL validation for invalid PostgreSQL URL."""
        with pytest.raises(ValueError) as exc_info:
            DataSourceUrlValidator.validate_and_convert_url(DataSourceType.POSTGRES, "not-a-postgres-url")
        
        assert "Invalid PostgreSQL DSN" in str(exc_info.value)

    def test_validate_and_convert_url_invalid_mssql(self):
        """Test URL validation for invalid MSSQL URL."""
        with pytest.raises(ValueError) as exc_info:
            DataSourceUrlValidator.validate_and_convert_url(DataSourceType.MSSQL, "not-a-valid-connection")
        
        assert "Invalid MSSQL connection string" in str(exc_info.value)

    def test_validate_and_convert_url_invalid_oracle(self):
        """Test URL validation for invalid Oracle URL."""
        with pytest.raises(ValueError) as exc_info:
            DataSourceUrlValidator.validate_and_convert_url(DataSourceType.ORACLE, "invalid-oracle-string")
        
        assert "Invalid Oracle connection string" in str(exc_info.value)