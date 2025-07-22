import pytest
from datetime import datetime, timedelta, timezone
from typing import List
from test_factories import DataSourceFactory
from app.schemas.enum import DataSourceType


@pytest.fixture
def multiple_data_sources() -> List:
    """Generate multiple data sources for testing pagination and filtering."""
    return [
        DataSourceFactory(
            data_source_id=1,
            data_source_user_id=1,
            data_source_name="CSV Sales Data",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/sales.csv",
            data_source_created_at=datetime.now(timezone.utc) - timedelta(days=5)
        ),
        DataSourceFactory(
            data_source_id=2,
            data_source_user_id=1,
            data_source_name="Excel Financial Data",
            data_source_type=DataSourceType.XLSX,
            data_source_url="https://example.com/finance.xlsx",
            data_source_created_at=datetime.now(timezone.utc) - timedelta(days=3)
        ),
        DataSourceFactory(
            data_source_id=3,
            data_source_user_id=1,
            data_source_name="Postgres User Database",
            data_source_type=DataSourceType.POSTGRES,
            data_source_url="postgresql://user:pass@localhost:5432/users",
            data_source_created_at=datetime.now(timezone.utc) - timedelta(days=1)
        ),
        DataSourceFactory(
            data_source_id=4,
            data_source_user_id=2,  # Different user
            data_source_name="Other User Data",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/other.csv",
            data_source_created_at=datetime.now(timezone.utc)
        )
    ]


@pytest.fixture
def database_connection_strings():
    """Sample database connection strings for testing validation."""
    return {
        "valid_postgres": "postgresql://user:password@localhost:5432/testdb",
        "invalid_postgres": "not-a-postgres-url",
        "valid_mysql": "mysql://user:password@localhost:3306/testdb",
        "invalid_mysql": "mysql://incomplete",
        "valid_mssql": "Server=localhost;Database=testdb;Trusted_Connection=yes;",
        "invalid_mssql": "invalid-mssql-connection",
        "valid_oracle": "localhost:1521:TESTDB",
        "invalid_oracle": "incomplete-oracle",
        "valid_mongodb": "mongodb://user:password@localhost:27017/testdb",
        "invalid_mongodb": "mongodb://incomplete"
    }


@pytest.fixture
def performance_test_data():
    """Generate large dataset for performance testing."""
    return [
        DataSourceFactory(
            data_source_id=i,
            data_source_user_id=1,
            data_source_name=f"Data Source {i}",
            data_source_type=DataSourceType.CSV,
            data_source_url=f"https://example.com/data_{i}.csv"
        )
        for i in range(1, 101)  # 100 data sources
    ]