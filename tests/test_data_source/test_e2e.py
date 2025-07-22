import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session
from fastapi.testclient import TestClient
from fastapi import FastAPI

from app.models.data_source import DataSource
from app.routes.data_source import router
from app.schemas.enum import DataSourceType


@pytest.fixture(scope="session")
def test_db_engine():
    """Create test database engine."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


@pytest.fixture
def test_db_session(test_db_engine):
    """Create test database session."""
    with Session(test_db_engine) as session:
        yield session


@pytest.fixture
def test_app_e2e():
    """Create test FastAPI application for E2E tests."""
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.fixture
def test_client_e2e(test_app_e2e):
    """Create test client for E2E tests."""
    return TestClient(test_app_e2e)


class TestDataSourceE2E:
    """End-to-end test cases for DataSource module."""
    
    @patch('api.data_source.get_current_user')
    @patch('api.data_source.get_data_source_service')
    @patch('api.data_source.get_data_source_repo')
    def test_complete_crud_workflow(
        self, mock_get_repo, mock_get_service, mock_get_user, test_client_e2e
    ):
        """Test complete CRUD workflow for data sources."""
        # Mock user
        mock_user = {"user_id": 1, "username": "testuser"}
        mock_get_user.return_value = mock_user
        
        # Mock data source objects
        created_data_source = DataSource(
            data_source_id=1,
            data_source_user_id=1,
            data_source_name="Test CSV Data",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/data.csv",
            data_source_created_at=datetime.now(timezone.utc),
            data_source_updated_at=datetime.now(timezone.utc)
        )
        
        updated_data_source = DataSource(
            data_source_id=1,
            data_source_user_id=1,
            data_source_name="Updated CSV Data",
            data_source_type=DataSourceType.XLSX,
            data_source_url="https://example.com/updated.xlsx",
            data_source_created_at=created_data_source.data_source_created_at,
            data_source_updated_at=datetime.now(timezone.utc)
        )
        
        # Mock repository
        mock_repo = AsyncMock()
        mock_get_repo.return_value = mock_repo
        
        # Mock service
        mock_service = AsyncMock()
        mock_get_service.return_value = mock_service
        
        # 1. CREATE DATA SOURCE
        mock_service.create_data_source = AsyncMock(return_value=created_data_source)
        mock_repo.get_data_source_by_id = AsyncMock(return_value=created_data_source)
        
        create_data = {
            "data_source_name": "Test CSV Data",
            "data_source_type": "CSV",
            "data_source_url": "https://example.com/data.csv"
        }
        
        create_response = test_client_e2e.post("/api/v1/data-sources/", json=create_data)
        assert create_response.status_code == 201
        create_result = create_response.json()
        assert create_result["data_source"]["data_source_name"] == "Test CSV Data"
        
        # 2. GET ALL USER DATA SOURCES
        mock_repo.get_user_data_sources = AsyncMock(return_value=[created_data_source])
        
        list_response = test_client_e2e.get("/api/v1/data-sources/")
        assert list_response.status_code == 200
        list_result = list_response.json()
        assert len(list_result["data_sources"]) == 1
        assert list_result["data_sources"][0]["data_source_name"] == "Test CSV Data"
        
        # 3. GET DATA SOURCE BY ID
        get_response = test_client_e2e.get("/api/v1/data-sources/1")
        assert get_response.status_code == 200
        get_result = get_response.json()
        assert get_result["data_source_name"] == "Test CSV Data"
        
        # 4. UPDATE DATA SOURCE
        mock_service.update_data_source = AsyncMock(return_value=updated_data_source)
        mock_repo.get_data_source_by_id.side_effect = [created_data_source, updated_data_source]
        
        update_data = {
            "data_source_name": "Updated CSV Data",
            "data_source_type": "XLSX",
            "data_source_url": "https://example.com/updated.xlsx"
        }
        
        update_response = test_client_e2e.put("/api/v1/data-sources/1", json=update_data)
        assert update_response.status_code == 200
        update_result = update_response.json()
        assert update_result["data_source"]["data_source_name"] == "Updated CSV Data"
        
        # 5. GET PAGINATED DATA SOURCES
        mock_repo.get_user_data_sources_paginated = AsyncMock(
            return_value=([updated_data_source], 1)
        )
        
        paginated_response = test_client_e2e.get(
            "/api/v1/data-sources/paginated?page=1&per_page=10"
        )
        assert paginated_response.status_code == 200
        paginated_result = paginated_response.json()
        assert len(paginated_result["data_sources"]) == 1
        assert paginated_result["pagination"]["total"] == 1
        
        # 6. DELETE DATA SOURCE
        mock_service.delete_data_source = AsyncMock(return_value="Data source deleted successfully")
        
        delete_response = test_client_e2e.delete("/api/v1/data-sources/1")
        assert delete_response.status_code == 200
        delete_result = delete_response.json()
        assert delete_result["message"] == "Data source deleted successfully"
    
    @patch('api.data_source.get_current_user')
    @patch('api.data_source.get_data_source_service')
    @patch('api.data_source.get_data_source_repo')
    def test_data_source_validation_workflow(
        self, mock_get_repo, mock_get_service, mock_get_user, test_client_e2e
    ):
        """Test data source validation workflow."""
        # Mock user
        mock_user = {"user_id": 1, "username": "testuser"}
        mock_get_user.return_value = mock_user
        
        # Mock repository and service
        mock_repo = AsyncMock()
        mock_get_repo.return_value = mock_repo
        mock_service = AsyncMock()
        mock_get_service.return_value = mock_service
        
        # Test invalid URL for PostgreSQL
        invalid_postgres_data = {
            "data_source_name": "Invalid Postgres",
            "data_source_type": "POSTGRES",
            "data_source_url": "invalid-url"
        }
        
        response = test_client_e2e.post("/api/v1/data-sources/", json=invalid_postgres_data)
        assert response.status_code == 422  # Validation error
        
        # Test empty name
        empty_name_data = {
            "data_source_name": "",
            "data_source_type": "CSV",
            "data_source_url": "https://example.com/data.csv"
        }
        
        response = test_client_e2e.post("/api/v1/data-sources/", json=empty_name_data)
        assert response.status_code == 422  # Validation error
        
        # Test valid MySQL DSN
        valid_mysql_data = {
            "data_source_name": "Valid MySQL",
            "data_source_type": "MYSQL",
            "data_source_url": "mysql://user:password@localhost:3306/testdb"
        }
        
        mock_data_source = DataSource(
            data_source_id=1,
            data_source_user_id=1,
            data_source_name="Valid MySQL",
            data_source_type=DataSourceType.MYSQL,
            data_source_url="mysql://user:password@localhost:3306/testdb",
            data_source_created_at=datetime.now(timezone.utc),
            data_source_updated_at=datetime.now(timezone.utc)
        )
        
        mock_service.create_data_source = AsyncMock(return_value=mock_data_source)
        mock_repo.get_data_source_by_id = AsyncMock(return_value=mock_data_source)
        
        response = test_client_e2e.post("/api/v1/data-sources/", json=valid_mysql_data)
        assert response.status_code == 201
        result = response.json()
        assert result["data_source"]["data_source_type"] == "MYSQL"
    
    @patch('api.data_source.get_current_user')
    @patch('api.data_source.get_data_source_service')
    @patch('api.data_source.get_data_source_repo')
    def test_authorization_workflow(
        self, mock_get_repo, mock_get_service, mock_get_user, test_client_e2e
    ):
        """Test authorization workflow for data sources."""
        # User 1 data source
        user1_data_source = DataSource(
            data_source_id=1,
            data_source_user_id=1,
            data_source_name="User 1 Data",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/user1.csv",
            data_source_created_at=datetime.now(timezone.utc),
            data_source_updated_at=datetime.now(timezone.utc)
        )
        
        # Mock repository
        mock_repo = AsyncMock()
        mock_repo.get_data_source_by_id = AsyncMock(return_value=user1_data_source)
        mock_get_repo.return_value = mock_repo
        
        # Mock service
        mock_service = AsyncMock()
        mock_get_service.return_value = mock_service
        
        # Test: User 2 trying to access User 1's data source
        mock_get_user.return_value = {"user_id": 2, "username": "user2"}
        
        # GET - should be forbidden
        response = test_client_e2e.get("/api/v1/data-sources/1")
        assert response.status_code == 403
        assert "Access denied" in response.json()["detail"]
        
        # PUT - should be forbidden
        update_data = {"data_source_name": "Hacked Name"}
        response = test_client_e2e.put("/api/v1/data-sources/1", json=update_data)
        assert response.status_code == 403
        assert "Access denied" in response.json()["detail"]
        
        # DELETE - should be forbidden
        response = test_client_e2e.delete("/api/v1/data-sources/1")
        assert response.status_code == 403
        assert "Access denied" in response.json()["detail"]
        
        # Test: User 1 accessing their own data source
        mock_get_user.return_value = {"user_id": 1, "username": "user1"}
        
        # GET - should be allowed
        response = test_client_e2e.get("/api/v1/data-sources/1")
        assert response.status_code == 200
        result = response.json()
        assert result["data_source_name"] == "User 1 Data"
    
    @patch('api.data_source.get_current_user')
    @patch('api.data_source.get_data_source_repo')
    def test_filtering_and_pagination_workflow(
        self, mock_get_repo, mock_get_user, test_client_e2e
    ):
        """Test filtering and pagination workflow."""
        # Mock user
        mock_user = {"user_id": 1, "username": "testuser"}
        mock_get_user.return_value = mock_user
        
        # Sample data sources
        csv_data_source = DataSource(
            data_source_id=1,
            data_source_user_id=1,
            data_source_name="CSV Data",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/data.csv",
            data_source_created_at=datetime.now(timezone.utc),
            data_source_updated_at=datetime.now(timezone.utc)
        )
        
        xlsx_data_source = DataSource(
            data_source_id=2,
            data_source_user_id=1,
            data_source_name="XLSX Data",
            data_source_type=DataSourceType.XLSX,
            data_source_url="https://example.com/data.xlsx",
            data_source_created_at=datetime.now(timezone.utc),
            data_source_updated_at=datetime.now(timezone.utc)
        )
        
        # Mock repository
        mock_repo = AsyncMock()
        mock_get_repo.return_value = mock_repo
        
        # Test: Filter by type
        mock_repo.get_user_data_sources = AsyncMock(return_value=[csv_data_source])
        
        response = test_client_e2e.get("/api/v1/data-sources/?data_source_type=CSV")
        assert response.status_code == 200
        result = response.json()
        assert len(result["data_sources"]) == 1
        assert result["data_sources"][0]["data_source_type"] == "CSV"
        
        # Test: Pagination
        mock_repo.get_user_data_sources_paginated = AsyncMock(
            return_value=([csv_data_source, xlsx_data_source], 2)
        )
        
        response = test_client_e2e.get(
            "/api/v1/data-sources/paginated?page=1&per_page=2&sort_by=data_source_name&sort_order=asc"
        )
        assert response.status_code == 200
        result = response.json()
        assert len(result["data_sources"]) == 2
        assert result["pagination"]["page"] == 1
        assert result["pagination"]["per_page"] == 2
        assert result["pagination"]["total"] == 2
        assert result["pagination"]["total_pages"] == 1
        assert result["pagination"]["has_next"] is False
        assert result["pagination"]["has_prev"] is False
        
        # Test: Search functionality
        mock_repo.get_user_data_sources_paginated = AsyncMock(
            return_value=([csv_data_source], 1)
        )
        
        response = test_client_e2e.get(
            "/api/v1/data-sources/paginated?search=CSV&page=1&per_page=10"
        )
        assert response.status_code == 200
        result = response.json()
        assert len(result["data_sources"]) == 1
        assert "CSV" in result["data_sources"][0]["data_source_name"]


