import pytest
from unittest.mock import AsyncMock, patch
from fastapi import status
from fastapi.testclient import TestClient
from fastapi import FastAPI
from app.routes.data_source import router
from app.schemas.enum import DataSourceType
from app.models.data_source import DataSource


@pytest.fixture
def test_app():
    """Create test FastAPI application."""
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.fixture
def test_client(test_app):
    """Create test client."""
    return TestClient(test_app)


class TestDataSourceAPI:
    """Test cases for DataSource API endpoints."""
    
    @patch('api.data_source.get_current_user')
    @patch('api.data_source.get_data_source_service')
    @patch('api.data_source.get_data_source_repo')
    def test_create_data_source_success(
        self, mock_get_repo, mock_get_service, mock_get_user, test_client, 
        mock_current_user, sample_data_source
    ):
        """Test successful data source creation via API."""
        # Setup mocks
        mock_get_user.return_value = mock_current_user
        mock_service = AsyncMock()
        mock_service.create_data_source = AsyncMock(return_value=sample_data_source)
        mock_get_service.return_value = mock_service
        
        mock_repo = AsyncMock()
        mock_repo.get_data_source_by_id = AsyncMock(return_value=sample_data_source)
        mock_get_repo.return_value = mock_repo
        
        # Request data
        request_data = {
            "data_source_name": "Test Data Source",
            "data_source_type": "CSV",
            "data_source_url": "https://example.com/data.csv"
        }
        
        # Execute
        response = test_client.post("/api/v1/data-sources/", json=request_data)
        
        # Assert
        assert response.status_code == status.HTTP_201_CREATED
        response_data = response.json()
        assert response_data["message"] == "Data source created successfully"
        assert response_data["data_source"]["data_source_name"] == "Test Data Source"
    
    @patch('api.data_source.get_current_user')
    @patch('api.data_source.get_data_source_repo')
    def test_get_user_data_sources_success(
        self, mock_get_repo, mock_get_user, test_client, 
        mock_current_user, sample_data_source
    ):
        """Test successful retrieval of user data sources via API."""
        # Setup mocks
        mock_get_user.return_value = mock_current_user
        mock_repo = AsyncMock()
        mock_repo.get_user_data_sources = AsyncMock(return_value=[sample_data_source])
        mock_get_repo.return_value = mock_repo
        
        # Execute
        response = test_client.get("/api/v1/data-sources/")
        
        # Assert
        assert response.status_code == status.HTTP_200_OK
        response_data = response.json()
        assert response_data["message"] == "Data sources retrieved successfully"
        assert len(response_data["data_sources"]) == 1
        assert response_data["total_count"] == 1
    
    @patch('api.data_source.get_current_user')
    @patch('api.data_source.get_data_source_repo')
    def test_get_user_data_sources_with_filter(
        self, mock_get_repo, mock_get_user, test_client, 
        mock_current_user, sample_data_source
    ):
        """Test retrieval of user data sources with type filter via API."""
        # Setup mocks
        mock_get_user.return_value = mock_current_user
        mock_repo = AsyncMock()
        mock_repo.get_user_data_sources = AsyncMock(return_value=[sample_data_source])
        mock_get_repo.return_value = mock_repo
        
        # Execute
        response = test_client.get("/api/v1/data-sources/?data_source_type=CSV")
        
        # Assert
        assert response.status_code == status.HTTP_200_OK
        response_data = response.json()
        assert len(response_data["data_sources"]) == 1
    
    @patch('api.data_source.get_current_user')
    @patch('api.data_source.get_data_source_repo')
    def test_get_data_source_by_id_success(
        self, mock_get_repo, mock_get_user, test_client, 
        mock_current_user, sample_data_source
    ):
        """Test successful retrieval of data source by ID via API."""
        # Setup mocks
        mock_get_user.return_value = mock_current_user
        mock_repo = AsyncMock()
        mock_repo.get_data_source_by_id = AsyncMock(return_value=sample_data_source)
        mock_get_repo.return_value = mock_repo
        
        # Execute
        response = test_client.get("/api/v1/data-sources/1")
        
        # Assert
        assert response.status_code == status.HTTP_200_OK
        response_data = response.json()
        assert response_data["data_source_name"] == "Test Data Source"
    
    @patch('api.data_source.get_current_user')
    @patch('api.data_source.get_data_source_repo')
    def test_get_data_source_by_id_not_found(
        self, mock_get_repo, mock_get_user, test_client, mock_current_user
    ):
        """Test retrieval of non-existent data source via API."""
        # Setup mocks
        mock_get_user.return_value = mock_current_user
        mock_repo = AsyncMock()
        mock_repo.get_data_source_by_id = AsyncMock(return_value=None)
        mock_get_repo.return_value = mock_repo
        
        # Execute
        response = test_client.get("/api/v1/data-sources/999")
        
        # Assert
        assert response.status_code == status.HTTP_404_NOT_FOUND
        response_data = response.json()
        assert response_data["detail"] == "Data source not found"
    
    @patch('api.data_source.get_current_user')
    @patch('api.data_source.get_data_source_repo')
    def test_get_data_source_by_id_forbidden(
        self, mock_get_repo, mock_get_user, test_client, sample_data_source
    ):
        """Test retrieval of data source belonging to another user via API."""
        # Setup mocks - different user ID
        mock_get_user.return_value = {"user_id": 2, "username": "otheruser"}
        mock_repo = AsyncMock()
        mock_repo.get_data_source_by_id = AsyncMock(return_value=sample_data_source)
        mock_get_repo.return_value = mock_repo
        
        # Execute
        response = test_client.get("/api/v1/data-sources/1")
        
        # Assert
        assert response.status_code == status.HTTP_403_FORBIDDEN
        response_data = response.json()
        assert "Access denied" in response_data["detail"]
    
    @patch('api.data_source.get_current_user')
    @patch('api.data_source.get_data_source_service')
    @patch('api.data_source.get_data_source_repo')
    def test_update_data_source_success(
        self, mock_get_repo, mock_get_service, mock_get_user, test_client, 
        mock_current_user, sample_data_source
    ):
        """Test successful data source update via API."""
        # Setup mocks
        mock_get_user.return_value = mock_current_user
        
        updated_data_source = DataSource(
            data_source_id=1,
            data_source_user_id=1,
            data_source_name="Updated Data Source",
            data_source_type=DataSourceType.XLSX,
            data_source_url="https://example.com/updated.xlsx",
            data_source_created_at=sample_data_source.data_source_created_at,
            data_source_updated_at=sample_data_source.data_source_updated_at
        )
        
        mock_repo = AsyncMock()
        mock_repo.get_data_source_by_id.side_effect = [sample_data_source, updated_data_source]
        mock_get_repo.return_value = mock_repo
        
        mock_service = AsyncMock()
        mock_service.update_data_source = AsyncMock(return_value=updated_data_source)
        mock_get_service.return_value = mock_service
        
        # Request data
        request_data = {
            "data_source_name": "Updated Data Source",
            "data_source_type": "XLSX",
            "data_source_url": "https://example.com/updated.xlsx"
        }
        
        # Execute
        response = test_client.put("/api/v1/data-sources/1", json=request_data)
        
        # Assert
        assert response.status_code == status.HTTP_200_OK
        response_data = response.json()
        assert response_data["message"] == "Data source updated successfully"
        assert response_data["data_source"]["data_source_name"] == "Updated Data Source"
    
    @patch('api.data_source.get_current_user')
    @patch('api.data_source.get_data_source_service')
    @patch('api.data_source.get_data_source_repo')
    def test_delete_data_source_success(
        self, mock_get_repo, mock_get_service, mock_get_user, test_client, 
        mock_current_user, sample_data_source
    ):
        """Test successful data source deletion via API."""
        # Setup mocks
        mock_get_user.return_value = mock_current_user
        
        mock_repo = AsyncMock()
        mock_repo.get_data_source_by_id = AsyncMock(return_value=sample_data_source)
        mock_get_repo.return_value = mock_repo
        
        mock_service = AsyncMock()
        mock_service.delete_data_source = AsyncMock(return_value="Data source deleted successfully")
        mock_get_service.return_value = mock_service
        
        # Execute
        response = test_client.delete("/api/v1/data-sources/1")
        
        # Assert
        assert response.status_code == status.HTTP_200_OK
        response_data = response.json()
        assert response_data["message"] == "Data source deleted successfully"
    
    @patch('api.data_source.get_current_user')
    @patch('api.data_source.get_data_source_repo')
    def test_get_paginated_data_sources_success(
        self, mock_get_repo, mock_get_user, test_client, 
        mock_current_user, sample_data_source
    ):
        """Test successful retrieval of paginated data sources via API."""
        # Setup mocks
        mock_get_user.return_value = mock_current_user
        mock_repo = AsyncMock()
        mock_repo.get_user_data_sources_paginated = AsyncMock(
            return_value=([sample_data_source], 1)
        )
        mock_get_repo.return_value = mock_repo
        
        # Execute
        response = test_client.get("/api/v1/data-sources/paginated?page=1&per_page=10")
        
        # Assert
        assert response.status_code == status.HTTP_200_OK
        response_data = response.json()
        assert response_data["message"] == "Data sources retrieved successfully"
        assert len(response_data["data_sources"]) == 1
        assert response_data["pagination"]["page"] == 1
        assert response_data["pagination"]["total"] == 1