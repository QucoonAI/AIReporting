import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException
from fastapi.testclient import TestClient

from main import app
from core.dependencies import get_current_user, get_data_source_service, get_data_source_repo
from services.data_source import DataSourceService
from repositories.data_source import DataSourceRepository
from models.data_source import DataSource
from schemas.enum import DataSourceType


class TestDataSourceRoutes:
    """Integration tests for data source routes."""

    def setup_method(self):
        """Set up test dependencies."""
        self.mock_repo = AsyncMock(spec=DataSourceRepository)
        self.mock_service = AsyncMock(spec=DataSourceService)
        self.current_user = {
            "user_id": 1,
            "session_id": "test-session",
            "roles": ["user"],
            "session_data": {}
        }

    def override_dependencies(self):
        """Override FastAPI dependencies for testing."""
        app.dependency_overrides[get_current_user] = lambda: self.current_user
        app.dependency_overrides[get_data_source_service] = lambda: self.mock_service
        app.dependency_overrides[get_data_source_repo] = lambda: self.mock_repo

    def clear_dependencies(self):
        """Clear dependency overrides after test."""
        app.dependency_overrides.clear()

    def test_create_data_source_success(self, sample_data_source_create):
        """Test successful data source creation via API."""
        self.override_dependencies()
        
        try:
            # Arrange
            created_data_source = DataSource(
                data_source_id=1,
                data_source_user_id=1,
                data_source_name="Test CSV Source",
                data_source_type=DataSourceType.CSV,
                data_source_url="https://example.com/test.csv"
            )
            
            self.mock_service.create_data_source = AsyncMock(return_value=created_data_source)
            self.mock_repo.get_data_source_by_id = AsyncMock(return_value=created_data_source)

            # Act
            with TestClient(app) as client:
                response = client.post(
                    "/api/v1/data-sources/",
                    json=sample_data_source_create,
                    headers={"Authorization": "Bearer test-token"}
                )

            # Assert
            assert response.status_code == 201
            data = response.json()
            assert data["message"] == "Data source created successfully"
            assert data["data_source"]["data_source_name"] == "Test CSV Source"
            assert data["data_source"]["data_source_type"] == "csv"
            
            self.mock_service.create_data_source.assert_called_once()
            self.mock_repo.get_data_source_by_id.assert_called_once_with(1)
            
        finally:
            self.clear_dependencies()

    def test_create_data_source_validation_error(self, invalid_data_source_create):
        """Test data source creation with validation errors."""
        self.override_dependencies()
        
        try:
            # Act
            with TestClient(app) as client:
                response = client.post(
                    "/api/v1/data-sources/",
                    json=invalid_data_source_create,
                    headers={"Authorization": "Bearer test-token"}
                )

            # Assert
            assert response.status_code == 422  # Validation error
            
        finally:
            self.clear_dependencies()

    def test_create_data_source_duplicate_name(self, sample_data_source_create):
        """Test data source creation with duplicate name."""
        self.override_dependencies()
        
        try:
            # Arrange
            self.mock_service.create_data_source = AsyncMock(
                side_effect=HTTPException(
                    status_code=400,
                    detail="Data source with name 'Test CSV Source' already exists"
                )
            )

            # Act
            with TestClient(app) as client:
                response = client.post(
                    "/api/v1/data-sources/",
                    json=sample_data_source_create,
                    headers={"Authorization": "Bearer test-token"}
                )

            # Assert
            assert response.status_code == 400
            data = response.json()
            assert "already exists" in data["detail"]
            
        finally:
            self.clear_dependencies()

    def test_create_data_source_unauthorized(self, sample_data_source_create):
        """Test data source creation without authentication."""
        # No dependency override for authentication
        
        # Act
        with TestClient(app) as client:
            response = client.post(
                "/api/v1/data-sources/",
                json=sample_data_source_create
            )

        # Assert
        assert response.status_code == 403  # Forbidden (no auth header)

    def test_get_user_data_sources_success(self):
        """Test getting user's data sources."""
        self.override_dependencies()
        
        try:
            # Arrange
            mock_data_sources = [
                DataSource(
                    data_source_id=1,
                    data_source_user_id=1,
                    data_source_name="Source 1",
                    data_source_type=DataSourceType.CSV,
                    data_source_url="https://example.com/1.csv"
                ),
                DataSource(
                    data_source_id=2,
                    data_source_user_id=1,
                    data_source_name="Source 2",
                    data_source_type=DataSourceType.XLSX,
                    data_source_url="https://example.com/2.xlsx"
                )
            ]
            
            self.mock_repo.get_user_data_sources = AsyncMock(return_value=mock_data_sources)

            # Act
            with TestClient(app) as client:
                response = client.get(
                    "/api/v1/data-sources/",
                    headers={"Authorization": "Bearer test-token"}
                )

            # Assert
            assert response.status_code == 200
            data = response.json()
            assert data["message"] == "Data sources retrieved successfully"
            assert len(data["data_sources"]) == 2
            assert data["total_count"] == 2
            
            self.mock_repo.get_user_data_sources.assert_called_once_with(
                user_id=1,
                data_source_type=None
            )
            
        finally:
            self.clear_dependencies()

    def test_get_user_data_sources_with_filter(self):
        """Test getting user's data sources with type filter."""
        self.override_dependencies()
        
        try:
            # Arrange
            mock_data_sources = [
                DataSource(
                    data_source_id=1,
                    data_source_user_id=1,
                    data_source_name="CSV Source",
                    data_source_type=DataSourceType.CSV,
                    data_source_url="https://example.com/1.csv"
                )
            ]
            
            self.mock_repo.get_user_data_sources = AsyncMock(return_value=mock_data_sources)

            # Act
            with TestClient(app) as client:
                response = client.get(
                    "/api/v1/data-sources/",
                    params={"data_source_type": "csv"},
                    headers={"Authorization": "Bearer test-token"}
                )

            # Assert
            assert response.status_code == 200
            data = response.json()
            assert len(data["data_sources"]) == 1
            assert data["data_sources"][0]["data_source_type"] == "csv"
            
            self.mock_repo.get_user_data_sources.assert_called_once_with(
                user_id=1,
                data_source_type=DataSourceType.CSV
            )
            
        finally:
            self.clear_dependencies()

    def test_get_user_data_sources_paginated(self):
        """Test paginated data sources retrieval."""
        self.override_dependencies()
        
        try:
            # Arrange
            mock_data_sources = [
                DataSource(
                    data_source_id=1,
                    data_source_user_id=1,
                    data_source_name="Source 1",
                    data_source_type=DataSourceType.CSV,
                    data_source_url="https://example.com/1.csv"
                )
            ]
            
            self.mock_repo.get_user_data_sources_paginated = AsyncMock(
                return_value=(mock_data_sources, 1)
            )

            # Act
            with TestClient(app) as client:
                response = client.get(
                    "/api/v1/data-sources/paginated",
                    params={"page": 1, "per_page": 10, "sort_by": "data_source_name", "sort_order": "asc"},
                    headers={"Authorization": "Bearer test-token"}
                )

            # Assert
            assert response.status_code == 200
            data = response.json()
            assert data["message"] == "Data sources retrieved successfully"
            assert len(data["data_sources"]) == 1
            assert data["pagination"]["page"] == 1
            assert data["pagination"]["total"] == 1
            assert data["pagination"]["total_pages"] == 1
            assert data["pagination"]["has_next"] is False
            assert data["pagination"]["has_prev"] is False
            
        finally:
            self.clear_dependencies()

    def test_get_data_source_by_id_success(self):
        """Test getting specific data source by ID."""
        self.override_dependencies()
        
        try:
            # Arrange
            mock_data_source = DataSource(
                data_source_id=1,
                data_source_user_id=1,
                data_source_name="Test Source",
                data_source_type=DataSourceType.CSV,
                data_source_url="https://example.com/test.csv"
            )
            
            self.mock_repo.get_data_source_by_id = AsyncMock(return_value=mock_data_source)

            # Act
            with TestClient(app) as client:
                response = client.get(
                    "/api/v1/data-sources/1",
                    headers={"Authorization": "Bearer test-token"}
                )

            # Assert
            assert response.status_code == 200
            data = response.json()
            assert data["data_source_id"] == 1
            assert data["data_source_name"] == "Test Source"
            
        finally:
            self.clear_dependencies()

    def test_get_data_source_by_id_not_found(self):
        """Test getting non-existent data source."""
        self.override_dependencies()
        
        try:
            # Arrange
            self.mock_repo.get_data_source_by_id = AsyncMock(return_value=None)

            # Act
            with TestClient(app) as client:
                response = client.get(
                    "/api/v1/data-sources/999",
                    headers={"Authorization": "Bearer test-token"}
                )

            # Assert
            assert response.status_code == 404
            data = response.json()
            assert "not found" in data["detail"]
            
        finally:
            self.clear_dependencies()

    def test_get_data_source_by_id_forbidden(self):
        """Test getting data source owned by different user."""
        self.override_dependencies()
        
        try:
            # Arrange
            other_user_data_source = DataSource(
                data_source_id=1,
                data_source_user_id=2,  # Different user
                data_source_name="Other User Source",
                data_source_type=DataSourceType.CSV,
                data_source_url="https://example.com/other.csv"
            )
            
            self.mock_repo.get_data_source_by_id = AsyncMock(return_value=other_user_data_source)

            # Act
            with TestClient(app) as client:
                response = client.get(
                    "/api/v1/data-sources/1",
                    headers={"Authorization": "Bearer test-token"}
                )

            # Assert
            assert response.status_code == 403
            data = response.json()
            assert "Access denied" in data["detail"]
            
        finally:
            self.clear_dependencies()

    def test_update_data_source_success(self, sample_data_source_update):
        """Test successful data source update."""
        self.override_dependencies()
        
        try:
            # Arrange
            existing_data_source = DataSource(
                data_source_id=1,
                data_source_user_id=1,
                data_source_name="Old Name",
                data_source_type=DataSourceType.CSV,
                data_source_url="https://example.com/old.csv"
            )
            
            updated_data_source = DataSource(
                data_source_id=1,
                data_source_user_id=1,
                data_source_name="Updated CSV Source",
                data_source_type=DataSourceType.XLSX,
                data_source_url="https://example.com/updated.xlsx"
            )
            
            self.mock_repo.get_data_source_by_id = AsyncMock(side_effect=[existing_data_source, updated_data_source])
            self.mock_service.update_data_source = AsyncMock(return_value=updated_data_source)

            # Act
            with TestClient(app) as client:
                response = client.put(
                    "/api/v1/data-sources/1",
                    json=sample_data_source_update,
                    headers={"Authorization": "Bearer test-token"}
                )

            # Assert
            assert response.status_code == 200
            data = response.json()
            assert data["message"] == "Data source updated successfully"
            assert data["data_source"]["data_source_name"] == "Updated CSV Source"
            
        finally:
            self.clear_dependencies()

    def test_update_data_source_forbidden(self, sample_data_source_update):
        """Test updating data source owned by different user."""
        self.override_dependencies()
        
        try:
            # Arrange
            other_user_data_source = DataSource(
                data_source_id=1,
                data_source_user_id=2,  # Different user
                data_source_name="Other User Source",
                data_source_type=DataSourceType.CSV,
                data_source_url="https://example.com/other.csv"
            )
            
            self.mock_repo.get_data_source_by_id = AsyncMock(return_value=other_user_data_source)

            # Act
            with TestClient(app) as client:
                response = client.put(
                    "/api/v1/data-sources/1",
                    json=sample_data_source_update,
                    headers={"Authorization": "Bearer test-token"}
                )

            # Assert
            assert response.status_code == 403
            data = response.json()
            assert "Access denied" in data["detail"]
            
        finally:
            self.clear_dependencies()

    def test_delete_data_source_success(self):
        """Test successful data source deletion."""
        self.override_dependencies()
        
        try:
            # Arrange
            existing_data_source = DataSource(
                data_source_id=1,
                data_source_user_id=1,
                data_source_name="To Delete",
                data_source_type=DataSourceType.CSV,
                data_source_url="https://example.com/delete.csv"
            )
            
            self.mock_repo.get_data_source_by_id = AsyncMock(return_value=existing_data_source)
            self.mock_service.delete_data_source = AsyncMock(return_value="Data source deleted successfully")

            # Act
            with TestClient(app) as client:
                response = client.delete(
                    "/api/v1/data-sources/1",
                    headers={"Authorization": "Bearer test-token"}
                )

            # Assert
            assert response.status_code == 200
            data = response.json()
            assert data["message"] == "Data source deleted successfully"
            
        finally:
            self.clear_dependencies()

    def test_delete_data_source_forbidden(self):
        """Test deleting data source owned by different user."""
        self.override_dependencies()
        
        try:
            # Arrange
            other_user_data_source = DataSource(
                data_source_id=1,
                data_source_user_id=2,  # Different user
                data_source_name="Other User Source",
                data_source_type=DataSourceType.CSV,
                data_source_url="https://example.com/other.csv"
            )
            
            self.mock_repo.get_data_source_by_id = AsyncMock(return_value=other_user_data_source)

            # Act
            with TestClient(app) as client:
                response = client.delete(
                    "/api/v1/data-sources/1",
                    headers={"Authorization": "Bearer test-token"}
                )

            # Assert
            assert response.status_code == 403
            data = response.json()
            assert "Access denied" in data["detail"]
            
        finally:
            self.clear_dependencies()

    def test_route_internal_server_error(self, sample_data_source_create):
        """Test handling of internal server errors."""
        self.override_dependencies()
        
        try:
            # Arrange
            self.mock_service.create_data_source = AsyncMock(side_effect=Exception("Database connection failed"))

            # Act
            with TestClient(app) as client:
                response = client.post(
                    "/api/v1/data-sources/",
                    json=sample_data_source_create,
                    headers={"Authorization": "Bearer test-token"}
                )

            # Assert
            assert response.status_code == 500
            data = response.json()
            assert "An error occurred" in data["detail"]
            
        finally:
            self.clear_dependencies()


class TestDataSourceRoutesEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_invalid_pagination_parameters(self):
        """Test pagination with invalid parameters."""
        app.dependency_overrides[get_current_user] = lambda: {
            "user_id": 1,
            "session_id": "test-session",
            "roles": ["user"],
            "session_data": {}
        }
        
        mock_repo = AsyncMock(spec=DataSourceRepository)
        mock_repo.get_user_data_sources_paginated = AsyncMock(return_value=([], 0))
        app.dependency_overrides[get_data_source_repo] = lambda: mock_repo
        
        try:
            # Act
            with TestClient(app) as client:
                response = client.get(
                    "/api/v1/data-sources/paginated",
                    params={"page": -1, "per_page": 0},  # Invalid parameters
                    headers={"Authorization": "Bearer test-token"}
                )

            # Assert
            assert response.status_code == 200  # Repository handles validation
            # Repository should have corrected invalid parameters
            mock_repo.get_user_data_sources_paginated.assert_called_once()
            args = mock_repo.get_user_data_sources_paginated.call_args
            assert args.kwargs["page"] >= 1  # Should be corrected
            assert args.kwargs["per_page"] >= 1  # Should be corrected
            
        finally:
            app.dependency_overrides.clear()

    def test_large_per_page_limit(self):
        """Test pagination with per_page exceeding limit."""
        app.dependency_overrides[get_current_user] = lambda: {
            "user_id": 1,
            "session_id": "test-session", 
            "roles": ["user"],
            "session_data": {}
        }
        
        mock_repo = AsyncMock(spec=DataSourceRepository)
        mock_repo.get_user_data_sources_paginated = AsyncMock(return_value=([], 0))
        app.dependency_overrides[get_data_source_repo] = lambda: mock_repo
        
        try:
            # Act
            with TestClient(app) as client:
                response = client.get(
                    "/api/v1/data-sources/paginated",
                    params={"per_page": 1000},  # Exceeds limit
                    headers={"Authorization": "Bearer test-token"}
                )

            # Assert
            assert response.status_code == 200
            # Repository should limit per_page to maximum allowed
            args = mock_repo.get_user_data_sources_paginated.call_args
            assert args.kwargs["per_page"] <= 100  # Should be limited
            
        finally:
            app.dependency_overrides.clear()

    def test_invalid_sort_order(self):
        """Test sorting with invalid sort order."""
        # Act
        with TestClient(app) as client:
            response = client.get(
                "/api/v1/data-sources/paginated",
                params={"sort_order": "invalid"},  # Invalid sort order
                headers={"Authorization": "Bearer test-token"}
            )

        # Assert
        assert response.status_code == 422  # Validation error