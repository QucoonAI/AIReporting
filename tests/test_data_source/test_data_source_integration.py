import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch

from main import app
from config.database import get_session
from config.redis import redis_manager
from core.dependencies import get_current_user
from models.data_source import DataSource
from models.user import User, UserProfile
from schemas.enum import DataSourceType


@pytest.mark.integration
class TestDataSourceIntegration:
    """Full integration tests for data source module."""

    @pytest_asyncio.fixture
    async def test_db_session(self, engine):
        """Create test database session with real data."""
        from sqlmodel import Session, SQLModel
        
        # Create tables
        SQLModel.metadata.create_all(engine)
        
        session = Session(engine)
        
        # Create test user
        test_user = User(
            user_id=1,
            user_email="test@example.com",
            user_first_name="Test",
            user_last_name="User",
            user_password="hashed_password",
            user_is_verified=True,
            user_is_active=True
        )
        session.add(test_user)
        
        # Create test user profile
        test_profile = UserProfile(
            user_profile_id=1,
            user_profile_user_id=1,
            user_profile_bio="Test bio"
        )
        session.add(test_profile)
        
        session.commit()
        yield session
        session.close()

    @pytest_asyncio.fixture
    async def authenticated_integration_client(self, test_db_session):
        """Create authenticated client with real database session."""
        
        def override_get_session():
            return test_db_session
        
        def override_get_current_user():
            return {
                "user_id": 1,
                "session_id": "test-session",
                "roles": ["user"],
                "session_data": {}
            }
        
        # Mock Redis operations
        with patch.object(redis_manager, 'get_client') as mock_redis:
            mock_redis_client = AsyncMock()
            mock_redis.return_value = mock_redis_client
            
            app.dependency_overrides[get_session] = override_get_session
            app.dependency_overrides[get_current_user] = override_get_current_user
            
            with TestClient(app) as client:
                yield client, test_db_session
            
            app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_full_data_source_lifecycle(self, authenticated_integration_client):
        """Test complete data source CRUD lifecycle."""
        client, session = authenticated_integration_client
        
        # 1. Create data source
        create_data = {
            "data_source_name": "Integration Test CSV",
            "data_source_type": "csv",
            "data_source_url": "https://example.com/integration-test.csv"
        }
        
        create_response = client.post(
            "/api/v1/data-sources/",
            json=create_data,
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert create_response.status_code == 201
        created_data = create_response.json()
        assert created_data["message"] == "Data source created successfully"
        data_source_id = created_data["data_source"]["data_source_id"]
        
        # Verify in database
        db_data_source = session.get(DataSource, data_source_id)
        assert db_data_source is not None
        assert db_data_source.data_source_name == "Integration Test CSV"
        assert db_data_source.data_source_user_id == 1
        
        # 2. Get data source by ID
        get_response = client.get(
            f"/api/v1/data-sources/{data_source_id}",
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert get_response.status_code == 200
        get_data = get_response.json()
        assert get_data["data_source_name"] == "Integration Test CSV"
        assert get_data["data_source_type"] == "csv"
        
        # 3. Update data source
        update_data = {
            "data_source_name": "Updated Integration Test",
            "data_source_type": "xlsx",
            "data_source_url": "https://example.com/updated-integration-test.xlsx"
        }
        
        update_response = client.put(
            f"/api/v1/data-sources/{data_source_id}",
            json=update_data,
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert update_response.status_code == 200
        updated_data = update_response.json()
        assert updated_data["message"] == "Data source updated successfully"
        assert updated_data["data_source"]["data_source_name"] == "Updated Integration Test"
        
        # Verify update in database
        session.refresh(db_data_source)
        assert db_data_source.data_source_name == "Updated Integration Test"
        assert db_data_source.data_source_type == DataSourceType.XLSX
        
        # 4. List user data sources
        list_response = client.get(
            "/api/v1/data-sources/",
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert list_response.status_code == 200
        list_data = list_response.json()
        assert list_data["total_count"] == 1
        assert len(list_data["data_sources"]) == 1
        assert list_data["data_sources"][0]["data_source_name"] == "Updated Integration Test"
        
        # 5. Paginated list
        paginated_response = client.get(
            "/api/v1/data-sources/paginated",
            params={"page": 1, "per_page": 10},
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert paginated_response.status_code == 200
        paginated_data = paginated_response.json()
        assert paginated_data["pagination"]["total"] == 1
        assert paginated_data["pagination"]["total_pages"] == 1
        assert len(paginated_data["data_sources"]) == 1
        
        # 6. Delete data source
        delete_response = client.delete(
            f"/api/v1/data-sources/{data_source_id}",
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert delete_response.status_code == 200
        delete_data = delete_response.json()
        assert delete_data["message"] == "Data source deleted successfully"
        
        # Verify deletion in database
        deleted_data_source = session.get(DataSource, data_source_id)
        assert deleted_data_source is None
        
        # 7. Verify empty list after deletion
        final_list_response = client.get(
            "/api/v1/data-sources/",
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert final_list_response.status_code == 200
        final_list_data = final_list_response.json()
        assert final_list_data["total_count"] == 0
        assert len(final_list_data["data_sources"]) == 0

    @pytest.mark.asyncio
    async def test_data_source_user_isolation(self, test_db_session):
        """Test that users can only access their own data sources."""
        
        # Create second user
        user2 = User(
            user_id=2,
            user_email="user2@example.com",
            user_first_name="User",
            user_last_name="Two",
            user_password="hashed_password",
            user_is_verified=True,
            user_is_active=True
        )
        test_db_session.add(user2)
        test_db_session.commit()
        
        # Create data source for user 2
        user2_data_source = DataSource(
            data_source_user_id=2,
            data_source_name="User 2 Source",
            data_source_type=DataSourceType.CSV,
            data_source_url="https://example.com/user2.csv"
        )
        test_db_session.add(user2_data_source)
        test_db_session.commit()
        
        def override_get_session():
            return test_db_session
        
        def override_get_current_user():
            return {
                "user_id": 1,  # User 1 trying to access User 2's data
                "session_id": "test-session",
                "roles": ["user"],
                "session_data": {}
            }
        
        with patch.object(redis_manager, 'get_client') as mock_redis:
            mock_redis_client = AsyncMock()
            mock_redis.return_value = mock_redis_client
            
            app.dependency_overrides[get_session] = override_get_session
            app.dependency_overrides[get_current_user] = override_get_current_user
            
            try:
                with TestClient(app) as client:
                    # Try to access User 2's data source as User 1
                    response = client.get(
                        f"/api/v1/data-sources/{user2_data_source.data_source_id}",
                        headers={"Authorization": "Bearer test-token"}
                    )
                    
                    assert response.status_code == 403
                    data = response.json()
                    assert "Access denied" in data["detail"]
                    
            finally:
                app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_data_source_name_uniqueness_per_user(self, authenticated_integration_client):
        """Test that data source names must be unique per user."""
        client, session = authenticated_integration_client
        
        # Create first data source
        create_data = {
            "data_source_name": "Unique Test Source",
            "data_source_type": "csv",
            "data_source_url": "https://example.com/unique-test.csv"
        }
        
        first_response = client.post(
            "/api/v1/data-sources/",
            json=create_data,
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert first_response.status_code == 201
        
        # Try to create second data source with same name
        second_response = client.post(
            "/api/v1/data-sources/",
            json=create_data,  # Same data
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert second_response.status_code == 400
        error_data = second_response.json()
        assert "already exists" in error_data["detail"]

    @pytest.mark.asyncio
    async def test_data_source_filtering_and_search(self, authenticated_integration_client):
        """Test data source filtering and search functionality."""
        client, session = authenticated_integration_client
        
        # Create multiple data sources with different types
        test_sources = [
            {
                "data_source_name": "CSV Test Source",
                "data_source_type": "csv",
                "data_source_url": "https://example.com/csv-test.csv"
            },
            {
                "data_source_name": "Excel Test Source",
                "data_source_type": "xlsx",
                "data_source_url": "https://example.com/excel-test.xlsx"
            },
            {
                "data_source_name": "PostgreSQL Database",
                "data_source_type": "postgres",
                "data_source_url": "postgresql://user:pass@localhost:5432/testdb"
            }
        ]
        
        created_ids = []
        for source_data in test_sources:
            response = client.post(
                "/api/v1/data-sources/",
                json=source_data,
                headers={"Authorization": "Bearer test-token"}
            )
            assert response.status_code == 201
            created_ids.append(response.json()["data_source"]["data_source_id"])
        
        # Test filtering by type
        csv_filter_response = client.get(
            "/api/v1/data-sources/",
            params={"data_source_type": "csv"},
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert csv_filter_response.status_code == 200
        csv_data = csv_filter_response.json()
        assert csv_data["total_count"] == 1
        assert csv_data["data_sources"][0]["data_source_type"] == "csv"
        
        # Test search functionality
        search_response = client.get(
            "/api/v1/data-sources/paginated",
            params={"search": "Test"},
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert search_response.status_code == 200
        search_data = search_response.json()
        assert search_data["pagination"]["total"] == 2  # CSV and Excel contain "Test"
        
        # Test search with specific term
        postgres_search_response = client.get(
            "/api/v1/data-sources/paginated",
            params={"search": "PostgreSQL"},
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert postgres_search_response.status_code == 200
        postgres_data = postgres_search_response.json()
        assert postgres_data["pagination"]["total"] == 1
        assert postgres_data["data_sources"][0]["data_source_type"] == "postgres"

    @pytest.mark.asyncio
    async def test_data_source_pagination_edge_cases(self, authenticated_integration_client):
        """Test pagination edge cases and limits."""
        client, session = authenticated_integration_client
        
        # Create multiple data sources
        for i in range(15):
            source_data = {
                "data_source_name": f"Test Source {i+1:02d}",
                "data_source_type": "csv",
                "data_source_url": f"https://example.com/test-{i+1:02d}.csv"
            }
            
            response = client.post(
                "/api/v1/data-sources/",
                json=source_data,
                headers={"Authorization": "Bearer test-token"}
            )
            assert response.status_code == 201
        
        # Test first page with limit
        page1_response = client.get(
            "/api/v1/data-sources/paginated",
            params={"page": 1, "per_page": 10},
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert page1_response.status_code == 200
        page1_data = page1_response.json()
        assert len(page1_data["data_sources"]) == 10
        assert page1_data["pagination"]["total"] == 15
        assert page1_data["pagination"]["total_pages"] == 2
        assert page1_data["pagination"]["has_next"] is True
        assert page1_data["pagination"]["has_prev"] is False
        
        # Test second page
        page2_response = client.get(
            "/api/v1/data-sources/paginated",
            params={"page": 2, "per_page": 10},
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert page2_response.status_code == 200
        page2_data = page2_response.json()
        assert len(page2_data["data_sources"]) == 5  # Remaining items
        assert page2_data["pagination"]["has_next"] is False
        assert page2_data["pagination"]["has_prev"] is True
        
        # Test sorting
        sorted_response = client.get(
            "/api/v1/data-sources/paginated",
            params={"sort_by": "data_source_name", "sort_order": "asc", "per_page": 5},
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert sorted_response.status_code == 200
        sorted_data = sorted_response.json()
        names = [ds["data_source_name"] for ds in sorted_data["data_sources"]]
        assert names == sorted(names)  # Should be in ascending order

    @pytest.mark.asyncio
    async def test_data_source_validation_integration(self, authenticated_integration_client):
        """Test validation scenarios in integration environment."""
        client, session = authenticated_integration_client
        
        # Test invalid data source type
        invalid_type_data = {
            "data_source_name": "Invalid Type Source",
            "data_source_type": "invalid_type",
            "data_source_url": "https://example.com/test.csv"
        }
        
        response = client.post(
            "/api/v1/data-sources/",
            json=invalid_type_data,
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert response.status_code == 422  # Validation error
        
        # Test empty name
        empty_name_data = {
            "data_source_name": "",
            "data_source_type": "csv",
            "data_source_url": "https://example.com/test.csv"
        }
        
        response = client.post(
            "/api/v1/data-sources/",
            json=empty_name_data,
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert response.status_code == 422  # Validation error
        
        # Test invalid URL for type
        invalid_url_data = {
            "data_source_name": "Invalid URL Source",
            "data_source_type": "postgres",
            "data_source_url": "not-a-postgres-url"
        }
        
        response = client.post(
            "/api/v1/data-sources/",
            json=invalid_url_data,
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert response.status_code == 422  # Validation error

    @pytest.mark.asyncio
    async def test_data_source_concurrent_operations(self, test_db_session):
        """Test concurrent operations on data sources."""
        
        def override_get_session():
            return test_db_session
        
        def override_get_current_user():
            return {
                "user_id": 1,
                "session_id": "test-session",
                "roles": ["user"],
                "session_data": {}
            }
        
        with patch.object(redis_manager, 'get_client') as mock_redis:
            mock_redis_client = AsyncMock()
            mock_redis.return_value = mock_redis_client
            
            app.dependency_overrides[get_session] = override_get_session
            app.dependency_overrides[get_current_user] = override_get_current_user
            
            try:
                with TestClient(app) as client:
                    # Create a data source
                    create_data = {
                        "data_source_name": "Concurrent Test Source",
                        "data_source_type": "csv",
                        "data_source_url": "https://example.com/concurrent-test.csv"
                    }
                    
                    create_response = client.post(
                        "/api/v1/data-sources/",
                        json=create_data,
                        headers={"Authorization": "Bearer test-token"}
                    )
                    
                    assert create_response.status_code == 201
                    data_source_id = create_response.json()["data_source"]["data_source_id"]
                    
                    # Simulate concurrent updates
                    update_data_1 = {
                        "data_source_name": "Updated by Client 1",
                        "data_source_url": "https://example.com/updated-by-client-1.csv"
                    }
                    
                    update_data_2 = {
                        "data_source_name": "Updated by Client 2", 
                        "data_source_url": "https://example.com/updated-by-client-2.csv"
                    }
                    
                    # Both updates should work (last one wins)
                    update_response_1 = client.put(
                        f"/api/v1/data-sources/{data_source_id}",
                        json=update_data_1,
                        headers={"Authorization": "Bearer test-token"}
                    )
                    
                    update_response_2 = client.put(
                        f"/api/v1/data-sources/{data_source_id}",
                        json=update_data_2,
                        headers={"Authorization": "Bearer test-token"}
                    )
                    
                    assert update_response_1.status_code == 200
                    assert update_response_2.status_code == 200
                    
                    # Verify final state
                    final_response = client.get(
                        f"/api/v1/data-sources/{data_source_id}",
                        headers={"Authorization": "Bearer test-token"}
                    )
                    
                    assert final_response.status_code == 200
                    final_data = final_response.json()
                    assert final_data["data_source_name"] == "Updated by Client 2"
                    
            finally:
                app.dependency_overrides.clear()

    @pytest.mark.asyncio 
    async def test_data_source_error_recovery(self, authenticated_integration_client):
        """Test error recovery and rollback scenarios."""
        client, session = authenticated_integration_client
        
        # Create a valid data source first
        valid_data = {
            "data_source_name": "Valid Test Source",
            "data_source_type": "csv", 
            "data_source_url": "https://example.com/valid-test.csv"
        }
        
        create_response = client.post(
            "/api/v1/data-sources/",
            json=valid_data,
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert create_response.status_code == 201
        data_source_id = create_response.json()["data_source"]["data_source_id"]
        
        # Attempt to update with conflicting name (create another source first)
        another_valid_data = {
            "data_source_name": "Another Valid Source",
            "data_source_type": "xlsx",
            "data_source_url": "https://example.com/another-valid.xlsx"
        }
        
        another_create_response = client.post(
            "/api/v1/data-sources/",
            json=another_valid_data,
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert another_create_response.status_code == 201
        
        # Try to update first source to have same name as second
        conflict_update_data = {
            "data_source_name": "Another Valid Source"  # This should conflict
        }
        
        conflict_response = client.put(
            f"/api/v1/data-sources/{data_source_id}",
            json=conflict_update_data,
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert conflict_response.status_code == 400
        error_data = conflict_response.json()
        assert "already exists" in error_data["detail"]
        
        # Verify original data source is unchanged
        get_response = client.get(
            f"/api/v1/data-sources/{data_source_id}",
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert get_response.status_code == 200
        get_data = get_response.json()
        assert get_data["data_source_name"] == "Valid Test Source"  # Should be unchanged

    @pytest.mark.asyncio
    async def test_data_source_different_url_types_integration(self, authenticated_integration_client):
        """Test creation of data sources with different URL types."""
        client, session = authenticated_integration_client
        
        test_cases = [
            {
                "name": "CSV File Source",
                "type": "csv",
                "url": "https://example.com/data.csv"
            },
            {
                "name": "Excel File Source", 
                "type": "xlsx",
                "url": "https://example.com/data.xlsx"
            },
            {
                "name": "PostgreSQL Database",
                "type": "postgres", 
                "url": "postgresql://user:password@localhost:5432/database"
            },
            {
                "name": "MySQL Database",
                "type": "mysql",
                "url": "mysql://user:password@localhost:3306/database"
            },
            {
                "name": "MongoDB Database",
                "type": "mongodb",
                "url": "mongodb://user:password@localhost:27017/database"
            },
            {
                "name": "MSSQL Database",
                "type": "mssql",
                "url": "Server=localhost;Database=testdb;User Id=user;Password=password;"
            },
            {
                "name": "Oracle Database",
                "type": "oracle", 
                "url": "localhost:1521:XE"
            }
        ]
        
        created_sources = []
        
        for test_case in test_cases:
            create_data = {
                "data_source_name": test_case["name"],
                "data_source_type": test_case["type"],
                "data_source_url": test_case["url"]
            }
            
            response = client.post(
                "/api/v1/data-sources/",
                json=create_data,
                headers={"Authorization": "Bearer test-token"}
            )
            
            assert response.status_code == 201, f"Failed to create {test_case['type']} source"
            response_data = response.json()
            created_sources.append(response_data["data_source"])
            
            # Verify the created source
            assert response_data["data_source"]["data_source_name"] == test_case["name"]
            assert response_data["data_source"]["data_source_type"] == test_case["type"]
        
        # Verify all sources are listed
        list_response = client.get(
            "/api/v1/data-sources/",
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert list_response.status_code == 200
        list_data = list_response.json()
        assert list_data["total_count"] == len(test_cases)
        
        # Verify each type is represented
        source_types = {source["data_source_type"] for source in list_data["data_sources"]}
        expected_types = {test_case["type"] for test_case in test_cases}
        assert source_types == expected_types


@pytest.mark.integration
class TestDataSourcePerformance:
    """Performance tests for data source operations."""

    @pytest.mark.asyncio
    async def test_bulk_data_source_operations(self, authenticated_integration_client):
        """Test performance with bulk operations."""
        client, session = authenticated_integration_client
        
        import time
        
        # Create many data sources
        num_sources = 50
        start_time = time.time()
        
        created_ids = []
        for i in range(num_sources):
            create_data = {
                "data_source_name": f"Bulk Test Source {i+1:03d}",
                "data_source_type": "csv",
                "data_source_url": f"https://example.com/bulk-test-{i+1:03d}.csv"
            }
            
            response = client.post(
                "/api/v1/data-sources/",
                json=create_data,
                headers={"Authorization": "Bearer test-token"}
            )
            
            assert response.status_code == 201
            created_ids.append(response.json()["data_source"]["data_source_id"])
        
        create_time = time.time() - start_time
        print(f"Created {num_sources} data sources in {create_time:.2f} seconds")
        
        # Test paginated retrieval performance
        start_time = time.time()
        
        page_response = client.get(
            "/api/v1/data-sources/paginated",
            params={"page": 1, "per_page": 25},
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert page_response.status_code == 200
        page_data = page_response.json()
        assert len(page_data["data_sources"]) == 25
        
        retrieval_time = time.time() - start_time
        print(f"Retrieved 25 data sources in {retrieval_time:.2f} seconds")
        
        # Test search performance
        start_time = time.time()
        
        search_response = client.get(
            "/api/v1/data-sources/paginated",
            params={"search": "Bulk", "per_page": 50},
            headers={"Authorization": "Bearer test-token"}
        )
        
        assert search_response.status_code == 200
        search_data = search_response.json()
        assert search_data["pagination"]["total"] == num_sources
        
        search_time = time.time() - start_time
        print(f"Searched {num_sources} data sources in {search_time:.2f} seconds")
        
        # Performance assertions (adjust thresholds as needed)
        assert create_time < 10.0, "Bulk creation took too long"
        assert retrieval_time < 1.0, "Paginated retrieval took too long"
        assert search_time < 2.0, "Search took too long"