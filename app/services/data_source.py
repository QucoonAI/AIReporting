from typing import Optional
from fastapi import HTTPException, status
from repositories.data_source import DataSourceRepository
from schemas.data_source import DataSourceCreateRequest, DataSourceUpdateRequest
from models.data_source import DataSource
from core.utils import logger
from core.exceptions import (
    DataSourceNotFoundError,
    DataSourceLimitExceededError
)


class DataSourceService:
    def __init__(self, data_source_repo: DataSourceRepository):
        self.data_source_repo = data_source_repo
        self.MAX_DATA_SOURCES_PER_USER = 10

    async def create_data_source(
        self, 
        user_id: int, 
        data_source_data: DataSourceCreateRequest
    ) -> DataSource:
        """
        Create a new data source for a user.
        
        Args:
            user_id: ID of the user creating the data source
            data_source_data: Data source creation data
            
        Returns:
            Created DataSource object
            
        Raises:
            DataSourceLimitExceededError: If user has reached the maximum limit of data sources
            HTTPException: If data source name already exists for user or creation fails
        """
        try:
            # Check if user has reached the maximum limit of data sources
            existing_data_sources = await self.data_source_repo.get_user_data_sources(user_id=user_id)
            
            if len(existing_data_sources) >= self.MAX_DATA_SOURCES_PER_USER:
                raise DataSourceLimitExceededError(self.MAX_DATA_SOURCES_PER_USER)
            
            # Check if data source with same name already exists for this user
            existing_data_source = await self.data_source_repo.get_data_source_by_name(
                user_id=user_id,
                name=data_source_data.data_source_name
            )
            
            if existing_data_source:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Data source with name '{data_source_data.data_source_name}' already exists"
                )
            
            # Create the data source
            data_source = DataSource(
                data_source_user_id=user_id,
                data_source_name=data_source_data.data_source_name,
                data_source_type=data_source_data.data_source_type,
                data_source_url=str(data_source_data.data_source_url)
            )
            
            created_data_source = await self.data_source_repo.create_data_source(data_source)
            logger.info(f"Data source created successfully: {created_data_source.data_source_id}")
            
            return created_data_source
            
        except (DataSourceLimitExceededError, HTTPException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating data source: {e}")
            raise

    async def update_data_source(
        self, 
        data_source_id: int, 
        update_data: DataSourceUpdateRequest
    ) -> DataSource:
        """
        Update an existing data source.
        
        Args:
            data_source_id: ID of the data source to update
            update_data: Data to update
            
        Returns:
            Updated DataSource object
            
        Raises:
            DataSourceNotFoundError: If data source not found
            HTTPException: If name conflict or update fails
        """
        try:
            # Get the existing data source
            existing_data_source = await self.data_source_repo.get_data_source_by_id(data_source_id)
            if not existing_data_source:
                raise DataSourceNotFoundError(data_source_id)
            
            # If name is being updated, check for conflicts
            if (update_data.data_source_name and 
                update_data.data_source_name != existing_data_source.data_source_name):
                
                name_conflict = await self.data_source_repo.get_data_source_by_name(
                    user_id=existing_data_source.data_source_user_id,
                    name=update_data.data_source_name
                )
                
                if name_conflict:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Data source with name '{update_data.data_source_name}' already exists"
                    )
            
            # Update the data source
            updated_data_source = await self.data_source_repo.update_data_source(
                data_source_id=data_source_id,
                update_data=update_data
            )
            
            logger.info(f"Data source updated successfully: {data_source_id}")
            return updated_data_source
            
        except (DataSourceNotFoundError, HTTPException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error updating data source {data_source_id}: {e}")
            raise
    
    async def delete_data_source(self, data_source_id: int) -> str:
        """
        Delete a data source.
        
        Args:
            data_source_id: ID of the data source to delete
            
        Returns:
            Success message
            
        Raises:
            DataSourceNotFoundError: If data source not found
        """
        try:
            # Check if data source exists
            existing_data_source = await self.data_source_repo.get_data_source_by_id(data_source_id)
            if not existing_data_source:
                raise DataSourceNotFoundError(data_source_id)
            
            # Delete the data source
            await self.data_source_repo.delete_data_source(data_source_id)
            
            logger.info(f"Data source deleted successfully: {data_source_id}")
            return "Data source deleted successfully"
            
        except DataSourceNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Unexpected error deleting data source {data_source_id}: {e}")
            raise

    async def get_data_source_by_id(self, data_source_id: int) -> DataSource:
        """
        Get a data source by ID.
        
        Args:
            data_source_id: ID of the data source to retrieve
            
        Returns:
            DataSource object
            
        Raises:
            DataSourceNotFoundError: If data source not found
        """
        try:
            data_source = await self.data_source_repo.get_data_source_by_id(data_source_id)
            if not data_source:
                raise DataSourceNotFoundError(data_source_id)
            
            return data_source
            
        except DataSourceNotFoundError:
            # Let custom exception bubble up
            raise
        except Exception as e:
            # Log the error and let the general exception handler deal with it
            logger.error(f"Unexpected error retrieving data source {data_source_id}: {e}")
            raise


