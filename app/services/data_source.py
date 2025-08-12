import json
import io
import base64
from datetime import datetime
from typing import Optional, Dict, Any
from fastapi import UploadFile, HTTPException, status
from app.models.data_source import DataSource
from app.repositories.data_source import DataSourceRepository
from app.schemas.data_source import DataSourceUpdateRequest
from app.schemas.enum import DataSourceType
from app.core.utils import logger
from app.core.utils.s3_functions import upload_file_to_s3, validate_file
from app.core.utils.extractor import ExtactorService
from app.core.exceptions import (
    DataSourceNotFoundError,
    DataSourceLimitExceededError
)
from .ai_service import AIQuery
from .redis_managers.data_source import TempDataSourceService


llm = AIQuery()
extractor = ExtactorService()

class DataSourceService:
    """Service class for handling DataSource business logic."""
    
    # Class constants
    MAX_DATA_SOURCES_PER_USER = 10

    def __init__(self, data_source_repo: DataSourceRepository, temp_service: TempDataSourceService):
        self.data_source_repo = data_source_repo
        self.FILE_BASED_TYPES = {'csv', 'xlsx', 'pdf'}
        self.DATABASE_TYPES = {'postgres', 'mysql'}
        self.temp_service = temp_service


    async def upload_and_extract_schema(
        self, 
        user_id: int, 
        data_source_name: str,
        data_source_type: str,
        data_source_url: Optional[str] = None,
        file: Optional[UploadFile] = None
    ) -> Dict[str, Any]:
        """
        Enhanced schema extraction - no longer handles caching directly.
        Returns extraction result that will be cached by the route handler.
        """
        try:
            # Validate user limits and name uniqueness
            await self._validate_user_limits(user_id)
            await self._validate_unique_name(user_id, data_source_name)
            
            json_schema = None
            final_url = data_source_url
            file_content = None
            
            # Handle file-based data sources
            if data_source_type in self.FILE_BASED_TYPES:
                if not file:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"File is required for {data_source_type} data source"
                    )
                
                validate_file(file, file_content)
                
                raise HTTPException(
                    status_code=status.HTTP_501_NOT_IMPLEMENTED,
                    detail=f"{data_source_type.title()} integration is not implemented yet"
                )
                # # Extract schema from file content
                # file_content = await file.read()
                # json_schema = await extractor._extract_schema_from_file(
                #     data_source_type, 
                #     file_content=file_content
                # )
                
                # # Generate placeholder URL for display
                # file_extension = get_file_extension(file.filename)
                # final_url = f"pending_upload://{data_source_name}_{uuid.uuid4().hex}.{file_extension}"
                
            # Handle database connections
            elif data_source_type in self.DATABASE_TYPES:
                json_schema = await extractor._extract_schema_from_database(
                    data_source_type, 
                    data_source_url
                )
                final_url = data_source_url
                
            else:
                raise HTTPException(
                    status_code=status.HTTP_501_NOT_IMPLEMENTED,
                    detail=f"{data_source_type.title()} integration is not implemented yet"
                )
            
            # Generate LLM description
            llm_description = self._get_llm_prompt_from_schema(json_schema)
            
            # Prepare result data
            result = {
                "data_source_name": data_source_name,
                "data_source_type": data_source_type,
                "data_source_url": final_url,
                "tables": json_schema, # for ui display
                "llm_description": json.dumps(llm_description) if isinstance(llm_description, dict) else str(llm_description),
                "raw_schema": json_schema # Keep raw schema for database creation
            }
            
            # Add file metadata for file-based sources
            if file:
                result["file_metadata"] = {
                    "filename": file.filename,
                    "content_type": file.content_type,
                }
            
            # Get file content for caching if it's a file-based source
            file_content = None
            if file and data_source_type in self.FILE_BASED_TYPES:
                await file.seek(0)  # Reset file pointer
                file_content = await file.read()
            
            # Store in temporary cache
            temp_identifier = await self.temp_service.store_extraction(
                user_id=user_id,
                data_source_name=data_source_name,
                extraction_result=result,
                file_content=file_content
            )

            result["temp_identifier"] = temp_identifier
            
            return result
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error during schema extraction: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to extract schema from data source"
            )

    async def create_data_source_with_cached_extraction(
        self,
        user_id: int,
        temp_identifier: str,
        llm_description: str,
    ) -> DataSource:
        """
        Create data source using cached extraction result.
        Handles both file and database sources uniformly.
        """
        try:
            # Retrieve cached extraction data
            cached_data = await self.temp_service.get_extraction(temp_identifier, user_id)
            
            if not cached_data:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Extraction data not found or expired. Please re-extract the schema."
                )
            
            extraction_result = cached_data["extraction_result"]
            
            # Re-validate user limits and name uniqueness (in case time passed)
            data_source_name = extraction_result["data_source_name"]
            await self._validate_user_limits(user_id)
            await self._validate_unique_name(user_id, data_source_name)
            
            # Prepare schema with updated LLM description
            final_schema = extraction_result.get("raw_schema", {})
            if "metadata" not in final_schema:
                final_schema["metadata"] = {}
            
            final_schema["metadata"]["llm_description"] = llm_description
            final_schema["metadata"]["user_approved"] = True
            final_schema["metadata"]["approved_at"] = datetime.now().isoformat()
            
            # Determine actual URL and data source type
            data_source_type_str = extraction_result["data_source_type"]
            data_source_type = DataSourceType(data_source_type_str)
            actual_url = extraction_result["data_source_url"]
            
            # Handle file upload to S3 for file-based sources
            if data_source_type.value in self.FILE_BASED_TYPES:
                if not cached_data.get("has_file"):
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="File content not found in cached data"
                    )
                
                # Retrieve and decode file content
                file_content = base64.b64decode(cached_data["file_content"])
                file_metadata = extraction_result.get("file_metadata", {})
                
                # Recreate UploadFile object
                file_obj = UploadFile(
                    filename=file_metadata.get("filename", "unknown"),
                    file=io.BytesIO(file_content),
                    content_type=file_metadata.get("content_type", "application/octet-stream")
                )
                
                # Upload to S3
                actual_url = await upload_file_to_s3(
                    file=file_obj,
                    user_id=user_id,
                    data_source_name=data_source_name
                )
                
                logger.info(f"File uploaded to S3 from cached extraction: {actual_url}")
            
            # Create the data source
            data_source = DataSource(
                data_source_user_id=user_id,
                data_source_name=data_source_name,
                data_source_type=data_source_type,
                data_source_url=actual_url,
                data_source_schema=final_schema["metadata"]["llm_description"], # For now, save only the updated_llm_desciption
            )
            
            created_data_source = await self.data_source_repo.create_data_source(data_source)
            
            # Clean up cached data
            await self.temp_service.delete_extraction(temp_identifier, user_id)
            
            logger.info(f"Data source created from cached extraction: {created_data_source.data_source_id}")
            return created_data_source
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating data source from cache: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create data source"
            )

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
            
            # Validate name uniqueness if name is being updated
            if (update_data.data_source_name and 
                update_data.data_source_name != existing_data_source.data_source_name):
                await self._validate_unique_name(
                    user_id=existing_data_source.data_source_user_id,
                    name=update_data.data_source_name,
                    exclude_id=data_source_id
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
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update data source"
            )
    
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
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to delete data source"
            )

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
            raise
        except Exception as e:
            logger.error(f"Unexpected error retrieving data source {data_source_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve data source"
            )

    async def get_user_data_sources_paginated(
        self,
        user_id: int,
        page: int = 1,
        per_page: int = 10,
        data_source_type: Optional[DataSourceType] = None,
        search: Optional[str] = None,
        sort_by: str = "data_source_created_at",
        sort_order: str = "desc"
    ):
        """
        Get paginated data sources for a user.
        
        Args:
            user_id: ID of the user
            page: Page number (1-based)
            per_page: Number of items per page
            data_source_type: Optional filter by data source type
            search: Optional search term for data source name
            sort_by: Field to sort by
            sort_order: Sort order (asc or desc)
            
        Returns:
            Tuple of (data_sources_list, total_count)
        """
        try:
            return await self.data_source_repo.get_user_data_sources_paginated(
                user_id=user_id,
                page=page,
                per_page=per_page,
                data_source_type=data_source_type,
                search=search,
                sort_by=sort_by,
                sort_order=sort_order
            )
        except Exception as e:
            logger.error(f"Error getting paginated data sources for user {user_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve data sources"
            )

    def _get_llm_prompt_from_schema(self, schema_dict: Dict[str, Any]) -> Any:
        """
        Convert schema dictionary to LLM-friendly prompt.
        """
        try:
            refactored_schema = llm.schema_refactor(schema_dict)
            return refactored_schema
        except Exception as e:
            logger.error(f"Failed to generate LLM prompt from schema: {e}")
            return "Schema information unavailable"

    async def _validate_user_limits(self, user_id: int) -> None:
        """
        Validate user hasn't exceeded data source limits.
        Raises:
            DataSourceLimitExceededError: If limit exceeded
        """
        existing_data_sources = await self.data_source_repo.get_user_data_sources(user_id=user_id)
        if len(existing_data_sources) >= self.MAX_DATA_SOURCES_PER_USER:
            raise DataSourceLimitExceededError(self.MAX_DATA_SOURCES_PER_USER)

    async def _validate_unique_name(self, user_id: int, name: str, exclude_id: Optional[int] = None) -> None:
        """
        Validate data source name is unique for user.
        
        Args:
            user_id: ID of the user
            name: Name to validate
            exclude_id: Optional data source ID to exclude from check (for updates)
            
        Raises:
            HTTPException: If name already exists
        """
        existing_data_source = await self.data_source_repo.get_data_source_by_name(
            user_id=user_id,
            name=name
        )
        
        if existing_data_source and (exclude_id is None or existing_data_source.data_source_id != exclude_id):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Data source with name '{name}' already exists"
            )

