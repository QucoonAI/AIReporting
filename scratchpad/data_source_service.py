import json
from datetime import datetime
from typing import Optional, Dict, Any
from fastapi import UploadFile, HTTPException, status
from app.models.data_source import DataSource
from app.repositories.data_source import DataSourceRepository
from app.schemas.data_source import DataSourceUpdateRequest
from app.schemas.enum import DataSourceType
from app.core.utils import logger
from .enhanced_temp_data_service import EnhancedTempDataService
from .streaming_file_service import StreamingFileService
from .transaction_manager import DataSourceTransactionManager
from .connection_pool_manager import connection_pool_manager
from .validation_middleware import (
    OwnershipValidator, 
    UserLimitValidator, 
    NameUniquenessValidator,
    validation_cache
)
from .distributed_lock_manager import require_data_source_update_lock

class EnhancedDataSourceService:
    """Enhanced Data Source Service with improved error handling, validation, and performance"""
    
    def __init__(
        self, 
        data_source_repo: DataSourceRepository, 
        temp_service: EnhancedTempDataService,
        streaming_service: StreamingFileService
    ):
        self.data_source_repo = data_source_repo
        self.temp_service = temp_service
        self.streaming_service = streaming_service
        self.FILE_BASED_TYPES = {'csv', 'xlsx', 'pdf'}
        self.DATABASE_TYPES = {'postgres', 'mysql'}
        
        # Initialize validators
        self.ownership_validator = OwnershipValidator(self)
        self.limit_validator = UserLimitValidator(self)
        self.name_validator = NameUniquenessValidator(self)
    
    async def upload_and_extract_schema_streaming(
        self, 
        user_id: int, 
        data_source_name: str,
        data_source_type: str,
        data_source_url: Optional[str] = None,
        file: Optional[UploadFile] = None
    ) -> Dict[str, Any]:
        """
        Enhanced schema extraction with streaming file processing and connection pooling
        """
        try:
            # Validate user limits (with caching)
            await self.limit_validator.validate_user_limits(user_id)
            
            # Validate name uniqueness (with caching)
            await self.name_validator.validate_name_uniqueness(user_id, data_source_name)
            
            json_schema = None
            final_url = data_source_url
            temp_file_path = None
            
            # Handle file-based data sources with streaming
            if data_source_type in self.FILE_BASED_TYPES:
                if not file:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"File is required for {data_source_type} data source"
                    )
                
                # Validate file without loading into memory
                file_info = await self.streaming_service.validate_file_streaming(file)
                
                # Create temporary file for processing
                async with self.streaming_service.temporary_file_from_upload(file) as temp_path:
                    temp_file_path = temp_path
                    
                    # Extract schema from temporary file
                    json_schema = await self._extract_schema_from_file_path(
                        data_source_type, 
                        temp_path
                    )
                    
                    # Generate placeholder URL for display
                    final_url = f"pending_upload://{data_source_name}_{hash(temp_path)}.{data_source_type}"
                
            # Handle database connections with connection pooling
            elif data_source_type in self.DATABASE_TYPES:
                # Test connection first
                connection_test = await connection_pool_manager.test_connection(
                    data_source_type, 
                    data_source_url
                )
                
                if not connection_test["success"]:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Cannot connect to database: {connection_test['error']}"
                    )
                
                # Extract schema using connection pool
                json_schema = await self._extract_schema_from_database_pooled(
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
            llm_description = await self._get_llm_prompt_from_schema_async(json_schema)
            
            # Prepare result data
            result = {
                "data_source_name": data_source_name,
                "data_source_type": data_source_type,
                "data_source_url": final_url,
                "tables": json_schema, # for ui display
                "llm_description": json.dumps(llm_description) if isinstance(llm_description, dict) else str(llm_description),
                "raw_schema": json_schema
            }
            
            # Add file metadata for file-based sources
            if file:
                result["file_metadata"] = {
                    "filename": file.filename,
                    "content_type": file.content_type
                }
            
            return result
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error during schema extraction: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to extract schema from data source"
            )
    
    async def create_data_source_with_transaction(
        self,
        user_id: int,
        temp_identifier: str,
        updated_llm_description: str,
    ) -> DataSource:
        """
        Create data source using transaction manager for atomic operations
        """
        try:
            # Retrieve cached extraction data
            cached_data = await self.temp_service.get_extraction_with_file_content(
                temp_identifier,
                user_id, 
                include_file_content=True
            )
            
            if not cached_data:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Extraction data not found or expired. Please re-extract the schema."
                )
            
            extraction_result = cached_data["extraction_result"]
            
            # Re-validate with cache (should be fast due to caching)
            data_source_name = extraction_result["data_source_name"]
            await self.limit_validator.validate_user_limits(user_id)
            await self.name_validator.validate_name_uniqueness(user_id, data_source_name)
            
            # Prepare transaction
            transaction = DataSourceTransactionManager()
            
            # Prepare schema with updated LLM description
            final_schema = extraction_result.get("raw_schema", {})
            if "metadata" not in final_schema:
                final_schema["metadata"] = {}
            
            final_schema["metadata"]["llm_description"] = updated_llm_description
            final_schema["metadata"]["user_approved"] = True
            final_schema["metadata"]["approved_at"] = datetime.now().isoformat()
            
            data_source_type_str = extraction_result["data_source_type"]
            data_source_type = DataSourceType(data_source_type_str)
            actual_url = extraction_result["data_source_url"]
            
            # Handle file upload to S3 for file-based sources
            if data_source_type.value in self.FILE_BASED_TYPES:
                if not cached_data.get("file_content"):
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="File content not found in cached data"
                    )
                
                # Recreate UploadFile object from cached content
                import io
                file_metadata = extraction_result.get("file_metadata", {})
                file_obj = UploadFile(
                    filename=file_metadata.get("filename", "unknown"),
                    file=io.BytesIO(cached_data["file_content"]),
                    content_type=file_metadata.get("content_type", "application/octet-stream")
                )
                
                # Add S3 upload operation to transaction
                transaction.add_s3_upload_operation(file_obj, user_id, data_source_name, None)
            
            # Add database creation operation
            def create_data_source():
                data_source = DataSource(
                    data_source_user_id=user_id,
                    data_source_name=data_source_name,
                    data_source_type=data_source_type,
                    data_source_url=actual_url,  # Will be updated by S3 upload if needed
                    data_source_schema=final_schema["metadata"]["llm_description"],
                )
                return self.data_source_repo.create_data_source(data_source)
            
            transaction.add_database_update_operation(create_data_source)
            
            # Add cleanup operation
            transaction.add_temp_data_cleanup_operation(
                self.temp_service, 
                temp_identifier, 
                user_id
            )
            
            # Execute transaction
            result = await transaction.execute_transaction()
            
            if not result["success"]:
                logger.error(f"Transaction failed: {result}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to create data source: {result.get('error', 'Unknown error')}"
                )
            
            # Get the created data source from transaction results
            created_data_source = result["results"]["database_update"]
            
            # Update URL if S3 upload was performed
            if "s3_upload" in result["results"]:
                s3_url = result["results"]["s3_upload"]
                # Update the data source with the actual S3 URL
                update_request = DataSourceUpdateRequest(data_source_url=s3_url)
                created_data_source = await self.data_source_repo.update_data_source(
                    created_data_source.data_source_id,
                    update_request
                )
            
            # Invalidate relevant caches
            validation_cache.invalidate(user_id, "user_limits")
            validation_cache.invalidate(user_id, "name_uniqueness", name=data_source_name)
            
            logger.info(f"Data source created successfully: {created_data_source.data_source_id}")
            return created_data_source
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating data source from cache: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create data source"
            )
    
    @require_data_source_update_lock
    async def update_data_source_with_validation(
        self, 
        data_source_id: int,
        user_id: int,
        update_data: DataSourceUpdateRequest
    ) -> DataSource:
        """
        Update data source with distributed locking and validation
        """
        try:
            # Validate ownership (with caching)
            validation_result = await self.ownership_validator.validate_ownership(user_id, data_source_id)
            existing_data_source = validation_result["data_source"]
            
            # Validate name uniqueness if name is being updated
            if (update_data.data_source_name and 
                update_data.data_source_name != existing_data_source.data_source_name):
                await self.name_validator.validate_name_uniqueness(
                    user_id, 
                    update_data.data_source_name, 
                    exclude_id=data_source_id
                )
            
            # Update the data source
            updated_data_source = await self.data_source_repo.update_data_source(
                data_source_id=data_source_id,
                update_data=update_data
            )
            
            # Invalidate relevant caches
            validation_cache.invalidate(user_id, "ownership_check", data_source_id=data_source_id)
            if update_data.data_source_name:
                validation_cache.invalidate(user_id, "name_uniqueness", name=update_data.data_source_name)
            
            logger.info(f"Data source updated successfully: {data_source_id}")
            return updated_data_source
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error updating data source {data_source_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update data source"
            )
    
    async def delete_data_source_with_cleanup(self, data_source_id: int, user_id: int) -> str:
        """
        Delete data source with proper cleanup using transaction
        """
        try:
            # Validate ownership
            validation_result = await self.ownership_validator.validate_ownership(user_id, data_source_id)
            existing_data_source = validation_result["data_source"]
            
            # Create transaction for cleanup
            transaction = DataSourceTransactionManager()
            
            # Add database deletion operation
            def delete_data_source():
                return self.data_source_repo.delete_data_source(data_source_id)
            
            transaction.add_database_update_operation(delete_data_source)
            
            # Add S3 cleanup for file-based sources
            if existing_data_source.data_source_type.value in self.FILE_BASED_TYPES:
                if existing_data_source.data_source_url.startswith("https://"):
                    transaction.add_old_file_cleanup_operation(existing_data_source.data_source_url)
            
            # Execute transaction
            result = await transaction.execute_transaction()
            
            if not result["success"]:
                logger.error(f"Delete transaction failed: {result}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to delete data source: {result.get('error', 'Unknown error')}"
                )
            
            # Invalidate caches
            validation_cache.invalidate(user_id, "ownership_check", data_source_id=data_source_id)
            validation_cache.invalidate(user_id, "user_limits")
            
            logger.info(f"Data source deleted successfully: {data_source_id}")
            return "Data source deleted successfully"
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error deleting data source {data_source_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to delete data source"
            )
    
    async def get_data_source_by_id_with_validation(self, data_source_id: int, user_id: int) -> DataSource:
        """
        Get data source with ownership validation
        """
        try:
            validation_result = await self.ownership_validator.validate_ownership(user_id, data_source_id)
            return validation_result["data_source"]
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error retrieving data source {data_source_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve data source"
            )
    
    async def _extract_schema_from_database_pooled(
        self, 
        data_source_type: str, 
        connection_url: str
    ) -> Dict[str, Any]:
        """
        Extract schema using connection pool
        """
        try:
            async with connection_pool_manager.get_connection(data_source_type, connection_url) as conn:
                # Use the existing extractor service but with pooled connection
                from .extractor import ExtactorService
                extractor = ExtactorService()
                
                # Pass the connection to the extractor instead of creating new one
                return await extractor._extract_schema_with_connection(data_source_type, conn)
                
        except Exception as e:
            logger.error(f"Error extracting schema from {data_source_type}: {e}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to extract schema: {str(e)}"
            )
    
    async def _extract_schema_from_file_path(
        self, 
        data_source_type: str, 
        file_path: str
    ) -> Dict[str, Any]:
        """
        Extract schema from file path instead of file content
        """
        try:
            from .extractor import ExtactorService
            extractor = ExtactorService()
            
            # Read file content for extraction
            file_content = await self.streaming_service.get_file_binary_content(file_path)
            return await extractor._extract_schema_from_file(data_source_type, file_content=file_content)
            
        except Exception as e:
            logger.error(f"Error extracting schema from file: {e}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to extract schema from file: {str(e)}"
            )
    
    async def _get_llm_prompt_from_schema_async(self, schema_dict: Dict[str, Any]) -> Any:
        """
        Convert schema dictionary to LLM-friendly prompt asynchronously
        """
        try:
            # Run LLM processing in thread pool to avoid blocking
            import asyncio
            from concurrent.futures import ThreadPoolExecutor
            
            def process_schema():
                from .llm_services.ai_function import AIQuery
                llm = AIQuery()
                return llm.schema_refactor(schema_dict)
            
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                return await loop.run_in_executor(executor, process_schema)
                
        except Exception as e:
            logger.error(f"Failed to generate LLM prompt from schema: {e}")
            return "Schema information unavailable"
    
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
        Get paginated data sources with input sanitization
        """
        try:
            # Sanitize search input
            if search:
                # Remove potentially dangerous characters and limit length
                search = search.strip()[:100]
                # Remove special SQL characters
                search = ''.join(c for c in search if c.isalnum() or c in ' -_.')
            
            return await self.data_source_repo.get_user_data_sources_paginated(
                user_id=user_id,
                page=max(1, page),  # Ensure page is at least 1
                per_page=min(100, max(1, per_page)),  # Limit per_page between 1 and 100
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
    
