import base64
import boto3
import uuid
import json
from datetime import datetime
from typing import Optional, Dict, Any, List
from botocore.exceptions import ClientError
from fastapi import UploadFile, HTTPException, status
from app.repositories.data_source import DataSourceRepository
from app.schemas.data_source import DataSourceUpdateRequest
from app.schemas.enum import DataSourceType
from app.models.data_source import DataSource
from app.config.settings import get_settings
from app.core.utils import logger
from app.core.exceptions import (
    DataSourceNotFoundError,
    DataSourceLimitExceededError
)
from .redis_managers.factory import RedisServiceFactory
from .llm_services.ai_function import AIQuery
from .extractor import ExtactorService


settings = get_settings()
llm = AIQuery()
extractor = ExtactorService()

class DataSourceService:
    """Service class for handling DataSource business logic."""
    
    # Class constants
    MAX_DATA_SOURCES_PER_USER = 10
    MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB
    
    ALLOWED_EXTENSIONS = {
        'csv': ['text/csv', 'application/csv'],
        'xlsx': ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'],
        'pdf': ['application/pdf']
    }


    def __init__(
        self,
        data_source_repo: DataSourceRepository,
        redis_factory: RedisServiceFactory
    ):
        self.data_source_repo = data_source_repo
        self.redis_factory = redis_factory
        self.temp_data_service = redis_factory.temp_data_service
        self.s3_client = boto3.client('s3')
        self.s3_bucket = settings.S3_BUCKET_NAME
        
        self.FILE_BASED_TYPES = {'csv', 'xlsx'}
        self.DATABASE_TYPES = {'postgres', 'mysql'}
        


    async def download_file_from_s3(self, file_key: str) -> bytes:
        try:
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key=file_key
            )
            return response['Body'].read()
        except ClientError as e:
            logger.error(f"S3 download error for key {file_key}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to download file from storage"
            )

    def _validate_file(self, file: UploadFile, file_content: bytes) -> None:
        """
        Validate uploaded file size and type.
        
        Args:
            file: The uploaded file
            file_content: File content as bytes
            
        Raises:
            HTTPException: If validation fails
        """
        # Validate file size
        if len(file_content) > self.MAX_FILE_SIZE:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"File size exceeds maximum limit of {self.MAX_FILE_SIZE // (1024*1024)}MB"
            )
        
        # Validate file extension
        file_extension = self._get_file_extension(file.filename)
        if file_extension not in self.ALLOWED_EXTENSIONS:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported file type: {file_extension}"
            )
        
        # Validate content type if provided
        if file.content_type and file_extension in self.ALLOWED_EXTENSIONS:
            allowed_types = self.ALLOWED_EXTENSIONS[file_extension]
            if file.content_type not in allowed_types:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Content type mismatch: {file.content_type} for {file_extension}"
                )

    def _get_file_extension(self, filename: Optional[str]) -> str:
        """Extract file extension from filename."""
        if not filename or '.' not in filename:
            return 'unknown'
        return filename.split('.')[-1].lower()

    def _generate_s3_key(self, user_id: int, data_source_name: str, file_extension: str) -> str:
        """Generate unique S3 object key."""
        return f"{user_id}/{data_source_name}_{uuid.uuid4().hex}.{file_extension}"

    def _generate_s3_url(self, s3_key: str) -> str:
        """Generate S3 URL from object key."""
        return f"https://{self.s3_bucket}.s3.{settings.REGION}.amazonaws.com/{s3_key}"

    def _extract_s3_key_from_url(self, s3_url: str) -> str:
        """Extract S3 key from S3 URL."""
        url_prefix = f"{self.s3_bucket}.s3.{settings.REGION}.amazonaws.com/"
        if url_prefix not in s3_url:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid S3 URL format"
            )
        return s3_url.split(url_prefix)[1]

    async def _upload_file_to_s3(self, file: UploadFile, user_id: int, data_source_name: str) -> str:
        """
        Upload file to S3 and return the object URL.
        
        Args:
            file: The uploaded file
            user_id: ID of the user uploading the file
            data_source_name: Name of the data source
            
        Returns:
            S3 object URL
            
        Raises:
            HTTPException: If upload fails
        """
        try:
            file_content = await file.read()
            self._validate_file(file, file_content)
            
            file_extension = self._get_file_extension(file.filename)
            s3_key = self._generate_s3_key(user_id, data_source_name, file_extension)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=file_content,
                ContentType=file.content_type or 'application/octet-stream',
                ServerSideEncryption='AES256',
                Metadata={
                    'user_id': str(user_id),
                    'data_source_name': data_source_name,
                    'original_filename': file.filename or 'unknown'
                }
            )
            
            s3_url = self._generate_s3_url(s3_key)
            logger.info(f"File uploaded successfully to S3: {s3_key}")
            return s3_url
            
        except ClientError as e:
            logger.error(f"S3 upload error: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to upload file to storage"
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error uploading file: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="File upload failed"
            )

    def _get_llm_prompt_from_schema(self, schema_dict: Dict[str, Any]) -> Any:
        """
        Convert schema dictionary to LLM-friendly prompt.
        
        Args:
            schema_dict: Schema as dictionary
            
        Returns:
            LLM-optimized description string
        """
        try:
            refactored_schema = llm.schema_refactor(schema_dict)
            return refactored_schema
            # # Reconstruct DataSourceSchema from dictionary
            # schema = DataSourceSchema.from_dict(schema_dict)
            # return schema.to_llm_prompt()
        except Exception as e:
            logger.error(f"Failed to generate LLM prompt from schema: {e}")
            return "Schema information unavailable"

    async def _validate_user_limits(self, user_id: int) -> None:
        """
        Validate user hasn't exceeded data source limits.
        
        Args:
            user_id: ID of the user
            
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

    async def refresh_data_source_schema(self, data_source_id: int) -> DataSource:
        """
        Enhanced refresh method with support for all database types.
        """
        try:
            # Get the existing data source
            existing_data_source = await self.data_source_repo.get_data_source_by_id(data_source_id)
            if not existing_data_source:
                raise DataSourceNotFoundError(data_source_id)
            
            data_source_type = existing_data_source.data_source_type.value
            data_source_url = existing_data_source.data_source_url
            
            # Extract schema based on data source type
            json_schema = None
            
            if data_source_type in self.FILE_BASED_TYPES:
                if not data_source_url.startswith('https://'):
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid {data_source_type.upper()} file URL"
                    )
                
                # Download file from S3 and extract schema
                s3_key = self._extract_s3_key_from_url(data_source_url)
                file_content = await self.download_file_from_s3(s3_key)
                json_schema = await self._extract_schema_from_file(
                    data_source_type, 
                    file_content=file_content
                )
                
            elif data_source_type in self.DATABASE_TYPES:
                # Use enhanced database extraction with proper connection management
                json_schema = await self._extract_schema_from_database(data_source_type, data_source_url)
                
            else:
                raise HTTPException(
                    status_code=status.HTTP_501_NOT_IMPLEMENTED,
                    detail=f"Schema refresh for {data_source_type} is not supported yet"
                )
            
            if json_schema is None:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to extract schema from data source"
                )
            
            # Preserve user-added descriptions from existing schema
            enhanced_schema = self._preserve_user_descriptions(
                new_schema=json_schema,
                existing_schema=existing_data_source.data_source_schema
            )
            
            # Update the data source with new schema
            updated_data_source = await self.data_source_repo.refresh_data_source_schema(
                data_source_id=data_source_id,
                new_schema=enhanced_schema
            )
            
            logger.info(f"Data source schema refreshed successfully: {data_source_id}")
            return updated_data_source
            
        except (DataSourceNotFoundError, HTTPException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error refreshing schema for data source {data_source_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to refresh data source schema"
            )

    def _preserve_user_descriptions(
        self, 
        new_schema: Dict[str, Any], 
        existing_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Preserve user-added descriptions when refreshing schema.
        
        Args:
            new_schema: Newly extracted schema
            existing_schema: Current schema with user descriptions
            
        Returns:
            New schema with preserved user descriptions
        """
        try:
            import copy
            enhanced_schema = copy.deepcopy(new_schema)
            
            if not existing_schema or "tables" not in existing_schema:
                return enhanced_schema
            
            # Create lookup maps for existing descriptions
            existing_table_descriptions = {}
            existing_column_descriptions = {}
            
            for table in existing_schema.get("tables", []):
                table_name = table.get("name")
                if table_name and table.get("description"):
                    existing_table_descriptions[table_name] = table["description"]
                
                existing_column_descriptions[table_name] = {}
                for column in table.get("columns", []):
                    col_name = column.get("name")
                    if col_name and column.get("description"):
                        existing_column_descriptions[table_name][col_name] = column["description"]
            
            # Apply preserved descriptions to new schema
            for table in enhanced_schema.get("tables", []):
                table_name = table.get("name")
                if table_name in existing_table_descriptions:
                    table["description"] = existing_table_descriptions[table_name]
                
                if table_name in existing_column_descriptions:
                    for column in table.get("columns", []):
                        col_name = column.get("name")
                        if col_name in existing_column_descriptions[table_name]:
                            column["description"] = existing_column_descriptions[table_name][col_name]
            
            # Update metadata
            if "metadata" not in enhanced_schema:
                enhanced_schema["metadata"] = {}
            
            enhanced_schema["metadata"]["descriptions_preserved"] = True
            enhanced_schema["metadata"]["refresh_timestamp"] = datetime.now().isoformat()
            
            return enhanced_schema
            
        except Exception as e:
            logger.error(f"Error preserving user descriptions: {e}")
            return new_schema

    async def upload_and_extract_schema(
        self, 
        user_id: int, 
        data_source_name: str,
        data_source_type: str,
        data_source_url: Optional[str] = None,
        file: Optional[UploadFile] = None
    ) -> Dict[str, Any]:
        """
        Enhanced schema extraction with support for all database types.
        """
        try:
            # Validate user limits and name uniqueness
            await self._validate_user_limits(user_id)
            await self._validate_unique_name(user_id, data_source_name)
            
            json_schema = None
            final_url = data_source_url
            temp_file_identifier = None
            file_content = None
            
            # Handle file-based data sources
            if data_source_type in self.FILE_BASED_TYPES:
                if not file:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"File is required for {data_source_type} data source"
                    )
                raise HTTPException(
                    status_code=status.HTTP_501_NOT_IMPLEMENTED,
                    detail=f"{data_source_type.title()} integration is not implemented yet"
                )
                
                # # Validate file without uploading to S3
                # file_content = await file.read()
                # self._validate_file(file, file_content)
                
                # # Store file temporarily in Redis
                # if self.temp_data_service:
                #     temp_file_identifier = f"{user_id}_{data_source_name}_{uuid.uuid4().hex}"
                    
                #     temp_file_data = {
                #         "filename": file.filename,
                #         "content_type": file.content_type,
                #         "content": base64.b64encode(file_content).decode('utf-8'),
                #         "user_id": user_id,
                #         "data_source_name": data_source_name,
                #         "data_source_type": data_source_type,
                #         "size": len(file_content)
                #     }
                    
                #     await self.temp_data_service.store_temp_data(
                #         operation="file_upload_extract",
                #         identifier=temp_file_identifier,
                #         data=temp_file_data,
                #         expiry_minutes=30
                #     )
                
                # # Reset file pointer for schema extraction
                # await file.seek(0)
                # json_schema = await self._extract_schema_from_file(data_source_type, file=file)
                
                # # Generate placeholder URL
                # file_extension = self._get_file_extension(file.filename)
                # final_url = f"pending_upload://{data_source_name}_{uuid.uuid4().hex}.{file_extension}"
                
            # Handle database connections with enhanced support
            elif data_source_type in self.DATABASE_TYPES:
                json_schema = await extractor._extract_schema_from_database(data_source_type, data_source_url)
                final_url = data_source_url
                
            else:
                raise HTTPException(
                    status_code=status.HTTP_501_NOT_IMPLEMENTED,
                    detail=f"{data_source_type.title()} integration is not implemented yet"
                )
            
            # Generate LLM description
            llm_description = self._get_llm_prompt_from_schema(json_schema)
            
            # Convert to UI-friendly format
            tables_for_ui = self._convert_schema_for_ui(json_schema)
            
            # Prepare response data
            result = {
                "data_source_name": data_source_name,
                "data_source_type": data_source_type,
                "data_source_url": final_url,
                "tables": tables_for_ui,
                "llm_description": json.dumps(llm_description)
            }
            
            # Add file metadata and temp identifier for file-based sources
            if file and temp_file_identifier:
                result.update({
                    "file_metadata": {
                        "filename": file.filename,
                        "content_type": file.content_type,
                        "size": len(file_content)
                    },
                    "temp_file_identifier": temp_file_identifier
                })
            
            return result
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error during schema extraction: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to extract schema from data source"
            )

    def _convert_schema_for_ui(self, schema_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Convert internal schema to UI-friendly format
        
        Args:
            schema_dict: Internal schema dictionary
            
        Returns:
            List of table dictionaries for UI display
        """
        try:
            # Reconstruct DataSourceSchema from dictionary
            from .schema_extractors import DataSourceSchema
            schema = DataSourceSchema.from_dict(schema_dict)
            
            tables_for_ui = []
            for table in schema.tables:
                table_dict = {
                    "name": table.name,
                    "row_count": table.row_count,
                    "table_type": table.table_type,
                    "description": table.description or "",
                    "primary_keys": table.primary_keys or [],
                    "columns": []
                }
                
                for col in table.columns:
                    col_dict = {
                        "name": col.name,
                        "data_type": col.data_type.value,
                        "original_type": col.original_type,
                        "is_nullable": col.is_nullable,
                        "is_primary_key": col.is_primary_key,
                        "is_foreign_key": col.is_foreign_key,
                        "is_unique": col.is_unique,
                        "sample_values": col.sample_values or [],
                        "description": col.description or "",
                        "value_count": col.value_count,
                        "null_count": col.null_count,
                        "unique_count": col.unique_count,
                        "constraints": col.constraints or []
                    }
                    
                    # Add numeric statistics if available
                    if col.min_value is not None:
                        col_dict["min_value"] = col.min_value
                    if col.max_value is not None:
                        col_dict["max_value"] = col.max_value
                    if col.avg_value is not None:
                        col_dict["avg_value"] = col.avg_value
                    
                    # Add text statistics if available
                    if col.min_length is not None:
                        col_dict["min_length"] = col.min_length
                    if col.max_length is not None:
                        col_dict["max_length"] = col.max_length
                    if col.avg_length is not None:
                        col_dict["avg_length"] = col.avg_length
                    
                    # Add foreign key reference if available
                    if col.references_table:
                        col_dict["references_table"] = col.references_table
                    if col.references_column:
                        col_dict["references_column"] = col.references_column
                    
                    table_dict["columns"].append(col_dict)
                
                # Add foreign keys information
                if table.foreign_keys:
                    table_dict["foreign_keys"] = table.foreign_keys
                
                # Add indexes information
                if table.indexes:
                    table_dict["indexes"] = table.indexes
                
                tables_for_ui.append(table_dict)
            
            return tables_for_ui
            
        except Exception as e:
            logger.error(f"Error converting schema for UI: {e}")
            return []
    
    async def create_data_source_with_schema(
        self, 
        user_id: int, 
        data_source_name: str,
        data_source_type: DataSourceType,
        data_source_url: str,
        final_schema: Dict[str, Any],
        table_descriptions: Optional[Dict[str, str]] = None,
        column_descriptions: Optional[Dict[str, Dict[str, str]]] = None,
        temp_file_identifier: Optional[str] = None
    ) -> DataSource:
        """
        Create a data source with the final user-approved schema.
        For file-based sources, retrieves file from Redis and uploads to S3.
        
        Args:
            user_id: ID of the user creating the data source
            data_source_name: Name of the data source
            data_source_type: Type of data source (enum)
            data_source_url: URL of the data source (or pending_upload:// for files)
            final_schema: Final schema approved by user (with descriptions)
            table_descriptions: User-added table descriptions
            column_descriptions: User-added column descriptions
            temp_file_identifier: Identifier for temporarily stored file
            
        Returns:
            Created DataSource object
            
        Raises:
            HTTPException: If validation fails or creation fails
        """
        try:
            # Re-validate user limits and name uniqueness (in case time passed)
            await self._validate_user_limits(user_id)
            await self._validate_unique_name(user_id, data_source_name)
            
            # Apply user modifications to schema
            enhanced_schema = self._apply_user_modifications(
                final_schema, 
                table_descriptions, 
                column_descriptions
            )
            
            actual_url = data_source_url
            
            # Handle S3 upload for file-based data sources
            if data_source_type.value in self.FILE_BASED_TYPES:
                if not temp_file_identifier:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Temporary file identifier is required for file-based data sources"
                    )
                
                # Retrieve file from Redis
                if not self.temp_data_service:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Temporary data service not available"
                    )
                
                temp_file_data = await self.temp_data_service.get_temp_data(
                    operation="file_upload_extract",
                    identifier=temp_file_identifier
                )
                
                if not temp_file_data:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Temporary file not found or expired. Please re-upload the file."
                    )
                
                # Verify ownership and data source name match
                if (temp_file_data.get("user_id") != user_id or 
                    temp_file_data.get("data_source_name") != data_source_name):
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail="Unauthorized access to temporary file"
                    )
                
                # Recreate UploadFile object from Redis data
                try:
                    file_content = base64.b64decode(temp_file_data["content"])
                    
                    import io
                    from fastapi import UploadFile
                    
                    file_obj = UploadFile(
                        filename=temp_file_data["filename"],
                        file=io.BytesIO(file_content),
                        content_type=temp_file_data["content_type"]
                    )
                    
                    # Upload to S3
                    actual_url = await self._upload_file_to_s3(
                        file=file_obj,
                        user_id=user_id,
                        data_source_name=data_source_name
                    )
                    
                    logger.info(f"File uploaded to S3 from Redis temp storage: {actual_url}")
                    
                except Exception as e:
                    logger.error(f"Error processing temporary file: {e}")
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Failed to process temporary file"
                    )
            
            # Create the data source with user-approved schema and actual URL
            data_source = DataSource(
                data_source_user_id=user_id,
                data_source_name=data_source_name,
                data_source_type=data_source_type,
                data_source_url=actual_url,
                data_source_schema=enhanced_schema,
            )
            
            created_data_source = await self.data_source_repo.create_data_source(data_source)
            
            # Clean up temporary file from Redis
            if temp_file_identifier and self.temp_data_service:
                try:
                    await self.temp_data_service.delete_temp_data(
                        operation="file_upload_extract",
                        identifier=temp_file_identifier
                    )
                    logger.info(f"Cleaned up temporary file: {temp_file_identifier}")
                except Exception as cleanup_error:
                    logger.warning(f"Failed to cleanup temporary file: {cleanup_error}")
                    # Don't fail the whole operation for cleanup errors
            
            logger.info(f"Data source created successfully: {created_data_source.data_source_id}")
            return created_data_source
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating data source: {e}")
            # Attempt cleanup on error
            if temp_file_identifier and self.temp_data_service:
                try:
                    await self.temp_data_service.delete_temp_data(
                        operation="file_upload_extract",
                        identifier=temp_file_identifier
                    )
                except:
                    pass  # Don't let cleanup errors mask the original error
            
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create data source"
            )

    def _apply_user_modifications(
        self, 
        base_schema: Dict[str, Any],
        table_descriptions: Optional[Dict[str, str]] = None,
        column_descriptions: Optional[Dict[str, Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """
        Apply user-provided descriptions to the schema
        
        Args:
            base_schema: Original extracted schema
            table_descriptions: Dict mapping table names to descriptions
            column_descriptions: Dict mapping table names to column description dicts
            
        Returns:
            Enhanced schema with user modifications
        """
        try:
            # Make a deep copy to avoid modifying original
            import copy
            enhanced_schema = copy.deepcopy(base_schema)
            
            # Apply table descriptions
            if table_descriptions and "tables" in enhanced_schema:
                for table in enhanced_schema["tables"]:
                    table_name = table.get("name")
                    if table_name and table_name in table_descriptions:
                        # Update the table description
                        table["description"] = table_descriptions[table_name]
            
            # Apply column descriptions
            if column_descriptions and "tables" in enhanced_schema:
                for table in enhanced_schema["tables"]:
                    table_name = table.get("name")
                    if table_name and table_name in column_descriptions:
                        table_col_descriptions = column_descriptions[table_name]
                        
                        for column in table.get("columns", []):
                            col_name = column.get("name")
                            if col_name and col_name in table_col_descriptions:
                                # Update the column description
                                column["description"] = table_col_descriptions[col_name]
            
            # Update metadata to indicate user modifications
            if "metadata" not in enhanced_schema:
                enhanced_schema["metadata"] = {}
            
            enhanced_schema["metadata"]["user_modified"] = True
            enhanced_schema["metadata"]["modification_timestamp"] = datetime.now().isoformat()
            
            # Track what was modified
            modifications = {}
            if table_descriptions:
                modifications["table_descriptions_added"] = len(table_descriptions)
            if column_descriptions:
                total_col_desc = sum(len(cols) for cols in column_descriptions.values())
                modifications["column_descriptions_added"] = total_col_desc
            
            enhanced_schema["metadata"]["modifications_applied"] = modifications
            
            return enhanced_schema
            
        except Exception as e:
            logger.error(f"Error applying user modifications: {e}")
            # Return original schema if modification fails
            return base_schema


