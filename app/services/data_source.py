import base64
import boto3
import uuid
import json
from typing import Optional, Dict, Any
from botocore.exceptions import ClientError
from fastapi import UploadFile, HTTPException, status
from app.repositories.data_source import DataSourceRepository
from app.schemas.data_source import DataSourceCreateRequest, DataSourceUpdateRequest
from app.schemas.enum import DataSourceType
from app.models.data_source import DataSource
from app.config.settings import get_settings
from app.core.utils import logger
from app.core.exceptions import (
    DataSourceNotFoundError,
    DataSourceLimitExceededError
)
from .redis_managers.factory import RedisServiceFactory
from .schema_extractors.factory import SchemaExtractorFactory
from .schema_extractors import DataSourceSchema
from .llm_services.ai_function import AIQuery


settings = get_settings()
llm = AIQuery()

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

    SUPPORTED_EXTRACTORS = SchemaExtractorFactory.get_supported_types()

    def __init__(self, data_source_repo: DataSourceRepository, redis_factory: RedisServiceFactory):
        self.data_source_repo = data_source_repo
        self.redis_factory = redis_factory
        self.temp_data_service = redis_factory.temp_data_service
        self._initialize_s3_client()

        # Update supported types from factory
        self.SUPPORTED_TYPES = set(SchemaExtractorFactory.get_supported_types())
        
        # Update existing type sets to use supported types
        self.FILE_BASED_TYPES = {'csv', 'xlsx'} & self.SUPPORTED_TYPES
        self.DATABASE_TYPES = {'postgres', 'mssql', 'mysql'} & self.SUPPORTED_TYPES
        
    def _initialize_s3_client(self) -> None:
        """Initialize S3 client and validate configuration."""
        session = boto3.Session(
                aws_access_key_id=settings.ACCESS_KEY_ID,
                aws_secret_access_key=settings.SECRET_ACCESS_KEY
            )
        s3 = boto3.resource('s3')
        self.s3_bucket_resource = s3.Bucket(settings.S3_BUCKET_NAME)
        self.s3_client = boto3.client('s3')
        self.s3_bucket = settings.S3_BUCKET_NAME
        
        if not self.s3_bucket:
            logger.error("AWS_S3_BUCKET environment variable not set")
            raise ValueError("S3 bucket configuration missing")

    async def download_file_from_s3(self, file_key: str) -> bytes:
        """
        Download file content from S3.
        
        Args:
            file_key: S3 object key
            
        Returns:
            File content as bytes
            
        Raises:
            HTTPException: If download fails
        """
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
                logger.warning(f"Content type mismatch: {file.content_type} for {file_extension}")

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
            self.s3_bucket_resource.put_object(
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

    async def _extract_schema_from_file(
        self, 
        data_source_type: str, 
        file: Optional[UploadFile] = None,
        file_content: Optional[bytes] = None
    ) -> Dict[str, Any]:
        """
        Extract schema from file-based data sources using unified extractors.
        
        Args:
            data_source_type: Type of data source
            file: Upload file object (for creation)
            file_content: File content bytes (for refresh)
            
        Returns:
            Extracted schema as dictionary
            
        Raises:
            HTTPException: If extraction fails
        """
        try:
            extractor = SchemaExtractorFactory.get_extractor(data_source_type)
            
            if data_source_type == 'csv':
                if file:
                    # print('yes')
                    schema = await extractor.extract_schema(file)
                elif file_content:
                    # Create a mock UploadFile from bytes for CSV
                    import io
                    from fastapi import UploadFile
                    mock_file = UploadFile(
                        filename="data.csv",
                        file=io.BytesIO(file_content),
                        content_type="text/csv"
                    )
                    schema = await extractor.extract_schema(mock_file)
                else:
                    raise ValueError("Either file or file_content must be provided")
                    
            elif data_source_type == 'xlsx':
                if file:
                    schema = await extractor.extract_schema(file)
                elif file_content:
                    # Create a mock UploadFile from bytes for XLSX
                    import io
                    from fastapi import UploadFile
                    mock_file = UploadFile(
                        filename="data.xlsx",
                        file=io.BytesIO(file_content),
                        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    schema = await extractor.extract_schema(mock_file)
                else:
                    raise ValueError("Either file or file_content must be provided")
                    
            elif data_source_type == 'pdf':
                raise HTTPException(
                    status_code=status.HTTP_501_NOT_IMPLEMENTED,
                    detail="PDF processing is not implemented yet"
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Unsupported file-based data source type: {data_source_type}"
                )
            
            # Convert DataSourceSchema to dictionary
            return schema.to_json()
            
        except Exception as e:
            logger.error(f"Schema extraction failed for {data_source_type}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to extract schema from {data_source_type} file"
            )

    async def _extract_schema_from_database(self, data_source_type: str, connection_string: str) -> Dict[str, Any]:
        """
        Extract schema from database connections using unified extractors.
        
        Args:
            data_source_type: Type of database
            connection_string: Database connection string
            
        Returns:
            Extracted schema as dictionary
            
        Raises:
            HTTPException: If extraction fails
        """
        try:
            extractor = SchemaExtractorFactory.get_extractor(data_source_type)
            
            if data_source_type == 'postgres':
                schema = await extractor.extract_schema(connection_string, schema_name='public')
            elif data_source_type in ['mysql']:
                schema = await extractor.extract_schema(connection_string)
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Unsupported database type: {data_source_type}"
                )
            
            # Convert DataSourceSchema to dictionary
            return schema.to_dict()
            
        except Exception as e:
            logger.error(f"Database schema extraction failed for {data_source_type}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to extract schema from {data_source_type} database"
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

    async def _extract_schema(
        self, 
        data_source_type: str, 
        data_source_url: str,
        file: Optional[UploadFile] = None
    ) -> Dict[str, Any]:
        """
        Extract schema based on data source type.
        
        Args:
            data_source_type: Type of data source
            data_source_url: URL or connection string
            file: Optional file for file-based sources
            
        Returns:
            Extracted schema
            
        Raises:
            HTTPException: If extraction fails or type is unsupported
        """
        if data_source_type in self.FILE_BASED_TYPES:
            return await self._extract_schema_from_file(data_source_type, file=file)
        elif data_source_type in self.DATABASE_TYPES:
            return await self._extract_schema_from_database(data_source_type, data_source_url)
        else:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail=f"{data_source_type.title()} integration is not implemented yet"
            )

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

    async def create_data_source(
        self, 
        user_id: int, 
        data_source_data: DataSourceCreateRequest,
        file: Optional[UploadFile] = None
    ) -> DataSource:
        """
        Create a new data source for a user.
        
        Args:
            user_id: ID of the user creating the data source
            data_source_data: Data source creation data
            file: Optional uploaded file for file-based data sources
            
        Returns:
            Created DataSource object
            
        Raises:
            DataSourceLimitExceededError: If user has reached the maximum limit
            HTTPException: If validation fails or creation fails
        """
        try:
            # Validate user limits and name uniqueness
            await self._validate_user_limits(user_id)
            await self._validate_unique_name(user_id, data_source_data.data_source_name)
            
            data_source_type = data_source_data.data_source_type.value
            data_source_url = data_source_data.data_source_url
            
            # Handle file upload for file-based data sources
            if data_source_type in self.FILE_BASED_TYPES:
                if not file:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"File is required for {data_source_type} data source"
                    )
                data_source_url = await self._upload_file_to_s3(
                    file=file,
                    user_id=user_id,
                    data_source_name=data_source_data.data_source_name
                )
            
            # Extract schema
            json_schema = await self._extract_schema(data_source_type, data_source_url, file)
            
            # Create the data source
            data_source = DataSource(
                data_source_user_id=user_id,
                data_source_name=data_source_data.data_source_name,
                data_source_type=data_source_data.data_source_type,
                data_source_url=str(data_source_url),
                data_source_schema=json_schema,
            )
            
            created_data_source = await self.data_source_repo.create_data_source(data_source)
            logger.info(f"Data source created successfully: {created_data_source.data_source_id}")
            
            return created_data_source
            
        except (DataSourceLimitExceededError, HTTPException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating data source: {e}")
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

    async def refresh_data_source_schema(self, data_source_id: int) -> DataSource:
        """
        Refresh the schema of an existing data source by re-extracting it.
        
        Args:
            data_source_id: ID of the data source to refresh
            
        Returns:
            Updated DataSource object with refreshed schema
            
        Raises:
            DataSourceNotFoundError: If data source not found
            HTTPException: If schema extraction fails
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
            
            # Update the data source with new schema
            updated_data_source = await self.data_source_repo.refresh_data_source_schema(
                data_source_id=data_source_id,
                new_schema=json_schema
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

    async def get_data_source_llm_description(self, data_source_id: int) -> str:
        """
        Get LLM-friendly description of a data source.
        
        Args:
            data_source_id: ID of the data source
            
        Returns:
            LLM-optimized description string
            
        Raises:
            DataSourceNotFoundError: If data source not found
        """
        try:
            data_source = await self.get_data_source_by_id(data_source_id)
            return self._get_llm_prompt_from_schema(data_source.data_source_schema)
        except DataSourceNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Error generating LLM description for data source {data_source_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to generate data source description"
            )

    
    async def upload_and_extract_schema(
        self, 
        user_id: int, 
        data_source_name: str,
        data_source_type: str,
        data_source_url: Optional[str] = None,
        file: Optional[UploadFile] = None
    ) -> Dict[str, Any]:
        """
        Extract schema from file or database without uploading file to S3.
        For file-based sources, stores file temporarily in Redis.
        
        Args:
            user_id: ID of the user uploading
            data_source_name: Name of the data source
            data_source_type: Type of data source
            data_source_url: URL for database connections
            file: Optional uploaded file for file-based sources
            
        Returns:
            Dictionary containing schema and metadata for user review
            
        Raises:
            HTTPException: If validation or extraction fails
        """
        try:
            # Validate user limits and name uniqueness
            await self._validate_user_limits(user_id)
            await self._validate_unique_name(user_id, data_source_name)
            
            json_schema = None
            final_url = data_source_url
            temp_file_identifier = None
            
            # Handle file-based data sources
            if data_source_type in self.FILE_BASED_TYPES:
                if not file:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"File is required for {data_source_type} data source"
                    )
                
                # Validate file without uploading to S3
                file_content = await file.read()
                self._validate_file(file, file_content)
                
                # Store file temporarily in Redis
                if self.temp_data_service:
                    temp_file_identifier = f"{user_id}_{data_source_name}_{uuid.uuid4().hex}"
                    
                    # Prepare file data for Redis storage
                    temp_file_data = {
                        "filename": file.filename,
                        "content_type": file.content_type,
                        "content": base64.b64encode(file_content).decode('utf-8'),
                        "user_id": user_id,
                        "data_source_name": data_source_name,
                        "data_source_type": data_source_type,
                        "size": len(file_content)
                    }
                    
                    # Store in Redis with 30-minute expiry
                    await self.temp_data_service.store_temp_data(
                        operation="file_upload_extract",
                        identifier=temp_file_identifier,
                        data=temp_file_data,
                        expiry_minutes=30
                    )
                    
                    logger.info(f"File stored temporarily in Redis: {temp_file_identifier}")
                
                # Reset file pointer for schema extraction
                await file.seek(0)
                
                # Extract schema directly from uploaded file
                json_schema = await self._extract_schema_from_file(data_source_type, file=file)
                
                # Generate placeholder URL
                file_extension = self._get_file_extension(file.filename)
                final_url = f"pending_upload://{data_source_name}_{uuid.uuid4().hex}.{file_extension}"
                
            # Handle database connections
            elif data_source_type in self.DATABASE_TYPES:
                json_schema = await self._extract_schema_from_database(data_source_type, data_source_url)
                final_url = data_source_url
            else:
                raise HTTPException(
                    status_code=status.HTTP_501_NOT_IMPLEMENTED,
                    detail=f"{data_source_type.title()} integration is not implemented yet"
                )
            
            # Prepare response data
            result = {
                "data_source_name": data_source_name,
                "data_source_type": data_source_type,
                "data_source_url": final_url,
                "extracted_schema": json_schema,
                "llm_description": self._get_llm_prompt_from_schema(json_schema)
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

    async def create_data_source_with_schema(
        self, 
        user_id: int, 
        data_source_name: str,
        data_source_type: DataSourceType,
        data_source_url: str,
        final_schema: Dict[str, Any],
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
                data_source_schema=final_schema,
            )
            
            created_data_source = await self.data_source_repo.create_data_source(data_source)
            
            # Clean up temporary file from Redis
            if temp_file_identifier and self.temp_data_service:
                await self.temp_data_service.delete_temp_data(
                    operation="file_upload_extract",
                    identifier=temp_file_identifier
                )
                logger.info(f"Cleaned up temporary file: {temp_file_identifier}")
            
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

