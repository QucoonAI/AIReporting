import boto3
import uuid
from typing import Optional
from botocore.exceptions import ClientError
from fastapi import UploadFile, HTTPException, status
from repositories.data_source import DataSourceRepository
from schemas.data_source import DataSourceCreateRequest, DataSourceUpdateRequest
from schemas.enum import DataSourceType
from models.data_source import DataSource
from config.settings import get_settings
from core.utils import logger
from core.exceptions import (
    DataSourceNotFoundError,
    DataSourceLimitExceededError
)


settings = get_settings()

class DataSourceService:

    def __init__(self, data_source_repo: DataSourceRepository):
        self.data_source_repo = data_source_repo
        self.MAX_DATA_SOURCES_PER_USER = 10

        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
        self.s3_bucket = 'report-ai-data-sources'
        
        if not self.s3_bucket:
            logger.error("AWS_S3_BUCKET environment variable not set")
            raise ValueError("S3 bucket configuration missing")

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
            # Generate a unique filename
            file_extension = file.filename.split('.')[-1] if file.filename and '.' in file.filename else 'unknown'
            unique_filename = f"{user_id}/{data_source_name}_{uuid.uuid4().hex}.{file_extension}"
            
            # Validate file size (e.g., max 100MB)
            max_file_size = 100 * 1024 * 1024  # 100MB
            file_content = await file.read()
            
            if len(file_content) > max_file_size:
                raise HTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail="File size exceeds maximum limit of 100MB"
                )
            
            # Validate file type based on extension
            allowed_extensions = {
                'csv': ['text/csv', 'application/csv'],
                'xlsx': ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'],
                'pdf': ['application/pdf']
            }
            
            if file_extension.lower() not in allowed_extensions:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Unsupported file type: {file_extension}"
                )
            
            # Validate content type if provided
            if file.content_type and file_extension.lower() in allowed_extensions:
                if file.content_type not in allowed_extensions[file_extension.lower()]:
                    logger.warning(f"Content type mismatch: {file.content_type} for {file_extension}")
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=unique_filename,
                Body=file_content,
                ContentType=file.content_type or 'application/octet-stream',
                Metadata={
                    'user_id': str(user_id),
                    'data_source_name': data_source_name,
                    'original_filename': file.filename or 'unknown'
                }
            )
            
            # Generate S3 URL
            s3_url = f"https://{self.s3_bucket}.s3.{settings.AWS_REGION}.amazonaws.com/{unique_filename}"
            
            logger.info(f"File uploaded successfully to S3: {unique_filename}")
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
            
             # Handle file upload for file-based data sources
            data_source_url = data_source_data.data_source_url
            file_based_types = ['csv', 'xlsx', 'pdf']
            
            if data_source_data.data_source_type.value in file_based_types:
                if not file:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"File is required for {data_source_data.data_source_type.value} data source"
                    )
                
                # Upload file to S3 and get URL
                data_source_url = await self._upload_file_to_s3(
                    file=file,
                    user_id=user_id,
                    data_source_name=data_source_data.data_source_name
                )
            
            # Create the data source
            data_source = DataSource(
                data_source_user_id=user_id,
                data_source_name=data_source_data.data_source_name,
                data_source_type=data_source_data.data_source_type,
                data_source_url=str(data_source_url)
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
            raise

