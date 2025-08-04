import uuid
import boto3
from typing import Optional
from fastapi import HTTPException, status, UploadFile
from botocore.exceptions import ClientError
from app.config.settings import get_settings
from app.core.utils import logger



MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB

ALLOWED_EXTENSIONS = {
    'csv': ['text/csv', 'application/csv'],
    'xlsx': ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'],
    'pdf': ['application/pdf']
}

settings = get_settings()
s3_client = boto3.client('s3')
s3_bucket = settings.S3_BUCKET_NAME

def extract_s3_key_from_url(s3_url: str) -> str:
    """Extract S3 key from S3 URL."""
    url_prefix = f"{settings.S3_BUCKET_NAME}.s3.{settings.REGION}.amazonaws.com/"
    if url_prefix not in s3_url:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid S3 URL format"
        )
    return s3_url.split(url_prefix)[1]

def get_file_extension(filename: Optional[str]) -> str:
        """Extract file extension from filename."""
        if not filename or '.' not in filename:
            return 'unknown'
        return filename.split('.')[-1].lower()

def validate_file(file: UploadFile, file_content: bytes) -> None:
        """
        Validate uploaded file size and type.
        
        Args:
            file: The uploaded file
            file_content: File content as bytes
            
        Raises:
            HTTPException: If validation fails
        """
        # Validate file size
        if len(file_content) > MAX_FILE_SIZE:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"File size exceeds maximum limit of {MAX_FILE_SIZE // (1024*1024)}MB"
            )
        
        # Validate file extension
        file_extension = get_file_extension(file.filename)
        if file_extension not in ALLOWED_EXTENSIONS:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported file type: {file_extension}"
            )
        
        # Validate content type if provided
        if file.content_type and file_extension in ALLOWED_EXTENSIONS:
            allowed_types = ALLOWED_EXTENSIONS[file_extension]
            if file.content_type not in allowed_types:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Content type mismatch: {file.content_type} for {file_extension}"
                )

async def upload_file_to_s3(file: UploadFile, user_id: int, data_source_name: str) -> str:
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
        validate_file(file, file_content)
        
        file_extension = get_file_extension(file.filename)
        s3_key = f"{user_id}/{data_source_name}_{uuid.uuid4().hex}.{file_extension}"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=s3_bucket,
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
        
        s3_url = f"https://{s3_bucket}.s3.{settings.REGION}.amazonaws.com/{s3_key}"
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

async def download_file_from_s3(file_key: str) -> bytes:
    try:
        response = s3_client.get_object(
            Bucket=s3_bucket,
            Key=file_key
        )
        return response['Body'].read()
    except ClientError as e:
        logger.error(f"S3 download error for key {file_key}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to download file from storage"
        )

async def delete_file_from_s3(s3_key: str):
    try:
        s3_client.delete_object(
            Bucket=s3_bucket,
            Key=s3_key
        )
        logger.info(f"Deleted old S3 file: {s3_key}")
    except ClientError as e:
        logger.error(f"S3 delete error for key {s3_key}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete file from storage"
        )

