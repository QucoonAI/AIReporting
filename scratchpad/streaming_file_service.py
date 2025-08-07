import tempfile
import os
from typing import AsyncGenerator, Dict, Any
from contextlib import asynccontextmanager
from fastapi import UploadFile, HTTPException, status
from app.core.utils import logger


class StreamingFileService:
    """Service for handling large file processing without loading entire content into memory"""
    
    def __init__(self, max_file_size: int = 100 * 1024 * 1024):  # 100MB default
        self.max_file_size = max_file_size
        self.chunk_size = 8192  # 8KB chunks
    
    @asynccontextmanager
    async def temporary_file_from_upload(self, file: UploadFile) -> AsyncGenerator[str, None]:
        """
        Create a temporary file from UploadFile without loading entire content into memory
        """
        temp_file_path = None
        try:
            # Create temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{file.filename}") as temp_file:
                temp_file_path = temp_file.name
                
                # Stream file content to temporary file
                await file.seek(0)
                total_size = 0
                
                while chunk := await file.read(self.chunk_size):
                    total_size += len(chunk)
                    
                    # Check file size limit
                    if total_size > self.max_file_size:
                        raise HTTPException(
                            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                            detail=f"File size exceeds maximum allowed size of {self.max_file_size} bytes"
                        )
                    
                    temp_file.write(chunk)
                
                temp_file.flush()
                logger.info(f"Created temporary file: {temp_file_path}, size: {total_size} bytes")
            
            yield temp_file_path
            
        except Exception as e:
            logger.error(f"Error creating temporary file: {e}")
            raise
        finally:
            # Clean up temporary file
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                    logger.debug(f"Cleaned up temporary file: {temp_file_path}")
                except Exception as cleanup_error:
                    logger.warning(f"Failed to cleanup temporary file {temp_file_path}: {cleanup_error}")
    
    async def validate_file_streaming(self, file: UploadFile) -> Dict[str, Any]:
        """
        Validate file without loading entire content into memory
        """
        try:
            await file.seek(0)
            
            # Read first chunk to validate file type
            first_chunk = await file.read(self.chunk_size)
            if not first_chunk:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Empty file provided"
                )
            
            # Reset file position
            await file.seek(0)
            
            # Basic file validation
            file_info = {
                "filename": file.filename,
                "content_type": file.content_type,
                "has_content": len(first_chunk) > 0,
                "estimated_size": len(first_chunk)  # Will be updated during streaming
            }
            
            return file_info
            
        except Exception as e:
            logger.error(f"Error validating file: {e}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File validation failed: {str(e)}"
            )
    
    async def get_file_binary_content(self, file_path: str) -> bytes:
        """
        Read file content for operations that require binary data
        Use with caution for large files
        """
        try:
            with open(file_path, 'rb') as f:
                return f.read()
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            raise

