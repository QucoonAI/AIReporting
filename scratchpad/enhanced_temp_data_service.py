import json
import pickle
from typing import Dict, Any, Optional, Union
from datetime import datetime, timedelta
from app.core.utils import logger
from .redis_managers.factory import RedisServiceFactory

class EnhancedTempDataService:
    """Enhanced service for managing temporary data with binary support"""
    
    def __init__(self, redis_factory: RedisServiceFactory):
        self.redis_factory = redis_factory
        self.temp_data_service = redis_factory.temp_data_service
        self.DEFAULT_EXPIRY_MINUTES = 30
        
        # Operation types
        self.EXTRACTION_OPERATION = "data_source_extraction"
        self.USER_EXTRACTIONS_OPERATION = "user_extractions_list"
        self.FILE_REFERENCE_OPERATION = "file_reference"
    
    async def store_extraction_with_file_reference(
        self,
        user_id: int,
        data_source_name: str,
        extraction_result: Dict[str, Any],
        file_path: Optional[str] = None,
        expiry_minutes: int = None
    ) -> str:
        """
        Store extraction result with file reference instead of binary content
        """
        try:
            temp_identifier = self._generate_temp_identifier(user_id, data_source_name)
            expiry = expiry_minutes or self.DEFAULT_EXPIRY_MINUTES
            
            extraction_data = {
                "user_id": user_id,
                "temp_identifier": temp_identifier,
                "extraction_result": extraction_result,
                "created_at": datetime.now().isoformat(),
                "expires_at": (datetime.now() + timedelta(minutes=expiry)).isoformat(),
                "status": "extracted",
                "has_file": file_path is not None
            }
            
            # Store file reference separately if file exists
            if file_path:
                file_reference_key = f"file_ref_{temp_identifier}"
                await self._store_file_reference(file_reference_key, file_path, expiry)
                extraction_data["file_reference_key"] = file_reference_key
            
            # Store extraction data
            await self.temp_data_service.store_temp_data(
                operation=self.EXTRACTION_OPERATION,
                identifier=temp_identifier,
                data=extraction_data,
                expiry_minutes=expiry
            )
            
            # Add to user's extraction list
            await self._add_to_user_extractions(user_id, temp_identifier, expiry)
            
            logger.info(f"Stored extraction with file reference: {temp_identifier}")
            return temp_identifier
            
        except Exception as e:
            logger.error(f"Error storing extraction with file reference: {e}")
            raise
    
    async def get_extraction_with_file_content(
        self, 
        temp_identifier: str, 
        user_id: int,
        include_file_content: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Get extraction data with optional file content
        """
        try:
            extraction_data = await self.temp_data_service.get_temp_data(
                operation=self.EXTRACTION_OPERATION,
                identifier=temp_identifier
            )
            
            if not extraction_data or extraction_data.get("user_id") != user_id:
                return None
            
            # Include file content if requested and available
            if include_file_content and extraction_data.get("file_reference_key"):
                file_content = await self._get_file_content(
                    extraction_data["file_reference_key"]
                )
                if file_content:
                    extraction_data["file_content"] = file_content
            
            return extraction_data
            
        except Exception as e:
            logger.error(f"Error retrieving extraction {temp_identifier}: {e}")
            return None
    
    async def _store_file_reference(self, reference_key: str, file_path: str, expiry_minutes: int):
        """Store file path reference in Redis"""
        try:
            # Read file content and store as binary
            with open(file_path, 'rb') as f:
                file_content = f.read()
            
            await self.temp_data_service.store_temp_data(
                operation=self.FILE_REFERENCE_OPERATION,
                identifier=reference_key,
                data={"file_content": file_content},  # Store as bytes
                expiry_minutes=expiry_minutes
            )
            
        except Exception as e:
            logger.error(f"Error storing file reference {reference_key}: {e}")
            raise
    
    async def _get_file_content(self, reference_key: str) -> Optional[bytes]:
        """Retrieve file content from Redis"""
        try:
            file_data = await self.temp_data_service.get_temp_data(
                operation=self.FILE_REFERENCE_OPERATION,
                identifier=reference_key
            )
            
            if file_data and "file_content" in file_data:
                return file_data["file_content"]
            
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving file content {reference_key}: {e}")
            return None
    
    def _generate_temp_identifier(self, user_id: int, data_source_name: str) -> str:
        """Generate unique temporary identifier"""
        import uuid
        return f"{user_id}_{data_source_name}_{uuid.uuid4().hex}"
    
    async def _add_to_user_extractions(self, user_id: int, temp_identifier: str, expiry_minutes: int):
        """Add extraction to user's list"""
        try:
            current_data = await self.temp_data_service.get_temp_data(
                operation=self.USER_EXTRACTIONS_OPERATION,
                identifier=str(user_id)
            )
            
            extraction_ids = current_data.get("extraction_ids", []) if current_data else []
            
            if temp_identifier not in extraction_ids:
                extraction_ids.append(temp_identifier)
            
            user_extractions_data = {
                "user_id": user_id,
                "extraction_ids": extraction_ids,
                "updated_at": datetime.now().isoformat()
            }
            
            await self.temp_data_service.store_temp_data(
                operation=self.USER_EXTRACTIONS_OPERATION,
                identifier=str(user_id),
                data=user_extractions_data,
                expiry_minutes=expiry_minutes + 5
            )
            
        except Exception as e:
            logger.error(f"Error adding extraction to user list: {e}")
    
    async def cleanup_extraction_and_files(self, temp_identifier: str, user_id: int) -> bool:
        """Clean up extraction data and associated file references"""
        try:
            # Get extraction data to find file reference
            extraction_data = await self.get_extraction_with_file_content(
                temp_identifier, user_id, include_file_content=False
            )
            
            if not extraction_data:
                return False
            
            # Delete file reference if exists
            if extraction_data.get("file_reference_key"):
                await self.temp_data_service.delete_temp_data(
                    operation=self.FILE_REFERENCE_OPERATION,
                    identifier=extraction_data["file_reference_key"]
                )
            
            # Delete extraction data
            success = await self.temp_data_service.delete_temp_data(
                operation=self.EXTRACTION_OPERATION,
                identifier=temp_identifier
            )
            
            if success:
                await self._remove_from_user_extractions(user_id, temp_identifier)
            
            return success
            
        except Exception as e:
            logger.error(f"Error cleaning up extraction {temp_identifier}: {e}")
            return False
    
    async def _remove_from_user_extractions(self, user_id: int, temp_identifier: str):
        """Remove extraction from user's list"""
        try:
            current_data = await self.temp_data_service.get_temp_data(
                operation=self.USER_EXTRACTIONS_OPERATION,
                identifier=str(user_id)
            )
            
            if not current_data or "extraction_ids" not in current_data:
                return
            
            extraction_ids = current_data["extraction_ids"]
            
            if temp_identifier in extraction_ids:
                extraction_ids.remove(temp_identifier)
                
                user_extractions_data = {
                    "user_id": user_id,
                    "extraction_ids": extraction_ids,
                    "updated_at": datetime.now().isoformat()
                }
                
                await self.temp_data_service.store_temp_data(
                    operation=self.USER_EXTRACTIONS_OPERATION,
                    identifier=str(user_id),
                    data=user_extractions_data,
                    expiry_minutes=self.DEFAULT_EXPIRY_MINUTES + 5
                )
            
        except Exception as e:
            logger.error(f"Error removing extraction from user list: {e}")

