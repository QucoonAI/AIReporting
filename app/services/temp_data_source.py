import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from app.core.utils import logger
from .redis_managers.factory import RedisServiceFactory


class TempDataSourceService:
    """Enhanced service for managing temporary data source extractions"""
    
    def __init__(self, redis_factory: RedisServiceFactory):
        self.redis_factory = redis_factory
        self.temp_data_service = redis_factory.temp_data_service
        self.DEFAULT_EXPIRY_MINUTES = 30
        
        # Operation types for temp data storage
        self.EXTRACTION_OPERATION = "data_source_extraction"
        self.USER_EXTRACTIONS_OPERATION = "user_extractions_list"
    
    def _generate_temp_identifier(self, user_id: int, data_source_name: str) -> str:
        """Generate unique temporary identifier"""
        return f"{user_id}_{data_source_name}_{uuid.uuid4().hex}"
    
    async def store_extraction(
        self,
        user_id: int,
        data_source_name: str,
        extraction_result: Dict[str, Any],
        file_content: Optional[bytes] = None,
        expiry_minutes: int = None
    ) -> str:
        """
        Store extraction result and return temp_identifier
        Uses the existing temp_data_service for storage
        """
        try:
            temp_identifier = self._generate_temp_identifier(user_id, data_source_name)
            expiry = expiry_minutes or self.DEFAULT_EXPIRY_MINUTES
            
            # Prepare extraction data
            extraction_data = {
                "user_id": user_id,
                "temp_identifier": temp_identifier,
                "extraction_result": extraction_result,
                "created_at": datetime.now().isoformat(),
                "expires_at": (datetime.now() + timedelta(minutes=expiry)).isoformat(),
                "status": "extracted"
            }
            
            # Add file content if present (base64 encoded for JSON serialization)
            if file_content:
                import base64
                extraction_data["file_content"] = base64.b64encode(file_content).decode('utf-8')
                extraction_data["has_file"] = True
            else:
                extraction_data["has_file"] = False
            
            # Store extraction data using temp_data_service
            await self.temp_data_service.store_temp_data(
                operation=self.EXTRACTION_OPERATION,
                identifier=temp_identifier,
                data=extraction_data,
                expiry_minutes=expiry
            )
            
            # Add to user's extraction list
            await self._add_to_user_extractions(user_id, temp_identifier, expiry)
            
            logger.info(f"Stored extraction: {temp_identifier} for user {user_id}")
            return temp_identifier
            
        except Exception as e:
            logger.error(f"Error storing extraction: {e}")
            raise
    
    async def get_extraction(self, temp_identifier: str, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Get extraction data by temp_identifier with ownership validation
        Uses the existing temp_data_service for retrieval
        """
        try:
            # Get extraction data using temp_data_service
            extraction_data = await self.temp_data_service.get_temp_data(
                operation=self.EXTRACTION_OPERATION,
                identifier=temp_identifier
            )
            
            if not extraction_data:
                return None
            
            # Validate ownership
            if extraction_data.get("user_id") != user_id:
                logger.warning(f"Unauthorized access attempt to extraction {temp_identifier} by user {user_id}")
                return None
            
            return extraction_data
            
        except Exception as e:
            logger.error(f"Error retrieving extraction {temp_identifier}: {e}")
            return None
    
    async def get_user_extractions(self, user_id: int) -> List[Dict[str, Any]]:
        """
        Get all pending extractions for a user
        Combines temp_data_service with user extraction list management
        """
        try:
            # Get user's extraction list
            user_extractions_data = await self.temp_data_service.get_temp_data(
                operation=self.USER_EXTRACTIONS_OPERATION,
                identifier=str(user_id)
            )
            
            if not user_extractions_data or "extraction_ids" not in user_extractions_data:
                return []
            
            extraction_ids = user_extractions_data["extraction_ids"]
            extractions = []
            
            for temp_id in extraction_ids:
                extraction_data = await self.get_extraction(temp_id, user_id)
                
                if extraction_data:
                    # Return summary info (not full extraction result)
                    summary = {
                        "temp_identifier": temp_id,
                        "data_source_name": extraction_data["extraction_result"]["data_source_name"],
                        "data_source_type": extraction_data["extraction_result"]["data_source_type"],
                        "has_file": extraction_data.get("has_file", False),
                        "table_count": len(extraction_data["extraction_result"].get("tables", [])),
                        "created_at": extraction_data["created_at"],
                        "expires_at": extraction_data["expires_at"],
                        "status": extraction_data.get("status", "unknown")
                    }
                    extractions.append(summary)
                else:
                    # Clean up expired/invalid entries
                    await self._remove_from_user_extractions(user_id, temp_id)
            
            return extractions
            
        except Exception as e:
            logger.error(f"Error getting user extractions for user {user_id}: {e}")
            return []
    
    async def delete_extraction(self, temp_identifier: str, user_id: int) -> bool:
        """
        Delete extraction data
        Uses temp_data_service for deletion
        """
        try:
            # Verify ownership first
            extraction_data = await self.get_extraction(temp_identifier, user_id)
            if not extraction_data:
                return False
            
            # Delete extraction data using temp_data_service
            success = await self.temp_data_service.delete_temp_data(
                operation=self.EXTRACTION_OPERATION,
                identifier=temp_identifier
            )
            
            if success:
                # Remove from user's list
                await self._remove_from_user_extractions(user_id, temp_identifier)
                logger.info(f"Deleted extraction: {temp_identifier}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error deleting extraction {temp_identifier}: {e}")
            return False
    
    async def _add_to_user_extractions(self, user_id: int, temp_identifier: str, expiry_minutes: int):
        """
        Add extraction to user's list using temp_data_service
        """
        try:
            # Get current user extractions
            current_data = await self.temp_data_service.get_temp_data(
                operation=self.USER_EXTRACTIONS_OPERATION,
                identifier=str(user_id)
            )
            
            if current_data and "extraction_ids" in current_data:
                extraction_ids = current_data["extraction_ids"]
            else:
                extraction_ids = []
            
            # Add new extraction ID if not already present
            if temp_identifier not in extraction_ids:
                extraction_ids.append(temp_identifier)
            
            # Update user extractions list
            user_extractions_data = {
                "user_id": user_id,
                "extraction_ids": extraction_ids,
                "updated_at": datetime.now().isoformat()
            }
            
            # Store with slightly longer expiry than individual extractions
            await self.temp_data_service.store_temp_data(
                operation=self.USER_EXTRACTIONS_OPERATION,
                identifier=str(user_id),
                data=user_extractions_data,
                expiry_minutes=expiry_minutes + 5
            )
            
        except Exception as e:
            logger.error(f"Error adding extraction to user list: {e}")
    
    async def _remove_from_user_extractions(self, user_id: int, temp_identifier: str):
        """
        Remove extraction from user's list using temp_data_service
        """
        try:
            # Get current user extractions
            current_data = await self.temp_data_service.get_temp_data(
                operation=self.USER_EXTRACTIONS_OPERATION,
                identifier=str(user_id)
            )
            
            if not current_data or "extraction_ids" not in current_data:
                return
            
            extraction_ids = current_data["extraction_ids"]
            
            # Remove the extraction ID
            if temp_identifier in extraction_ids:
                extraction_ids.remove(temp_identifier)
                
                # Update user extractions list
                user_extractions_data = {
                    "user_id": user_id,
                    "extraction_ids": extraction_ids,
                    "updated_at": datetime.now().isoformat()
                }
                
                # Store updated list
                await self.temp_data_service.store_temp_data(
                    operation=self.USER_EXTRACTIONS_OPERATION,
                    identifier=str(user_id),
                    data=user_extractions_data,
                    expiry_minutes=self.DEFAULT_EXPIRY_MINUTES + 5
                )
            
        except Exception as e:
            logger.error(f"Error removing extraction from user list: {e}")
    
    async def cleanup_expired_extractions(self, user_id: int):
        """
        Clean up expired extractions for a user
        Works with temp_data_service's automatic expiration
        """
        try:
            extractions = await self.get_user_extractions(user_id)
            current_time = datetime.now()
            
            cleaned_count = 0
            for extraction in extractions:
                try:
                    expires_at = datetime.fromisoformat(extraction["expires_at"])
                    if current_time > expires_at:
                        success = await self.delete_extraction(extraction["temp_identifier"], user_id)
                        if success:
                            cleaned_count += 1
                except Exception as extraction_error:
                    logger.error(f"Error cleaning individual extraction: {extraction_error}")
            
            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} expired extractions for user {user_id}")
                
        except Exception as e:
            logger.error(f"Error cleaning up expired extractions for user {user_id}: {e}")
    
    async def get_extraction_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about extractions using temp_data_service
        This method provides insights into current extraction usage
        """
        try:
            # Since temp_data_service doesn't expose pattern-based queries,
            # we'll provide basic statistics that we can track
            stats = {
                "service_status": "active",
                "default_expiry_minutes": self.DEFAULT_EXPIRY_MINUTES,
                "operations": {
                    "extraction_operation": self.EXTRACTION_OPERATION,
                    "user_list_operation": self.USER_EXTRACTIONS_OPERATION
                },
                "timestamp": datetime.now().isoformat()
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting extraction statistics: {e}")
            return {"error": "Failed to get statistics"}
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Health check for the temp data source service
        """
        try:
            # Test basic operations
            test_data = {"test": "health_check", "timestamp": datetime.now().isoformat()}
            test_identifier = f"health_check_{uuid.uuid4().hex}"
            
            # Store test data
            await self.temp_data_service.store_temp_data(
                operation="health_check",
                identifier=test_identifier,
                data=test_data,
                expiry_minutes=1  # Very short expiry for health check
            )
            
            # Retrieve test data
            retrieved_data = await self.temp_data_service.get_temp_data(
                operation="health_check",
                identifier=test_identifier
            )
            
            # Clean up test data
            await self.temp_data_service.delete_temp_data(
                operation="health_check",
                identifier=test_identifier
            )
            
            # Verify round trip
            is_healthy = (
                retrieved_data is not None and
                retrieved_data.get("test") == "health_check"
            )
            
            return {
                "status": "healthy" if is_healthy else "unhealthy",
                "timestamp": datetime.now().isoformat(),
                "test_successful": is_healthy
            }
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "timestamp": datetime.now().isoformat(),
                "error": str(e)
            }


