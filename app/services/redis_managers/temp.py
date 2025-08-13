import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Any
import redis.asyncio as redis
from fastapi import HTTPException
from . import RedisKeyManager
from app.core.utils import logger



class TempDataService:
    """Temporary data service with proper Redis key management"""
    
    def __init__(self, redis_client: redis.Redis, app_name: str = "reportai"):
        self.redis_client = redis_client
        self.key_manager = RedisKeyManager(app_name)
        self.default_expiry_minutes = 30
    
    async def store_temp_data(
        self, 
        operation: str, 
        identifier: str, 
        data: Dict[str, Any], 
        expiry_minutes: int = None
    ) -> str:
        """Store temporary data in Redis and return the key"""
        try:
            expiry = expiry_minutes or self.default_expiry_minutes
            temp_key = self.key_manager.temp_data_key(operation, identifier)
            
            # Add metadata
            temp_data = {
                "data": data,
                "operation": operation,
                "identifier": identifier,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=expiry)).isoformat()
            }
            
            await self.redis_client.setex(
                temp_key,
                timedelta(minutes=expiry),
                json.dumps(temp_data, default=str)
            )
            
            logger.debug(f"Stored temp data for {operation}:{identifier}")
            return temp_key
            
        except Exception as e:
            logger.error(f"Error storing temp data: {e}")
            raise HTTPException(status_code=500, detail="Failed to store temporary data")
    
    async def get_temp_data(self, operation: str, identifier: str) -> Optional[Dict[str, Any]]:
        """Get temporary data from Redis"""
        try:
            temp_key = self.key_manager.temp_data_key(operation, identifier)
            data = await self.redis_client.get(temp_key)
            
            if data:
                try:
                    temp_data = json.loads(data)
                    return temp_data.get("data")
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON in temp data: {temp_key}")
                    await self.redis_client.delete(temp_key)
                    return None
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting temp data: {e}")
            return None
    
    async def delete_temp_data(self, operation: str, identifier: str) -> bool:
        """Delete temporary data from Redis"""
        try:
            temp_key = self.key_manager.temp_data_key(operation, identifier)
            result = await self.redis_client.delete(temp_key)
            
            if result:
                logger.debug(f"Deleted temp data for {operation}:{identifier}")
            
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error deleting temp data: {e}")
            return False
    
    async def cleanup_expired_temp_data(self) -> int:
        """Clean up expired temporary data"""
        try:
            pattern = self.key_manager.get_temp_data_pattern()
            keys = await self.redis_client.keys(pattern)
            
            expired_count = 0
            
            for key in keys:
                ttl = await self.redis_client.ttl(key)
                
                if ttl == -2:  # Key doesn't exist
                    continue
                elif ttl <= 0:  # Expired or no expiry
                    await self.redis_client.delete(key)
                    expired_count += 1
            
            if expired_count > 0:
                logger.info(f"Cleaned up {expired_count} expired temp data entries")
            
            return expired_count
            
        except Exception as e:
            logger.error(f"Error during temp data cleanup: {e}")
            return 0

