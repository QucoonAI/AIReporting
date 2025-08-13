from typing import Dict, Any
import redis.asyncio as redis
from . import RedisKeyManager
from app.core.utils import logger
from .temp import TempDataService
from .chat import ChatCacheService



class RedisHealthService:
    """Redis health monitoring and maintenance service"""
    
    def __init__(self, redis_client: redis.Redis, app_name: str = "reportai"):
        self.redis_client = redis_client
        self.key_manager = RedisKeyManager(app_name)
    
    async def ping(self) -> bool:
        """Check Redis connection"""
        try:
            return await self.redis_client.ping()
        except Exception:
            return False
    
    async def get_comprehensive_stats(self) -> Dict[str, Any]:
        """Get comprehensive Redis statistics"""
        try:
            info = await self.redis_client.info()
            
            # Get key counts by type
            auth_keys = await self.redis_client.keys(self.key_manager.get_auth_session_pattern())
            user_keys = await self.redis_client.keys(self.key_manager.get_user_sessions_pattern())
            chat_keys = await self.redis_client.keys(self.key_manager.get_chat_session_pattern())
            temp_keys = await self.redis_client.keys(self.key_manager.get_temp_data_pattern())
            
            return {
                "connection": {
                    "connected": True,
                    "uptime_seconds": info.get('uptime_in_seconds', 0)
                },
                "memory": {
                    "used_memory_human": info.get('used_memory_human', 'Unknown'),
                    "used_memory_peak_human": info.get('used_memory_peak_human', 'Unknown'),
                    "mem_fragmentation_ratio": info.get('mem_fragmentation_ratio', 0)
                },
                "keys": {
                    "auth_sessions": len(auth_keys),
                    "user_sessions": len(user_keys),
                    "chat_sessions": len(chat_keys),
                    "temp_data": len(temp_keys),
                    "total_app_keys": len(auth_keys) + len(user_keys) + len(chat_keys) + len(temp_keys)
                },
                "performance": {
                    "total_commands_processed": info.get('total_commands_processed', 0),
                    "instantaneous_ops_per_sec": info.get('instantaneous_ops_per_sec', 0)
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting Redis stats: {e}")
            return {
                "connection": {"connected": False},
                "error": str(e)
            }
    
    async def cleanup_all_expired(self) -> Dict[str, int]:
        """Run cleanup across all service types"""
        try:
            results = {}
            
            # Cleanup temp data
            temp_service = TempDataService(self.redis_client, self.key_manager.app_name)
            results["temp_data_cleaned"] = await temp_service.cleanup_expired_temp_data()
            
            # Cleanup chat sessions
            chat_service = ChatCacheService(self.redis_client, app_name=self.key_manager.app_name)
            chat_cleanup = await chat_service.cleanup_expired_sessions()
            results.update(chat_cleanup)
            
            # Check for orphaned keys (keys without TTL)
            all_app_keys = await self.redis_client.keys(f"{self.key_manager.app_name}:*")
            orphaned_count = 0
            
            for key in all_app_keys:
                ttl = await self.redis_client.ttl(key)
                if ttl == -1:  # No expiry set
                    # Set default expiry based on key type
                    if "auth_session:" in key or "chat_session:" in key:
                        await self.redis_client.expire(key, 3600)  # 1 hour
                    elif "temp_data:" in key:
                        await self.redis_client.expire(key, 1800)  # 30 minutes
                    orphaned_count += 1
            
            results["orphaned_keys_fixed"] = orphaned_count
            
            logger.info(f"Redis cleanup completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error during Redis cleanup: {e}")
            return {"error": str(e)}

