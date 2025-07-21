import redis.asyncio as redis
from typing import Optional
from .settings import get_settings


settings = get_settings()

class RedisConnectionManager:
    def __init__(self, redis_url: str = settings.REDIS_URL):
        self.redis_url = redis_url
        self.redis_client: Optional[redis.Redis] = None
    
    async def connect(self):
        """Initialize Redis connection"""
        if not self.redis_client:
            self.redis_client = redis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True,
                retry_on_timeout=True,
                health_check_interval=30
            )
            # Test connection
            await self.redis_client.ping()
    
    async def disconnect(self):
        """Close Redis connection"""
        if self.redis_client:
            await self.redis_client.close()
            self.redis_client = None
    
    def get_client(self) -> redis.Redis:
        """Get Redis client instance"""
        if not self.redis_client:
            raise RuntimeError("Redis client not initialized. Call connect() first.")
        return self.redis_client

redis_manager = RedisConnectionManager()

