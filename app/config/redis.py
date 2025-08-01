import asyncio
import redis.asyncio as redis
from typing import Optional
from contextlib import asynccontextmanager
from .settings import get_settings
from app.core.utils import logger

settings = get_settings()

class RedisManager:
    """
    Redis connection manager with health checks, reconnection logic, and graceful shutdown
    """
    
    def __init__(self):
        self._redis: Optional[redis.Redis] = None
        self._connection_pool: Optional[redis.ConnectionPool] = None
        self._is_connected = False
        self._shutdown_event = asyncio.Event()
    
    async def connect(self, max_retries: int = 3, retry_delay: int = 1) -> bool:
        """
        Connect to Redis with retry logic and health check
        
        Args:
            max_retries: Maximum connection retry attempts
            retry_delay: Delay between retries in seconds
            
        Returns:
            bool: True if connected successfully
        """
        if self._is_connected:
            logger.info("Redis already connected")
            return True
        
        for attempt in range(max_retries + 1):
            try:
                logger.info(f"Connecting to Redis (attempt {attempt + 1}/{max_retries + 1})...")
                
                # Create connection pool for better connection management
                self._connection_pool = redis.ConnectionPool.from_url(
                    settings.REDIS_URL,
                    max_connections=20,
                    retry_on_timeout=True,
                    socket_keepalive=True,
                    socket_keepalive_options={},
                    health_check_interval=30,
                    decode_responses=True
                )
                
                # Create Redis client
                self._redis = redis.Redis(
                    connection_pool=self._connection_pool,
                    socket_connect_timeout=5,
                    socket_timeout=5
                )
                
                # Test connection with ping
                await self._redis.ping()
                
                self._is_connected = True
                logger.info("✅ Redis connected successfully")
                
                # Start background health check
                asyncio.create_task(self._health_check_loop())
                
                return True
                
            except Exception as e:
                logger.error(f"❌ Redis connection attempt {attempt + 1} failed: {e}")
                
                if attempt < max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error("❌ All Redis connection attempts failed")
                    return False
        
        return False
    
    async def disconnect(self, timeout: int = 5) -> None:
        """
        Gracefully disconnect from Redis
        
        Args:
            timeout: Maximum time to wait for graceful shutdown
        """
        if not self._is_connected:
            logger.info("Redis already disconnected")
            return
        
        logger.info("🔄 Disconnecting from Redis...")
        
        # Signal shutdown to background tasks
        self._shutdown_event.set()
        
        try:
            if self._redis:
                # Close Redis client gracefully
                await asyncio.wait_for(self._redis.aclose(), timeout=timeout)
                
            if self._connection_pool:
                # Close connection pool
                await asyncio.wait_for(self._connection_pool.aclose(), timeout=timeout)
                
            self._is_connected = False
            logger.info("✅ Redis disconnected successfully")
            
        except asyncio.TimeoutError:
            logger.warning("⚠️ Redis disconnect timed out, forcing closure")
            self._is_connected = False
        except Exception as e:
            logger.error(f"❌ Error during Redis disconnect: {e}")
            self._is_connected = False
    
    async def health_check(self) -> bool:
        """
        Check if Redis connection is healthy
        
        Returns:
            bool: True if healthy, False otherwise
        """
        if not self._is_connected or not self._redis:
            return False
        
        try:
            await self._redis.ping()
            return True
        except Exception as e:
            logger.warning(f"Redis health check failed: {e}")
            return False
    
    async def _health_check_loop(self):
        """Background task to monitor Redis health and attempt reconnection"""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                if self._shutdown_event.is_set():
                    break
                
                if not await self.health_check():
                    logger.warning("Redis health check failed, attempting reconnection...")
                    self._is_connected = False
                    
                    # Attempt reconnection
                    if await self.connect(max_retries=3):
                        logger.info("Redis reconnection successful")
                    else:
                        logger.error("Redis reconnection failed")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in Redis health check loop: {e}")
                await asyncio.sleep(5)  # Brief pause before retrying
    
    def get_client(self) -> redis.Redis:
        """
        Get Redis client instance
        
        Returns:
            Redis client
            
        Raises:
            RuntimeError: If Redis is not connected
        """
        if not self._is_connected or not self._redis:
            raise RuntimeError("Redis is not connected. Call connect() first.")
        
        return self._redis
    
    @property
    def is_connected(self) -> bool:
        """Check if Redis is connected"""
        return self._is_connected
    
    @asynccontextmanager
    async def get_session(self):
        """
        Context manager for Redis operations with automatic error handling
        
        Usage:
            async with redis_manager.get_session() as redis_client:
                await redis_client.set("key", "value")
        """
        if not self._is_connected:
            raise RuntimeError("Redis is not connected")
        
        try:
            yield self._redis
        except Exception as e:
            logger.error(f"Redis operation failed: {e}")
            # Check if we need to reconnect
            if not await self.health_check():
                logger.warning("Redis connection lost, marking as disconnected")
                self._is_connected = False
            raise


# Global Redis manager instance
redis_manager = RedisManager()