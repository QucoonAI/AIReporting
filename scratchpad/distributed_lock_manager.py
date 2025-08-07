import asyncio
import uuid
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from app.core.utils import logger
from .redis_managers.factory import RedisServiceFactory

class DistributedLock:
    """Distributed lock implementation using Redis"""
    
    def __init__(self, redis_client, lock_key: str, lock_timeout: int = 30, acquire_timeout: int = 10):
        self.redis_client = redis_client
        self.lock_key = f"lock:{lock_key}"
        self.lock_timeout = lock_timeout  # seconds
        self.acquire_timeout = acquire_timeout  # seconds
        self.lock_identifier = str(uuid.uuid4())
        self.acquired = False
    
    async def acquire(self) -> bool:
        """Acquire the distributed lock"""
        end_time = datetime.now() + timedelta(seconds=self.acquire_timeout)
        
        while datetime.now() < end_time:
            # Try to acquire lock
            result = await self.redis_client.set(
                self.lock_key,
                self.lock_identifier,
                ex=self.lock_timeout,
                nx=True  # Only set if key doesn't exist
            )
            
            if result:
                self.acquired = True
                logger.debug(f"Acquired distributed lock: {self.lock_key}")
                return True
            
            # Wait before retrying
            await asyncio.sleep(0.1)
        
        logger.warning(f"Failed to acquire distributed lock: {self.lock_key}")
        return False
    
    async def release(self) -> bool:
        """Release the distributed lock"""
        if not self.acquired:
            return True
        
        # Lua script to atomically check and delete the lock
        lua_script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
        """
        
        try:
            result = await self.redis_client.eval(
                lua_script,
                1,
                self.lock_key,
                self.lock_identifier
            )
            
            if result:
                self.acquired = False
                logger.debug(f"Released distributed lock: {self.lock_key}")
                return True
            else:
                logger.warning(f"Lock was not owned by this process: {self.lock_key}")
                return False
                
        except Exception as e:
            logger.error(f"Error releasing lock {self.lock_key}: {e}")
            return False
    
    async def extend(self, additional_time: int = 30) -> bool:
        """Extend the lock timeout"""
        if not self.acquired:
            return False
        
        lua_script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("EXPIRE", KEYS[1], ARGV[2])
        else
            return 0
        end
        """
        
        try:
            result = await self.redis_client.eval(
                lua_script,
                1,
                self.lock_key,
                self.lock_identifier,
                additional_time
            )
            
            if result:
                logger.debug(f"Extended lock timeout: {self.lock_key}")
                return True
            else:
                self.acquired = False
                return False
                
        except Exception as e:
            logger.error(f"Error extending lock {self.lock_key}: {e}")
            return False

class DistributedLockManager:
    """Manager for distributed locks"""
    
    def __init__(self, redis_factory: RedisServiceFactory):
        self.redis_client = redis_factory.get_redis_client()
        self.active_locks: Dict[str, DistributedLock] = {}
    
    @asynccontextmanager
    async def acquire_lock(
        self, 
        lock_key: str, 
        lock_timeout: int = 30, 
        acquire_timeout: int = 10
    ):
        """Context manager for acquiring and releasing locks"""
        lock = DistributedLock(
            self.redis_client, 
            lock_key, 
            lock_timeout, 
            acquire_timeout
        )
        
        try:
            acquired = await lock.acquire()
            if not acquired:
                raise Exception(f"Failed to acquire lock: {lock_key}")
            
            self.active_locks[lock_key] = lock
            yield lock
            
        finally:
            await lock.release()
            if lock_key in self.active_locks:
                del self.active_locks[lock_key]
    
    async def is_locked(self, lock_key: str) -> bool:
        """Check if a resource is currently locked"""
        full_key = f"lock:{lock_key}"
        result = await self.redis_client.get(full_key)
        return result is not None
    
    async def get_lock_info(self, lock_key: str) -> Optional[Dict[str, Any]]:
        """Get information about a lock"""
        full_key = f"lock:{lock_key}"
        
        lock_value = await self.redis_client.get(full_key)
        if not lock_value:
            return None
        
        ttl = await self.redis_client.ttl(full_key)
        
        return {
            "lock_key": lock_key,
            "lock_identifier": lock_value,
            "ttl_seconds": ttl,
            "expires_at": (datetime.now() + timedelta(seconds=ttl)).isoformat() if ttl > 0 else None
        }
    
    async def cleanup_expired_locks(self) -> int:
        """Clean up any expired locks (should be automatic, but just in case)"""
        pattern = "lock:*"
        keys = await self.redis_client.keys(pattern)
        
        cleaned_count = 0
        for key in keys:
            ttl = await self.redis_client.ttl(key)
            if ttl == -2:  # Key doesn't exist
                cleaned_count += 1
            elif ttl == -1:  # Key exists but has no expiry (shouldn't happen)
                await self.redis_client.delete(key)
                cleaned_count += 1
        
        if cleaned_count > 0:
            logger.info(f"Cleaned up {cleaned_count} expired locks")
        
        return cleaned_count
    
    def create_data_source_lock_key(self, user_id: int, data_source_id: int, operation: str) -> str:
        """Create standardized lock key for data source operations"""
        return f"datasource:{user_id}:{data_source_id}:{operation}"
    
    def create_user_operation_lock_key(self, user_id: int, operation: str) -> str:
        """Create standardized lock key for user-level operations"""
        return f"user:{user_id}:{operation}"

# Global lock manager instance
lock_manager = None

def get_lock_manager(redis_factory: RedisServiceFactory) -> DistributedLockManager:
    """Get or create lock manager instance"""
    global lock_manager
    if lock_manager is None:
        lock_manager = DistributedLockManager(redis_factory)
    return lock_manager

def require_lock(lock_key_generator):
    """Decorator that requires a distributed lock for the operation"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # Assume first argument contains user info and extract lock key
            lock_key = lock_key_generator(*args, **kwargs)
            
            async with lock_manager.acquire_lock(lock_key, lock_timeout=60, acquire_timeout=15):
                return await func(*args, **kwargs)
        
        return wrapper
    return decorator

# Example usage decorators for common operations
def require_data_source_update_lock(func):
    """Decorator for data source update operations"""
    async def wrapper(self, data_source_id: int, user_id: int, *args, **kwargs):
        lock_key = lock_manager.create_data_source_lock_key(user_id, data_source_id, "update")
        
        async with lock_manager.acquire_lock(lock_key, lock_timeout=120, acquire_timeout=30):
            return await func(self, data_source_id, user_id, *args, **kwargs)
    
    return wrapper

def require_user_limit_check_lock(func):
    """Decorator for user limit checking operations"""
    async def wrapper(self, user_id: int, *args, **kwargs):
        lock_key = lock_manager.create_user_operation_lock_key(user_id, "limit_check")
        
        async with lock_manager.acquire_lock(lock_key, lock_timeout=30, acquire_timeout=10):
            return await func(self, user_id, *args, **kwargs)
    
    return wrapper