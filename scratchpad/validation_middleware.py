from typing import Dict, Any, Optional, Callable
from datetime import datetime, timedelta
from functools import wraps
from fastapi import HTTPException, status, Depends
from pydantic import BaseModel, validator
from app.core.utils import logger
from app.models.user import User

class ValidationCache:
    """Cache for validation results to avoid repeated validations"""
    
    def __init__(self, default_ttl_minutes: int = 10):
        self._cache: Dict[str, Dict[str, Any]] = {}
        self.default_ttl = timedelta(minutes=default_ttl_minutes)
    
    def _generate_cache_key(self, user_id: int, operation: str, **kwargs) -> str:
        """Generate cache key from parameters"""
        key_parts = [str(user_id), operation]
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}:{v}")
        return ":".join(key_parts)
    
    def get(self, user_id: int, operation: str, **kwargs) -> Optional[Dict[str, Any]]:
        """Get cached validation result"""
        cache_key = self._generate_cache_key(user_id, operation, **kwargs)
        
        if cache_key in self._cache:
            cached_data = self._cache[cache_key]
            
            # Check if cache is still valid
            if datetime.now() < cached_data["expires_at"]:
                logger.debug(f"Validation cache hit for {operation}")
                return cached_data["result"]
            else:
                # Remove expired cache entry
                del self._cache[cache_key]
        
        return None
    
    def set(self, user_id: int, operation: str, result: Dict[str, Any], ttl: Optional[timedelta] = None, **kwargs):
        """Store validation result in cache"""
        cache_key = self._generate_cache_key(user_id, operation, **kwargs)
        expiry_time = datetime.now() + (ttl or self.default_ttl)
        
        self._cache[cache_key] = {
            "result": result,
            "expires_at": expiry_time,
            "cached_at": datetime.now()
        }
        
        logger.debug(f"Cached validation result for {operation}")
    
    def invalidate(self, user_id: int, operation: str, **kwargs):
        """Invalidate specific cache entry"""
        cache_key = self._generate_cache_key(user_id, operation, **kwargs)
        if cache_key in self._cache:
            del self._cache[cache_key]
            logger.debug(f"Invalidated validation cache for {operation}")
    
    def cleanup_expired(self):
        """Remove expired cache entries"""
        current_time = datetime.now()
        expired_keys = [
            key for key, data in self._cache.items()
            if current_time >= data["expires_at"]
        ]
        
        for key in expired_keys:
            del self._cache[key]
        
        if expired_keys:
            logger.debug(f"Cleaned up {len(expired_keys)} expired validation cache entries")

# Global validation cache instance
validation_cache = ValidationCache()

class DataSourceValidationRequest(BaseModel):
    """Enhanced validation request model"""
    data_source_name: str
    data_source_type: str
    data_source_url: Optional[str] = None
    file_size: Optional[int] = None
    
    @validator('data_source_name')
    def validate_name(cls, v):
        if not v or not v.strip():
            raise ValueError("Data source name cannot be empty")
        if len(v.strip()) > 255:
            raise ValueError("Data source name too long (max 255 characters)")
        return v.strip()
    
    @validator('data_source_type')
    def validate_type(cls, v):
        allowed_types = ['postgres', 'mysql', 'csv', 'xlsx', 'pdf']
        if v.lower() not in allowed_types:
            raise ValueError(f"Unsupported data source type: {v}")
        return v.lower()

def cached_validation(operation: str, ttl_minutes: int = 10):
    """Decorator for caching validation results"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Extract user_id from arguments (assuming it's the first argument)
            user_id = args[0] if args else kwargs.get('user_id')
            
            if not user_id:
                # If no user_id, skip caching
                return await func(*args, **kwargs)
            
            # Check cache first
            cache_kwargs = {k: v for k, v in kwargs.items() if k != 'user_id'}
            cached_result = validation_cache.get(user_id, operation, **cache_kwargs)
            
            if cached_result is not None:
                return cached_result
            
            # Execute validation
            try:
                result = await func(*args, **kwargs)
                
                # Cache successful result
                validation_cache.set(
                    user_id, 
                    operation, 
                    result, 
                    ttl=timedelta(minutes=ttl_minutes),
                    **cache_kwargs
                )
                
                return result
                
            except Exception as e:
                # Don't cache errors
                raise e
        
        return wrapper
    return decorator

class OwnershipValidator:
    """Utility for validating data source ownership"""
    
    def __init__(self, data_source_service):
        self.data_source_service = data_source_service
    
    @cached_validation("ownership_check", ttl_minutes=5)
    async def validate_ownership(self, user_id: int, data_source_id: int) -> Dict[str, Any]:
        """Validate that user owns the data source"""
        try:
            data_source = await self.data_source_service.get_data_source_by_id(data_source_id)
            
            if data_source.data_source_user_id != user_id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You don't have permission to access this data source"
                )
            
            return {
                "valid": True,
                "data_source": data_source,
                "validated_at": datetime.now().isoformat()
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Ownership validation failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to validate ownership"
            )

class UserLimitValidator:
    """Utility for validating user limits"""
    
    def __init__(self, data_source_service):
        self.data_source_service = data_source_service
        self.max_data_sources = 10
    
    @cached_validation("user_limits", ttl_minutes=2)
    async def validate_user_limits(self, user_id: int) -> Dict[str, Any]:
        """Validate user hasn't exceeded limits"""
        try:
            existing_sources = await self.data_source_service.data_source_repo.get_user_data_sources(user_id)
            current_count = len(existing_sources)
            
            if current_count >= self.max_data_sources:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Maximum number of data sources ({self.max_data_sources}) exceeded"
                )
            
            return {
                "valid": True,
                "current_count": current_count,
                "max_allowed": self.max_data_sources,
                "remaining": self.max_data_sources - current_count,
                "validated_at": datetime.now().isoformat()
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"User limit validation failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to validate user limits"
            )

class NameUniquenessValidator:
    """Utility for validating name uniqueness"""
    
    def __init__(self, data_source_service):
        self.data_source_service = data_source_service
    
    @cached_validation("name_uniqueness", ttl_minutes=1)
    async def validate_name_uniqueness(
        self, 
        user_id: int, 
        name: str, 
        exclude_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Validate data source name is unique for user"""
        try:
            existing_source = await self.data_source_service.data_source_repo.get_data_source_by_name(
                user_id, name
            )
            
            if existing_source and (exclude_id is None or existing_source.data_source_id != exclude_id):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Data source with name '{name}' already exists"
                )
            
            return {
                "valid": True,
                "name": name,
                "is_unique": True,
                "validated_at": datetime.now().isoformat()
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Name uniqueness validation failed: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to validate name uniqueness"
            )

def require_ownership(data_source_service):
    """Dependency for validating data source ownership"""
    async def validate_ownership_dependency(
        data_source_id: int,
        current_user: User = Depends(get_current_user)
    ):
        validator = OwnershipValidator(data_source_service)
        result = await validator.validate_ownership(current_user["user_id"], data_source_id)
        return result["data_source"]
    
    return validate_ownership_dependency

def validate_user_limits_dependency(data_source_service):
    """Dependency for validating user limits"""
    async def validate_limits(current_user: User = Depends(get_current_user)):
        validator = UserLimitValidator(data_source_service)
        await validator.validate_user_limits(current_user["user_id"])
        return True
    
    return validate_limits