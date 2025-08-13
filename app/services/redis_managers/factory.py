import redis.asyncio as redis
from .auth import AuthService
from .otp import OTPService
from .chat import ChatCacheService
from .temp import TempDataService
from .health import RedisHealthService


class RedisServiceFactory:
    """Factory class to create all Redis services with consistent configuration"""
    
    def __init__(self, redis_client: redis.Redis, app_name: str = "reportai"):
        self.redis_client = redis_client
        self.app_name = app_name
        self._auth_service = None
        self._otp_service = None
        self._chat_cache_service = None
        self._temp_data_service = None
        self._health_service = None
    
    @property
    def auth_service(self) -> AuthService:
        """Get AuthService instance (singleton)"""
        if self._auth_service is None:
            self._auth_service = AuthService(self.redis_client, self.app_name)
        return self._auth_service
    
    @property
    def otp_service(self) -> OTPService:
        """Get OTPService instance (singleton)"""
        if self._otp_service is None:
            self._otp_service = OTPService(self.redis_client, self.app_name)
        return self._otp_service
    
    @property
    def chat_cache_service(self) -> ChatCacheService:
        """Get ChatCacheService instance (singleton)"""
        if self._chat_cache_service is None:
            self._chat_cache_service = ChatCacheService(self.redis_client, app_name=self.app_name)
        return self._chat_cache_service
    
    @property
    def temp_data_service(self) -> TempDataService:
        """Get TempDataService instance (singleton)"""
        if self._temp_data_service is None:
            self._temp_data_service = TempDataService(self.redis_client, self.app_name)
        return self._temp_data_service
    
    @property
    def health_service(self) -> RedisHealthService:
        """Get RedisHealthService instance (singleton)"""
        if self._health_service is None:
            self._health_service = RedisHealthService(self.redis_client, self.app_name)
        return self._health_service

