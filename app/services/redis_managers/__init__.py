
class RedisKeyManager:
    """Centralized Redis key management with distinct prefixes"""
    
    # Key Prefixes for different data types
    AUTH_SESSION_PREFIX = "auth_session"          # Authentication sessions
    USER_SESSION_PREFIX = "user_sessions"         # User session collections
    OTP_PREFIX = "otp"                           # OTP tokens
    OTP_ATTEMPTS_PREFIX = "otp_attempts"         # OTP rate limiting
    CHAT_SESSION_PREFIX = "chat_session"         # Chat session data
    EXTRACTION_PREFIX = "data_source_extraction" # Data source extraction data 
    USER_EXTRACTIONS_PREFIX = "user_extractions_list" # Data
    TEMP_DATA_PREFIX = "temp_data"               # Temporary data
    RATE_LIMIT_PREFIX = "rate_limit"             # Rate limiting counters
    LOCK_PREFIX = "lock"                         # Distributed locks
    
    def __init__(self, app_name: str = "reportai"):
        self.app_name = app_name
    
    def _build_key(self, prefix: str, *parts: str) -> str:
        """Build a Redis key with app namespace and prefix"""
        key_parts = [self.app_name, prefix] + list(parts)
        return ":".join(key_parts)
    
    # Authentication Keys
    def auth_session_key(self, session_id: str) -> str:
        """Key for authentication session data"""
        return self._build_key(self.AUTH_SESSION_PREFIX, session_id)
    
    def user_sessions_key(self, user_id: int) -> str:
        """Key for user's active sessions collection"""
        return self._build_key(self.USER_SESSION_PREFIX, str(user_id))
    
    # OTP Keys
    def otp_key(self, otp_type: str, identifier: str) -> str:
        """Key for OTP token storage"""
        return self._build_key(self.OTP_PREFIX, otp_type, identifier)
    
    def otp_attempts_key(self, otp_type: str, identifier: str) -> str:
        """Key for OTP attempt rate limiting"""
        return self._build_key(self.OTP_ATTEMPTS_PREFIX, otp_type, identifier)
    
    # Chat Keys
    def chat_session_key(self, session_id: str) -> str:
        """Key for chat session data"""
        return self._build_key(self.CHAT_SESSION_PREFIX, session_id)
    
    def chat_session_lock_key(self, session_id: str) -> str:
        """Key for chat session operation locks"""
        return self._build_key(self.LOCK_PREFIX, "chat", session_id)
    
    # Temporary Data Keys
    def temp_data_key(self, operation: str, identifier: str) -> str:
        """Key for temporary data storage"""
        return self._build_key(self.TEMP_DATA_PREFIX, operation, identifier)
    
    # Rate Limiting Keys
    def rate_limit_key(self, user_id: int, action: str) -> str:
        """Key for rate limiting by user and action"""
        return self._build_key(self.RATE_LIMIT_PREFIX, str(user_id), action)
    
    # Pattern Methods
    def get_auth_session_pattern(self) -> str:
        """Pattern to match all auth session keys"""
        return self._build_key(self.AUTH_SESSION_PREFIX, "*")
    
    def get_user_sessions_pattern(self) -> str:
        """Pattern to match all user session keys"""
        return self._build_key(self.USER_SESSION_PREFIX, "*")
    
    def get_chat_session_pattern(self) -> str:
        """Pattern to match all chat session keys"""
        return self._build_key(self.CHAT_SESSION_PREFIX, "*")
    
    def get_temp_data_pattern(self) -> str:
        """Pattern to match all temporary data keys"""
        return self._build_key(self.TEMP_DATA_PREFIX, "*")

# Example usage and dependency injection
"""
# In your main application setup:

redis_client = redis.Redis(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    password=settings.REDIS_PASSWORD,
    decode_responses=True
)

# Create service factory
redis_services = RedisServiceFactory(redis_client, "reportai")

# Use services
auth_service = redis_services.auth_service
otp_service = redis_services.otp_service
chat_cache = redis_services.chat_cache_service
temp_data = redis_services.temp_data_service
health = redis_services.health_service

# In your FastAPI dependencies:
def get_auth_service() -> AuthService:
    return redis_services.auth_service

def get_otp_service() -> OTPService:
    return redis_services.otp_service

def get_chat_cache_service() -> ChatCacheService:
    return redis_services.chat_cache_service
"""


# Redis Key Structure Documentation
"""
Redis Key Structure for ReportAI:
================================

Authentication:
- reportai:auth_session:{session_id}        - JWT session data
- reportai:user_sessions:{user_id}          - Set of active session IDs per user

OTP:
- reportai:otp:{type}:{identifier}          - OTP token data
- reportai:otp_attempts:{type}:{identifier} - Rate limiting for OTP attempts

Chat:
- reportai:chat_session:{session_id}        - Chat context + metadata + tokens
- reportai:lock:chat:{session_id}           - Chat operation locks

Temporary Data:
- reportai:temp_data:{operation}:{id}       - Temporary storage

Examples:
- reportai:auth_session:550e8400-e29b-41d4-a716-446655440000
- reportai:user_sessions:12345
- reportai:otp:email_verification:user@example.com
- reportai:otp_attempts:email_verification:user@example.com
- reportai:chat_session:abc123-def456-ghi789
- reportai:temp_data:file_upload:upload123
- reportai:lock:chat:abc123-def456-ghi789

Benefits:
✅ No key conflicts between services
✅ Easy pattern matching and cleanup
✅ Clear service boundaries
✅ Efficient monitoring and debugging
✅ Environment separation (dev/staging/prod)
✅ Microservice ready
"""
