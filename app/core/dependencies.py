from typing import Dict, Any
import boto3
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.config.database import SessionDep
from app.config.redis import redis_manager
from app.config.settings import get_settings
from app.services.redis_managers.factory import RedisServiceFactory
from app.services.background_services.email_service import EmailService
from app.services.user import UserService
from app.services.temp_data_source import TempDataSourceService
from app.services.data_source import DataSourceService
from app.services.data_source_update import DataSourceUpdateService
from app.services.chat import ChatService
from app.services.llm_services.llm import MockLLMService
from app.repositories.user import UserRepository
from app.repositories.data_source import DataSourceRepository
from app.repositories.chat import ChatRepository
from app.repositories.message import MessageRepository


security = HTTPBearer()
settings = get_settings()


def get_user_repo(db_session: SessionDep = SessionDep) -> UserRepository:  # type: ignore
    return UserRepository(db_session=db_session)

def get_data_source_repo(db_session: SessionDep = SessionDep) -> DataSourceRepository:  # type: ignore
    """Dependency to get DataSourceRepository instance"""
    return DataSourceRepository(db_session)

def get_chat_repo() -> ChatRepository:
    """Dependency to get ChatRepository instance"""
    return ChatRepository()

def get_message_repo() -> MessageRepository:
    """Dependency to get MessageRepository instance"""
    return MessageRepository()




def get_redis_factory_service() -> RedisServiceFactory:
    return RedisServiceFactory(redis_client=redis_manager.get_client())

def get_email_service() -> EmailService:
    return EmailService(settings)

def get_user_service(
    db_session: SessionDep = SessionDep, # type: ignore
    email_service: EmailService = Depends(get_email_service),
    redis_factory: RedisServiceFactory = Depends(get_redis_factory_service),
) -> UserService:
    return UserService(
        db_session=db_session,
        email_service=email_service,
        redis_factory=redis_factory,
    )

async def get_temp_data_source_service(
    redis_factory: RedisServiceFactory = Depends(get_redis_factory_service)
) -> TempDataSourceService:
    return TempDataSourceService(redis_factory)

def get_data_source_service(
    data_source_repo: DataSourceRepository = Depends(get_data_source_repo),
    temp_service: TempDataSourceService = Depends(get_temp_data_source_service)
) -> DataSourceService:
    """Dependency to get DataSourceService instance"""
    return DataSourceService(data_source_repo, temp_service)

async def get_data_source_update_service(
    data_source_service: DataSourceService = Depends(get_data_source_service),
    temp_service: TempDataSourceService = Depends(get_temp_data_source_service)
) -> DataSourceUpdateService:
    return DataSourceUpdateService(data_source_service, temp_service)

def get_llm_service() -> MockLLMService:
    """Dependency to get MockLLMService instance"""
    return MockLLMService()

def get_chat_service(
    chat_repo: ChatRepository = Depends(get_chat_repo),
    message_repo: MessageRepository = Depends(get_message_repo),
    data_source_repo: DataSourceRepository = Depends(get_data_source_repo),
    llm_service: MockLLMService = Depends(get_llm_service),
    redis_factory: RedisServiceFactory = Depends(get_redis_factory_service),
) -> ChatService:
    """Dependency to get ChatService instance"""
    return ChatService(chat_repo, message_repo, data_source_repo, llm_service, redis_factory)





async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    redis_factory = Depends(get_redis_factory_service)
) -> Dict[str, Any]:
    """
    Get current authenticated user from access token.
    Raises HTTPException if token is invalid or expired.
    """
    try:
        token = credentials.credentials
        payload = await redis_factory.auth_service.verify_token(token, "access")
        return {
            "user_id": int(payload["sub"]),
            "session_id": payload["session_id"],
            "roles": payload.get("role", []),
            "session_data": payload.get("session_data", {})
        }
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

def require_roles(required_roles: list):
    """
    Dependency factory to require specific roles.
    Usage: @router.get("/admin", dependencies=[Depends(require_roles(["admin"]))])
    """
    def role_checker(current_user: Dict[str, Any] = Depends(get_current_user)):
        user_roles = current_user.get("roles", [])
        if not any(role in user_roles for role in required_roles):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions"
            )
        return current_user
    return role_checker

