from typing import Dict, Any
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from config.database import SessionDep
from config.redis import redis_manager
from config.settings import get_settings
from services.redis import AsyncRedisService
from services.email import EmailService
from services.user import UserService
from services.data_source import DataSourceService
from repositories.user import UserRepository
from repositories.data_source import DataSourceRepository


security = HTTPBearer()
settings = get_settings()


def get_user_repo(db_session: SessionDep = SessionDep) -> UserRepository:  # type: ignore
    return UserRepository(db_session=db_session)

def get_data_source_repo(db_session: SessionDep = SessionDep) -> DataSourceRepository:  # type: ignore
    """Dependency to get DataSourceRepository instance"""
    return DataSourceRepository(db_session)

def get_redis_service() -> AsyncRedisService:
    return AsyncRedisService(redis_client=redis_manager.get_client())

def get_email_service() -> EmailService:
    return EmailService(settings)

def get_user_service(
    db_session: SessionDep = SessionDep, # type: ignore
    redis_service: AsyncRedisService = Depends(get_redis_service),
    email_service: EmailService = Depends(get_email_service)
) -> UserService: # type: ignore
    return UserService(
        db_session=db_session,
        redis_service=redis_service,
        email_service=email_service
    )

def get_data_source_service(
    data_source_repo: DataSourceRepository = Depends(get_data_source_repo)
) -> DataSourceService:
    """Dependency to get DataSourceService instance"""
    return DataSourceService(data_source_repo)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    redis_service = Depends(get_redis_service)
) -> Dict[str, Any]:
    """
    Get current authenticated user from access token.
    Raises HTTPException if token is invalid or expired.
    """
    try:
        token = credentials.credentials
        payload = await redis_service.verify_token(token, "access")
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



