from fastapi import APIRouter, Depends, HTTPException, status, Request
from typing import Dict, Any
from app.services.user import UserService
from app.schemas.auth import (
    LoginRequest, LoginResponse, RefreshTokenRequest, RefreshTokenResponse,
    LogoutResponse, SessionsResponse, RevokeSessionRequest, RevokeSessionResponse
)
from app.core.dependencies import get_current_user, get_user_service
from app.core.utils import logger


router = APIRouter(prefix="/api/v1/auth", tags=["authentication"])


@router.post(
    "/login",
    response_model=LoginResponse,
    summary="User login",
    description="Authenticate user and create session with access and refresh tokens."
)
async def login(
    login_data: LoginRequest,
    request: Request,
    user_service: UserService = Depends(get_user_service),
) -> LoginResponse:
    """
    User login endpoint.
    
    - **user_email**: Valid email address
    - **user_password**: User's password
    - **device_info**: Optional device information
    
    Returns access token, refresh token, and user information.
    Creates a new session stored in Redis.
    """
    try:
        result = await user_service.login_user(login_data, request)

        return LoginResponse(
            message=result["message"],
            access_token=result["access_token"],
            refresh_token=result["refresh_token"],
            token_type=result["token_type"],
            expires_in=result["expires_in"],
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during login"
        )


@router.post(
    "/logout",
    response_model=LogoutResponse,
    summary="User logout",
    description="Logout user and revoke current session."
)
async def logout(
    request: Request,
    current_user: Dict[str, Any] = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service)
) -> LogoutResponse:
    """
    User logout endpoint.
    
    Revokes the current session and invalidates the access token.
    Requires authentication.
    """
    try:
        # Extract token from Authorization header
        authorization = request.headers.get("Authorization")
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authorization header"
            )
        
        token = authorization.split(" ")[1]
        message = await user_service.logout_user(token)
        return LogoutResponse(message=message)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during logout"
        )


@router.post(
    "/refresh",
    response_model=RefreshTokenResponse,
    summary="Refresh access token",
    description="Generate new access token using refresh token."
)
async def refresh_token(
    refresh_data: RefreshTokenRequest,
    user_service: UserService = Depends(get_user_service)
) -> RefreshTokenResponse:
    """
    Refresh access token endpoint.
    
    - **refresh_token**: Valid refresh token
    
    Returns a new access token without requiring re-authentication.
    """
    try:
        result = await user_service.refresh_user_token(refresh_data.refresh_token)
        return RefreshTokenResponse(
            message=result["message"],
            access_token=result["access_token"],
            token_type=result["token_type"],
            expires_in=result["expires_in"]
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while refreshing token"
        )


@router.post(
    "/logout-all",
    response_model=LogoutResponse,
    summary="Logout from all devices",
    description="Logout user from all devices and revoke all sessions."
)
async def logout_all_devices(
    current_user: Dict[str, Any] = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service)
) -> LogoutResponse:
    """
    Logout from all devices endpoint.
    
    Revokes all sessions for the current user across all devices.
    Requires authentication.
    """
    try:
        message = await user_service.logout_all_devices(current_user["user_id"])
        return LogoutResponse(message=message)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while logging out from all devices"
        )


@router.get(
    "/sessions",
    response_model=SessionsResponse,
    summary="List active sessions",
    description="Get all active sessions for the current user."
)
async def get_active_sessions(
    current_user: Dict[str, Any] = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service)
) -> SessionsResponse:
    """
    Get active sessions endpoint.
    
    Returns a list of all active sessions for the current user.
    Includes device information, IP addresses, and last activity.
    """
    try:
        result = await user_service.get_active_sessions(current_user["user_id"])
        return SessionsResponse(
            message=result["message"],
            sessions=result["sessions"],
            total_sessions=result["total_sessions"]
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while retrieving sessions"
        )


@router.post(
    "/revoke-session",
    response_model=RevokeSessionResponse,
    summary="Revoke a session",
    description="Revoke a specific session by session ID."
)
async def revoke_session(
    revoke_data: RevokeSessionRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service)
) -> RevokeSessionResponse:
    """
    Revoke session endpoint.
    
    - **session_id**: ID of the session to revoke
    
    Revokes a specific session for the current user.
    """
    try:
        message = await user_service.revoke_user_session(
            current_user["user_id"], 
            revoke_data.session_id
        )
        return RevokeSessionResponse(message=message)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while revoking session"
        )


