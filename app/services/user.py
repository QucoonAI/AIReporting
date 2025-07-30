import json
from typing import Optional, Dict, Any
from sqlmodel import select
from fastapi import HTTPException, status, BackgroundTasks, Request
from passlib.context import CryptContext
from models import User, UserProfile
from core.exceptions import (
    UserNotFoundError,
    InvalidOTPError,
    RateLimitExceededError
)
from core.utils import logger
from config.database import SessionDep
from services.redis import AsyncRedisService
from app.services.email_service import (
    EmailService, send_verification_email_task, send_password_reset_email_task,
    send_password_change_email_task
)
from schemas.user import (
    UserCreateRequest, UserUpdateRequest, ChangePasswordRequest,
    ChangePasswordConfirmRequest, PasswordResetRequest, PasswordResetConfirmRequest,
    VerifyUserRequest, VerifyUserConfirmRequest
)
from schemas.auth import LoginRequest


class UserService:
    def __init__(self, db_session: SessionDep, redis_service: AsyncRedisService, email_service: EmailService): # type: ignore
        self.session: SessionDep = db_session # type: ignore
        self.redis: AsyncRedisService = redis_service
        self.email_service: EmailService = email_service
        self.otp_expiry_minutes = 30
        self.max_otp_attempts = 5  # Maximum OTP attempts per hour
        
    def _hash_password(self, password: str) -> str:
        """Hash a password using bcrypt."""
        return self.pwd_context.hash(password)
    
    def _verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash."""
        pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        return pwd_context.verify(plain_password, hashed_password)
    
    async def _check_otp_rate_limit(self, email: str, otp_type: str) -> None:
        """Check if user has exceeded OTP request rate limit."""
        attempts = await self.redis.get_otp_attempts(email, otp_type)
        if attempts >= self.max_otp_attempts:
            raise RateLimitExceededError(
                f"Too many {otp_type} attempts. Please try again later."
            )
    
    def _send_otp_email(self, background_tasks: BackgroundTasks, user: User, otp: str, otp_type: str) -> None:
        """Send OTP email based on type."""
        user_name = f"{user.user_first_name} {user.user_last_name}"
        
        if otp_type == "email_verification":
            background_tasks.add_task(
                send_verification_email_task,
                self.email_service,
                user.user_email,
                user_name,
                otp
            )
        elif otp_type == "password_reset":
            background_tasks.add_task(
                send_password_reset_email_task,
                self.email_service,
                user.user_email,
                user_name,
                otp
            )
        elif otp_type == "password_change":
            background_tasks.add_task(
                send_password_change_email_task,
                self.email_service,
                user.user_email,
                user_name,
                otp
            )

    async def create_user(self, user_data: UserCreateRequest, background_tasks: BackgroundTasks) -> User:
        """Create a new user."""
        # Check if user already exists
        statement = (select(User).where(User.user_email == user_data.user_email))
        result = await self.session.exec(statement=statement)
        existing_user = result.first()
        
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User with this email already exists"
            )
        
        # Create user
        hashed_password = self._hash_password(user_data.user_password)
        
        user = User(
            user_email=user_data.user_email,
            user_first_name=user_data.user_first_name,
            user_last_name=user_data.user_last_name,
            user_password=hashed_password,
            user_is_verified=False,
            user_is_active=False
        )
        
        self.session.add(user)
        await self.session.flush()
        
        # Create user profile if provided
        if user_data.user_profile:
            profile = UserProfile(
                user_profile_user_id=user.user_id,
                user_profile_bio=user_data.user_profile.user_profile_bio,
                user_profile_avatar=user_data.user_profile.user_profile_avatar,
                user_phone_number=user_data.user_profile.user_phone_number
            )
            self.session.add(profile)
            await self.session.commit()
            await self.session.refresh(user)
        
        # Generate and send verification OTP
        otp = await self.redis.generate_otp()
        otp_key = f"email_verification:{user.user_email}"
        
        await self.redis.store_otp(otp_key, user.user_id, otp, self.otp_expiry_minutes)
        self._send_otp_email(background_tasks, user, otp, "email_verification")
        
        return user

    async def verify_user(self, request: VerifyUserRequest, background_tasks: BackgroundTasks) -> str:
        """Send verification OTP to user."""
        try:
            statement = (select(User).where(User.user_email == request.user_email))
            result = await self.session.exec(statement)
            user = result.first()
            
            if not user:
                raise UserNotFoundError(request.user_email)
            
            if user.user_is_verified:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="User is already verified"
                )
            
            # Check rate limit - will raise RateLimitExceededError if exceeded
            await self._check_otp_rate_limit(request.user_email, "email_verification")
            
            # Generate and send verification OTP
            otp = await self.redis.generate_otp()
            otp_key = f"email_verification:{request.user_email}"
            
            await self.redis.store_otp(otp_key, user.user_id, otp, self.otp_expiry_minutes)
            await self.redis.increment_otp_attempts(request.user_email, "email_verification")
            self._send_otp_email(background_tasks, user, otp, "email_verification")
            
            return "Verification OTP sent successfully"
        
        except (UserNotFoundError, RateLimitExceededError, HTTPException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error sending verification to {request.user_email}: {e}")
            raise

    async def verify_user_confirm(self, request: VerifyUserConfirmRequest) -> str:
        try:
            """Confirm user email verification with OTP."""
            otp_key = f"email_verification:{request.user_email}"
            
            token_data = await self.redis.verify_otp(otp_key, request.otp)
            if not token_data:
                raise InvalidOTPError("Invalid or expired OTP")
            
            user = await self.session.get(User, token_data["user_id"])
            if not user:
                raise UserNotFoundError(token_data["user_id"])
            
            # Update user verification status
            user.user_is_verified = True
            user.user_is_active = True

            self.session.add(user)
            await self.session.commit()
            
            # Clean up OTP and reset attempts
            await self.redis.delete_otp(otp_key)
            await self.redis.reset_otp_attempts(request.user_email, "email_verification")
            
            return "Email verified successfully"
        
        except (InvalidOTPError, UserNotFoundError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error confirming verification for {request.user_email}: {e}")
            raise

    async def update_user(self, user_id: int, update_data: UserUpdateRequest) -> User:
        """Update user information."""
        try:
            user = await self.session.get(User, user_id)
            if not user:
                raise UserNotFoundError(user_id)
            
            if not user.user_is_active:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="User account is deactivated"
                )
            
            # Update user fields
            if update_data.user_first_name is not None:
                user.user_first_name = update_data.user_first_name
            if update_data.user_last_name is not None:
                user.user_last_name = update_data.user_last_name
            
            self.session.add(user)
            
            # Update user profile if provided
            if update_data.user_profile is not None:
                statement = (select(UserProfile).where(UserProfile.user_profile_user_id == user_id))
                result = await self.session.exec(statement)
                profile = result.first()
                
                if not profile:
                    # Create new profile if it doesn't exist
                    profile = UserProfile(user_profile_user_id=user_id)
                    self.session.add(profile)
                
                # Update profile fields
                if update_data.user_profile.user_profile_bio is not None:
                    profile.user_profile_bio = update_data.user_profile.user_profile_bio
                if update_data.user_profile.user_profile_avatar is not None:
                    profile.user_profile_avatar = update_data.user_profile.user_profile_avatar
                if update_data.user_profile.user_phone_number is not None:
                    profile.user_phone_number = update_data.user_profile.user_phone_number
                
                self.session.add(profile)
            
            await self.session.commit()
            await self.session.refresh(user)
            
            return user
        
        except (UserNotFoundError, HTTPException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error updating user {user_id}: {e}")
            raise

    async def delete_user(self, user_id: int) -> str:
        """Soft delete a user (deactivate account)."""
        try:
            user = await self.session.get(User, user_id)
            if not user:
                raise UserNotFoundError(user_id)
            
            if not user.user_is_active:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="User account is already deactivated"
                )
            
            # Soft delete - set is_active to False
            user.user_is_active = False
            self.session.add(user)
            await self.session.commit()
            
            return "User account deactivated successfully"
        
        except (UserNotFoundError, HTTPException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error deleting user {user_id}: {e}")
            raise

    async def change_password(self, user_id: int, request: ChangePasswordRequest, background_tasks: BackgroundTasks) -> str:
        """Initiate password change process."""
        user = await self.session.get(User, user_id)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        if not user.user_is_active:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User account is deactivated"
            )
        
        # Verify current password
        if not self._verify_password(request.current_password, user.user_password):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Current password is incorrect"
            )
        
        # Check rate limit
        await self._check_otp_rate_limit(user.user_email, "password_change")
        
        # Generate OTP and store new password temporarily
        otp = await self.redis.generate_otp()
        otp_key = f"password_change:{user.user_email}"
        
        await self.redis.store_otp(otp_key, user.user_id, otp, self.otp_expiry_minutes)
        
        # Store new password hash temporarily
        new_password_key = f"new_password:{user.user_email}:{otp}"
        await self.redis.store_temp_data(
            new_password_key,
            {"password_hash": self._hash_password(request.new_password)},
            self.otp_expiry_minutes
        )
        
        await self.redis.increment_otp_attempts(user.user_email, "password_change")
        self._send_otp_email(background_tasks, user, otp, "password_change")
        
        return "Password change OTP sent successfully"

    async def change_password_confirm(self, request: ChangePasswordConfirmRequest) -> str:
        """Confirm password change with OTP."""
        otp_key = f"password_change:{request.user_email}"
        
        token_data = await self.redis.verify_otp(otp_key, request.otp)
        if not token_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid or expired OTP"
            )
        
        user = await self.session.get(User, token_data["user_id"])
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        # Get stored new password hash
        new_password_key = f"new_password:{user.user_email}:{request.otp}"
        password_data = await self.redis.get_temp_data(new_password_key)
        
        if not password_data or "password_hash" not in password_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Password change session expired"
            )
        
        # Update password
        user.user_password = password_data["password_hash"]
        self.session.add(user)
        await self.session.commit()
        
        # Clean up OTP and temporary data
        await self.redis.delete_otp(otp_key)
        await self.redis.delete_temp_data(new_password_key)
        await self.redis.reset_otp_attempts(request.user_email, "password_change")
        
        return "Password changed successfully"

    async def password_reset(self, request: PasswordResetRequest, background_tasks: BackgroundTasks) -> str:
        """Initiate password reset process."""
        statement = select(User).where(User.user_email == request.user_email)
        result = await self.session.exec(statement)
        user = result.first()
        
        if not user:
            # Don't reveal if user exists or not for security
            return "If the email exists, a password reset OTP has been sent"
        
        if not user.user_is_active:
            # Still send success message for security
            return "If the email exists, a password reset OTP has been sent"
        
        # Check rate limit
        try:
            await self._check_otp_rate_limit(request.user_email, "password_reset")
        except HTTPException:
            # Return success message even if rate limited for security
            return "If the email exists, a password reset OTP has been sent"
        
        # Generate and send reset OTP
        otp = await self.redis.generate_otp()
        otp_key = f"password_reset:{request.user_email}"
        
        await self.redis.store_otp(otp_key, user.user_id, otp, self.otp_expiry_minutes)
        await self.redis.increment_otp_attempts(request.user_email, "password_reset")
        self._send_otp_email(background_tasks, user, otp, "password_reset")
        
        return "If the email exists, a password reset OTP has been sent"

    async def password_reset_confirm(self, request: PasswordResetConfirmRequest) -> str:
        """Confirm password reset with OTP."""
        otp_key = f"password_reset:{request.user_email}"
        
        token_data = await self.redis.verify_otp(otp_key, request.otp)
        if not token_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid or expired OTP"
            )
        
        user = await self.session.get(User, token_data["user_id"])
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        # Update password
        user.user_password = self._hash_password(request.new_password)
        self.session.add(user)
        await self.session.commit()
        
        # Clean up OTP and reset attempts
        await self.redis.delete_otp(otp_key)
        await self.redis.reset_otp_attempts(request.user_email, "password_reset")
        
        return "Password reset successfully"
    
    # ------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------

    async def authenticate_user(self, email: str, password: str) -> Optional[User]:
        """Authenticate user with email and password."""
        try:
            statement = select(User).where(User.user_email == email)
            result = await self.session.exec(statement)
            user = result.first()
            
            if not user:
                return None
            
            if not user.user_is_active:
                return None
            
            if not user.user_is_verified:
                return None
            
            if not self._verify_password(password, user.user_password):
                return None
            
            return user
        
        except Exception as e:
            logger.error(f"Unexpected error during authentication: {e}")
            return None
    
    async def login_user(self, login_data: LoginRequest, request: Request) -> Dict[str, Any]:
        """Login user and create session."""
        user = await self.authenticate_user(login_data.user_email, login_data.user_password)
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid email or password"
            )
        
        # Get client info
        device_info = login_data.device_info or request.headers.get("User-Agent", "Unknown Device")
        ip_address = request.client.host if request.client else None
        
        # Create tokens and session
        tokens = await self.redis.create_tokens(
            user_id=user.user_id,
            roles=["user"],  # You can modify this based on your role system
            device_info=device_info,
            ip_address=ip_address
        )
        
        return {
            "message": "Login successful",
            "access_token": tokens["access_token"],
            "refresh_token": tokens["refresh_token"],
            "token_type": tokens["token_type"],
            "expires_in": tokens["expires_in"],
            "user": user
        }
    
    async def logout_user(self, token: str) -> str:
        """Logout user and revoke session."""
        await self.redis.revoke_session(token)
        return "Logout successful"

    async def refresh_user_token(self, refresh_token: str) -> Dict[str, Any]:
        """Refresh access token using refresh token."""
        try:
            new_tokens = await self.redis.refresh_access_token(refresh_token)
            return {
                "message": "Token refreshed successfully",
                "access_token": new_tokens["access_token"],
                "token_type": new_tokens["token_type"],
                "expires_in": new_tokens["expires_in"]
            }
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid refresh token"
            )

    async def logout_all_devices(self, user_id: int) -> str:
        """Logout user from all devices."""
        await self.redis.revoke_all_user_sessions(user_id)
        return "Logged out from all devices successfully"

    async def get_active_sessions(self, user_id: int) -> Dict[str, Any]:
        """Get all active sessions for a user."""
        sessions = await self.redis.get_user_sessions(user_id)
        
        return {
            "message": "Active sessions retrieved successfully",
            "sessions": sessions,
            "total_sessions": len(sessions)
        }

    async def revoke_user_session(self, user_id: int, session_id: str) -> str:
        """Revoke a specific user session."""
        # Get user sessions to verify the session belongs to the user
        user_sessions = await self.redis.get_user_sessions(user_id)
        
        # Check if session belongs to the user
        session_exists = False
        for session in user_sessions:
            # We need to check if this session_id exists in the user's sessions
            # Since session data doesn't contain session_id, we need to check Redis directly
            session_data = await self.redis.redis_client.get(f"session:{session_id}")
            if session_data:
                session_info = json.loads(session_data)
                if session_info.get("user_id") == user_id:
                    session_exists = True
                    break
        
        if not session_exists:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Session not found"
            )
        
        # Revoke the session
        async with self.redis.redis_client.pipeline(transaction=True) as pipe:
            await pipe.delete(f"session:{session_id}")
            await pipe.srem(f"user_sessions:{user_id}", session_id)
            await pipe.execute()
        
        return "Session revoked successfully"


