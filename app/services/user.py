from typing import Optional, Dict, Any
from sqlmodel import select
from fastapi import HTTPException, status, BackgroundTasks, Request, UploadFile
from passlib.context import CryptContext
from app.models import User, UserProfile
from app.core.exceptions import (
    UserNotFoundError,
    InvalidOTPError,
    RateLimitExceededError
)
from app.core.utils import logger
from app.core.utils.s3_functions import upload_image_to_s3
from app.config.database import SessionDep
from app.services.background_services.email_service import (
    EmailService, send_verification_email_task, send_password_reset_email_task,
    send_password_change_email_task
)
from .redis_managers.factory import RedisServiceFactory
from app.schemas.user import (
    UserCreateRequest, UserUpdateRequest, ChangePasswordRequest,
    ChangePasswordConfirmRequest, PasswordResetRequest, PasswordResetConfirmRequest,
    VerifyUserRequest, VerifyUserConfirmRequest
)
from app.schemas.auth import LoginRequest


class UserService:
    def __init__(
        self,
        db_session: SessionDep, # type: ignore
        email_service: EmailService,
        redis_factory: RedisServiceFactory
    ):
        self.session: SessionDep = db_session # type: ignore
        self.email_service: EmailService = email_service
        self.redis_factory = redis_factory

        # Access Redis services through the factory
        self.auth_service = redis_factory.auth_service
        self.otp_service = redis_factory.otp_service
        self.temp_data_service = redis_factory.temp_data_service

        self.otp_expiry_minutes = 30
        self.max_otp_attempts = 5  # Maximum OTP attempts per hour

        # Password hashing
        self.pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
        
    def _hash_password(self, password: str) -> str:
        """Hash a password using bcrypt."""
        return self.pwd_context.hash(password)
    
    def _verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash."""
        return self.pwd_context.verify(plain_password, hashed_password)
    
    async def _check_otp_rate_limit(self, email: str, otp_type: str) -> None:
        """Check if user has exceeded OTP request rate limit."""
        attempts = await self.otp_service.get_otp_attempts(email, otp_type)
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

    async def create_user(self, user_data: UserCreateRequest, user_profile_avatar: UploadFile | None, background_tasks: BackgroundTasks) -> User:
        """Create a new user."""
        try:
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
                user_profile_avatar_url = None
                if user_profile_avatar:
                    user_profile_avatar_url = await upload_image_to_s3(
                        image_file=user_profile_avatar,
                        user_id=user.user_id
                    )

                profile = UserProfile(
                    user_profile_user_id=user.user_id,
                    user_profile_bio=user_data.user_profile.user_profile_bio,
                    user_profile_avatar=user_profile_avatar_url,
                    user_phone_number=user_data.user_profile.user_phone_number
                )
                self.session.add(profile)

            await self.session.commit()
            await self.session.refresh(user)

            # Generate and send verification OTP using OTP service
            otp_info = await self.otp_service.create_and_store_otp(
                otp_type="email_verification",
                identifier=user.user_email,
                user_id=user.user_id,
                expiry_minutes=self.otp_expiry_minutes
            )

            self._send_otp_email(background_tasks, user, otp_info["otp"], "email_verification")
            
            logger.info(f"User created successfully: {user.user_email}")
            return user
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating user {user_data.user_email}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create user"
            )

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
            
            # Generate and send verification OTP using OTP service
            otp_info = await self.otp_service.create_and_store_otp(
                otp_type="email_verification",
                identifier=request.user_email,
                user_id=user.user_id,
                expiry_minutes=self.otp_expiry_minutes
            )
            
            self._send_otp_email(background_tasks, user, otp_info["otp"], "email_verification")
            
            logger.info(f"Verification OTP sent to: {request.user_email}")
            return "Verification OTP sent successfully"
        
        except (UserNotFoundError, RateLimitExceededError, HTTPException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error sending verification to {request.user_email}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to send verification OTP"
            )

    async def verify_user_confirm(self, request: VerifyUserConfirmRequest) -> str:
        try:
            # Verify OTP using OTP service
            token_data = await self.otp_service.verify_otp(
                otp_type="email_verification",
                identifier=request.user_email,
                provided_otp=request.otp
            )
            
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
            
            logger.info(f"Email verified successfully: {request.user_email}")
            return "Email verified successfully"
        
        except (InvalidOTPError, UserNotFoundError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error confirming verification for {request.user_email}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to verify email"
            )

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
            
            logger.info(f"User updated successfully: {user_id}")
            return user
        
        except (UserNotFoundError, HTTPException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error updating user {user_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update user"
            )

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
            
            # Revoke all user sessions when deactivating account
            await self.auth_service.revoke_all_user_sessions(user_id)
            
            logger.info(f"User account deactivated: {user_id}")
            return "User account deactivated successfully"
        
        except (UserNotFoundError, HTTPException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error deleting user {user_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to deactivate user account"
            )

    async def change_password(self, user_id: int, request: ChangePasswordRequest, background_tasks: BackgroundTasks) -> str:
        """Initiate password change process."""
        try:
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
            
            # Generate OTP using OTP service
            otp_info = await self.otp_service.create_and_store_otp(
                otp_type="password_change",
                identifier=user.user_email,
                user_id=user.user_id,
                expiry_minutes=self.otp_expiry_minutes
            )
            
            # Store new password hash temporarily using temp data service
            temp_key = f"password_change_{user.user_email}_{otp_info['otp']}"
            await self.temp_data_service.store_temp_data(
                "password_change",
                temp_key,
                {"password_hash": self._hash_password(request.new_password)},
                self.otp_expiry_minutes
            )
            
            self._send_otp_email(background_tasks, user, otp_info["otp"], "password_change")
            
            logger.info(f"Password change OTP sent to: {user.user_email}")
            return "Password change OTP sent successfully"
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error initiating password change for user {user_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to initiate password change"
            )

    async def change_password_confirm(self, request: ChangePasswordConfirmRequest) -> str:
        """Confirm password change with OTP."""
        try:
            # Verify OTP using OTP service
            token_data = await self.otp_service.verify_otp(
                otp_type="password_change",
                identifier=request.user_email,
                provided_otp=request.otp
            )
            
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
            
            # Get stored new password hash using temp data service
            temp_key = f"password_change_{request.user_email}_{request.otp}"
            password_data = await self.temp_data_service.get_temp_data("password_change", temp_key)
            
            if not password_data or "password_hash" not in password_data:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Password change session expired"
                )
            
            # Update password
            user.user_password = password_data["password_hash"]
            self.session.add(user)
            await self.session.commit()
            
            # Clean up temporary data
            await self.temp_data_service.delete_temp_data("password_change", temp_key)
            
            # Revoke all other user sessions for security
            await self.auth_service.revoke_all_user_sessions(user.user_id)
            
            logger.info(f"Password changed successfully for user: {user.user_email}")
            return "Password changed successfully"
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error confirming password change for {request.user_email}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to change password"
            )

    async def password_reset(self, request: PasswordResetRequest, background_tasks: BackgroundTasks) -> str:
        """Initiate password reset process."""
        try:
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
            except RateLimitExceededError:
                # Return success message even if rate limited for security
                return "If the email exists, a password reset OTP has been sent"
            
            # Generate and send reset OTP using OTP service
            otp_info = await self.otp_service.create_and_store_otp(
                otp_type="password_reset",
                identifier=request.user_email,
                user_id=user.user_id,
                expiry_minutes=self.otp_expiry_minutes
            )
            
            self._send_otp_email(background_tasks, user, otp_info["otp"], "password_reset")
            
            logger.info(f"Password reset OTP sent to: {request.user_email}")
            return "If the email exists, a password reset OTP has been sent"
            
        except Exception as e:
            logger.error(f"Error initiating password reset for {request.user_email}: {e}")
            # Return success message even on error for security
            return "If the email exists, a password reset OTP has been sent"

    async def password_reset_confirm(self, request: PasswordResetConfirmRequest) -> str:
        """Confirm password reset with OTP."""
        try:
            # Verify OTP using OTP service
            token_data = await self.otp_service.verify_otp(
                otp_type="password_reset",
                identifier=request.user_email,
                provided_otp=request.otp
            )
            
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
            
            # Revoke all user sessions for security
            await self.auth_service.revoke_all_user_sessions(user.user_id)
            
            logger.info(f"Password reset successfully for user: {request.user_email}")
            return "Password reset successfully"
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error confirming password reset for {request.user_email}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to reset password"
            )
    
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
        try:
            user = await self.authenticate_user(login_data.user_email, login_data.user_password)
            
            if not user:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid email or password"
                )
            
            # Get client info
            device_info = request.headers.get("User-Agent", "Unknown Device")
            ip_address = request.client.host if request.client else None
            
            # Create tokens and session using auth service
            tokens = await self.auth_service.create_tokens(
                user_id=user.user_id,
                roles=["user"],  # You can modify this based on your role system
                device_info=device_info,
                ip_address=ip_address
            )
            
            logger.info(f"User logged in successfully: {user.user_email}")
            
            return {
                "message": "Login successful",
                "access_token": tokens["access_token"],
                "refresh_token": tokens["refresh_token"],
                "token_type": tokens["token_type"],
                "expires_in": tokens["expires_in"],
                "user": user
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error during login for {login_data.user_email}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Login failed"
            )
    
    async def logout_user(self, token: str) -> str:
        """Logout user and revoke session."""
        try:
            await self.auth_service.revoke_session(token)
            logger.info("User logged out successfully")
            return "Logout successful"
        except Exception as e:
            logger.error(f"Error during logout: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Logout failed"
            )

    async def refresh_user_token(self, refresh_token: str) -> Dict[str, Any]:
        """Refresh access token using refresh token."""
        try:
            new_tokens = await self.auth_service.refresh_access_token(refresh_token)
            
            logger.info("Token refreshed successfully")
            return {
                "message": "Token refreshed successfully",
                "access_token": new_tokens["access_token"],
                "token_type": new_tokens["token_type"],
                "expires_in": new_tokens["expires_in"]
            }
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error refreshing token: {e}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid refresh token"
            )

    async def verify_user_token(self, token: str) -> Dict[str, Any]:
        """Verify user token using auth service."""
        try:
            payload = await self.auth_service.verify_token(token, "access")
            return payload
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error verifying token: {e}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token"
            )

    async def logout_all_devices(self, user_id: int) -> str:
        """Logout user from all devices using auth service."""
        try:
            revoked_count = await self.auth_service.revoke_all_user_sessions(user_id)
            logger.info(f"Logged out from {revoked_count} devices for user: {user_id}")
            return f"Logged out from all devices successfully ({revoked_count} sessions revoked)"
        except Exception as e:
            logger.error(f"Error logging out all devices for user {user_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to logout from all devices"
            )

    async def get_active_sessions(self, user_id: int) -> Dict[str, Any]:
        """Get all active sessions for a user using auth service."""
        try:
            sessions = await self.auth_service.get_user_sessions(user_id)
            
            return {
                "message": "Active sessions retrieved successfully",
                "sessions": sessions,
                "total_sessions": len(sessions)
            }
        except Exception as e:
            logger.error(f"Error getting active sessions for user {user_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve active sessions"
            )
    
    async def revoke_user_session(self, user_id: int, session_id: str) -> str:
        """Revoke a specific user session using auth service."""
        try:
            session_revoked = await self.auth_service.revoke_session_by_id(user_id, session_id)
            
            if not session_revoked:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Session not found or does not belong to user"
                )
            
            logger.info(f"Session {session_id} revoked successfully for user {user_id}")
            return "Session revoked successfully"
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error revoking session {session_id} for user {user_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to revoke session"
            )

