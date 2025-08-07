from datetime import datetime
from fastapi import APIRouter, Depends, Query, HTTPException, status, BackgroundTasks
from typing import Optional, Dict, Any
from app.services.user import UserService
from app.repositories.user import UserRepository
from app.schemas.user import (
    UserCreateRequest, UserCreateResponse, UserUpdateRequest, UserUpdateResponse,
    VerifyUserRequest, VerifyUserConfirmRequest,
    ChangePasswordRequest, ChangePasswordConfirmRequest, PasswordResetRequest,
    PasswordResetConfirmRequest, VerificationResponse, PasswordChangeResponse,
    PasswordResetResponse, UserDeleteResponse, UserResponse,
)
from app.core.dependencies import get_current_user, get_user_service, get_user_repo
from app.core.utils import logger


router = APIRouter(prefix="/api/v1/users", tags=["users"])


@router.post(
    "/sign-up",
    response_model=UserCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new user",
    description="Create a new user account with optional profile information. Sends verification email."
)
async def create_user(
    user_data: UserCreateRequest,
    background_tasks: BackgroundTasks,
    user_service: UserService = Depends(get_user_service),
    user_repo: UserRepository = Depends(get_user_repo)
) -> UserCreateResponse:
    """
    Create a new user account.
    
    - **user_email**: Valid email address (must be unique)
    - **user_first_name**: User's first name
    - **user_last_name**: User's last name
    - **user_password**: Password (min 8 chars, must contain uppercase, lowercase, digit)
    - **user_profile**: Optional profile information
    
    Returns the created user information and sends a verification email.
    """
    try:
        created_user = await user_service.create_user(user_data, background_tasks)
        user = await user_repo.get_user_by_id(created_user.user_id, include_profile=True)
        return UserCreateResponse(
            message="User created successfully. Please check your email for verification.",
            user=UserResponse.model_validate(user)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while creating the user"
        )


@router.post(
    "/verify-email",
    response_model=VerificationResponse,
    summary="Request email verification",
    description="Send verification email to user's email address."
)
async def verify_user(
    request: VerifyUserRequest,
    background_tasks: BackgroundTasks,
    user_service: UserService = Depends(get_user_service)
) -> VerificationResponse:
    """
    Send verification email to user.
    
    - **user_email**: Email address of the user to verify
    
    Sends a verification email with an OTP to confirm email ownership.
    """
    try:
        message = await user_service.verify_user(request, background_tasks)
        return VerificationResponse(message=message)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while sending verification email"
        )


@router.post(
    "/verify-email-confirm",
    response_model=VerificationResponse,
    summary="Confirm email verification",
    description="Confirm email verification using the OTP sent via email."
)
async def verify_user_confirm(
    request: VerifyUserConfirmRequest,
    user_service: UserService = Depends(get_user_service)
) -> VerificationResponse:
    """
    Confirm email verification.
    
    - **user_email**: Email address of the user
    - **otp**: Verification OTP received via email
    
    Activates the user account and marks email as verified.
    """
    try:
        message = await user_service.verify_user_confirm(request)
        return VerificationResponse(message=message)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while verifying email"
        )


@router.put(
    "/update-profile",
    response_model=UserUpdateResponse,
    summary="Update user information",
    description="Update user's basic information and profile. Requires authentication."
)
async def update_user(
    update_data: UserUpdateRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service),
    user_repo: UserRepository = Depends(get_user_repo)
) -> UserUpdateResponse:
    """
    Update user information.
    
    - **user_first_name**: Updated first name (optional)
    - **user_last_name**: Updated last name (optional)
    - **user_profile**: Updated profile information (optional)
    
    Only authenticated users can update their own information.
    """
    try:
        updated_user = await user_service.update_user(current_user["user_id"], update_data)
        user = await user_repo.get_user_by_id(updated_user.user_id, include_profile=True)
        return UserUpdateResponse(
            message="User updated successfully",
            user=UserResponse.model_validate(user)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while updating user information"
        )


@router.delete(
    "/delete-user",
    response_model=UserDeleteResponse,
    summary="Delete user account",
    description="Deactivate user account. Requires authentication."
)
async def delete_user(
    current_user: Dict[str, Any] = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service)
) -> UserDeleteResponse:
    """
    Delete user account.
    
    Deactivates the user account (soft delete).
    The account can potentially be reactivated by administrators.
    """
    try:
        message = await user_service.delete_user(current_user["user_id"])
        return UserDeleteResponse(message=message)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while deleting user account"
        )


@router.post(
    "/change-password",
    response_model=PasswordChangeResponse,
    summary="Request password change",
    description="Initiate password change process. Requires authentication and current password."
)
async def change_password(
    request: ChangePasswordRequest,
    background_tasks: BackgroundTasks,
    current_user: Dict[str, Any] = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service)
) -> PasswordChangeResponse:
    """
    Initiate password change.
    
    - **current_password**: Current password for verification
    - **new_password**: New password (min 8 chars, must contain uppercase, lowercase, digit)
    
    Sends a confirmation email with an OTP to complete the password change.
    """
    try:
        message = await user_service.change_password(current_user["user_id"], request, background_tasks)
        return PasswordChangeResponse(message=message)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing password change"
        )


@router.post(
    "/change-password-confirm",
    response_model=PasswordChangeResponse,
    summary="Confirm password change",
    description="Confirm password change using the OTP sent via email."
)
async def change_password_confirm(
    request: ChangePasswordConfirmRequest,
    user_service: UserService = Depends(get_user_service)
) -> PasswordChangeResponse:
    """
    Confirm password change.
    
    - **user_email**: Email address of the user
    - **otp**: Password change OTP received via email
    
    Completes the password change process using the stored new password.
    """
    try:
        message = await user_service.change_password_confirm(request)
        return PasswordChangeResponse(message=message)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while confirming password change"
        )


@router.post(
    "/password-reset",
    response_model=PasswordResetResponse,
    summary="Request password reset",
    description="Initiate password reset process for forgotten passwords."
)
async def password_reset(
    request: PasswordResetRequest,
    background_tasks: BackgroundTasks,
    user_service: UserService = Depends(get_user_service)
) -> PasswordResetResponse:
    """
    Initiate password reset.
    
    - **user_email**: Email address of the account to reset
    
    Sends a password reset email with an OTP if the email exists in the system.
    """
    try:
        message = await user_service.password_reset(request, background_tasks)
        return PasswordResetResponse(message=message)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing password reset"
        )


@router.post(
    "/password-reset-confirm",
    response_model=PasswordResetResponse,
    summary="Confirm password reset",
    description="Confirm password reset using the OTP sent via email."
)
async def password_reset_confirm(
    request: PasswordResetConfirmRequest,
    user_service: UserService = Depends(get_user_service)
) -> PasswordResetResponse:
    """
    Confirm password reset.
    
    - **user_email**: Email address of the user
    - **otp**: Password reset OTP received via email
    - **new_password**: New password (min 8 chars, must contain uppercase, lowercase, digit)
    
    Completes the password reset process and updates the user's password.
    """
    try:
        message = await user_service.password_reset_confirm(request)
        return PasswordResetResponse(message=message)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while confirming password reset"
        )

# ------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------

@router.get(
    "/me",
    response_model=UserResponse,
    summary="Get current user",
    description="Get the currently authenticated user's information."
)
async def get_current_user_info(
    current_user: Dict[str, Any] = Depends(get_current_user),
    user_repo: UserRepository = Depends(get_user_repo)
) -> UserResponse:
    """
    Get current user information.
    
    Returns the authenticated user's profile information.
    Requires valid authentication token.
    """
    try:
        user = await user_repo.get_user_by_id(current_user["user_id"], include_profile=True)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        return UserResponse.model_validate(user)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting current user: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while retrieving user information"
        )


# @router.get(
#     "/{user_id}",
#     response_model=UserResponse,
#     summary="Get user by ID",
#     description="Get a specific user by their ID. Requires authentication."
# )
# async def get_user_by_id(
#     user_id: int,
#     include_profile: bool = Query(False, description="Include user profile information"),
#     current_user: Dict[str, Any] = Depends(get_current_user),
#     user_repo: UserRepository = Depends(get_user_repo)
# ) -> UserResponse:
#     """
#     Get user by ID.
    
#     - **user_id**: ID of the user to retrieve
#     - **include_profile**: Whether to include profile information
    
#     Returns the user information if found and accessible.
#     """
#     try:
#         user = await user_repo.get_user_by_id(user_id, include_profile=include_profile)
#         if not user:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail="User not found"
#             )
        
#         # Optional: Add permission check if users should only see their own info
#         # if current_user["user_id"] != user_id and not current_user.get("is_admin", False):
#         #     raise HTTPException(
#         #         status_code=status.HTTP_403_FORBIDDEN,
#         #         detail="Access denied"
#         #     )
        
#         return UserResponse.model_validate(user)
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Error getting user by ID {user_id}: {e}")
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail="An error occurred while retrieving user information"
#         )


# @router.get(
#     "/email/{email}",
#     response_model=UserResponse,
#     summary="Get user by email",
#     description="Get a specific user by their email address. Requires authentication."
# )
# async def get_user_by_email(
#     email: str,
#     include_profile: bool = Query(False, description="Include user profile information"),
#     current_user: Dict[str, Any] = Depends(get_current_user),
#     user_repo: UserRepository = Depends(get_user_repo)
# ) -> UserResponse:
#     """
#     Get user by email address.
    
#     - **email**: Email address of the user to retrieve
#     - **include_profile**: Whether to include profile information
    
#     Returns the user information if found and accessible.
#     """
#     try:
#         user = await user_repo.get_user_by_email(email, include_profile=include_profile)
#         if not user:
#             raise HTTPException(
#                 status_code=status.HTTP_404_NOT_FOUND,
#                 detail="User not found"
#             )
        
#         # Optional: Add permission check
#         # if current_user["user_email"] != email and not current_user.get("is_admin", False):
#         #     raise HTTPException(
#         #         status_code=status.HTTP_403_FORBIDDEN,
#         #         detail="Access denied"
#         #     )
        
#         return UserResponse.model_validate(user)
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Error getting user by email {email}: {e}")
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail="An error occurred while retrieving user information"
#         )


# @router.get(
#     "/",
#     response_model=UserListResponse,
#     summary="Get list of users",
#     description="Get a paginated list of users with filtering and sorting options. Requires authentication."
# )
# async def get_users_list(
#     page: int = Query(1, ge=1, description="Page number (starts from 1)"),
#     per_page: int = Query(10, ge=1, le=100, description="Number of users per page (max 100)"),
#     search: Optional[str] = Query(None, description="Search term for name or email"),
#     is_active: Optional[bool] = Query(None, description="Filter by active status"),
#     is_verified: Optional[bool] = Query(None, description="Filter by verification status"),
#     sort_by: str = Query("user_created_at", description="Field to sort by"),
#     sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order (asc or desc)"),
#     include_profiles: bool = Query(False, description="Include user profiles"),
#     date_from: Optional[datetime] = Query(None, description="Filter users created from this date"),
#     date_to: Optional[datetime] = Query(None, description="Filter users created before this date"),
#     current_user: Dict[str, Any] = Depends(get_current_user),
#     user_repo: UserRepository = Depends(get_user_repo)
# ) -> UserListResponse:
#     """
#     Get paginated list of users.
    
#     - **page**: Page number (starting from 1)
#     - **per_page**: Number of users per page (1-100)
#     - **search**: Search term to filter by name or email
#     - **is_active**: Filter by active status
#     - **is_verified**: Filter by verification status
#     - **sort_by**: Field to sort by (user_created_at, user_updated_at, user_email, user_first_name, user_last_name)
#     - **sort_order**: Sort order (asc, desc)
#     - **include_profiles**: Whether to include user profiles
#     - **date_from**: Filter users created from this date
#     - **date_to**: Filter users created before this date
    
#     Returns paginated list of users with metadata.
#     """
#     try:
#         # Optional: Add admin check if this should be admin-only
#         # if not current_user.get("is_admin", False):
#         #     raise HTTPException(
#         #         status_code=status.HTTP_403_FORBIDDEN,
#         #         detail="Admin access required"
#         #     )
        
#         users, total_count = await user_repo.get_users_list(
#             page=page,
#             per_page=per_page,
#             search=search,
#             is_active=is_active,
#             is_verified=is_verified,
#             sort_by=sort_by,
#             sort_order=sort_order,
#             include_profiles=include_profiles,
#             date_from=date_from,
#             date_to=date_to
#         )
        
#         # Calculate pagination metadata
#         total_pages = (total_count + per_page - 1) // per_page
#         has_next = page < total_pages
#         has_prev = page > 1
        
#         pagination = PaginationMetadata(
#             page=page,
#             per_page=per_page,
#             total=total_count,
#             total_pages=total_pages,
#             has_next=has_next,
#             has_prev=has_prev
#         )
        
#         return UserListResponse(
#             message="Users retrieved successfully",
#             users=[UserResponse.model_validate(user) for user in users],
#             pagination=pagination
#         )
        
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Error getting users list: {e}")
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail="An error occurred while retrieving users"
#         )


# @router.get(
#     "/search/query",
#     response_model=UserSearchResponse,
#     summary="Search users",
#     description="Search users by name or email with optional limit. Requires authentication."
# )
# async def search_users(
#     q: str = Query(..., min_length=2, description="Search query (minimum 2 characters)"),
#     limit: int = Query(20, ge=1, le=50, description="Maximum number of results (max 50)"),
#     include_profiles: bool = Query(False, description="Include user profiles"),
#     current_user: Dict[str, Any] = Depends(get_current_user),
#     user_repo: UserRepository = Depends(get_user_repo)
# ) -> UserSearchResponse:
#     """
#     Search users by name or email.
    
#     - **q**: Search query (minimum 2 characters)
#     - **limit**: Maximum number of results (1-50)
#     - **include_profiles**: Whether to include user profiles
    
#     Returns list of users matching the search criteria.
#     """
#     try:
#         # Optional: Add admin check if search should be admin-only
#         # if not current_user.get("is_admin", False):
#         #     raise HTTPException(
#         #         status_code=status.HTTP_403_FORBIDDEN,
#         #         detail="Admin access required"
#         #     )
        
#         users = await user_repo.search_users(
#             search_term=q,
#             limit=limit,
#             include_profiles=include_profiles
#         )
        
#         return UserSearchResponse(
#             message="Search completed successfully",
#             users=[UserResponse.model_validate(user) for user in users],
#             total_results=len(users),
#             search_term=q
#         )
        
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Error searching users with query '{q}': {e}")
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail="An error occurred while searching users"
#         )

