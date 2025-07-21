from pydantic import BaseModel, EmailStr, Field, field_validator
from typing import Optional, List
from datetime import datetime


# Base schemas
class UserBase(BaseModel):
    user_email: EmailStr
    user_first_name: str = Field(..., min_length=1, max_length=255)
    user_last_name: str = Field(..., min_length=1, max_length=255)


class UserProfileBase(BaseModel):
    user_profile_bio: Optional[str] = None
    user_profile_avatar: Optional[str] = None
    user_phone_number: Optional[str] = None


# Request schemas
class UserCreateRequest(UserBase):
    user_password: str = Field(..., min_length=8, max_length=255)
    user_profile: Optional[UserProfileBase] = None

    @field_validator('user_password')
    def validate_password(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        if not any(c.isupper() for c in v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not any(c.islower() for c in v):
            raise ValueError('Password must contain at least one lowercase letter')
        if not any(c.isdigit() for c in v):
            raise ValueError('Password must contain at least one digit')
        return v


class UserUpdateRequest(BaseModel):
    user_first_name: Optional[str] = Field(None, min_length=1, max_length=255)
    user_last_name: Optional[str] = Field(None, min_length=1, max_length=255)
    user_profile: Optional[UserProfileBase] = None


class ChangePasswordRequest(BaseModel):
    current_password: str = Field(..., min_length=1)
    new_password: str = Field(..., min_length=8, max_length=255)

    @field_validator('new_password')
    def validate_password(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        if not any(c.isupper() for c in v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not any(c.islower() for c in v):
            raise ValueError('Password must contain at least one lowercase letter')
        if not any(c.isdigit() for c in v):
            raise ValueError('Password must contain at least one digit')
        return v


class ChangePasswordConfirmRequest(BaseModel):
    user_email: EmailStr
    otp: str = Field(..., min_length=1)


class PasswordResetRequest(BaseModel):
    user_email: EmailStr


class PasswordResetConfirmRequest(BaseModel):
    user_email: EmailStr
    otp: str = Field(..., min_length=1)
    new_password: str = Field(..., min_length=8, max_length=255)

    @field_validator('new_password')
    def validate_password(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        if not any(c.isupper() for c in v):
            raise ValueError('Password must contain at least one uppercase letter')
        if not any(c.islower() for c in v):
            raise ValueError('Password must contain at least one lowercase letter')
        if not any(c.isdigit() for c in v):
            raise ValueError('Password must contain at least one digit')
        return v


class VerifyUserRequest(BaseModel):
    user_email: EmailStr


class VerifyUserConfirmRequest(BaseModel):
    user_email: EmailStr
    otp: str = Field(..., min_length=1)


# Response schemas
class UserProfileResponse(UserProfileBase):
    user_profile_id: int
    user_phone_verified: bool
    user_profile_created_at: datetime
    user_profile_updated_at: datetime

    class Config:
        from_attributes = True


class UserResponse(UserBase):
    user_id: int
    user_is_verified: bool
    user_is_active: bool
    user_created_at: datetime
    user_updated_at: datetime
    user_profile: Optional[UserProfileResponse] = None

    class Config:
        from_attributes = True


class UserCreateResponse(BaseModel):
    message: str
    user: UserResponse


class VerificationResponse(BaseModel):
    message: str


class PasswordChangeResponse(BaseModel):
    message: str


class PasswordResetResponse(BaseModel):
    message: str


class UserUpdateResponse(BaseModel):
    message: str
    user: UserResponse


class UserDeleteResponse(BaseModel):
    message: str


class PaginationMetadata(BaseModel):
    page: int
    per_page: int
    total: int
    total_pages: int
    has_next: bool
    has_prev: bool


class UserListResponse(BaseModel):
    message: str
    users: List[UserResponse]
    pagination: PaginationMetadata


class UserSearchResponse(BaseModel):
    message: str
    users: List[UserResponse]
    total_results: int
    search_term: str

