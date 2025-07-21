from pydantic import BaseModel, EmailStr, field_validator
from typing import Optional, List
from datetime import datetime
from .user import UserResponse

class LoginRequest(BaseModel):
    user_email: EmailStr
    user_password: str
    device_info: Optional[str] = None
    
    @field_validator('user_password')
    def validate_password(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        return v


class LoginResponse(BaseModel):
    message: str
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    # user: 'UserResponse'


class RefreshTokenRequest(BaseModel):
    refresh_token: str


class RefreshTokenResponse(BaseModel):
    message: str
    access_token: str
    token_type: str = "bearer"
    expires_in: int


class LogoutResponse(BaseModel):
    message: str


class SessionInfo(BaseModel):
    user_id: int
    device_info: str
    ip_address: Optional[str]
    created_at: datetime
    last_used: datetime
    is_active: bool
    
    class Config:
        from_attributes = True


class SessionsResponse(BaseModel):
    message: str
    sessions: List[SessionInfo]
    total_sessions: int


class RevokeSessionRequest(BaseModel):
    session_id: str


class RevokeSessionResponse(BaseModel):
    message: str

