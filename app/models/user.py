from . import *
from typing import Optional, List
from datetime import datetime
from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import func
from pydantic import EmailStr


class User(SQLModel, table=True):
    __tablename__ = "User"

    user_id: Optional[int] = Field(default=None, primary_key=True)
    user_email: EmailStr = Field(index=True, unique=True)
    user_first_name: str = Field(max_length=255)
    user_last_name: str = Field(max_length=255)
    user_password: str = Field(max_length=255)
    user_is_verified: bool = Field(default=False, sa_column_kwargs={"server_default": "false"})
    user_is_active: bool = Field(default=False, sa_column_kwargs={"server_default": "false"})
    user_created_at: datetime = Field(sa_column_kwargs={"server_default": func.now()})
    user_updated_at: datetime = Field(sa_column_kwargs={"server_default": func.now(), "onupdate": func.now()})
    
    # Relationships
    user_profile: "UserProfile" = Relationship(back_populates="profile_user", cascade_delete=True)
    social_accounts: List["UserSocialConnection"] = Relationship(back_populates="social_user", cascade_delete=True)
    data_sources: List["DataSource"] = Relationship(back_populates="data_source_owner", cascade_delete=True)
    user_chats: List["Chat"] = Relationship(back_populates="chat_user", cascade_delete=True)

