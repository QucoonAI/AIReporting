from . import *
from typing import Optional
from datetime import datetime
from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import func


class UserProfile(SQLModel, table=True):
    __tablename__ = "UserProfile"

    user_profile_id: Optional[int] = Field(default=None, primary_key=True)
    user_profile_user_id: int = Field(index=True, foreign_key="User.user_id", unique=True, ondelete="CASCADE")
    user_profile_bio: Optional[str] = Field(default=None)
    user_profile_avatar: Optional[str] = Field(default=None)
    user_phone_number: Optional[str] = Field(default=None)
    user_phone_verified: bool = Field(default=False, sa_column_kwargs={"server_default": "false"})
    user_profile_created_at: datetime = Field(sa_column_kwargs={"server_default": func.now()})
    user_profile_updated_at: datetime = Field(sa_column_kwargs={"server_default": func.now(), "onupdate": func.now()})

    # Relationships
    profile_user: User = Relationship(back_populates="user_profile")
