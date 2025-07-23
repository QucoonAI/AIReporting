from . import *
from typing import Optional
from datetime import datetime
from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import func


class UserSocialConnection(SQLModel, table=True):
    __tablename__ = "UserSocialConnection"

    social_id: Optional[int] = Field(default=None, primary_key=True)
    social_user_id: int = Field(foreign_key="User.user_id", ondelete="CASCADE")
    social_provider: str
    social_provider_user_id: str
    social_access_token: str
    social_refresh_token: Optional[str] = None
    social_expires_at: Optional[datetime] = None
    social_created_at: Optional[datetime] = Field(sa_column_kwargs={"server_default": func.now()})
    social_updated_at: Optional[datetime] = Field(sa_column_kwargs={"server_default": func.now(), "onupdate": func.now()})

    # Relationships
    social_user: User = Relationship(back_populates="social_accounts")
