from . import *
from typing import Optional, List
from datetime import datetime
from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import func, UniqueConstraint
from schemas.enum import DataSourceType


class DataSource(SQLModel, table=True):
    __tablename__ = "DataSource"
    __table_args__ = (
        UniqueConstraint('data_source_user_id', 'data_source_name', name='unique_user_data_source_name'),
    )

    data_source_id: Optional[int] = Field(default=None, primary_key=True)
    data_source_user_id: int = Field(index=True, foreign_key="User.user_id", ondelete="CASCADE")
    data_source_name: str = Field(max_length=255)
    data_source_type: DataSourceType
    data_source_url: str = Field(max_length=255)
    data_source_created_at: datetime = Field(sa_column_kwargs={"server_default": func.now()})
    data_source_updated_at: datetime = Field(sa_column_kwargs={"server_default": func.now(), "onupdate": func.now()})

    # Relationships
    data_source_owner: User = Relationship(back_populates="data_sources")
    data_source_chats: List["Chat"] = Relationship(back_populates="chat_data_source", cascade_delete=True)

