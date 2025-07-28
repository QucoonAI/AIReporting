from . import *
from sqlmodel import SQLModel, Field, Relationship
from typing import Optional, List
from datetime import datetime
from sqlalchemy import func
from schemas.enum import ChatStatus


class Chat(SQLModel, table=True):
    __tablename__ = "Chat"
    
    chat_id: Optional[int] = Field(default=None, primary_key=True)
    chat_user_id: int = Field(index=True, foreign_key="User.user_id", ondelete="CASCADE")
    chat_data_source_id: int = Field(index=True, foreign_key="DataSource.data_source_id", ondelete="CASCADE")
    chat_title: str = Field(max_length=500)
    chat_message_count: int = Field(default=0)
    chat_status: ChatStatus
    
    # Message limit management
    chat_message_limit: int = Field(default=30, description="Maximum messages allowed in this chat")
    chat_messages_sent: int = Field(default=0, description="Total messages sent (user messages only)")
    
    # Token tracking
    chat_total_tokens_all_branches: int = Field(default=0, description="Total tokens across all branches")
    chat_active_branch_tokens: int = Field(default=0, description="Tokens in the active conversation branch")
    
    # Branch management
    chat_active_branch_id: Optional[str] = Field(default=None, description="ID of the currently active branch")
    chat_branch_count: int = Field(default=1, description="Total number of branches in this chat")
    
    chat_created_at: datetime = Field(sa_column_kwargs={"server_default": func.now()})
    chat_updated_at: datetime = Field(sa_column_kwargs={"server_default": func.now(), "onupdate": func.now()})
    
    # Relationships
    chat_messages: List["Message"] = Relationship(back_populates="chat", cascade_delete=True)
    chat_user: User = Relationship(back_populates="user_chats")
    chat_data_source: DataSource = Relationship(back_populates="data_source_chats")


