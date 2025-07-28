from . import *
import sqlalchemy as sa
from sqlmodel import SQLModel, Field, Relationship, Column
from typing import Optional, List, Dict, Any
from datetime import datetime
from sqlalchemy import func
from schemas.enum import MessageRole
import uuid


class Message(SQLModel, table=True):
    __tablename__ = "Message"
    
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True, max_length=36)
    message_chat_id: int = Field(foreign_key="Chat.chat_id", index=True)
    message_role: MessageRole = Field(..., description="Message role (user/assistant)")
    message_content: str = Field(sa_column=Column(sa.Text), description="Message content")
    
    # Branch management
    message_branch_id: str = Field(index=True, max_length=36, description="Branch identifier")
    message_parent_id: Optional[str] = Field(default=None, foreign_key="Message.message_id", max_length=36)
    message_index_in_branch: int = Field(..., description="Position in this branch")
    
    # Version management for edits
    message_is_active: bool = Field(default=True, description="Whether message is in active branch")
    message_version: int = Field(default=1, description="Version number for edited messages")
    message_is_edited: bool = Field(default=False, description="Whether message has been edited")
    message_original_id: Optional[str] = Field(default=None, max_length=36, description="ID of original message if edited")
    
    # Token and metadata
    message_token_count: Optional[int] = Field(None, description="Number of tokens in message")
    message_metadata: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(sa.JSON), description="Additional metadata")
    
    # Timestamps
    message_created_at: datetime = Field(sa_column_kwargs={"server_default": func.now()})
    message_updated_at: datetime = Field(sa_column_kwargs={"server_default": func.now(), "onupdate": func.now()})
    
    # Relationships
    chat: Optional[Chat] = Relationship(back_populates="chat_messages")
    parent_message: Optional["Message"] = Relationship(
        back_populates="child_messages",
        sa_relationship_kwargs={"remote_side": "Message.message_id"}
    )
    child_messages: List["Message"] = Relationship(
        back_populates="parent_message",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"}
    )


