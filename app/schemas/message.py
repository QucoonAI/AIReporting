from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
from .enum import MessageRole


class Message(BaseModel):
    message_id: str = Field(..., description="Unique message identifier")
    session_id: str = Field(..., description="Chat session ID")
    user_id: int = Field(..., description="ID of the user")
    role: MessageRole = Field(..., description="Message role (user/assistant)")
    content: str = Field(..., description="Message content")
    message_index: int = Field(..., description="Position in conversation")
    is_active: bool = Field(default=True, description="Whether message is active (not archived)")
    is_edited: bool = Field(default=False, description="Whether message was edited")
    parent_message_id: Optional[str] = Field(None, description="Parent message for responses/edits")
    version: int = Field(default=1, description="Message version number")
    token_count: Optional[int] = Field(None, description="Number of tokens in message")
    model_used: Optional[str] = Field(None, description="LLM model used (for assistant messages)")
    processing_time_ms: Optional[int] = Field(None, description="Processing time in milliseconds")
    created_at: datetime = Field(..., description="Message timestamp")
    edited_at: Optional[datetime] = Field(None, description="Last edit timestamp")
    archived_at: Optional[datetime] = Field(None, description="Archive timestamp")
    archive_reason: Optional[str] = Field(None, description="Reason for archiving")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class MessageCreate(BaseModel):
    content: str = Field(..., min_length=1, max_length=10000, description="Message content")
    role: MessageRole = Field(default=MessageRole.USER, description="Message role")


class MessageEdit(BaseModel):
    content: str = Field(..., min_length=1, max_length=10000, description="Edited message content")


class MessageEditResponse(BaseModel):
    original_message: Message
    edited_message: Message
    regenerated_responses: List[Message] = Field(default_factory=list, description="New assistant responses generated after edit")

