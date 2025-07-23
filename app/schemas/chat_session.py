from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
from .message import Message
from .enum import ChatSessionStatus


class SessionLimitConfig(BaseModel):
    # Token-based limits (primary)
    max_tokens: int = Field(default=50000, description="Maximum tokens per session")
    context_window_tokens: int = Field(default=8000, description="Maximum tokens for LLM context window")
    token_archive_threshold: float = Field(default=0.8, description="Archive when token usage hits this ratio")
    
    # Message-based limits (fallback)
    max_messages: int = Field(default=200, description="Maximum active messages (fallback limit)")
    context_window_messages: int = Field(default=50, description="Maximum messages for LLM context (fallback)")
    
    # Strategy configuration
    limiting_strategy: str = Field(default="token_based", description="Primary limiting strategy: 'token_based' or 'message_based'")
    archive_strategy: str = Field(default="oldest_first", description="How to select messages for archiving")
    preserve_conversation_pairs: bool = Field(default=True, description="Preserve user-assistant message pairs when archiving")
    
    # Editing settings
    allow_editing: bool = Field(default=True, description="Whether users can edit messages")
    regenerate_on_edit: bool = Field(default=True, description="Whether to regenerate responses after edits")
    
    # Token calculation settings
    estimate_tokens: bool = Field(default=True, description="Estimate tokens if not provided")
    chars_per_token: float = Field(default=4.0, description="Character to token ratio for estimation")


class ChatSession(BaseModel):
    session_id: str = Field(..., description="Unique session identifier")
    user_id: int = Field(..., description="ID of the user")
    data_source_id: int = Field(..., description="ID of the data source")
    data_source_name: str = Field(..., description="Name of the data source")
    data_source_type: str = Field(..., description="Type of data source")
    title: str = Field(..., description="Session title/name")
    is_active: bool = Field(default=True, description="Whether session is active")
    status: ChatSessionStatus = Field(default=ChatSessionStatus.ACTIVE, description="Session status")
    message_count: int = Field(default=0, description="Total number of messages")
    active_message_count: int = Field(default=0, description="Number of active (non-archived) messages")
    total_tokens: int = Field(default=0, description="Total tokens in all messages")
    active_tokens: int = Field(default=0, description="Total tokens in active messages")
    max_messages: int = Field(default=200, description="Maximum messages allowed in session")
    max_tokens: int = Field(default=50000, description="Maximum tokens allowed in session")
    created_at: datetime = Field(..., description="Session creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")
    settings: Optional[Dict[str, Any]] = Field(None, description="Session configuration")


class ChatSessionCreate(BaseModel):
    data_source_id: int = Field(..., description="ID of the data source")
    title: Optional[str] = Field(None, description="Optional session title")
    config: Optional[SessionLimitConfig] = Field(None, description="Session configuration")


class ChatSessionListResponse(BaseModel):
    sessions: List[ChatSession]
    total_sessions: int
    page: int
    per_page: int
    has_more: bool


class ConversationTreeResponse(BaseModel):
    session_id: str
    tree: Dict[str, Any]
    active_path: List[Message]
    analytics: Dict[str, Any]

