from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional, List
from enum import Enum


class MessageRole(str, Enum):
    USER = "user"
    ASSISTANT = "assistant"


# Base schemas
class MessageBase(BaseModel):
    content: str = Field(..., min_length=1, max_length=10000, description="Message content")
    role: MessageRole = Field(..., description="Message role")


class ChatSessionBase(BaseModel):
    title: str = Field(..., min_length=1, max_length=255, description="Chat session title")
    data_source_id: int = Field(..., description="Associated data source ID")


# Request schemas
class ChatSessionCreateRequest(ChatSessionBase):
    """Schema for creating a new chat session"""
    pass


class ChatSessionUpdateRequest(BaseModel):
    """Schema for updating a chat session"""
    title: Optional[str] = Field(None, min_length=1, max_length=255, description="Updated chat session title")


class ChatMessageRequest(BaseModel):
    """Schema for sending a message in a chat"""
    content: str = Field(..., min_length=1, max_length=10000, description="Message content")
    parent_message_id: Optional[str] = Field(None, description="Parent message ID for branching conversations")


class EditMessageRequest(BaseModel):
    """Schema for editing a message and regenerating response"""
    message_id: str = Field(..., description="ID of the message to edit")
    new_content: str = Field(..., min_length=1, max_length=10000, description="New message content")


# Response schemas
class MessageResponse(BaseModel):
    """Schema for message response"""
    message_id: str
    session_id: str
    user_id: int
    role: MessageRole
    content: str
    message_index: int
    parent_message_id: Optional[str] = None
    token_count: int
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


class ConversationTree(BaseModel):
    """Schema for representing a conversation tree structure"""
    message: MessageResponse
    children: List['ConversationTree'] = []

    class Config:
        from_attributes = True


class ChatSessionResponse(BaseModel):
    """Schema for chat session response"""
    session_id: str
    user_id: int
    data_source_id: int
    title: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ChatSessionDetailResponse(ChatSessionResponse):
    """Schema for detailed chat session response with conversation tree"""
    conversation_tree: List[ConversationTree] = []


class ChatSessionCreateResponse(BaseModel):
    """Response schema for chat session creation"""
    message: str
    session: ChatSessionResponse


class ChatSessionUpdateResponse(BaseModel):
    """Response schema for chat session update"""
    message: str
    session: ChatSessionResponse


class ChatSessionDeleteResponse(BaseModel):
    """Response schema for chat session deletion"""
    message: str


class ChatSessionListResponse(BaseModel):
    """Response schema for chat session list"""
    message: str
    sessions: List[ChatSessionResponse]
    total_count: int


class ChatMessageResponse(BaseModel):
    """Response schema for chat message"""
    message: str
    user_message: MessageResponse
    assistant_message: MessageResponse


class EditMessageResponse(BaseModel):
    """Response schema for message edit"""
    message: str
    edited_message: MessageResponse
    new_assistant_message: MessageResponse
    session: ChatSessionResponse


class PaginationMetadata(BaseModel):
    """Pagination metadata for chat sessions"""
    page: int
    per_page: int
    total: int
    total_pages: int
    has_next: bool
    has_prev: bool


class ChatSessionPaginatedListResponse(BaseModel):
    """Response schema for paginated chat session list"""
    message: str
    sessions: List[ChatSessionResponse]
    pagination: PaginationMetadata


class TokenUsageInfo(BaseModel):
    """Token usage information"""
    total_tokens_all_branches: int
    active_branch_tokens: int
    max_tokens: int
    tokens_remaining: int
    usage_percentage: float

