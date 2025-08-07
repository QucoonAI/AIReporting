from fastapi import APIRouter, Depends, Query, HTTPException, status, Path
from typing import Optional, Dict, Any
from app.services.chat import ChatService
from app.services.message import MessageService
from app.schemas.chat import (
    ChatSessionCreateRequest, ChatSessionCreateResponse, ChatSessionUpdateRequest,
    ChatSessionUpdateResponse, ChatSessionDeleteResponse, ChatSessionListResponse,
    ChatSessionDetailResponse, ChatMessageRequest, ChatMessageResponse,
    ChatSessionResponse,
    ChatSessionPaginatedListResponse, PaginationMetadata,
    MessageResponse, MessageRole
)
from app.core.dependencies import get_current_user, get_chat_service, get_message_service
from app.core.utils import logger


router = APIRouter(prefix="/api/v1/chat", tags=["chat"])


@router.post(
    "/sessions",
    response_model=ChatSessionCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new chat session",
    description="Create a new chat session associated with a data source."
)
async def create_chat_session(
    session_data: ChatSessionCreateRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)
) -> ChatSessionCreateResponse:
    """
    Create a new chat session.
    
    - **title**: Session title
    - **data_source_id**: ID of the associated data source (must belong to user)
    
    Returns the created chat session information.
    """
    try:
        created_session = await chat_service.create_chat_session(
            user_id=current_user["user_id"],
            session_data=session_data
        )
        
        session_response = ChatSessionResponse(
            session_id=created_session["session_id"],
            user_id=created_session["user_id"],
            data_source_id=created_session["data_source_id"],
            title=created_session["title"],
            created_at=created_session["created_at"],
            updated_at=created_session["updated_at"]
        )
        
        return ChatSessionCreateResponse(
            message="Chat session created successfully",
            session=session_response
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating chat session: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while creating the chat session"
        )


@router.get(
    "/sessions",
    response_model=ChatSessionListResponse,
    summary="Get user's chat sessions",
    description="Get all chat sessions for the authenticated user."
)
async def get_user_chat_sessions(
    limit: int = Query(50, ge=1, le=100, description="Maximum number of sessions to return"),
    current_user: Dict[str, Any] = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)
) -> ChatSessionListResponse:
    """
    Get all chat sessions for the current user.
    
    - **limit**: Maximum number of sessions to return (1-100)
    
    Returns all chat sessions owned by the authenticated user.
    """
    try:
        sessions = await chat_service.get_user_chat_sessions(
            user_id=current_user["user_id"],
            limit=limit
        )
        
        session_responses = []
        for session in sessions:
            session_responses.append(ChatSessionResponse(
                session_id=session["session_id"],
                user_id=session["user_id"],
                data_source_id=session["data_source_id"],
                title=session["title"],
                created_at=session["created_at"],
                updated_at=session["updated_at"]
            ))
        
        return ChatSessionListResponse(
            message="Chat sessions retrieved successfully",
            sessions=session_responses,
            total_count=len(session_responses)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting user chat sessions: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while retrieving chat sessions"
        )


@router.get(
    "/sessions/paginated",
    response_model=ChatSessionPaginatedListResponse,
    summary="Get paginated user's chat sessions",
    description="Get paginated chat sessions for the authenticated user."
)
async def get_user_chat_sessions_paginated(
    limit: int = Query(10, ge=1, le=50, description="Number of sessions per page"),
    last_key: Optional[str] = Query(None, description="Last evaluated key for pagination"),
    current_user: Dict[str, Any] = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)
) -> ChatSessionPaginatedListResponse:
    """
    Get paginated list of user's chat sessions.
    
    - **limit**: Number of sessions per page (1-50)
    - **last_key**: Last evaluated key from previous page for pagination
    
    Returns paginated list of chat sessions with metadata.
    """
    try:
        sessions, next_key = await chat_service.get_user_chat_sessions_paginated(
            user_id=current_user["user_id"],
            limit=limit,
            last_evaluated_key=last_key
        )
        
        session_responses = []
        for session in sessions:
            session_responses.append(ChatSessionResponse(
                session_id=session["session_id"],
                user_id=session["user_id"],
                data_source_id=session["data_source_id"],
                title=session["title"],
                created_at=session["created_at"],
                updated_at=session["updated_at"]
            ))
        
        # Calculate pagination metadata (simplified for DynamoDB)
        has_next = next_key is not None
        has_prev = last_key is not None
        
        pagination = PaginationMetadata(
            page=1,  # DynamoDB doesn't use traditional page numbers
            per_page=limit,
            total=-1,  # Total count is expensive in DynamoDB
            total_pages=-1,  # Cannot calculate without total
            has_next=has_next,
            has_prev=has_prev
        )
        
        return ChatSessionPaginatedListResponse(
            message="Chat sessions retrieved successfully",
            sessions=session_responses,
            pagination=pagination
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting paginated chat sessions: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while retrieving chat sessions"
        )


@router.get(
    "/sessions/{session_id}",
    response_model=ChatSessionDetailResponse,
    summary="Get chat session with conversation tree",
    description="Get a specific chat session with its complete conversation tree."
)
async def get_chat_session_detail(
    session_id: str = Path(..., description="ID of the chat session to retrieve"),
    current_user: Dict[str, Any] = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)
) -> ChatSessionDetailResponse:
    """
    Get chat session with conversation tree.
    
    - **session_id**: ID of the chat session to retrieve
    
    Returns the chat session information with complete conversation tree.
    """
    try:
        session, conversation_tree = await chat_service.get_chat_session_with_conversation(
            user_id=current_user["user_id"],
            session_id=session_id
        )
        
        return ChatSessionDetailResponse(
            session_id=session["session_id"],
            user_id=session["user_id"],
            data_source_id=session["data_source_id"],
            title=session["title"],
            created_at=session["created_at"],
            updated_at=session["updated_at"],
            conversation_tree=conversation_tree
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting chat session detail: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while retrieving the chat session"
        )


@router.put(
    "/sessions/{session_id}",
    response_model=ChatSessionUpdateResponse,
    summary="Update chat session",
    description="Update chat session details (e.g., title)."
)
async def update_chat_session(
    session_id: str = Path(..., description="ID of the chat session to update"),
    update_data: ChatSessionUpdateRequest = ...,
    current_user: Dict[str, Any] = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)
) -> ChatSessionUpdateResponse:
    """
    Update chat session information.
    
    - **session_id**: ID of the chat session to update
    - **title**: Updated session title (optional)
    
    Only the owner can update their chat session.
    """
    try:
        updated_session = await chat_service.update_chat_session(
            user_id=current_user["user_id"],
            session_id=session_id,
            update_data=update_data
        )
        
        session_response = ChatSessionResponse(
            session_id=updated_session["session_id"],
            user_id=updated_session["user_id"],
            data_source_id=updated_session["data_source_id"],
            title=updated_session["title"],
            created_at=updated_session["created_at"],
            updated_at=updated_session["updated_at"]
        )
        
        return ChatSessionUpdateResponse(
            message="Chat session updated successfully",
            session=session_response
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating chat session: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while updating the chat session"
        )


@router.delete(
    "/sessions/{session_id}",
    response_model=ChatSessionDeleteResponse,
    summary="Delete chat session",
    description="Delete a chat session and all its messages."
)
async def delete_chat_session(
    session_id: str = Path(..., description="ID of the chat session to delete"),
    current_user: Dict[str, Any] = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)
) -> ChatSessionDeleteResponse:
    """
    Delete chat session.
    
    - **session_id**: ID of the chat session to delete
    
    Permanently deletes the chat session and all its messages. Only the owner can delete their session.
    """
    try:
        message = await chat_service.delete_chat_session(
            user_id=current_user["user_id"],
            session_id=session_id
        )
        
        return ChatSessionDeleteResponse(message=message)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting chat session: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while deleting the chat session"
        )


# Message Route
@router.post(
    "/sessions/{session_id}/send-message",
    response_model=ChatMessageResponse,
    summary="Send a message to the chat",
    description="Send a message to the AI and get a response."
)
async def send_message(
    session_id: str = Path(..., description="ID of the chat session"),
    message_data: ChatMessageRequest = ...,
    current_user: Dict[str, Any] = Depends(get_current_user),
    message_service: MessageService = Depends(get_message_service)
) -> ChatMessageResponse:
    """
    Send a message to the AI chat.
    
    - **session_id**: ID of the chat session
    - **content**: Message content to send to the AI
    - **parent_message_id**: Optional parent message ID for branching conversations
    
    Returns both the user message and AI response.
    """
    try:
        assistant_message, limit_message = await message_service.send_message(
            user_id=current_user["user_id"],
            session_id=session_id,
            message_data=message_data
        )
        
        assistant_message_response = MessageResponse(
            message_id=assistant_message["message_id"],
            session_id=assistant_message["session_id"],
            user_id=assistant_message["user_id"],
            role=MessageRole.ASSISTANT,
            content=assistant_message["content"],
            message_index=assistant_message["message_index"],
            parent_message_id=assistant_message["parent_message_id"],
            token_count=assistant_message["token_count"],
            is_active=assistant_message["is_active"],
            created_at=assistant_message["created_at"]
        )
        
        return ChatMessageResponse(
            message="Message sent successfully",
            assistant_message=assistant_message_response,
            limit_message=limit_message
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error sending message: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while sending the message"
        )

