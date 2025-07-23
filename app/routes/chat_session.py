# api/v1/chat.py
from fastapi import APIRouter, Depends, HTTPException, status, Query, Path
from typing import Dict, Any, Optional, List
from services.chat_session import ChatService
from repositories.chat_session import ChatRepository
from repositories.data_source import DataSourceRepository
from schemas.chat_session import (
    ChatSessionCreate, ChatSession, ChatSessionListResponse, ConversationTreeResponse
)
from schemas.message import MessageCreate, MessageEdit, Message, MessageEditResponse
from core.dependencies import get_current_user, get_data_source_repo
from core.utils import logger


router = APIRouter(prefix="/api/v1/chat", tags=["chat"])


def get_chat_repository() -> ChatRepository:
    """Dependency to get ChatRepository instance"""
    return ChatRepository()


def get_chat_service(
    chat_repo: ChatRepository = Depends(get_chat_repository)
) -> ChatService:
    """Dependency to get ChatService instance"""
    return ChatService(chat_repo)


@router.post(
    "/sessions",
    response_model=ChatSession,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new chat session",
    description="Create a new chat session with a data source for the authenticated user."
)
async def create_chat_session(
    session_data: ChatSessionCreate,
    current_user: Dict[str, Any] = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service),
    data_source_repo: DataSourceRepository = Depends(get_data_source_repo)
) -> ChatSession:
    """
    Create a new chat session.
    
    - **data_source_id**: ID of the data source to chat with
    - **title**: Optional custom title for the session
    - **config**: Optional session configuration settings
    
    Returns the created chat session information.
    """
    try:
        # Verify data source exists and belongs to user
        data_source = await data_source_repo.get_data_source_by_id(session_data.data_source_id)
        if not data_source:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source not found"
            )
        
        if data_source.data_source_user_id != current_user["user_id"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied. You can only create sessions with your own data sources."
            )
        
        # Create the chat session
        session = await chat_service.create_session(
            user_id=current_user["user_id"],
            create_data=session_data,
            data_source_name=data_source.data_source_name,
            data_source_type=data_source.data_source_type.value
        )
        
        logger.info(f"Chat session created: {session.session_id} for user {current_user['user_id']}")
        return session
        
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
    summary="List user's chat sessions",
    description="Get a paginated list of chat sessions for the authenticated user."
)
async def list_chat_sessions(
    page: int = Query(1, ge=1, description="Page number (starts from 1)"),
    per_page: int = Query(20, ge=1, le=100, description="Number of sessions per page (max 100)"),
    current_user: Dict[str, Any] = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)
) -> ChatSessionListResponse:
    """
    Get paginated list of user's chat sessions.
    
    - **page**: Page number (starting from 1)
    - **per_page**: Number of sessions per page (1-100)
    
    Returns paginated list of chat sessions ordered by most recent first.
    """
    try:
        result = await chat_service.list_user_sessions(
            user_id=current_user["user_id"],
            page=page,
            per_page=per_page
        )
        
        return ChatSessionListResponse(**result)
        
    except Exception as e:
        logger.error(f"Error listing chat sessions for user {current_user['user_id']}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while retrieving chat sessions"
        )


@router.get(
    "/sessions/{session_id}/conversation",
    response_model=ConversationTreeResponse,
    summary="Get conversation tree",
    description="Get the complete conversation tree for a chat session, including all message branches."
)
async def get_conversation_tree(
    session_id: str = Path(..., description="ID of the chat session"),
    current_user: Dict[str, Any] = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)
) -> ConversationTreeResponse:
    """
    Get conversation tree for a chat session.
    
    - **session_id**: ID of the chat session
    
    Returns the complete conversation tree including:
    - Full message hierarchy with branches
    - Active conversation path
    - Session analytics
    """
    try:
        conversation_tree = await chat_service.get_conversation_tree(
            session_id=session_id,
            user_id=current_user["user_id"]
        )
        
        return conversation_tree
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error getting conversation tree for session {session_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while retrieving the conversation"
        )


@router.post(
    "/sessions/{session_id}/messages",
    response_model=Message,
    status_code=status.HTTP_201_CREATED,
    summary="Add message to chat session",
    description="Add a new message to a chat session."
)
async def add_message_to_session(
    session_id: str = Path(..., description="ID of the chat session"),
    message_data: MessageCreate = ...,
    current_user: Dict[str, Any] = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)
) -> Message:
    """
    Add a message to a chat session.
    
    - **session_id**: ID of the chat session
    - **content**: Message content
    - **role**: Message role (user/assistant)
    
    Returns the created message. Automatically handles:
    - Message indexing
    - Session limit enforcement
    - Automatic archiving of old messages if needed
    """
    try:
        message = await chat_service.add_message(
            session_id=session_id,
            user_id=current_user["user_id"],
            message_data=message_data
        )
        
        logger.info(f"Message added to session {session_id}: {message.message_id}")
        return message
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error adding message to session {session_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while adding the message"
        )


@router.put(
    "/messages/{message_id}",
    response_model=MessageEditResponse,
    summary="Edit a message",
    description="Edit a message using cascade regeneration pattern."
)
async def edit_message(
    message_id: str = Path(..., description="ID of the message to edit"),
    edit_data: MessageEdit = ...,
    current_user: Dict[str, Any] = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)
) -> MessageEditResponse:
    """
    Edit a message with cascade regeneration.
    
    - **message_id**: ID of the message to edit
    - **content**: New message content
    
    When a message is edited:
    1. Original message is archived
    2. New edited version is created
    3. All subsequent messages in the conversation are archived (cascade)
    4. LLM service can regenerate responses from the edit point
    
    Returns both the original and edited messages.
    """
    try:
        edit_response = await chat_service.edit_message(
            user_id=current_user["user_id"],
            message_id=message_id,
            edit_data=edit_data
        )
        
        logger.info(f"Message edited: {message_id} -> {edit_response.edited_message.message_id}")
        return edit_response
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error editing message {message_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while editing the message"
        )


@router.get(
    "/sessions/{session_id}/analytics",
    summary="Get session analytics",
    description="Get comprehensive analytics for a chat session."
)
async def get_session_analytics(
    session_id: str = Path(..., description="ID of the chat session"),
    current_user: Dict[str, Any] = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)
) -> Dict[str, Any]:
    """
    Get analytics for a chat session.
    
    - **session_id**: ID of the chat session
    
    Returns analytics including:
    - Message counts (total, active, archived, by role)
    - Token usage statistics
    - Response time metrics
    - Conversation duration
    - Edit statistics
    """
    try:
        conversation_tree = await chat_service.get_conversation_tree(
            session_id=session_id,
            user_id=current_user["user_id"]
        )
        
        return conversation_tree.analytics
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error getting analytics for session {session_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while retrieving session analytics"
        )


@router.get(
    "/sessions/{session_id}/context",
    response_model=List[Message],
    summary="Get optimized LLM context",
    description="Get optimized message context for LLM based on token or message limits."
)
async def get_llm_context(
    session_id: str = Path(..., description="ID of the chat session"),
    max_tokens: Optional[int] = Query(None, ge=1, le=32000, description="Maximum tokens for context"),
    max_messages: Optional[int] = Query(None, ge=1, le=200, description="Maximum messages for context"),
    current_user: Dict[str, Any] = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)
) -> List[Message]:
    """
    Get optimized context for LLM.
    
    - **session_id**: ID of the chat session
    - **max_tokens**: Override default token limit for context
    - **max_messages**: Override default message limit for context
    
    Uses intelligent selection based on session configuration:
    - Token-based limiting: Selects messages that fit within token budget
    - Message-based limiting: Selects recent messages by count
    - Preserves conversation pairs when possible
    
    Returns messages in chronological order, optimized for LLM context.
    """
    try:
        context_messages = await chat_service.get_context_for_llm(
            session_id=session_id,
            user_id=current_user["user_id"],
            max_tokens=max_tokens,
            max_messages=max_messages
        )
        
        return context_messages
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error getting LLM context for session {session_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while retrieving LLM context"
        )


@router.get(
    "/sessions/{session_id}/token-usage",
    summary="Get token usage analytics",
    description="Get detailed token usage analytics and cost estimates for a session."
)
async def get_token_usage(
    session_id: str = Path(..., description="ID of the chat session"),
    current_user: Dict[str, Any] = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)
) -> Dict[str, Any]:
    """
    Get comprehensive token usage analytics.
    
    - **session_id**: ID of the chat session
    
    Returns detailed analytics including:
    - Token limits and current usage
    - Breakdown by message role (user/assistant)
    - Usage percentages and thresholds
    - Cost estimates based on token usage
    - Recommendations for optimization
    """
    try:
        usage_analytics = await chat_service.get_token_usage(
            session_id=session_id,
            user_id=current_user["user_id"]
        )
        
        return usage_analytics
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error getting token usage for session {session_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while retrieving token usage analytics"
        )


@router.post(
    "/sessions/{session_id}/archive-by-tokens",
    summary="Archive messages by token threshold",
    description="Manually trigger archiving of old messages based on token usage."
)
async def archive_by_token_threshold(
    session_id: str = Path(..., description="ID of the chat session"),
    threshold: Optional[float] = Query(None, ge=0.1, le=1.0, description="Custom archive threshold (0.1-1.0)"),
    current_user: Dict[str, Any] = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)
) -> Dict[str, Any]:
    """
    Manually archive messages by token threshold.
    
    - **session_id**: ID of the chat session
    - **threshold**: Custom threshold (optional, 0.1-1.0)
    
    Archives older messages to bring token usage below the threshold.
    Preserves conversation pairs when configured.
    
    Returns statistics about the archiving operation.
    """
    try:
        archive_result = await chat_service.archive_by_tokens(
            session_id=session_id,
            user_id=current_user["user_id"],
            threshold=threshold
        )
        
        logger.info(f"Manual archiving completed for session {session_id}: {archive_result.get('archived_messages', 0)} messages archived")
        return archive_result
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error archiving by tokens for session {session_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while archiving messages"
        )


@router.delete(
    "/sessions/{session_id}",
    summary="Archive chat session",
    description="Archive a chat session (soft delete)."
)
async def archive_chat_session(
    session_id: str = Path(..., description="ID of the chat session"),
    current_user: Dict[str, Any] = Depends(get_current_user),
    chat_repo: ChatRepository = Depends(get_chat_repository)
) -> Dict[str, str]:
    """
    Archive a chat session.
    
    - **session_id**: ID of the chat session to archive
    
    Marks the session as inactive (soft delete).
    The session and its messages are preserved but hidden from normal listings.
    """
    try:
        # This would need to be implemented in the repository
        # For now, just verify the session exists and belongs to user
        conversation_tree = await chat_repo.get_conversation_tree(session_id, current_user["user_id"])
        
        # TODO: Implement session archiving in repository
        # await chat_repo.archive_session(session_id, current_user["user_id"])
        
        logger.info(f"Chat session archived: {session_id} for user {current_user['user_id']}")
        return {"message": "Chat session archived successfully"}
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error archiving session {session_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while archiving the session"
        )


# Example usage with LLM integration endpoint
@router.post(
    "/sessions/{session_id}/chat",
    response_model=Message,
    summary="Chat with LLM",
    description="Send a message and get LLM response (combines add_message + LLM generation)."
)
async def chat_with_llm(
    session_id: str = Path(..., description="ID of the chat session"),
    message_data: MessageCreate = ...,
    current_user: Dict[str, Any] = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)
) -> Message:
    """
    Send a message and get LLM response.
    
    This endpoint:
    1. Adds the user message to the session
    2. Gets the active conversation context
    3. Calls LLM service to generate response
    4. Adds the assistant response to the session
    5. Returns the assistant message
    
    - **session_id**: ID of the chat session
    - **content**: User message content
    """
    try:
        # Add user message
        user_message = await chat_service.add_message(
            session_id=session_id,
            user_id=current_user["user_id"],
            message_data=message_data
        )
        
        # Get active conversation context
        conversation_tree = await chat_service.get_conversation_tree(
            session_id=session_id,
            user_id=current_user["user_id"]
        )
        
        active_context = conversation_tree.active_path
        
        # TODO: Integrate with your LLM service here
        # Calculate token count for the new message if not provided
        user_message_tokens = kwargs.get('token_count')
        if not user_message_tokens:
            # Estimate tokens (adjust chars_per_token based on your tokenizer)
            user_message_tokens = max(1, len(message_data.content) // 4)
        
        # Get optimized context for LLM
        context_messages = await chat_service.get_context_for_llm(
            session_id=session_id,
            user_id=current_user["user_id"]
        )
        
        # llm_response = await llm_service.generate_response(
        #     context=context_messages,
        #     data_source_id=session.data_source_id,
        #     user_message=user_message.content
        # )
        
        # For now, return a placeholder response with token estimation
        assistant_content = f"This is a placeholder LLM response to: '{message_data.content}'. Integrate with your LLM service here. Context includes {len(context_messages)} messages."
        assistant_tokens = max(1, len(assistant_content) // 4)
        
        assistant_response = await chat_service.add_message(
            session_id=session_id,
            user_id=current_user["user_id"],
            message_data=MessageCreate(
                content=assistant_content,
                role=MessageRole.ASSISTANT
            ),
            model_used="gpt-4",
            processing_time_ms=1500,
            token_count=assistant_tokens
        )
        
        return assistant_response
        
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error in chat with LLM for session {session_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the chat request"
        )

