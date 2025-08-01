from fastapi import APIRouter, Depends, Query, HTTPException, status, Path
from typing import Dict, Any
from app.services.chat_service import ChatService
from app.schemas import ai_request, ai_response
from core.dependencies import get_current_user, get_chat_service
from core.utils import logger


router = APIRouter(prefix="/api/v1/chat", tags=["chat"])


@router.post(
    "/sessions",
    response_model=ai_response.SessionIdResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new chat session",
    description="Create a new chat session associated with a data source."
)
async def create_chat_session(
    session_data: ai_request.CreateSessionRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)):
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
        
        session_response = ai_response.SessionIdResponse(
            message="Chat session created successfully",
            sessionId=created_session.session_id
        )

        return session_response
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating chat session: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while creating the chat session"
        )

@router.post(
    "/chat",
    response_model=ai_response.AIResponse,
    summary="Send a message to the chat session",
    description="Send a message to the chat session and receive a response."    
)
async def send_chat_message(
    message_request: ai_request.AIRequest,
    session_id: str = Path(..., description="The ID of the chat session"),
    current_user: Dict[str, Any] = Depends(get_current_user),
    chat_service: ChatService = Depends(get_chat_service)):
    """
    Send a message to the chat session.
    
    - **session_id**: ID of the chat session
    - **message_request**: The message to send
    
    Returns the AI response to the message.
    """
    try:
        response = await chat_service.send_message(
            user_id=current_user["user_id"],
            session_id=session_id,
            message_request=message_request
        )
        
        return response
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error sending message to chat session: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while sending the message"
        )


