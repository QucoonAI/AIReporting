from typing import Optional, List, Dict, Any
from fastapi import HTTPException, status
from repositories.message import MessageRepository
from repositories.chat import ChatRepository
from core.utils import logger


class MessageService:
    """Service class for handling message operations"""
    
    def __init__(
        self,
        message_repo: MessageRepository,
        chat_repo: ChatRepository
    ):
        self.message_repo = message_repo
        self.chat_repo = chat_repo
    
    async def get_session_messages(
        self,
        user_id: int,
        session_id: str,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all messages for a chat session.
        
        Args:
            user_id: ID of the user (for access control)
            session_id: ID of the chat session
            limit: Maximum number of messages to return
            
        Returns:
            List of message dicts ordered by message_index
            
        Raises:
            HTTPException: If session not found or access denied
        """
        try:
            # Verify session exists and belongs to user
            session = await self.chat_repo.get_chat_session(user_id, session_id)
            if not session:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Chat session not found"
                )
            
            messages = await self.message_repo.get_session_messages(session_id, limit)
            return messages
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting session messages: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve session messages"
            )
    
    async def deactivate_message(
        self,
        user_id: int,
        session_id: str,
        message_id: str
    ) -> Dict[str, Any]:
        """
        Deactivate a message (mark as inactive).
        
        Args:
            user_id: ID of the user (for access control)
            session_id: ID of the chat session
            message_id: ID of the message
            
        Returns:
            Updated message dict
            
        Raises:
            HTTPException: If session or message not found or access denied
        """
        try:
            return await self.update_message(
                user_id=user_id,
                session_id=session_id,
                message_id=message_id,
                is_active=False
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deactivating message: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to deactivate message"
            )
    
    async def activate_message(
        self,
        user_id: int,
        session_id: str,
        message_id: str
    ) -> Dict[str, Any]:
        """
        Activate a message (mark as active).
        
        Args:
            user_id: ID of the user (for access control)
            session_id: ID of the chat session
            message_id: ID of the message
            
        Returns:
            Updated message dict
            
        Raises:
            HTTPException: If session or message not found or access denied
        """
        try:
            return await self.update_message(
                user_id=user_id,
                session_id=session_id,
                message_id=message_id,
                is_active=True
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error activating message: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to activate message"
            )
    
    async def get_active_conversation_path(
        self,
        user_id: int,
        session_id: str,
        leaf_message_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get the active conversation path from root to a specific message.
        
        Args:
            user_id: ID of the user (for access control)
            session_id: ID of the chat session
            leaf_message_id: ID of the leaf message (if None, gets the latest active path)
            
        Returns:
            List of message dicts representing the conversation path
            
        Raises:
            HTTPException: If session not found or access denied
        """
        try:
            # Verify session exists and belongs to user
            session = await self.chat_repo.get_chat_session(user_id, session_id)
            if not session:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Chat session not found"
                )
            
            active_path = await self.message_repo.get_active_conversation_path(
                session_id, leaf_message_id
            )
            return active_path
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting active conversation path: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve conversation path"
            )
    
    async def get_message_context(
        self,
        user_id: int,
        session_id: str,
        message_id: str,
        context_size: int = 5
    ) -> Dict[str, Any]:
        """
        Get a message with its surrounding context (previous and next messages).
        
        Args:
            user_id: ID of the user (for access control)
            session_id: ID of the chat session
            message_id: ID of the target message
            context_size: Number of messages before and after to include
            
        Returns:
            Dict with target message and context
            
        Raises:
            HTTPException: If session or message not found or access denied
        """
        try:
            # Verify session exists and belongs to user
            session = await self.chat_repo.get_chat_session(user_id, session_id)
            if not session:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Chat session not found"
                )
            
            # Get target message
            target_message = await self.message_repo.get_message(session_id, message_id)
            if not target_message:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Message not found"
                )
            
            # Get all messages in the session
            all_messages = await self.message_repo.get_session_messages(session_id)
            
            # Sort by message index
            sorted_messages = sorted(all_messages, key=lambda x: x['message_index'])
            
            # Find the target message index
            target_index = None
            for i, message in enumerate(sorted_messages):
                if message['message_id'] == message_id:
                    target_index = i
                    break
            
            if target_index is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Message not found in session"
                )
            
            # Get context messages
            start_index = max(0, target_index - context_size)
            end_index = min(len(sorted_messages), target_index + context_size + 1)
            
            context_messages = sorted_messages[start_index:end_index]
            
            return {
                "target_message": target_message,
                "context_messages": context_messages,
                "total_context_size": len(context_messages),
                "target_position": target_index - start_index
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting message context: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve message context"
            )
    
    async def get_message(
        self,
        user_id: int,
        session_id: str,
        message_id: str
    ) -> Dict[str, Any]:
        """
        Get a specific message by session ID and message ID.
        
        Args:
            user_id: ID of the user (for access control)
            session_id: ID of the chat session
            message_id: ID of the message
            
        Returns:
            Message dict
            
        Raises:
            HTTPException: If session or message not found or access denied
        """
        try:
            # Verify session exists and belongs to user
            session = await self.chat_repo.get_chat_session(user_id, session_id)
            if not session:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Chat session not found"
                )
            
            message = await self.message_repo.get_message(session_id, message_id)
            if not message:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Message not found"
                )
            
            return message
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting message: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve message"
            )
    
    async def update_message(
        self,
        user_id: int,
        session_id: str,
        message_id: str,
        **updates
    ) -> Dict[str, Any]:
        """
        Update a message.
        
        Args:
            user_id: ID of the user (for access control)
            session_id: ID of the chat session
            message_id: ID of the message
            **updates: Fields to update
            
        Returns:
            Updated message dict
            
        Raises:
            HTTPException: If session or message not found or access denied
        """
        try:
            # Verify session exists and belongs to user
            session = await self.chat_repo.get_chat_session(user_id, session_id)
            if not session:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Chat session not found"
                )
            
            # Verify message exists
            message = await self.message_repo.get_message(session_id, message_id)
            if not message:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Message not found"
                )
            
            updated_message = await self.message_repo.update_message(
                session_id, message_id, **updates
            )
            
            if not updated_message:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Message not found"
                )
            
            logger.info(f"Message updated successfully: {message_id}")
            return updated_message
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating message: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update message"
            )

