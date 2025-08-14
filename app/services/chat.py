from typing import Optional, List, Dict, Any, Tuple
from fastapi import HTTPException, status
from app.schemas.chat import MessageResponse, MessageRole
from app.repositories.chat import ChatRepository
from app.repositories.message import MessageRepository
from app.repositories.data_source import DataSourceRepository
from app.services.mock_llm import MockLLMService
from app.services.ai_service import AIQuery
from .redis_managers.factory import RedisServiceFactory
from app.schemas.chat import (
    ChatSessionCreateRequest, ChatSessionUpdateRequest, ConversationTree
)
from app.core.utils import logger


agent = AIQuery()

class ChatService:
    """Service class for handling chat operations"""
    
    def __init__(
        self,
        chat_repo: ChatRepository,
        message_repo: MessageRepository,
        data_source_repo: DataSourceRepository,
        llm_service: MockLLMService,
        redis_factory: RedisServiceFactory
    ):
        self.chat_repo = chat_repo
        self.message_repo = message_repo
        self.data_source_repo = data_source_repo
        self.mock_llm_service = llm_service
        self.redis_factory = redis_factory
        self.chat_cache = redis_factory.chat_cache_service
    
    async def create_chat_session(
        self,
        user_id: int,
        session_data: ChatSessionCreateRequest
    ) -> Dict[str, Any]:
        """
        Create a new chat session.
        
        Args:
            user_id: ID of the user creating the session
            session_data: Chat session creation data
            
        Returns:
            Created chat session dict
            
        Raises:
            HTTPException: If data source not found or creation fails
        """
        try:
            # Verify data source exists and belongs to user
            data_source = await self.data_source_repo.get_data_source_by_id(session_data.data_source_id)
            if not data_source:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Data source not found"
                )
            
            if data_source.data_source_user_id != user_id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Access denied. You can only create sessions with your own data sources."
                )
            
            # Create the chat session
            session = await self.chat_repo.create_chat_session(
                user_id=user_id,
                data_source_id=session_data.data_source_id,
                title=session_data.title
            )
            
            logger.info(f"Chat session created successfully: {session['session_id']}")
            return session
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating chat session: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create chat session"
            )
    
    async def get_user_chat_sessions(
        self,
        user_id: int,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get all chat sessions for a user.
        
        Args:
            user_id: ID of the user
            limit: Maximum number of sessions to return
            
        Returns:
            List of chat session dicts
        """
        try:
            sessions = await self.chat_repo.get_user_chat_sessions(user_id, limit)
            return sessions
            
        except Exception as e:
            logger.error(f"Error getting user chat sessions: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve chat sessions"
            )
    
    async def get_user_chat_sessions_paginated(
        self,
        user_id: int,
        limit: int = 10,
        last_evaluated_key: Optional[Dict[str, Any]] = None
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        Get paginated chat sessions for a user.
        
        Args:
            user_id: ID of the user
            limit: Number of sessions per page
            last_evaluated_key: Key for pagination
            
        Returns:
            Tuple of (sessions_list, next_page_key)
        """
        try:
            return await self.chat_repo.get_user_chat_sessions_paginated(
                user_id, limit, last_evaluated_key
            )
            
        except Exception as e:
            logger.error(f"Error getting paginated chat sessions: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve chat sessions"
            )

    async def get_data_source_sessions(
        self,
        user_id: int,
        data_source_id: int,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get all chat sessions for a specific data source.
        
        Args:
            user_id: ID of the user (for access control)
            data_source_id: ID of the data source
            limit: Maximum number of sessions to return
            
        Returns:
            List of chat session dicts
            
        Raises:
            HTTPException: If data source not found or access denied
        """
        try:
            # Verify data source exists and belongs to user
            data_source = await self.data_source_repo.get_data_source_by_id(data_source_id)
            if not data_source:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Data source not found"
                )
            
            if data_source.data_source_user_id != user_id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Access denied. You can only view sessions for your own data sources."
                )
            
            sessions = await self.chat_repo.get_data_source_sessions(data_source_id, limit)
            return sessions
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting data source sessions: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve data source sessions"
            )
    
    async def get_chat_session_with_conversation(
        self,
        user_id: int,
        session_id: str
    ) -> Tuple[Dict[str, Any], List[ConversationTree]]:
        """
        Get a chat session with its complete conversation tree.
        
        Args:
            user_id: ID of the user
            session_id: ID of the chat session
            
        Returns:
            Tuple of (chat_session_dict, conversation_tree)
            
        Raises:
            HTTPException: If session not found or access denied
        """
        try:
            # Get the session
            session = await self.chat_repo.get_chat_session(user_id, session_id)
            if not session:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Chat session not found"
                )
            
            # Get all messages and build conversation tree
            messages = await self.message_repo.get_session_messages_active(session_id)
            conversation_tree = self._build_conversation_tree(messages)
            
            return session, conversation_tree
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting chat session with conversation: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve chat session"
            )
    
    async def update_chat_session(
        self,
        user_id: int,
        session_id: str,
        update_data: ChatSessionUpdateRequest
    ) -> Dict[str, Any]:
        """
        Update a chat session.
        
        Args:
            user_id: ID of the user
            session_id: ID of the chat session
            update_data: Update data
            
        Returns:
            Updated chat session dict
            
        Raises:
            HTTPException: If session not found or update fails
        """
        try:
            # Verify session exists and belongs to user
            session = await self.chat_repo.get_chat_session(user_id, session_id)
            if not session:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Chat session not found"
                )
            
            # Update the session
            updates = {}
            if update_data.title is not None:
                updates['title'] = update_data.title
            
            updated_session = await self.chat_repo.update_chat_session(
                user_id, session_id, **updates
            )
            
            if not updated_session:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Chat session not found"
                )
            
            logger.info(f"Chat session updated successfully: {session_id}")
            return updated_session
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating chat session: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update chat session"
            )
    
    async def delete_chat_session(
        self,
        user_id: int,
        session_id: str
    ) -> str:
        """
        Delete a chat session and all its messages.
        
        Args:
            user_id: ID of the user
            session_id: ID of the chat session
            
        Returns:
            Success message
            
        Raises:
            HTTPException: If session not found or deletion fails
        """
        try:
            # Verify session exists and belongs to user
            session = await self.chat_repo.get_chat_session(user_id, session_id)
            if not session:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Chat session not found"
                )
            
            # Invalidate cache first
            await self.chat_cache.invalidate_session_cache(session_id)
            
            # Delete all messages
            await self.message_repo.delete_all_session_messages(session_id)
            
            # Then delete the session
            success = await self.chat_repo.delete_chat_session(user_id, session_id)
            
            if not success:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Chat session not found"
                )
            
            logger.info(f"Chat session deleted successfully: {session_id}")
            return "Chat session deleted successfully"
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting chat session: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to delete chat session"
            )

    def _build_conversation_tree(self, messages: List[Dict[str, Any]]) -> List[ConversationTree]:
        """
        Build a conversation tree from a list of messages.
        
        Args:
            messages: List of message dicts
            
        Returns:
            List of ConversationTree nodes (root messages)
        """
        # Create a map of message_id to message for quick lookup
        message_map = {msg['message_id']: msg for msg in messages}
        
        # Create a map of parent_id to list of children
        children_map = {}
        root_messages = []
        
        for message in messages:
            if message.get('parent_message_id') is None:
                root_messages.append(message)
            else:
                parent_id = message['parent_message_id']
                if parent_id not in children_map:
                    children_map[parent_id] = []
                children_map[parent_id].append(message)
        
        def build_tree_node(message: Dict[str, Any]) -> ConversationTree:
            # Convert message dict to MessageResponse
            message_response = MessageResponse(
                message_id=message['message_id'],
                session_id=message['session_id'],
                user_id=message['user_id'],
                role=MessageRole(message['role']),
                content=message['content'],
                message_index=message['message_index'],
                parent_message_id=message.get('parent_message_id'),
                token_count=message['token_count'],
                is_active=message.get('is_active', True),
                created_at=message['created_at']
            )
            
            # Build children
            children = []
            if message['message_id'] in children_map:
                for child_message in sorted(children_map[message['message_id']], key=lambda x: x['message_index']):
                    children.append(build_tree_node(child_message))
            
            return ConversationTree(
                message=message_response,
                children=children
            )
        
        # Build tree for each root message
        tree = []
        for root_message in sorted(root_messages, key=lambda x: x['message_index']):
            tree.append(build_tree_node(root_message))
        
        return tree

    # # Fix A: Proper Delete Operation with Rollback
    # async def delete_chat_session_transactional(
    #     self,
    #     user_id: int,
    #     session_id: str
    # ) -> str:
    #     """Delete chat session with proper transaction handling"""
    #     transaction_id = None
    #     try:
    #         # Verify session exists and belongs to user
    #         session = await self.chat_repo.get_chat_session(user_id, session_id)
    #         if not session:
    #             raise HTTPException(
    #                 status_code=status.HTTP_404_NOT_FOUND,
    #                 detail="Chat session not found"
    #             )
            
    #         # Begin cache transaction
    #         transaction_id = await self.chat_cache.begin_transaction(session_id)
            
    #         # Step 1: Delete messages (can be rolled back)
    #         deleted_count = await self.message_repo.delete_all_session_messages(session_id)
            
    #         # Step 2: Delete session
    #         success = await self.chat_repo.delete_chat_session(user_id, session_id)
    #         if not success:
    #             # Rollback message deletion if session deletion fails
    #             raise Exception("Failed to delete chat session")
            
    #         # Step 3: Invalidate cache (after successful DB operations)
    #         await self.chat_cache.invalidate_session_cache(session_id)
    #         await self.chat_cache.commit_transaction(session_id, transaction_id)
            
    #         logger.info(f"Chat session deleted: {session_id}, messages: {deleted_count}")
    #         return "Chat session deleted successfully"
            
    #     except HTTPException:
    #         if transaction_id:
    #             await self.chat_cache.rollback_transaction(session_id, transaction_id)
    #         raise
    #     except Exception as e:
    #         if transaction_id:
    #             await self.chat_cache.rollback_transaction(session_id, transaction_id)
    #         logger.error(f"Error deleting chat session: {e}")
    #         raise HTTPException(
    #             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    #             detail="Failed to delete chat session"
    #         )

