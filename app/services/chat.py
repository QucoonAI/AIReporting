from typing import Optional, List, Dict, Any, Tuple
from fastapi import HTTPException, status
from repositories.chat import ChatRepository
from repositories.message import MessageRepository
from repositories.data_source import DataSourceRepository
from services.llm import MockLLMService
from schemas.chat import (
    ChatSessionCreateRequest, ChatSessionUpdateRequest, ChatMessageRequest,
    EditMessageRequest, MessageRole, ConversationTree, MessageResponse
)
from core.utils import logger


class ChatService:
    """Service class for handling chat operations"""
    
    def __init__(
        self,
        chat_repo: ChatRepository,
        message_repo: MessageRepository,
        data_source_repo: DataSourceRepository,
        llm_service: MockLLMService
    ):
        self.chat_repo = chat_repo
        self.message_repo = message_repo
        self.data_source_repo = data_source_repo
        self.llm_service = llm_service
        self.default_max_tokens = 50000
        self.token_warning_threshold = 0.8  # Warn when 80% of tokens used
    
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
            messages = await self.message_repo.get_session_messages(session_id)
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
            
            # Delete all messages first
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
    
    async def send_message(
        self,
        user_id: int,
        session_id: str,
        message_data: ChatMessageRequest
    ) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        """
        Send a message and get AI response.
        
        Args:
            user_id: ID of the user
            session_id: ID of the chat session
            message_data: Message data
            
        Returns:
            Tuple of (user_message, assistant_message, updated_session)
            
        Raises:
            HTTPException: If session not found, token limit exceeded, or processing fails
        """
        try:
            # Verify session exists and belongs to user
            session = await self.chat_repo.get_chat_session(user_id, session_id)
            if not session:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Chat session not found"
                )
            
            # Calculate user message tokens
            user_token_count = self.llm_service.calculate_token_count(message_data.content)
            
            # Check token limits (using default max_tokens since it's not in the simplified session)
            current_active_tokens = await self.message_repo.calculate_active_branch_tokens(session_id)
            estimated_response_tokens = user_token_count * 2  # Rough estimate
            
            if current_active_tokens + user_token_count + estimated_response_tokens > self.default_max_tokens:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Token limit would be exceeded. Current: {current_active_tokens}, "
                           f"Max: {self.default_max_tokens}"
                )
            
            # Get next message index (using current message count from session)
            next_index = session.get('message_count', 0)
            
            # Create user message
            user_message = await self.message_repo.create_message(
                session_id=session_id,
                user_id=user_id,
                role=MessageRole.USER,
                content=message_data.content,
                token_count=user_token_count,
                message_index=next_index,
                parent_message_id=message_data.parent_message_id
            )
            
            # Get data source info for context
            data_source = await self.data_source_repo.get_data_source_by_id(session['data_source_id'])
            data_source_info = {
                "type": data_source.data_source_type.value if data_source else "unknown",
                "name": data_source.data_source_name if data_source else "Unknown"
            }
            
            # Get conversation history for context
            conversation_history = await self._get_conversation_context(session_id, user_message['message_id'])
            
            # Generate AI response
            ai_response = await self.llm_service.generate_response_with_context(
                message=message_data.content,
                conversation_history=conversation_history,
                data_source_info=data_source_info
            )
            
            # Create assistant message
            assistant_message = await self.message_repo.create_message(
                session_id=session_id,
                user_id=user_id,
                role=MessageRole.ASSISTANT,
                content=ai_response["content"],
                token_count=ai_response["token_count"],
                message_index=next_index + 1,
                parent_message_id=user_message['message_id']
            )
            
            # Update session statistics
            new_total_tokens = await self.message_repo.calculate_total_session_tokens(session_id)
            new_active_tokens = await self.message_repo.calculate_active_branch_tokens(session_id)
            
            updated_session = await self.chat_repo.update_chat_session(
                user_id=user_id,
                session_id=session_id,
                message_count=session.get('message_count', 0) + 2,
                total_tokens_all_branches=new_total_tokens,
                active_branch_tokens=new_active_tokens
            )
            
            logger.info(f"Message exchange completed for session: {session_id}")
            return user_message, assistant_message, updated_session
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to process message"
            )
    
    async def edit_message(
        self,
        user_id: int,
        session_id: str,
        edit_data: EditMessageRequest
    ) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        """
        Edit a message and regenerate the conversation from that point.
        
        Args:
            user_id: ID of the user
            session_id: ID of the chat session
            edit_data: Edit message data
            
        Returns:
            Tuple of (edited_message, new_assistant_message, updated_session)
            
        Raises:
            HTTPException: If message not found or edit fails
        """
        try:
            # Verify session exists and belongs to user
            session = await self.chat_repo.get_chat_session(user_id, session_id)
            if not session:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Chat session not found"
                )
            
            # Get the message to edit
            original_message = await self.message_repo.get_message(session_id, edit_data.message_id)
            if not original_message:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Message not found"
                )
            
            if original_message['role'] != MessageRole.USER.value:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Only user messages can be edited"
                )
            
            # Deactivate all messages that come after this message in the branch
            await self.message_repo.deactivate_branch_messages(session_id, edit_data.message_id)
            
            # Calculate new token count for edited message
            new_token_count = self.llm_service.calculate_token_count(edit_data.new_content)
            
            # Update the original message
            edited_message = await self.message_repo.update_message(
                session_id=session_id,
                message_id=edit_data.message_id,
                content=edit_data.new_content,
                token_count=new_token_count
            )
            
            # Get data source info for context
            data_source = await self.data_source_repo.get_data_source_by_id(session['data_source_id'])
            data_source_info = {
                "type": data_source.data_source_type.value if data_source else "unknown",
                "name": data_source.data_source_name if data_source else "Unknown"
            }
            
            # Get conversation history up to the edited message
            conversation_history = await self._get_conversation_context(session_id, edit_data.message_id)
            
            # Generate new AI response
            ai_response = await self.llm_service.generate_response_with_context(
                message=edit_data.new_content,
                conversation_history=conversation_history,
                data_source_info=data_source_info
            )
            
            # Create new assistant message
            new_assistant_message = await self.message_repo.create_message(
                session_id=session_id,
                user_id=user_id,
                role=MessageRole.ASSISTANT,
                content=ai_response["content"],
                token_count=ai_response["token_count"],
                message_index=original_message['message_index'] + 1,
                parent_message_id=edit_data.message_id
            )
            
            # Update session statistics
            new_total_tokens = await self.message_repo.calculate_total_session_tokens(session_id)
            new_active_tokens = await self.message_repo.calculate_active_branch_tokens(session_id)
            
            updated_session = await self.chat_repo.update_chat_session(
                user_id=user_id,
                session_id=session_id,
                total_tokens_all_branches=new_total_tokens,
                active_branch_tokens=new_active_tokens
            )
            
            logger.info(f"Message edited and regenerated for session: {session_id}")
            return edited_message, new_assistant_message, updated_session
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error editing message: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to edit message"
            )

    async def check_token_usage(
        self,
        user_id: int,
        session_id: str
    ) -> Dict[str, Any]:
        """
        Check token usage for a session and provide warnings if needed.
        
        Args:
            user_id: ID of the user
            session_id: ID of the chat session
            
        Returns:
            Dict with token usage information
            
        Raises:
            HTTPException: If session not found
        """
        try:
            session = await self.chat_repo.get_chat_session(user_id, session_id)
            if not session:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Chat session not found"
                )
            
            active_tokens = await self.message_repo.calculate_active_branch_tokens(session_id)
            total_tokens = await self.message_repo.calculate_total_session_tokens(session_id)
            
            usage_percentage = active_tokens / self.default_max_tokens if self.default_max_tokens > 0 else 0
            warning_needed = usage_percentage >= self.token_warning_threshold
            
            return {
                "active_branch_tokens": active_tokens,
                "total_session_tokens": total_tokens,
                "max_tokens": self.default_max_tokens,
                "usage_percentage": usage_percentage,
                "warning_needed": warning_needed,
                "tokens_remaining": self.default_max_tokens - active_tokens
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error checking token usage: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to check token usage"
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
    
    async def _get_conversation_context(
        self,
        session_id: str,
        up_to_message_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get conversation history up to a specific message for LLM context.
        
        Args:
            session_id: ID of the chat session
            up_to_message_id: Get history up to this message ID
            
        Returns:
            List of message dictionaries for LLM context
        """
        try:
            # Get the active conversation path up to the specified message
            active_path = await self.message_repo.get_active_conversation_path(
                session_id, up_to_message_id
            )
            
            # Convert to format suitable for LLM
            context = []
            for message in active_path[:-1]:  # Exclude the current message
                context.append({
                    "role": message['role'],
                    "content": message['content'],
                    "token_count": message['token_count']
                })
            
            return context
            
        except Exception as e:
            logger.error(f"Error getting conversation context: {e}")
            return []

