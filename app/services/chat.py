import json
from typing import Optional, List, Dict, Any, Tuple
from fastapi import HTTPException, status
from app.repositories.chat import ChatRepository
from app.repositories.message import MessageRepository
from app.repositories.data_source import DataSourceRepository
from app.services.llm_services.llm import MockLLMService
from app.services.ai_service import AIQuery
from .redis_managers.factory import RedisServiceFactory
from app.schemas.chat import (
    ChatSessionCreateRequest, ChatSessionUpdateRequest, ChatMessageRequest,
    MessageRole, ConversationTree, MessageResponse
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
        self.default_max_tokens = 50000
    
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
    
    # Message service method
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
    
    # Message service method
    async def send_message(
        self,
        user_id: int,
        session_id: str,
        message_data: ChatMessageRequest
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        """
        Send message following the specified algorithm with ChatCacheService.
        
        Returns:
            Tuple of (user_message, assistant_message, updated_session, limit_message)
        """
        try:
            logger.info("# 1. Basic validation of session")
            # 1. Basic validation of session
            session = await self.chat_repo.get_chat_session(user_id, session_id)
            if not session:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Chat session not found"
                )
            
            logger.info("# 2. NEW: Check if session is already at token limit")
            # 2. NEW: Check if session is already at token limit
            is_at_limit = await self.chat_cache.is_session_at_limit(session_id)
            if is_at_limit:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Chat session has reached the maximum token limit. Please start a new session to continue."
                )
            
            logger.info("# 2.1 Token check")
            # 2.1 Token check
            user_token_count = agent.token_count(message_data.content)    # Real llm
            # user_token_count = self.mock_llm_service.calculate_token_count(message_data.content) # Mock llm
            
            logger.info("# 2.2 Get context messages, tokens, and session info (cache-first)")
            # 2.2 Get context messages, tokens, and session info (cache-first)
            context_messages, context_token_count, session_info = await self._get_context_with_tokens_cached(
                session_id, session['data_source_id'], message_data.parent_message_id
            )
            
            logger.info("# 2.3 Block if user message alone exceeds limit")
            # 2.3 Block if user message alone exceeds limit
            if context_token_count + user_token_count > self.default_max_tokens:
                token_info = await self.chat_cache.get_session_token_info(session_id)
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Message too long. Current session: {context_token_count:,} tokens, "
                        f"Your message: {user_token_count:,} tokens, "
                        f"Would total: {context_token_count + user_token_count:,} tokens. "
                        f"Session limit: {self.default_max_tokens:,} tokens. "
                        f"Please start a new session or send a shorter message."
                )
            
            logger.info("# 3. Generate AI response")
            # 3. Generate AI response
            ai_response = await self._generate_ai_response(
                message_data.content, context_messages, session_info
            )
            
            logger.info("# 3.1 NEW: Check if AI response would exceed the limit")
            # 3.1 NEW: Check if AI response would exceed the limit
            ai_response_count = agent.token_count(message_data.content)    # Real llm
            # ai_response_count = self.mock_llm_service.calculate_token_count(message_data.content) # Mock llm
            
            total_after_ai = context_token_count + user_token_count + ai_response_count
            if total_after_ai > self.default_max_tokens:
                logger.warning(f"Session {session_id} will reach limit after AI response: {total_after_ai} tokens")
            
            logger.info("# 4. Store both messages in database")
            # 4. Store both messages in database
            next_index = session.get('message_count', 0)
            
            user_message = await self.message_repo.create_message(
                session_id=session_id,
                user_id=user_id,
                role=MessageRole.USER,
                content=message_data.content,
                token_count=user_token_count,
                message_index=next_index,
                parent_message_id=message_data.parent_message_id
            )
            
            assistant_message = await self.message_repo.create_message(
                session_id=session_id,
                user_id=user_id,
                role=MessageRole.ASSISTANT,
                content=json.dumps(ai_response),
                token_count=ai_response_count,
                message_index=next_index + 1,
                parent_message_id=user_message['message_id']
            )
            
            logger.info("# 5. Update cache with new messages (no trimming)")
            # 5. Update cache with new messages (no trimming)
            new_messages = [
                {
                    "role": MessageRole.USER,
                    "content": message_data.content,
                    "token_count": user_token_count,
                    "message_id": user_message['message_id'],
                    "created_at": user_message['created_at']
                },
                {
                    "role": MessageRole.ASSISTANT,
                    "content": ai_response["response"],
                    "token_count": ai_response_count,
                    "message_id": assistant_message['message_id'],
                    "created_at": assistant_message['created_at']
                }
            ]

            updated_context, total_tokens_after_ai = await self.chat_cache.append_messages(
                session_id, new_messages, session_info
            )
            logger.info("# 6. Check if addition of AI response exceeds token limit")
            # 6. Check if addition of AI response exceeds token limit
            limit_message = None
            if total_tokens_after_ai >= self.default_max_tokens:
                limit_message = (
                    f"🚫 Chat session has reached the maximum token limit ({self.default_max_tokens:,} tokens). "
                    f"This session is now read-only. Please start a new chat session to continue the conversation."
                )
            
            # UPDATED: Enhanced logging with more details
            logger.info(f"Message exchange completed for session: {session_id}, "
                    f"Total tokens: {total_tokens_after_ai:,}, "
                    f"Messages in context: {len(updated_context)}, "
                    f"At limit: {total_tokens_after_ai >= self.default_max_tokens}")
            
            return assistant_message, limit_message
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to process message"
            )

    # Message service method
    async def _get_context_with_tokens_cached(
        self, 
        session_id: str,
        data_source_id: int,
        parent_message_id: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], int, Dict[str, Any]]:
        """
        Get context messages, tokens, and session info with intelligent caching.
        """
        try:
            # Try cache first using ChatCacheService
            context_messages, total_tokens, session_info = await self.chat_cache.get_session_data(session_id)
            
            if context_messages and session_info:
                logger.debug(f"Cache hit for session {session_id}: {len(context_messages)} messages, "
                            f"{total_tokens} tokens")
                return context_messages, total_tokens, session_info
            
            # Cache miss - load from database
            logger.debug(f"Cache miss for session {session_id}, loading from database")
            
            # Get conversation context
            active_path = await self.message_repo.get_active_conversation_path(
                session_id, parent_message_id
            )
            
            # Get data source information
            data_source = await self.data_source_repo.get_data_source_by_id(data_source_id)
            
            # Build session info with data source details
            session_info = {
                "data_source_id": data_source_id,
                "data_source_name": data_source.data_source_name if data_source else "Unknown",
                "data_source_type": data_source.data_source_type.value if data_source else "unknown",
                "data_source_url": data_source.data_source_url if data_source else None,
                "data_source_schema": data_source.data_source_schema if data_source else None
            }
            
            # Build context messages
            context_messages = []
            total_tokens = 0
            
            for message in active_path:
                msg_data = {
                    "role": message['role'],
                    "content": message['content'],
                    "token_count": message['token_count'],
                    "message_id": message['message_id'],
                    "created_at": message['created_at']
                }
                context_messages.append(msg_data)
                total_tokens += message['token_count']
            
            # Cache for future requests with token-based limits using ChatCacheService
            await self.chat_cache.update_session_data(
                session_id, context_messages, total_tokens, session_info
            )
            
            # Get the potentially trimmed data
            final_context, final_tokens, _ = await self.chat_cache.get_session_data(session_id)
            
            logger.debug(f"Loaded and cached session {session_id}: {len(final_context)} messages, "
                        f"{final_tokens} tokens")
            
            return final_context, final_tokens, session_info
            
        except Exception as e:
            logger.error(f"Error getting cached context: {e}")
            return [], 0, {}

    # Message service method
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

    # Message service method
    async def _generate_ai_response(
        self,
        user_message: str,
        context_messages: List[Dict[str, Any]],
        session_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate AI response with context and cached data source information.
        """
        try:
            # Use cached data source info from session_info
            data_source_info = {
                "type": session_info.get("data_source_type", "unknown"),
                "name": session_info.get("data_source_name", "Unknown"),
                "url": session_info.get("data_source_url"),
                "schema": session_info.get("data_source_schema")
            }

            message = user_message # user message (str)
            memory = json.dumps(context_messages) # chat context (str)

            initial_response = agent.initial_processor(message, memory)
            json_extractor = agent.agentic_call(initial_response, data_source_info)
            final_response = agent.final_processor(message, json_extractor)

            ai_response = {
                "response_type": json_extractor[0],
                "response": final_response
            }

            # Generate AI response (Mock LLM)

            # ai_response = await self.mock_llm_service.generate_response_with_context(
            #     message=user_message,
            #     conversation_history=context_messages,
            #     data_source_info=data_source_info
            # )

            # mock_response = await self.mock_llm_service.generate_response(message)

            # ai_response = {
            #     "response_type": "text",
            #     "response": mock_response["content"]
            # }
            
            return ai_response
            
        except Exception as e:
            logger.error(f"Error generating AI response: {e}")
            # Return minimal fallback response
            fallback_content = "I understand your message, but I'm having trouble generating a detailed response right now."
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=fallback_content
            )

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