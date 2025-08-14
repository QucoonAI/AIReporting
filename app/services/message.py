import json
from typing import Optional, List, Dict, Any, Tuple
from fastapi import HTTPException, status
from app.repositories.message import MessageRepository
from app.repositories.chat import ChatRepository
from app.repositories.data_source import DataSourceRepository
from app.services.ai_service import AIQuery
from app.services.mock_llm import MockLLMService
from app.schemas.chat import ChatMessageRequest, MessageRole
from .redis_managers.factory import RedisServiceFactory
from app.core.utils import logger, make_json_serializable
from app.config.settings import get_settings


settings = get_settings()
agent = AIQuery()

class MessageService:
    """Service class for handling message operations"""
    
    def __init__(
        self,
        message_repo: MessageRepository,
        chat_repo: ChatRepository,
        data_source_repo: DataSourceRepository,
        llm_service: MockLLMService,
        redis_factory: RedisServiceFactory,
    ):
        self.message_repo = message_repo
        self.chat_repo = chat_repo
        self.data_source_repo = data_source_repo
        self.redis_factory = redis_factory
        self.llm_service = llm_service
        self.chat_cache = redis_factory.chat_cache_service
    
    async def send_message(
        self,
        user_id: int,
        session_id: str,
        message_data: ChatMessageRequest
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        """
        Send message following the specified algorithm with ChatCacheService.
        
        Returns:
            Tuple of (assistant_message, limit_message)
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
            # user_token_count = self.llm_service.calculate_token_count(message_data.content) # Mock llm
            
            logger.info("# 2.2 Get context messages, tokens, and session info (cache-first)")
            # 2.2 Get context messages, tokens, and session info (cache-first)
            context_messages, context_token_count, session_info = await self._get_context_with_tokens_cached(
                session_id, session['data_source_id'], message_data.parent_message_id
            )
            
            logger.info("# 2.3 Block if user message alone exceeds limit")
            # 2.3 Block if user message alone exceeds limit
            if context_token_count + user_token_count > settings.DEFAULT_MAX_TOKENS:
                token_info = await self.chat_cache.get_session_token_info(session_id)
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Message too long. Current session: {context_token_count:,} tokens, "
                        f"Your message: {user_token_count:,} tokens, "
                        f"Would total: {context_token_count + user_token_count:,} tokens. "
                        f"Session limit: {settings.DEFAULT_MAX_TOKENS:,} tokens. "
                        f"Please start a new session or send a shorter message."
                )
            
            logger.info("# 3. Generate AI response")
            # 3. Generate AI response
            ai_response = await self._generate_ai_response(
                message_data.content, context_messages, session_info
            )

            serializable_response = make_json_serializable(ai_response)
            
            logger.info("# 3.1 NEW: Check if AI response would exceed the limit")
            # 3.1 NEW: Check if AI response would exceed the limit
            ai_response_count = agent.token_count(message_data.content)    # Real llm
            # ai_response_count = self.llm_service.calculate_token_count(message_data.content) # Mock llm
            
            total_after_ai = context_token_count + user_token_count + ai_response_count
            if total_after_ai > settings.DEFAULT_MAX_TOKENS:
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
                content=json.dumps(serializable_response),
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
            if total_tokens_after_ai >= settings.DEFAULT_MAX_TOKENS:
                limit_message = (
                    f"🚫 Chat session has reached the maximum token limit ({settings.DEFAULT_MAX_TOKENS:,} tokens). "
                    f"This session is now read-only. Please start a new chat session to continue the conversation."
                )
            
            # UPDATED: Enhanced logging with more details
            logger.info(f"Message exchange completed for session: {session_id}, "
                    f"Total tokens: {total_tokens_after_ai:,}, "
                    f"Messages in context: {len(updated_context)}, "
                    f"At limit: {total_tokens_after_ai >= settings.DEFAULT_MAX_TOKENS}")
            
            return assistant_message, limit_message
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to process message"
            )

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
                logger.info(f"Cache hit for session {session_id}: {len(context_messages)} messages, "
                            f"{total_tokens} tokens")
                return context_messages, total_tokens, session_info
            
            # Cache miss - load from database
            logger.info(f"Cache miss for session {session_id}, loading from database")
            
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
            
            logger.info(f"Loaded and cached session {session_id}: {len(final_context)} messages, "
                        f"{final_tokens} tokens")
            return final_context, final_tokens, session_info
            
        except Exception as e:
            logger.error(f"Error getting cached context: {e}")
            return [], 0, {}

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
            if initial_response[1]['requestType'] == "query_response":
                json_extractor = agent.agentic_call(initial_response, data_source_info)
                final_response = agent.final_processor(initial_response, json_extractor)
            else:
                final_response = agent.final_processor(initial_response, None)

            ai_response = {
                "response_type": initial_response[1]['requestType'],
                "response": final_response
            }

            # Generate AI response (Mock LLM)

            # ai_response = await self.llm_service.generate_response_with_context(
            #     message=user_message,
            #     conversation_history=context_messages,
            #     data_source_info=data_source_info
            # )

            # mock_response = await self.llm_service.generate_response(message)

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

