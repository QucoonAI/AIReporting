import json
from datetime import datetime, timezone
from typing import Dict, List, Any, Tuple
import redis.asyncio as redis
from . import RedisKeyManager
from app.core.utils import logger


class ChatCacheService:
    """Chat session caching service with proper Redis key management"""
    
    def __init__(self, redis_client: redis.Redis, default_max_tokens: int = 50000, app_name: str = "reportai"):
        self.redis_client = redis_client
        self.key_manager = RedisKeyManager(app_name)
        self.default_max_tokens = default_max_tokens
        self.context_ttl = 3600  # 1 hour
    
    async def get_session_data(self, session_id: str) -> Tuple[List[Dict[str, Any]], int, Dict[str, Any]]:
        """Get context messages, token count, and session info from cache"""
        try:
            session_key = self.key_manager.chat_session_key(session_id)
            session_data = await self.redis_client.get(session_key)
            
            if session_data:
                data = json.loads(session_data)
                return (
                    data.get('context_messages', []),
                    data.get('total_tokens', 0),
                    data.get('session_info', {})
                )
            
            return [], 0, {}
            
        except Exception as e:
            logger.error(f"Error getting session data from cache: {e}")
            return [], 0, {}
    
    async def update_session_data(
        self, 
        session_id: str, 
        context_messages: List[Dict[str, Any]], 
        total_tokens: int,
        session_info: Dict[str, Any]
    ) -> None:
        """Update context, tokens, and session info in cache with token-based limits"""
        try:
            session_data = {
                'context_messages': context_messages,
                'total_tokens': total_tokens,
                'session_info': session_info,
                'last_updated': datetime.now(timezone.utc).isoformat()
            }
            
            session_key = self.key_manager.chat_session_key(session_id)
            await self.redis_client.setex(
                session_key,
                self.context_ttl,
                json.dumps(session_data, separators=(',', ':'))
            )
            
            logger.debug(f"Updated session {session_id}: {len(context_messages)} messages, "
                        f"{total_tokens} tokens, at_limit: {total_tokens >= self.default_max_tokens}")
            
        except Exception as e:
            logger.error(f"Error updating session data in cache: {e}")

    async def append_messages(
        self, 
        session_id: str, 
        new_messages: List[Dict[str, Any]],
        session_info: Dict[str, Any]
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Append new messages to cached context with token-based limits"""
        try:
            # Get current data
            current_context, current_tokens, _ = await self.get_session_data(session_id)
            
            # Append new messages
            updated_context = current_context + new_messages
            new_tokens = sum(msg.get('token_count', 0) for msg in new_messages)
            updated_tokens = current_tokens + new_tokens
            
            # Update cache with token-based trimming
            await self.update_session_data(session_id, updated_context, updated_tokens, session_info)
            
            return updated_context, updated_tokens
            
        except Exception as e:
            logger.error(f"Error appending messages to cache: {e}")
            return current_context, current_tokens
    
    async def invalidate_session_cache(self, session_id: str) -> bool:
        """Invalidate session cache"""
        try:
            session_key = self.key_manager.chat_session_key(session_id)
            result = await self.redis_client.delete(session_key)
            
            if result:
                logger.info(f"Invalidated cache for session: {session_id}")
            
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error invalidating cache: {e}")
            return False
    
    async def get_session_info_from_cache(self, session_id: str) -> Dict[str, Any]:
        """Get just the session info from cache"""
        try:
            _, _, session_info = await self.get_session_data(session_id)
            return session_info
        except Exception as e:
            logger.error(f"Error getting session info from cache: {e}")
            return {}
    
    async def cleanup_expired_sessions(self) -> Dict[str, int]:
        """Clean up expired or old chat sessions"""
        try:
            pattern = self.key_manager.get_chat_session_pattern()
            keys = await self.redis_client.keys(pattern)
            
            expired_count = 0
            old_count = 0
            
            for key in keys:
                ttl = await self.redis_client.ttl(key)
                
                if ttl == -2:  # Key doesn't exist
                    continue
                elif ttl == -1:  # Key exists but no expiry set
                    await self.redis_client.expire(key, self.context_ttl)
                    old_count += 1
                elif ttl < 300:  # Less than 5 minutes remaining
                    await self.redis_client.delete(key)
                    expired_count += 1
            
            logger.info(f"Chat cache cleanup: {expired_count} expired, {old_count} TTL fixed")
            return {"expired_sessions": expired_count, "ttl_fixed": old_count}
            
        except Exception as e:
            logger.error(f"Error during chat cache cleanup: {e}")
            return {"expired_sessions": 0, "ttl_fixed": 0}
    
    async def is_session_at_limit(self, session_id: str) -> bool:
        """Check if session has reached token limit"""
        try:
            _, total_tokens, _ = await self.get_session_data(session_id)
            return total_tokens >= self.default_max_tokens
        except Exception as e:
            logger.error(f"Error checking session limit: {e}")
            return False

    async def get_session_token_info(self, session_id: str) -> Dict[str, Any]:
        """Get detailed token information for a session"""
        try:
            context_messages, total_tokens, session_info = await self.get_session_data(session_id)
            
            usage_percentage = (total_tokens / self.default_max_tokens) if self.default_max_tokens > 0 else 0
            tokens_remaining = max(0, self.default_max_tokens - total_tokens)
            is_at_limit = total_tokens >= self.default_max_tokens
            
            return {
                "session_id": session_id,
                "total_tokens": total_tokens,
                "max_tokens": self.default_max_tokens,
                "tokens_remaining": tokens_remaining,
                "usage_percentage": usage_percentage,
                "is_at_limit": is_at_limit,
                "message_count": len(context_messages),
                "can_send_messages": not is_at_limit
            }
            
        except Exception as e:
            logger.error(f"Error getting token info for session {session_id}: {e}")
            return {
                "session_id": session_id,
                "total_tokens": 0,
                "max_tokens": self.default_max_tokens,
                "tokens_remaining": self.default_max_tokens,
                "usage_percentage": 0.0,
                "is_at_limit": False,
                "message_count": 0,
                "can_send_messages": True
            }

