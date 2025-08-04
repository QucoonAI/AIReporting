import json
import uuid
from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict, List, Any, Tuple
import redis.asyncio as redis
from . import RedisKeyManager
from app.core.utils import logger

# Solution 1: Custom JSON Encoder (Recommended)
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)  # or str(obj) if you need exact precision
        return super().default(obj)
    
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
        """Update session data with activity-based TTL"""
        try:
            # Validate messages are active
            validated_messages = [
                msg for msg in context_messages 
                if msg.get('is_active', True)
            ]
            
            session_data = {
                'context_messages': validated_messages,
                'total_tokens': total_tokens,
                'session_info': session_info,
                'last_updated': datetime.now(timezone.utc).isoformat(),
                'message_count': len(validated_messages)
            }
            
            # Adaptive TTL based on activity
            now = datetime.now(timezone.utc)
            last_message_time = max(
                (datetime.fromisoformat(msg['created_at'].replace('Z', '+00:00')) 
                for msg in validated_messages),
                default=now
            )
            
            time_since_last = (now - last_message_time).total_seconds()
            
            # More recent activity = longer TTL
            if time_since_last < 300:  # 5 minutes
                ttl = 7200  # 2 hours
            elif time_since_last < 1800:  # 30 minutes
                ttl = 3600  # 1 hour
            else:
                ttl = 1800  # 30 minutes
            
            session_key = self.key_manager.chat_session_key(session_id)
            await self.redis_client.setex(
                session_key,
                ttl,
                json.dumps(session_data, cls=DecimalEncoder, separators=(',', ':'))
            )
            
            logger.debug(f"Updated session {session_id} with adaptive TTL: {ttl}s")
            
        except Exception as e:
            logger.error(f"Error updating session data with adaptive TTL: {e}")

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



class TransactionalCacheService(ChatCacheService):
    """Enhanced cache service with transactional operations"""
    
    async def begin_transaction(self, session_id: str) -> str:
        """Begin a cache transaction and return transaction ID"""
        transaction_id = str(uuid.uuid4())
        backup_key = f"backup:{session_id}:{transaction_id}"
        session_key = self.key_manager.chat_session_key(session_id)
        
        # Backup current state
        current_data = await self.redis_client.get(session_key)
        if current_data:
            await self.redis_client.setex(backup_key, 300, current_data)  # 5 min TTL
        
        return transaction_id
    
    async def commit_transaction(self, session_id: str, transaction_id: str) -> None:
        """Commit transaction and clean up backup"""
        backup_key = f"backup:{session_id}:{transaction_id}"
        await self.redis_client.delete(backup_key)
    
    async def rollback_transaction(self, session_id: str, transaction_id: str) -> None:
        """Rollback cache to backup state"""
        backup_key = f"backup:{session_id}:{transaction_id}"
        session_key = self.key_manager.chat_session_key(session_id)
        
        backup_data = await self.redis_client.get(backup_key)
        if backup_data:
            await self.redis_client.setex(session_key, self.context_ttl, backup_data)
        else:
            await self.redis_client.delete(session_key)
        
        await self.redis_client.delete(backup_key)


