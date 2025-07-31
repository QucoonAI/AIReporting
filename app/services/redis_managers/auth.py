import uuid
import json
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
import jwt
import redis.asyncio as redis
from fastapi import HTTPException
from . import RedisKeyManager
from app.config.settings import get_settings
from app.core.utils import logger


settings = get_settings()

class AuthService:
    """Authentication service with proper Redis key management"""
    
    def __init__(self, redis_client: redis.Redis, app_name: str = "reportai"):
        self.redis_client = redis_client
        self.key_manager = RedisKeyManager(app_name)
        self.algorithm = "HS256"
        self.access_token_expire = timedelta(minutes=15)
        self.refresh_token_expire = timedelta(days=1)
        self.secret_key = settings.SECRET_KEY
    
    async def create_tokens(
        self, 
        user_id: int, 
        roles: List[str] = None, 
        device_info: str = None, 
        ip_address: str = None
    ) -> Dict[str, Any]:
        """Create access and refresh tokens with Redis session storage"""
        try:
            now = datetime.now(timezone.utc)
            session_id = str(uuid.uuid4())
            
            # Access token payload
            access_payload = {
                "sub": str(user_id),
                "session_id": session_id,
                "type": "access",
                "iat": now,
                "exp": now + self.access_token_expire
            }
            
            # Refresh token payload
            refresh_payload = {
                "sub": str(user_id),
                "session_id": session_id,
                "type": "refresh",
                "iat": now,
                "exp": now + self.refresh_token_expire
            }

            if roles:
                access_payload.update({"role": roles})
                refresh_payload.update({"role": roles})
            
            # Generate tokens
            access_token = jwt.encode(access_payload, self.secret_key, algorithm=self.algorithm)
            refresh_token = jwt.encode(refresh_payload, self.secret_key, algorithm=self.algorithm)
            
            # Store session data in Redis
            session_data = {
                "user_id": user_id,
                "device_info": device_info or "Unknown",
                "ip_address": ip_address,
                "created_at": now.isoformat(),
                "last_used": now.isoformat(),
                "is_active": True,
                "access_token_hash": hashlib.sha256(access_token.encode()).hexdigest()[:16],
                "refresh_token_hash": hashlib.sha256(refresh_token.encode()).hexdigest()[:16]
            }
            
            # Use pipeline for atomic operations
            async with self.redis_client.pipeline(transaction=True) as pipe:
                # Store session with expiration
                session_key = self.key_manager.auth_session_key(session_id)
                await pipe.setex(
                    session_key,
                    int(self.refresh_token_expire.total_seconds()),
                    json.dumps(session_data, default=str)
                )
                
                # Add session to user's active sessions
                user_sessions_key = self.key_manager.user_sessions_key(user_id)
                await pipe.sadd(user_sessions_key, session_id)
                await pipe.expire(user_sessions_key, self.refresh_token_expire)
                
                # Execute pipeline
                await pipe.execute()
            
            logger.info(f"Created auth session for user {user_id}: {session_id}")
            
            return {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "token_type": "bearer",
                "expires_in": int(self.access_token_expire.total_seconds()),
                "session_id": session_id
            }
            
        except Exception as e:
            logger.error(f"Error creating tokens for user {user_id}: {e}")
            raise HTTPException(status_code=500, detail="Failed to create authentication tokens")
    
    async def verify_token(self, token: str, token_type: str = "access") -> Dict[str, Any]:
        """Verify token and update session activity"""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            
            if payload.get("type") != token_type:
                raise HTTPException(status_code=401, detail=f"Invalid {token_type} token")
            
            session_id = payload.get("session_id")
            user_id = int(payload.get("sub"))
            
            # Check session exists and is active
            session_key = self.key_manager.auth_session_key(session_id)
            session_data = await self.redis_client.get(session_key)
            
            if not session_data:
                raise HTTPException(status_code=401, detail="Session expired")
            
            session = json.loads(session_data)
            if not session.get("is_active"):
                raise HTTPException(status_code=401, detail="Session deactivated")
            
            # Update last used timestamp for access tokens
            if token_type == "access":
                session["last_used"] = datetime.now(timezone.utc).isoformat()
                ttl = await self.redis_client.ttl(session_key)
                await self.redis_client.setex(
                    session_key,
                    ttl if ttl > 0 else int(self.refresh_token_expire.total_seconds()),
                    json.dumps(session, default=str)
                )
            
            payload["session_data"] = session
            logger.debug(f"Verified {token_type} token for user {user_id}")
            return payload
            
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token expired")
        except jwt.PyJWTError as e:
            logger.error(f"JWT verification error: {e}")
            raise HTTPException(status_code=401, detail="Invalid token")
        except Exception as e:
            logger.error(f"Token verification error: {e}")
            raise HTTPException(status_code=401, detail="Token verification failed")
    
    async def refresh_access_token(self, refresh_token: str) -> Dict[str, Any]:
        """Generate new access token using refresh token"""
        try:
            payload = await self.verify_token(refresh_token, "refresh")
            
            user_id = int(payload.get("sub"))
            roles = payload.get("role", None)
            session_id = payload.get("session_id")
            
            # Create new access token
            now = datetime.now(timezone.utc)
            new_access_payload = {
                "sub": str(user_id),
                "session_id": session_id,
                "type": "access",
                "iat": now,
                "exp": now + self.access_token_expire
            }
            
            if roles:
                new_access_payload.update({"role": roles})
            
            new_access_token = jwt.encode(new_access_payload, self.secret_key, algorithm=self.algorithm)
            
            # Update session with new access token hash
            session_data = payload["session_data"]
            session_data["access_token_hash"] = hashlib.sha256(new_access_token.encode()).hexdigest()[:16]
            session_data["last_used"] = now.isoformat()
            
            session_key = self.key_manager.auth_session_key(session_id)
            ttl = await self.redis_client.ttl(session_key)
            await self.redis_client.setex(
                session_key,
                ttl if ttl > 0 else int(self.refresh_token_expire.total_seconds()),
                json.dumps(session_data, default=str)
            )
            
            logger.info(f"Refreshed access token for user {user_id}")
            
            return {
                "access_token": new_access_token,
                "token_type": "bearer",
                "expires_in": int(self.access_token_expire.total_seconds())
            }
            
        except Exception as e:
            logger.error(f"Error refreshing access token: {e}")
            raise
    
    async def revoke_session(self, token: str) -> None:
        """Revoke a single session"""
        try:
            payload = jwt.decode(
                token, 
                self.secret_key, 
                algorithms=[self.algorithm],
                options={"verify_exp": False}
            )
            
            session_id = payload.get("session_id")
            user_id = payload.get("sub")
            
            if session_id and user_id:
                async with self.redis_client.pipeline(transaction=True) as pipe:
                    # Remove session
                    session_key = self.key_manager.auth_session_key(session_id)
                    await pipe.delete(session_key)
                    
                    # Remove from user sessions
                    user_sessions_key = self.key_manager.user_sessions_key(int(user_id))
                    await pipe.srem(user_sessions_key, session_id)
                    
                    await pipe.execute()
                
                logger.info(f"Revoked session {session_id} for user {user_id}")
                
        except jwt.PyJWTError:
            # Token might be malformed, but we'll still try to clean up
            logger.warning("Attempted to revoke malformed token")
        except Exception as e:
            logger.error(f"Error revoking session: {e}")
    
    async def revoke_all_user_sessions(self, user_id: int) -> int:
        """Revoke all sessions for a user, returns count of revoked sessions"""
        try:
            user_sessions_key = self.key_manager.user_sessions_key(user_id)
            session_ids = await self.redis_client.smembers(user_sessions_key)
            
            if not session_ids:
                return 0
            
            async with self.redis_client.pipeline(transaction=True) as pipe:
                # Delete all sessions
                for session_id in session_ids:
                    sid = session_id.decode() if isinstance(session_id, bytes) else session_id
                    session_key = self.key_manager.auth_session_key(sid)
                    await pipe.delete(session_key)
                
                # Clear user sessions set
                await pipe.delete(user_sessions_key)
                
                await pipe.execute()
            
            revoked_count = len(session_ids)
            logger.info(f"Revoked {revoked_count} sessions for user {user_id}")
            return revoked_count
            
        except Exception as e:
            logger.error(f"Error revoking all sessions for user {user_id}: {e}")
            return 0
    
    async def get_user_sessions(self, user_id: int) -> List[Dict[str, Any]]:
        """Get all active sessions for a user"""
        try:
            user_sessions_key = self.key_manager.user_sessions_key(user_id)
            session_ids = await self.redis_client.smembers(user_sessions_key)
            
            if not session_ids:
                return []
            
            sessions = []
            
            # Use pipeline for efficient batch operations
            async with self.redis_client.pipeline(transaction=False) as pipe:
                for session_id in session_ids:
                    sid = session_id.decode() if isinstance(session_id, bytes) else session_id
                    session_key = self.key_manager.auth_session_key(sid)
                    await pipe.get(session_key)
                
                session_data_list = await pipe.execute()
                
                for i, session_data in enumerate(session_data_list):
                    if session_data:
                        session = json.loads(session_data)
                        # Add session_id to the session data
                        sid = session_ids[i].decode() if isinstance(session_ids[i], bytes) else session_ids[i]
                        session["session_id"] = sid
                        # Remove sensitive data
                        session.pop("access_token_hash", None)
                        session.pop("refresh_token_hash", None)
                        sessions.append(session)
            
            return sessions
            
        except Exception as e:
            logger.error(f"Error getting sessions for user {user_id}: {e}")
            return []

    async def revoke_session_by_id(self, user_id: int, session_id: str) -> bool:
        """
        Revoke a specific session by session ID.
        
        Args:
            user_id: ID of the user who owns the session
            session_id: ID of the session to revoke
            
        Returns:
            True if session was revoked, False if session not found
            
        Raises:
            Exception: If revocation fails due to Redis errors
        """
        try:
            # First verify the session exists and belongs to the user
            session_key = self.key_manager.auth_session_key(session_id)
            session_data = await self.redis_client.get(session_key)
            
            if not session_data:
                logger.warning(f"Session {session_id} not found")
                return False
            
            try:
                session = json.loads(session_data)
                if session.get("user_id") != user_id:
                    logger.warning(f"Session {session_id} does not belong to user {user_id}")
                    return False
            except json.JSONDecodeError:
                logger.error(f"Invalid session data for session {session_id}")
                return False
            
            # Revoke the session using atomic pipeline operations
            async with self.redis_client.pipeline(transaction=True) as pipe:
                # Remove session data
                await pipe.delete(session_key)
                
                # Remove from user sessions set
                user_sessions_key = self.key_manager.user_sessions_key(user_id)
                await pipe.srem(user_sessions_key, session_id)
                
                # Execute pipeline
                result = await pipe.execute()
                
                # Check if session was actually deleted (first operation result)
                session_deleted = result[0] > 0
                
                if session_deleted:
                    logger.info(f"Session {session_id} revoked successfully for user {user_id}")
                    return True
                else:
                    logger.warning(f"Session {session_id} was not found during deletion")
                    return False
            
        except Exception as e:
            logger.error(f"Error revoking session {session_id} for user {user_id}: {e}")
            raise Exception(f"Failed to revoke session: {str(e)}")
    
    async def get_session_info(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get session information by session ID.
        
        Args:
            session_id: ID of the session
            
        Returns:
            Session information dict or None if not found
        """
        try:
            session_key = self.key_manager.auth_session_key(session_id)
            session_data = await self.redis_client.get(session_key)
            
            if not session_data:
                return None
            
            session = json.loads(session_data)
            session["session_id"] = session_id
            
            # Remove sensitive data
            session.pop("access_token_hash", None)
            session.pop("refresh_token_hash", None)
            
            return session
            
        except Exception as e:
            logger.error(f"Error getting session info for {session_id}: {e}")
            return None
    
    async def is_session_valid(self, session_id: str, user_id: int) -> bool:
        """
        Check if a session is valid and belongs to the specified user.
        
        Args:
            session_id: ID of the session to check
            user_id: ID of the user who should own the session
            
        Returns:
            True if session is valid and belongs to user, False otherwise
        """
        try:
            session_info = await self.get_session_info(session_id)
            
            if not session_info:
                return False
            
            return (
                session_info.get("user_id") == user_id and
                session_info.get("is_active", False)
            )
            
        except Exception as e:
            logger.error(f"Error checking session validity for {session_id}: {e}")
            return False

