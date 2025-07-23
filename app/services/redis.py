import uuid
import json
import random
import hashlib
import string
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
import jwt
import redis.asyncio as redis
from fastapi import HTTPException
from config.settings import get_settings


settings = get_settings()

class AsyncRedisService:
    def __init__(self, redis_client: redis.Redis):
        self.redis_client = redis_client
        self.algorithm = "HS256"
        self.access_token_expire = timedelta(minutes=15)
        self.refresh_token_expire = timedelta(days=7)
    
    async def create_tokens(self, user_id: int, roles: List[str] = None, device_info: str = None, ip_address: str = None) -> Dict:
        """Create access and refresh tokens with Redis session storage"""
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
        access_token = jwt.encode(access_payload, settings.SECRET_KEY, algorithm=self.algorithm)
        refresh_token = jwt.encode(refresh_payload, settings.SECRET_KEY, algorithm=self.algorithm)
        
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
            # Store session with expiration (7 days)
            await pipe.setex(
                f"session:{session_id}",
                int(self.refresh_token_expire.total_seconds()),
                json.dumps(session_data, default=str)
            )
            
            # Add session to user's active sessions
            await pipe.sadd(f"user_sessions:{user_id}", session_id)
            await pipe.expire(f"user_sessions:{user_id}", self.refresh_token_expire)
            
            # Execute pipeline
            await pipe.execute()
        
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
            "expires_in": int(self.access_token_expire.total_seconds()),
            "session_id": session_id
        }
    
    async def verify_token(self, token: str, token_type: str = "access") -> Dict:
        """Verify token and update session activity"""
        try:
            payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[self.algorithm])
            
            if payload.get("type") != token_type:
                raise HTTPException(status_code=401, detail=f"Invalid {token_type} token")
            
            session_id = payload.get("session_id")
            user_id = int(payload.get("sub"))
            
            # Check session exists and is active
            session_data = await self.redis_client.get(f"session:{session_id}")
            if not session_data:
                raise HTTPException(status_code=401, detail="Session expired")
            
            session = json.loads(session_data)
            if not session.get("is_active"):
                raise HTTPException(status_code=401, detail="Session deactivated")
            
            # Update last used timestamp for access tokens
            if token_type == "access":
                session["last_used"] = datetime.now(timezone.utc).isoformat()
                ttl = await self.redis_client.ttl(f"session:{session_id}")
                await self.redis_client.setex(
                    f"session:{session_id}",
                    ttl if ttl > 0 else int(self.refresh_token_expire.total_seconds()),
                    json.dumps(session, default=str)
                )
            
            payload["session_data"] = session
            return payload
            
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token expired")
        except jwt.PyJWTError:
            raise HTTPException(status_code=401, detail="Invalid token")
    
    async def refresh_access_token(self, refresh_token: str) -> Dict:
        """Generate new access token using refresh token"""
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
        
        new_access_token = jwt.encode(new_access_payload, settings.SECRET_KEY, algorithm=self.algorithm)
        
        # Update session with new access token hash
        session_data = payload["session_data"]
        session_data["access_token_hash"] = hashlib.sha256(new_access_token.encode()).hexdigest()[:16]
        session_data["last_used"] = now.isoformat()
        
        ttl = await self.redis_client.ttl(f"session:{session_id}")
        await self.redis_client.setex(
            f"session:{session_id}",
            ttl if ttl > 0 else int(self.refresh_token_expire.total_seconds()),
            json.dumps(session_data, default=str)
        )
        
        return {
            "access_token": new_access_token,
            "token_type": "bearer",
            "expires_in": int(self.access_token_expire.total_seconds())
        }
    
    async def revoke_session(self, token: str):
        """Revoke a single session"""
        try:
            payload = jwt.decode(
                token, 
                settings.SECRET_KEY, 
                algorithms=[self.algorithm],
                options={"verify_exp": False}
            )
            
            session_id = payload.get("session_id")
            user_id = payload.get("sub")
            
            if session_id and user_id:
                async with self.redis_client.pipeline(transaction=True) as pipe:
                    # Remove session
                    await pipe.delete(f"session:{session_id}")
                    # Remove from user sessions
                    await pipe.srem(f"user_sessions:{user_id}", session_id)
                    await pipe.execute()
                
        except jwt.PyJWTError:
            # Token might be malformed, but we'll still try to clean up
            pass
    
    async def revoke_all_user_sessions(self, user_id: int):
        """Revoke all sessions for a user"""
        # Get all user sessions
        session_ids = await self.redis_client.smembers(f"user_sessions:{user_id}")
        
        if session_ids:
            async with self.redis_client.pipeline(transaction=True) as pipe:
                # Delete all sessions
                session_keys = [f"session:{sid.decode() if isinstance(sid, bytes) else sid}" for sid in session_ids]
                await pipe.delete(*session_keys)
                
                # Clear user sessions set
                await pipe.delete(f"user_sessions:{user_id}")
                
                await pipe.execute()
    
    async def get_user_sessions(self, user_id: int) -> List[Dict]:
        """Get all active sessions for a user"""
        session_ids = await self.redis_client.smembers(f"user_sessions:{user_id}")
        sessions = []
        
        if session_ids:
            # Use pipeline for efficient batch operations
            async with self.redis_client.pipeline(transaction=False) as pipe:
                for session_id in session_ids:
                    sid = session_id.decode() if isinstance(session_id, bytes) else session_id
                    await pipe.get(f"session:{sid}")
                
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

    # ------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------

    async def generate_otp(self, length: int = 6) -> str:
        """Generate a random OTP (One Time Password)."""
        return ''.join(random.choices(string.digits, k=length))
    
    async def store_otp(self, key: str, user_id: int, otp: str, expiry_minutes: int = 30) -> None:
        """Store OTP in Redis with expiration."""
        data = {
            "user_id": user_id,
            "otp": otp,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=expiry_minutes)).isoformat()
        }
        
        # Store with expiration
        await self.redis_client.setex(
            key, 
            timedelta(minutes=expiry_minutes), 
            json.dumps(data)
        )
    
    async def verify_otp(self, key: str, provided_otp: str) -> Optional[Dict[str, Any]]:
        """Verify OTP and return user data if valid."""
        data = await self.redis_client.get(key)
        
        if not data:
            return None
        
        try:
            token_data = json.loads(data)
        except json.JSONDecodeError:
            return None
        
        # Check if OTP matches
        if token_data.get("otp") != provided_otp:
            return None
        
        # Check if expired
        expires_at = datetime.fromisoformat(token_data["expires_at"])
        if datetime.now(timezone.utc) > expires_at:
            await self.redis_client.delete(key)
            return None
        
        return token_data
    
    async def delete_otp(self, key: str) -> bool:
        """Delete OTP from Redis."""
        resp = await self.redis_client.delete(key)
        return bool(resp)
    
    async def get_otp_attempts(self, email: str, otp_type: str) -> int:
        """Get number of OTP attempts for rate limiting."""
        key = f"otp_attempts:{otp_type}:{email}"
        attempts = await self.redis_client.get(key)
        return int(attempts) if attempts else 0
    
    async def increment_otp_attempts(self, email: str, otp_type: str, expiry_minutes: int = 60) -> int:
        """Increment OTP attempts counter."""
        key = f"otp_attempts:{otp_type}:{email}"
        current_attempts = await self.redis_client.incr(key)
        
        # Set expiry on first attempt
        if current_attempts == 1:
            await self.redis_client.expire(key, timedelta(minutes=expiry_minutes))
        
        return current_attempts
    
    async def reset_otp_attempts(self, email: str, otp_type: str) -> None:
        """Reset OTP attempts counter."""
        key = f"otp_attempts:{otp_type}:{email}"
        await self.redis_client.delete(key)
    
    async def store_temp_data(self, key: str, data: Dict[str, Any], expiry_minutes: int = 30) -> None:
        """Store temporary data in Redis."""
        await self.redis_client.setex(
            key,
            timedelta(minutes=expiry_minutes),
            json.dumps(data, default=str)
        )
    
    async def get_temp_data(self, key: str) -> Optional[Dict[str, Any]]:
        """Get temporary data from Redis."""
        data = await self.redis_client.get(key)
        if data:
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                return None
        return None
    
    async def delete_temp_data(self, key: str) -> bool:
        """Delete temporary data from Redis."""
        return bool(self.redis_client.delete(key))
    
    async def ping(self) -> bool:
        """Check Redis connection."""
        try:
            return self.redis_client.ping()
        except Exception:
            return False

