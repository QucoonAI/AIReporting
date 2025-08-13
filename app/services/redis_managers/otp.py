import json
import random
import string
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Any
import redis.asyncio as redis
from fastapi import HTTPException
from . import RedisKeyManager
from app.core.utils import logger



class OTPService:
    """OTP (One Time Password) service with proper Redis key management"""
    
    def __init__(self, redis_client: redis.Redis, app_name: str = "reportai"):
        self.redis_client = redis_client
        self.key_manager = RedisKeyManager(app_name)
        self.default_expiry_minutes = 30
        self.max_attempts = 5
        self.attempt_window_minutes = 60
    
    async def generate_otp(self, length: int = 6) -> str:
        """Generate a random OTP (One Time Password)"""
        return ''.join(random.choices(string.digits, k=length))
    
    async def create_and_store_otp(
        self, 
        otp_type: str, 
        identifier: str, 
        user_id: int, 
        length: int = 6,
        expiry_minutes: int = None
    ) -> Dict[str, Any]:
        """Generate and store OTP with metadata"""
        try:
            otp = await self.generate_otp(length)
            expiry = expiry_minutes or self.default_expiry_minutes
            
            await self.store_otp(otp_type, identifier, user_id, otp, expiry)
            
            logger.info(f"Created OTP for {otp_type}:{identifier}, user {user_id}")
            
            return {
                "otp": otp,
                "expires_in_minutes": expiry,
                "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=expiry)).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error creating OTP for {otp_type}:{identifier}: {e}")
            raise HTTPException(status_code=500, detail="Failed to create OTP")
    
    async def store_otp(
        self, 
        otp_type: str, 
        identifier: str, 
        user_id: int, 
        otp: str, 
        expiry_minutes: int = 30
    ) -> None:
        """Store OTP in Redis with expiration"""
        try:
            data = {
                "user_id": user_id,
                "otp": otp,
                "otp_type": otp_type,
                "identifier": identifier,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=expiry_minutes)).isoformat()
            }
            
            otp_key = self.key_manager.otp_key(otp_type, identifier)
            
            # Store with expiration
            await self.redis_client.setex(
                otp_key, 
                timedelta(minutes=expiry_minutes), 
                json.dumps(data, default=str)
            )
            
            logger.debug(f"Stored OTP for {otp_type}:{identifier}")
            
        except Exception as e:
            logger.error(f"Error storing OTP: {e}")
            raise
    
    async def verify_otp(
        self, 
        otp_type: str, 
        identifier: str, 
        provided_otp: str
    ) -> Optional[Dict[str, Any]]:
        """Verify OTP and return user data if valid"""
        try:
            # Check rate limiting first
            attempts = await self.get_otp_attempts(identifier, otp_type)
            if attempts >= self.max_attempts:
                logger.warning(f"OTP verification blocked for {otp_type}:{identifier} - too many attempts")
                raise HTTPException(
                    status_code=429,
                    detail=f"Too many OTP attempts. Try again later."
                )
            
            otp_key = self.key_manager.otp_key(otp_type, identifier)
            data = await self.redis_client.get(otp_key)
            
            if not data:
                await self.increment_otp_attempts(identifier, otp_type)
                return None
            
            try:
                token_data = json.loads(data)
            except json.JSONDecodeError:
                await self.increment_otp_attempts(identifier, otp_type)
                return None
            
            # Check if OTP matches
            if token_data.get("otp") != provided_otp:
                await self.increment_otp_attempts(identifier, otp_type)
                logger.warning(f"Invalid OTP attempt for {otp_type}:{identifier}")
                return None
            
            # Check if expired (double-check since Redis should handle this)
            expires_at = datetime.fromisoformat(token_data["expires_at"])
            if datetime.now(timezone.utc) > expires_at:
                await self.redis_client.delete(otp_key)
                await self.increment_otp_attempts(identifier, otp_type)
                return None
            
            # OTP is valid - clean up
            await self.delete_otp(otp_type, identifier)
            await self.reset_otp_attempts(identifier, otp_type)
            
            logger.info(f"Successfully verified OTP for {otp_type}:{identifier}")
            return token_data
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error verifying OTP for {otp_type}:{identifier}: {e}")
            return None
    
    async def delete_otp(self, otp_type: str, identifier: str) -> bool:
        """Delete OTP from Redis"""
        try:
            otp_key = self.key_manager.otp_key(otp_type, identifier)
            result = await self.redis_client.delete(otp_key)
            
            if result:
                logger.debug(f"Deleted OTP for {otp_type}:{identifier}")
            
            return bool(result)
            
        except Exception as e:
            logger.error(f"Error deleting OTP: {e}")
            return False
    
    async def get_otp_attempts(self, identifier: str, otp_type: str) -> int:
        """Get number of OTP attempts for rate limiting"""
        try:
            attempts_key = self.key_manager.otp_attempts_key(otp_type, identifier)
            attempts = await self.redis_client.get(attempts_key)
            return int(attempts) if attempts else 0
        except Exception as e:
            logger.error(f"Error getting OTP attempts: {e}")
            return 0
    
    async def increment_otp_attempts(
        self, 
        identifier: str, 
        otp_type: str, 
        expiry_minutes: int = None
    ) -> int:
        """Increment OTP attempts counter"""
        try:
            attempts_key = self.key_manager.otp_attempts_key(otp_type, identifier)
            current_attempts = await self.redis_client.incr(attempts_key)
            
            # Set expiry on first attempt
            if current_attempts == 1:
                expiry = expiry_minutes or self.attempt_window_minutes
                await self.redis_client.expire(attempts_key, timedelta(minutes=expiry))
            
            logger.debug(f"OTP attempts for {otp_type}:{identifier}: {current_attempts}")
            return current_attempts
            
        except Exception as e:
            logger.error(f"Error incrementing OTP attempts: {e}")
            return 0
    
    async def reset_otp_attempts(self, identifier: str, otp_type: str) -> None:
        """Reset OTP attempts counter"""
        try:
            attempts_key = self.key_manager.otp_attempts_key(otp_type, identifier)
            await self.redis_client.delete(attempts_key)
            logger.debug(f"Reset OTP attempts for {otp_type}:{identifier}")
        except Exception as e:
            logger.error(f"Error resetting OTP attempts: {e}")

