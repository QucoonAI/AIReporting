import asyncio
import urllib.parse
from typing import Dict, Any, Optional, AsyncContextManager
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
import asyncpg
import aiomysql
from app.core.utils import logger

class ConnectionPoolManager:
    """
    Manages database connection pools for different database types
    """
    
    def __init__(self):
        self.pools: Dict[str, Any] = {}
        self.pool_configs: Dict[str, Dict] = {}
        self.connection_timeouts: Dict[str, datetime] = {}
        self.max_pool_size = 20
        self.min_pool_size = 5
        self.connection_timeout = 30  # seconds
        self.idle_timeout = 300  # 5 minutes
    
    async def get_connection_pool(self, db_type: str, connection_url: str) -> Any:
        """
        Get or create a connection pool for the given database type and URL
        """
        pool_key = f"{db_type}_{hash(connection_url)}"
        
        # Check if pool exists and is still valid
        if pool_key in self.pools:
            if await self._is_pool_healthy(pool_key):
                return self.pools[pool_key]
            else:
                # Remove unhealthy pool
                await self._close_pool(pool_key)
        
        # Create new pool
        return await self._create_pool(pool_key, db_type, connection_url)
    
    async def _create_pool(self, pool_key: str, db_type: str, connection_url: str) -> Any:
        """Create a new connection pool"""
        try:
            if db_type.lower() == 'postgres':
                pool = await asyncpg.create_pool(
                    connection_url,
                    min_size=self.min_pool_size,
                    max_size=self.max_pool_size,
                    command_timeout=self.connection_timeout,
                    server_settings={
                        'jit': 'off'  # Disable JIT for better connection performance
                    }
                )
            elif db_type.lower() == 'mysql':
                pool = await aiomysql.create_pool(
                    host=self._parse_mysql_host(connection_url),
                    port=self._parse_mysql_port(connection_url),
                    user=self._parse_mysql_user(connection_url),
                    password=self._parse_mysql_password(connection_url),
                    db=self._parse_mysql_database(connection_url),
                    minsize=self.min_pool_size,
                    maxsize=self.max_pool_size,
                    connect_timeout=self.connection_timeout,
                    autocommit=True
                )
            else:
                raise ValueError(f"Unsupported database type: {db_type}")
            
            self.pools[pool_key] = pool
            self.connection_timeouts[pool_key] = datetime.now()
            
            logger.info(f"Created connection pool for {db_type}: {pool_key}")
            return pool
            
        except Exception as e:
            logger.error(f"Failed to create connection pool for {db_type}: {e}")
            raise
    
    @asynccontextmanager
    async def get_connection(self, db_type: str, connection_url: str) -> AsyncContextManager:
        """
        Get a connection from the pool with automatic cleanup
        """
        pool = await self.get_connection_pool(db_type, connection_url)
        connection = None
        
        try:
            if db_type.lower() == 'postgres':
                connection = await pool.acquire()
            elif db_type.lower() == 'mysql':
                connection = await pool.acquire()
            
            yield connection
            
        except Exception as e:
            logger.error(f"Error with database connection: {e}")
            raise
        finally:
            if connection:
                try:
                    if db_type.lower() == 'postgres':
                        await pool.release(connection)
                    elif db_type.lower() == 'mysql':
                        pool.release(connection)
                except Exception as release_error:
                    logger.warning(f"Error releasing connection: {release_error}")
    
    async def test_connection(self, db_type: str, connection_url: str) -> Dict[str, Any]:
        """
        Test database connection without creating a persistent pool
        """
        start_time = datetime.now()
        
        try:
            if db_type.lower() == 'postgres':
                conn = await asyncpg.connect(connection_url, timeout=self.connection_timeout)
                await conn.execute('SELECT 1')
                await conn.close()
                
            elif db_type.lower() == 'mysql':
                conn = await aiomysql.connect(
                    host=self._parse_mysql_host(connection_url),
                    port=self._parse_mysql_port(connection_url),
                    user=self._parse_mysql_user(connection_url),
                    password=self._parse_mysql_password(connection_url),
                    db=self._parse_mysql_database(connection_url),
                    connect_timeout=self.connection_timeout
                )
                
                cursor = await conn.cursor()
                await cursor.execute('SELECT 1')
                await cursor.close()
                conn.close()
                
            else:
                raise ValueError(f"Unsupported database type: {db_type}")
            
            response_time = (datetime.now() - start_time).total_seconds()
            
            return {
                "success": True,
                "response_time": response_time,
                "message": f"Successfully connected to {db_type} database"
            }
            
        except Exception as e:
            response_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Connection test failed for {db_type}: {e}")
            
            return {
                "success": False,
                "response_time": response_time,
                "error": str(e),
                "message": f"Failed to connect to {db_type} database"
            }
    
    async def _is_pool_healthy(self, pool_key: str) -> bool:
        """Check if pool is healthy and not expired"""
        if pool_key not in self.pools:
            return False
        
        # Check timeout
        if pool_key in self.connection_timeouts:
            if datetime.now() - self.connection_timeouts[pool_key] > timedelta(seconds=self.idle_timeout):
                return False
        
        # Try a simple query to test pool health
        try:
            pool = self.pools[pool_key]
            
            # For PostgreSQL
            if hasattr(pool, 'acquire'):
                async with pool.acquire() as conn:
                    await conn.execute('SELECT 1')
                return True
            
            return True
            
        except Exception as e:
            logger.warning(f"Pool health check failed for {pool_key}: {e}")
            return False
    
    async def _close_pool(self, pool_key: str):
        """Close and remove a connection pool"""
        if pool_key in self.pools:
            try:
                pool = self.pools[pool_key]
                await pool.close()
                del self.pools[pool_key]
                
                if pool_key in self.connection_timeouts:
                    del self.connection_timeouts[pool_key]
                
                logger.info(f"Closed connection pool: {pool_key}")
            except Exception as e:
                logger.error(f"Error closing pool {pool_key}: {e}")
    
    async def cleanup_idle_pools(self):
        """Clean up idle connection pools"""
        current_time = datetime.now()
        idle_pools = []
        
        for pool_key, last_used in self.connection_timeouts.items():
            if current_time - last_used > timedelta(seconds=self.idle_timeout):
                idle_pools.append(pool_key)
        
        for pool_key in idle_pools:
            await self._close_pool(pool_key)
        
        if idle_pools:
            logger.info(f"Cleaned up {len(idle_pools)} idle connection pools")
    
    async def close_all_pools(self):
        """Close all connection pools"""
        pool_keys = list(self.pools.keys())
        for pool_key in pool_keys:
            await self._close_pool(pool_key)
        
        logger.info("Closed all connection pools")
    
    def _parse_mysql_host(self, connection_url: str) -> str:
        """Parse MySQL host from connection URL"""
        # Simple parsing - in production, use proper URL parsing
        parsed = urllib.parse.urlparse(connection_url)
        return parsed.hostname or 'localhost'
    
    def _parse_mysql_port(self, connection_url: str) -> int:
        """Parse MySQL port from connection URL"""
        parsed = urllib.parse.urlparse(connection_url)
        return parsed.port or 3306
    
    def _parse_mysql_user(self, connection_url: str) -> str:
        """Parse MySQL user from connection URL"""
        parsed = urllib.parse.urlparse(connection_url)
        return parsed.username or 'root'
    
    def _parse_mysql_password(self, connection_url: str) -> str:
        """Parse MySQL password from connection URL"""
        parsed = urllib.parse.urlparse(connection_url)
        return parsed.password or ''
    
    def _parse_mysql_database(self, connection_url: str) -> str:
        """Parse MySQL database from connection URL"""
        parsed = urllib.parse.urlparse(connection_url)
        return parsed.path.lstrip('/') if parsed.path else 'test'

# Global connection pool manager instance
connection_pool_manager = ConnectionPoolManager()

