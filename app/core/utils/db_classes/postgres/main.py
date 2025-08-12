from typing import List, Dict, Any, Optional, Union
import logging
from urllib.parse import urlparse
from .postgres_async import PostgresSchemaExtractorAsync
from .postgres_sync import PostgresSchemaExtractorSync


class PostgresSchemaExtractor:
    """
    General PostgreSQL schema extractor that automatically chooses between 
    async and sync implementations based on the connection string or user preference.
    """
    
    def __init__(self, connection_string: str, sample_data_limit: int = 100, 
                 use_async: Optional[bool] = None, prefer_async: bool = True):
        """
        Initialize the PostgreSQL schema extractor.
        
        Args:
            connection_string: PostgreSQL connection string
            sample_data_limit: Maximum number of sample values to extract per column
            use_async: Force async (True) or sync (False) mode. If None, auto-detect
            prefer_async: If ambiguous, prefer async over sync (default: True)
        """
        self.connection_string = connection_string
        self.sample_data_limit = sample_data_limit
        self.prefer_async = prefer_async
        
        # Determine which implementation to use
        self.is_async = self._determine_async_mode(connection_string, use_async, prefer_async)
        
        # Create the appropriate extractor
        if self.is_async:
            self._extractor = PostgresSchemaExtractorAsync(connection_string, sample_data_limit)
        else:
            self._extractor = PostgresSchemaExtractorSync(connection_string, sample_data_limit)
    
    def _determine_async_mode(self, connection_string: str, use_async: Optional[bool], prefer_async: bool) -> bool:
        """
        Determine whether to use async or sync mode.
        
        Args:
            connection_string: The connection string to analyze
            use_async: Explicit user preference (overrides auto-detection)
            prefer_async: Default preference when ambiguous
            
        Returns:
            True for async mode, False for sync mode
        """
        # If explicitly specified, use that
        if use_async is not None:
            return use_async
        
        # Auto-detect from connection string
        parsed = urlparse(connection_string)
        scheme = parsed.scheme.lower()
        
        # Clear indicators for async
        if 'asyncpg' in scheme:
            return True
        
        # Clear indicators for sync
        if 'psycopg2' in scheme or 'psycopg' in scheme:
            return False
        
        # For ambiguous cases (plain postgresql:// or postgres://), use preference
        if scheme in ['postgresql', 'postgres']:
            return prefer_async
        
        # Default fallback
        return prefer_async
    
    @property
    def mode(self) -> str:
        """Get the current mode (async or sync)."""
        return "async" if self.is_async else "sync"
    
    # Async interface methods
    async def extract_schema(self, schema_name: str = 'public', **kwargs) -> List[Dict[str, Any]]:
        """
        Extract complete schema information for all tables in the specified schema.
        
        This method works for both async and sync modes:
        - For async mode: Use await
        - For sync mode: Returns the result directly (wrapped in async for consistency)
        
        Args:
            schema_name: Database schema name to analyze
            **kwargs: Additional options for schema extraction
            
        Returns:
            List of table schema dictionaries
        """
        if self.is_async:
            return await self._extractor.extract_schema(schema_name, **kwargs)
        else:
            # For sync extractor, we wrap the result to maintain async interface
            return self._extractor.extract_schema(schema_name, **kwargs)
    
    async def close(self):
        """Close the database engine (works for both async and sync)."""
        if self.is_async:
            await self._extractor.close()
        else:
            self._extractor.close()
    
    # Sync interface methods (for when you know you want sync)
    def extract_schema_sync(self, schema_name: str = 'public', **kwargs) -> List[Dict[str, Any]]:
        """
        Synchronous version of extract_schema.
        
        Args:
            schema_name: Database schema name to analyze
            **kwargs: Additional options for schema extraction
            
        Returns:
            List of table schema dictionaries
            
        Raises:
            RuntimeError: If the extractor is in async mode
        """
        if self.is_async:
            raise RuntimeError(
                "Cannot call extract_schema_sync() on an async extractor. "
                "Use extract_schema() with await, or create with use_async=False"
            )
        return self._extractor.extract_schema(schema_name, **kwargs)
    
    def close_sync(self):
        """Synchronous version of close."""
        if self.is_async:
            raise RuntimeError(
                "Cannot call close_sync() on an async extractor. "
                "Use close() with await, or create with use_async=False"
            )
        self._extractor.close()
    
    # Context manager support (async)
    async def __aenter__(self):
        """Async context manager entry."""
        if not self.is_async:
            raise RuntimeError(
                "Cannot use async context manager with sync extractor. "
                "Use regular 'with' statement or create with use_async=True"
            )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    # Context manager support (sync)
    def __enter__(self):
        """Sync context manager entry."""
        if self.is_async:
            raise RuntimeError(
                "Cannot use sync context manager with async extractor. "
                "Use 'async with' statement or create with use_async=False"
            )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Sync context manager exit."""
        self.close_sync()
    
    # Convenience methods
    @classmethod
    def create_async(cls, connection_string: str, sample_data_limit: int = 100) -> 'PostgresSchemaExtractor':
        """
        Create an async extractor instance.
        
        Args:
            connection_string: PostgreSQL connection string
            sample_data_limit: Maximum number of sample values to extract per column
            
        Returns:
            PostgresSchemaExtractor configured for async mode
        """
        return cls(connection_string, sample_data_limit, use_async=True)
    
    @classmethod
    def create_sync(cls, connection_string: str, sample_data_limit: int = 100) -> 'PostgresSchemaExtractor':
        """
        Create a sync extractor instance.
        
        Args:
            connection_string: PostgreSQL connection string
            sample_data_limit: Maximum number of sample values to extract per column
            
        Returns:
            PostgresSchemaExtractor configured for sync mode
        """
        return cls(connection_string, sample_data_limit, use_async=False)
    
    def __repr__(self) -> str:
        """String representation of the extractor."""
        return f"PostgresSchemaExtractor(mode={self.mode}, connection={self.connection_string[:50]}...)"


# Convenience functions for direct usage
async def extract_schema_async(connection_string: str, schema_name: str = 'public', 
                             sample_data_limit: int = 100, **kwargs) -> List[Dict[str, Any]]:
    """
    Convenience function for async schema extraction.
    
    Args:
        connection_string: PostgreSQL connection string
        schema_name: Database schema name to analyze
        sample_data_limit: Maximum number of sample values to extract per column
        **kwargs: Additional options for schema extraction
        
    Returns:
        List of table schema dictionaries
    """
    async with PostgresSchemaExtractor.create_async(connection_string, sample_data_limit) as extractor:
        return await extractor.extract_schema(schema_name, **kwargs)


def extract_schema_sync(connection_string: str, schema_name: str = 'public',
                       sample_data_limit: int = 100, **kwargs) -> List[Dict[str, Any]]:
    """
    Convenience function for sync schema extraction.
    
    Args:
        connection_string: PostgreSQL connection string
        schema_name: Database schema name to analyze
        sample_data_limit: Maximum number of sample values to extract per column
        **kwargs: Additional options for schema extraction
        
    Returns:
        List of table schema dictionaries
    """
    with PostgresSchemaExtractor.create_sync(connection_string, sample_data_limit) as extractor:
        return extractor.extract_schema_sync(schema_name, **kwargs)


# Usage examples:
async def example_auto_detect():
    """Example showing auto-detection based on connection string."""
    # These will auto-detect to async
    async_connections = [
        "postgresql+asyncpg://user:pass@host:5432/db",
        "postgres+asyncpg://user:pass@host:5432/db?sslmode=require",
    ]
    
    # These will auto-detect to sync
    sync_connections = [
        "postgresql+psycopg2://user:pass@host:5432/db",
        "postgresql+psycopg://user:pass@host:5432/db",
    ]
    
    # These are ambiguous - will use prefer_async=True by default
    ambiguous_connections = [
        "postgresql://user:pass@host:5432/db",
        "postgres://user:pass@host:5432/db?sslmode=require",
    ]
    
    for conn_str in async_connections + ambiguous_connections:
        extractor = PostgresSchemaExtractor(conn_str)
        print(f"Connection: {conn_str[:50]}... -> Mode: {extractor.mode}")
        
        try:
            async with extractor:
                schema = await extractor.extract_schema('public')
                print(f"Found {len(schema)} tables")
        except Exception as e:
            print(f"Error: {e}")


def example_explicit_sync():
    """Example showing explicit sync usage."""
    connection_string = "postgresql://user:pass@host:5432/db"
    
    # Force sync mode
    with PostgresSchemaExtractor.create_sync(connection_string) as extractor:
        schema = extractor.extract_schema_sync('public')
        print(f"Found {len(schema)} tables using sync mode")


async def example_explicit_async():
    """Example showing explicit async usage."""
    connection_string = "postgresql://user:pass@host:5432/db"
    
    # Force async mode
    async with PostgresSchemaExtractor.create_async(connection_string) as extractor:
        schema = await extractor.extract_schema('public')
        print(f"Found {len(schema)} tables using async mode")


async def example_convenience_functions():
    """Example using convenience functions."""
    connection_string = "postgresql://user:pass@host:5432/db"
    
    # Using convenience functions
    try:
        # Async convenience function
        schema_async = await extract_schema_async(connection_string, 'public')
        print(f"Async convenience: Found {len(schema_async)} tables")
        
        # Sync convenience function
        schema_sync = extract_schema_sync(connection_string, 'public')
        print(f"Sync convenience: Found {len(schema_sync)} tables")
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    import asyncio
    
    print("=== Auto-Detection Example ===")
    asyncio.run(example_auto_detect())
    
    print("\n=== Explicit Sync Example ===")
    example_explicit_sync()
    
    print("\n=== Explicit Async Example ===")
    asyncio.run(example_explicit_async())
    
    print("\n=== Convenience Functions Example ===")
    asyncio.run(example_convenience_functions())


