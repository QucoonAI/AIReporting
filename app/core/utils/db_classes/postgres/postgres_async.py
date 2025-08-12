from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection
from sqlalchemy import text
from typing import List, Dict, Any
from app.core.utils import logger


class PostgresSchemaExtractorAsync:
    """
    Asynchronous PostgreSQL schema extractor using SQLAlchemy async.
    """

    def __init__(self, connection_string: str, sample_data_limit: int = 100):
        """
        Initialize the asynchronous PostgreSQL schema extractor.

        Args:
            connection_string: PostgreSQL connection string (will be normalized for async use)
            sample_data_limit: Maximum number of sample values to extract per column
        """
        self.connection_string = self._normalize_async_connection_string(
            connection_string
        )
        self.sample_data_limit = sample_data_limit
        self.engine = create_async_engine(self.connection_string)

    def _normalize_async_connection_string(self, connection_string: str) -> str:
        """Normalize connection string for asynchronous use."""
        return self._normalize_connection_string_robust(connection_string, 'asyncpg')

    async def extract_schema(
        self, schema_name: str = "public", **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Extract complete schema information for all tables in the specified schema.

        Args:
            schema_name: Database schema name to analyze
            **kwargs: Additional options for schema extraction

        Returns:
            List of table schema dictionaries
        """
        tables = []

        try:
            async with self.engine.connect() as conn:
                table_names = await self._get_table_names(conn, schema_name)

                for table_name in table_names:
                    logger.info(f"Analyzing table: {schema_name}.{table_name}")
                    table_schema = await self._analyze_table(
                        conn, schema_name, table_name, **kwargs
                    )
                    if table_schema:
                        tables.append(table_schema)

        except Exception as e:
            logger.error(f"Error extracting schema: {e}")
            raise

        return tables

    async def _get_table_names(
        self, conn: AsyncConnection, schema_name: str
    ) -> List[str]:
        """Get all table names in the specified schema."""
        query = text(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = :schema_name AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        )

        result = await conn.execute(query, {"schema_name": schema_name})
        return [row[0] for row in result.fetchall()]

    async def _analyze_table(
        self, conn: AsyncConnection, schema_name: str, table_name: str, **kwargs
    ) -> Dict[str, Any]:
        """
        Analyze a single table and extract its complete schema information.

        Args:
            conn: Database connection
            schema_name: Schema name
            table_name: Table name
            **kwargs: Additional analysis options

        Returns:
            Dictionary containing table schema information
        """
        try:
            # Get basic table info
            columns = await self._get_column_info(conn, schema_name, table_name)

            # Get constraints and relationships
            primary_keys = await self._get_primary_keys(conn, schema_name, table_name)
            foreign_keys = await self._get_foreign_keys(conn, schema_name, table_name)
            indexes = await self._get_indexes(conn, schema_name, table_name)

            # Enhance columns with additional metadata
            for column in columns:
                column_name = column["column_name"]

                # Check if column is primary key
                column["is_primary_key"] = await self._is_primary_key(
                    conn, schema_name, table_name, column_name
                )

                # Check if column is foreign key
                column["is_foreign_key"] = await self._is_foreign_key(
                    conn, schema_name, table_name, column_name
                )

                # Check if column has unique constraint
                column["is_unique"] = await self._is_unique(
                    conn, schema_name, table_name, column_name
                )

                # Get sample data if requested
                if kwargs.get("include_sample_data", True):
                    column["sample_values"] = await self._get_sample_data(
                        conn, schema_name, table_name, column_name
                    )

                # Get column statistics
                if kwargs.get("include_statistics", True):
                    stats = await self._get_column_statistics(
                        conn, schema_name, table_name, column_name
                    )
                    column.update(stats)

            return {
                "schema_name": schema_name,
                "table_name": table_name,
                "columns": columns,
                "primary_keys": primary_keys,
                "foreign_keys": foreign_keys,
                "indexes": indexes,
                "row_count": await self._get_row_count(conn, schema_name, table_name),
            }

        except Exception as e:
            logger.error(f"Error analyzing table {schema_name}.{table_name}: {e}")
            return None

    async def _get_column_info(
        self, conn: AsyncConnection, schema_name: str, table_name: str
    ) -> List[Dict[str, Any]]:
        """Get detailed column information for a table."""
        query = text(
            """
            SELECT 
                c.column_name,
                c.data_type,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                c.is_nullable,
                c.column_default,
                c.ordinal_position,
                c.udt_name,
                CASE 
                    WHEN c.column_default LIKE 'nextval%%' THEN true
                    ELSE false
                END as is_serial
            FROM information_schema.columns c
            WHERE c.table_schema = :schema_name AND c.table_name = :table_name
            ORDER BY c.ordinal_position
        """
        )

        result = await conn.execute(
            query, {"schema_name": schema_name, "table_name": table_name}
        )
        return [dict(row._mapping) for row in result.fetchall()]

    async def _get_primary_keys(
        self, conn: AsyncConnection, schema_name: str, table_name: str
    ) -> List[str]:
        """Get primary key columns for a table."""
        query = text(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY' 
                AND tc.table_schema = :schema_name 
                AND tc.table_name = :table_name
            ORDER BY kcu.ordinal_position
        """
        )

        result = await conn.execute(
            query, {"schema_name": schema_name, "table_name": table_name}
        )
        return [row[0] for row in result.fetchall()]

    async def _get_foreign_keys(
        self, conn: AsyncConnection, schema_name: str, table_name: str
    ) -> List[Dict[str, Any]]:
        """Get foreign key information for a table."""
        query = text(
            """
            SELECT 
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name,
                tc.constraint_name,
                rc.update_rule,
                rc.delete_rule
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu 
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            JOIN information_schema.referential_constraints rc
                ON tc.constraint_name = rc.constraint_name
                AND tc.table_schema = rc.constraint_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' 
                AND tc.table_schema = :schema_name
                AND tc.table_name = :table_name
        """
        )

        result = await conn.execute(
            query, {"schema_name": schema_name, "table_name": table_name}
        )
        return [dict(row._mapping) for row in result.fetchall()]

    async def _get_indexes(
        self, conn: AsyncConnection, schema_name: str, table_name: str
    ) -> List[Dict[str, Any]]:
        """Get index information for a table."""
        query = text(
            """
            SELECT 
                i.relname as index_name,
                ix.indisunique as is_unique,
                ix.indisprimary as is_primary,
                am.amname as access_method,
                array_agg(a.attname ORDER BY a.attnum) as columns
            FROM pg_index ix
            JOIN pg_class i ON ix.indexrelid = i.oid
            JOIN pg_class t ON ix.indrelid = t.oid
            JOIN pg_namespace n ON t.relnamespace = n.oid
            JOIN pg_am am ON i.relam = am.oid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE n.nspname = :schema_name AND t.relname = :table_name
            GROUP BY i.relname, ix.indisunique, ix.indisprimary, am.amname
        """
        )

        result = await conn.execute(
            query, {"schema_name": schema_name, "table_name": table_name}
        )
        return [dict(row._mapping) for row in result.fetchall()]

    async def _is_primary_key(
        self, conn: AsyncConnection, schema_name: str, table_name: str, column_name: str
    ) -> bool:
        """Check if a column is part of the primary key."""
        query = text(
            """
            SELECT COUNT(*) 
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY' 
                AND tc.table_schema = :schema_name 
                AND tc.table_name = :table_name
                AND kcu.column_name = :column_name
        """
        )

        result = await conn.execute(
            query,
            {
                "schema_name": schema_name,
                "table_name": table_name,
                "column_name": column_name,
            },
        )
        return result.scalar() > 0

    async def _is_foreign_key(
        self, conn: AsyncConnection, schema_name: str, table_name: str, column_name: str
    ) -> bool:
        """Check if a column is a foreign key."""
        query = text(
            """
            SELECT COUNT(*)
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' 
                AND tc.table_schema = :schema_name 
                AND tc.table_name = :table_name
                AND kcu.column_name = :column_name
        """
        )

        result = await conn.execute(
            query,
            {
                "schema_name": schema_name,
                "table_name": table_name,
                "column_name": column_name,
            },
        )
        return result.scalar() > 0

    async def _is_unique(
        self, conn: AsyncConnection, schema_name: str, table_name: str, column_name: str
    ) -> bool:
        """Check if a column has a unique constraint."""
        query = text(
            """
            SELECT COUNT(*)
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'UNIQUE' 
                AND tc.table_schema = :schema_name 
                AND tc.table_name = :table_name
                AND kcu.column_name = :column_name
        """
        )

        result = await conn.execute(
            query,
            {
                "schema_name": schema_name,
                "table_name": table_name,
                "column_name": column_name,
            },
        )
        return result.scalar() > 0

    async def _get_sample_data(
        self, conn: AsyncConnection, schema_name: str, table_name: str, column_name: str
    ) -> List[str]:
        """Get sample data for a column."""
        query_str = f"""
            SELECT "{column_name}" 
            FROM "{schema_name}"."{table_name}" 
            WHERE "{column_name}" IS NOT NULL 
            LIMIT :limit_val
        """

        try:
            result = await conn.execute(
                text(query_str), {"limit_val": self.sample_data_limit}
            )
            return [str(row[0]) for row in result.fetchall() if row[0] is not None]
        except Exception as e:
            logger.warning(
                f"Could not get sample data for {schema_name}.{table_name}.{column_name}: {e}"
            )
            return []

    async def _get_column_statistics(
        self, conn: AsyncConnection, schema_name: str, table_name: str, column_name: str
    ) -> Dict[str, Any]:
        """Get basic statistics for a column."""
        query_str = f"""
            SELECT 
                COUNT(*) as total_count,
                COUNT("{column_name}") as non_null_count,
                COUNT(DISTINCT "{column_name}") as unique_count
            FROM "{schema_name}"."{table_name}"
        """

        try:
            result = await conn.execute(text(query_str))
            row = result.fetchone()
            if row:
                total_count, non_null_count, unique_count = row
                return {
                    "total_count": total_count,
                    "non_null_count": non_null_count,
                    "null_count": total_count - non_null_count,
                    "unique_count": unique_count,
                    "null_percentage": (
                        ((total_count - non_null_count) / total_count * 100)
                        if total_count > 0
                        else 0
                    ),
                }
        except Exception as e:
            logger.warning(
                f"Could not get statistics for {schema_name}.{table_name}.{column_name}: {e}"
            )

        return {
            "total_count": 0,
            "non_null_count": 0,
            "null_count": 0,
            "unique_count": 0,
            "null_percentage": 0,
        }

    async def _get_row_count(
        self, conn: AsyncConnection, schema_name: str, table_name: str
    ) -> int:
        """Get total row count for a table."""
        query_str = f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"'

        try:
            result = await conn.execute(text(query_str))
            return result.scalar()
        except Exception as e:
            logger.warning(
                f"Could not get row count for {schema_name}.{table_name}: {e}"
            )
            return 0

    def _normalize_connection_string_robust(
        self, connection_string: str, target_driver: str
    ) -> str:
        """
        Robust connection string normalization that handles all query parameters.

        Args:
            connection_string: Original connection string
            target_driver: Target driver ('asyncpg' or 'psycopg2')

        Returns:
            Normalized connection string with proper driver
        """
        from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

        try:
            parsed = urlparse(connection_string)

            # Determine the base scheme
            if parsed.scheme.startswith("postgres"):
                base_scheme = "postgresql"
            else:
                base_scheme = parsed.scheme.split("+")[0]

            # Set the new scheme with target driver
            new_scheme = f"{base_scheme}+{target_driver}"

            # Parse and filter query parameters for the target driver
            query_params = parse_qs(parsed.query, keep_blank_values=True)
            filtered_params = self._filter_connection_params(query_params, target_driver)

            # Rebuild query string
            new_query = urlencode(filtered_params, doseq=True)

            # Reconstruct the connection string
            normalized_parsed = parsed._replace(scheme=new_scheme, query=new_query)
            return urlunparse(normalized_parsed)

        except Exception as e:
            logger.warning(f"Could not parse connection string, using as-is: {e}")
            return connection_string
    
    def _filter_connection_params(self, params: dict, target_driver: str) -> dict:
        """
        Filter connection parameters based on the target driver.

        Args:
            params: Dictionary of connection parameters
            target_driver: Target driver ('asyncpg' or 'psycopg2')

        Returns:
            Filtered parameters dictionary
        """
        # Common parameters supported by both drivers
        common_params = {
            "host",
            "port",
            "dbname",
            "user",
            "password",
            "sslmode",
            "sslcert",
            "sslkey",
            "sslrootcert",
            "sslcrl",
            "connect_timeout",
            "application_name",
            "fallback_application_name",
        }

        # Driver-specific parameters
        asyncpg_specific = {
            "ssl",
            "loop",
            "server_hostname",
            "command_timeout",
            "server_settings",
            "record_class",
            "statement_cache_size",
            "max_cached_statement_lifetime",
            "max_cacheable_statement_size",
        }

        psycopg2_specific = {
            "keepalives",
            "keepalives_idle",
            "keepalives_interval",
            "keepalives_count",
            "tcp_user_timeout",
            "channel_binding",
            "target_session_attrs",
            "gssencmode",
            "gsslib",
            "service",
            "passfile",
            "options",
        }

        if target_driver == "asyncpg":
            allowed_params = common_params | asyncpg_specific
        else:  # psycopg2
            allowed_params = common_params | psycopg2_specific

        # Filter parameters and log warnings for unsupported ones
        filtered = {}
        for key, value in params.items():
            if key in allowed_params:
                filtered[key] = value
            else:
                logger.warning(
                    f"Parameter '{key}' not supported by {target_driver}, removing from connection string"
                )

        return filtered

    async def close(self):
        """Close the database engine."""
        await self.engine.dispose()

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()


# Usage example:
# async def main():
#     """Example usage of the PostgresSchemaExtractorAsync."""
#     connection_string = "postgresql+asyncpg://user:pass@host:5432/db?sslmode=require"

#     try:
#         async with PostgresSchemaExtractorAsync(
#             connection_string, sample_data_limit=10
#         ) as extractor:
#             print("Extracting schema information...")

#             schema = await extractor.extract_schema(
#                 schema_name="public", include_sample_data=True, include_statistics=True
#             )

#             print(f"\nFound {len(schema)} tables in the 'public' schema:\n")

#             for table in schema:
#                 print(f"📋 Table: {table['table_name']}")
#                 print(f"   Rows: {table['row_count']:,}")
#                 print(f"   Columns: {len(table['columns'])}")
#                 print(f"   Primary Keys: {table['primary_keys']}")
#                 print(f"   Foreign Keys: {len(table['foreign_keys'])}")
#                 print(f"   Indexes: {len(table['indexes'])}")
#                 print()

#     except Exception as e:
#         print(f"❌ Error: {e}")
#         return 1

#     return 0


# if __name__ == "__main__":
#     import asyncio

#     asyncio.run(main())
