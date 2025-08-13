from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection
from sqlalchemy import text
from typing import Dict, List, Any, Optional
import urllib.parse
from app.core.utils import logger


class MSSQLSchemaExtractor:
    """
    Extracts schema information from Microsoft SQL Server databases using SQLAlchemy async.
    """
    
    def __init__(self, connection_string: str, sample_data_limit: int = 100):
        """
        Initialize the MSSQL schema extractor.
        
        Args:
            connection_string: MSSQL connection string with mssql+aioodbc:// scheme
            sample_data_limit: Maximum number of sample values to extract per column
        """
        self.connection_string = self._convert_to_async_connection_string(connection_string)
        self.sample_data_limit = sample_data_limit
        self.engine = create_async_engine(self.connection_string)
    
    def _convert_to_async_connection_string(self, connection_string: str) -> str:
        """Convert MSSQL connection string to async SQLAlchemy format"""
        if connection_string.startswith('mssql://'):
            # Convert mssql:// to mssql+aioodbc://
            return connection_string.replace('mssql://', 'mssql+aioodbc://', 1)
        elif connection_string.startswith('mssql+aioodbc://'):
            return connection_string
        elif connection_string.startswith('mssql+pyodbc://'):
            # Convert sync pyodbc to async aioodbc
            return connection_string.replace('mssql+pyodbc://', 'mssql+aioodbc://', 1)
        else:
            # Assume it needs the async driver prefix
            return f"mssql+aioodbc://{connection_string}"
    
    async def extract_schema(self, database_name: str = None, schema_name: str = 'dbo', **kwargs) -> List[Dict[str, Any]]:
        """
        Extract complete schema information for all tables in the specified database and schema.
        
        Args:
            database_name: Database name to analyze (extracted from connection string if not provided)
            schema_name: Schema name to analyze (default: 'dbo')
            **kwargs: Additional options for schema extraction
            
        Returns:
            List of table schema dictionaries
        """
        tables = []
        
        # Extract database name from connection string if not provided
        if not database_name:
            database_name = self._extract_database_name_from_connection()
        
        if not database_name:
            raise ValueError("Database name must be specified either in connection string or as parameter")
        
        try:
            async with self.engine.connect() as conn:
                table_names = await self._get_table_names(conn, database_name, schema_name)
                
                for table_name in table_names:
                    logger.info(f"Analyzing table: {database_name}.{schema_name}.{table_name}")
                    table_schema = await self._analyze_table(conn, database_name, schema_name, table_name, **kwargs)
                    if table_schema:
                        tables.append(table_schema)
                        
        except Exception as e:
            logger.error(f"Error extracting schema: {e}")
            raise
            
        return tables
    
    def _extract_database_name_from_connection(self) -> Optional[str]:
        """Extract database name from the connection string"""
        try:
            parsed = urllib.parse.urlparse(self.connection_string)
            return parsed.path.lstrip('/') if parsed.path else None
        except Exception:
            return None
    
    async def _get_table_names(self, conn: AsyncConnection, database_name: str, schema_name: str) -> List[str]:
        """Get all table names in the specified database and schema."""
        query = text("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_CATALOG = :database_name 
                AND TABLE_SCHEMA = :schema_name 
                AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """)
        
        result = await conn.execute(query, {"database_name": database_name, "schema_name": schema_name})
        return [row[0] for row in result.fetchall()]
    
    async def _analyze_table(self, conn: AsyncConnection, database_name: str, schema_name: str, table_name: str, **kwargs) -> Dict[str, Any]:
        """
        Analyze a single table and extract its complete schema information.
        
        Args:
            conn: Database connection
            database_name: Database name
            schema_name: Schema name
            table_name: Table name
            **kwargs: Additional analysis options
            
        Returns:
            Dictionary containing table schema information
        """
        try:
            # Get basic table info
            columns = await self._get_column_info(conn, database_name, schema_name, table_name)
            
            # Get constraints and relationships
            primary_keys = await self._get_primary_keys(conn, database_name, schema_name, table_name)
            foreign_keys = await self._get_foreign_keys(conn, database_name, schema_name, table_name)
            indexes = await self._get_indexes(conn, database_name, schema_name, table_name)
            
            # Enhance columns with additional metadata
            for column in columns:
                column_name = column['column_name']
                
                # Check if column is primary key
                column['is_primary_key'] = await self._is_primary_key(conn, database_name, schema_name, table_name, column_name)
                
                # Check if column is foreign key
                column['is_foreign_key'] = await self._is_foreign_key(conn, database_name, schema_name, table_name, column_name)
                
                # Check if column has unique constraint
                column['is_unique'] = await self._is_unique(conn, database_name, schema_name, table_name, column_name)
                
                # Check if column is identity (auto-increment)
                column['is_identity'] = await self._is_identity(conn, database_name, schema_name, table_name, column_name)
                
                # Get sample data if requested
                if kwargs.get('include_sample_data', True):
                    column['sample_values'] = await self._get_sample_data(conn, database_name, schema_name, table_name, column_name)
                
                # Get column statistics
                if kwargs.get('include_statistics', True):
                    stats = await self._get_column_statistics(conn, database_name, schema_name, table_name, column_name)
                    column.update(stats)
            
            return {
                'database_name': database_name,
                'schema_name': schema_name,
                'table_name': table_name,
                'columns': columns,
                'primary_keys': primary_keys,
                'foreign_keys': foreign_keys,
                'indexes': indexes,
                'row_count': await self._get_row_count(conn, database_name, schema_name, table_name),
                'table_info': await self._get_table_info(conn, database_name, schema_name, table_name)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing table {database_name}.{schema_name}.{table_name}: {e}")
            return None
    
    async def _get_column_info(self, conn: AsyncConnection, database_name: str, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get detailed column information for a table."""
        query = text("""
            SELECT 
                c.COLUMN_NAME as column_name,
                c.DATA_TYPE as data_type,
                c.IS_NULLABLE as is_nullable,
                c.COLUMN_DEFAULT as column_default,
                c.CHARACTER_MAXIMUM_LENGTH as character_maximum_length,
                c.NUMERIC_PRECISION as numeric_precision,
                c.NUMERIC_SCALE as numeric_scale,
                c.ORDINAL_POSITION as ordinal_position,
                c.COLLATION_NAME as collation_name,
                CASE 
                    WHEN c.DATA_TYPE IN ('varchar', 'nvarchar', 'char', 'nchar') 
                    THEN c.DATA_TYPE + '(' + 
                         CASE 
                             WHEN c.CHARACTER_MAXIMUM_LENGTH = -1 THEN 'MAX'
                             ELSE CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR)
                         END + ')'
                    WHEN c.DATA_TYPE IN ('decimal', 'numeric')
                    THEN c.DATA_TYPE + '(' + CAST(c.NUMERIC_PRECISION AS VARCHAR) + ',' + CAST(c.NUMERIC_SCALE AS VARCHAR) + ')'
                    ELSE c.DATA_TYPE
                END as full_data_type
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_CATALOG = :database_name 
                AND c.TABLE_SCHEMA = :schema_name 
                AND c.TABLE_NAME = :table_name
            ORDER BY c.ORDINAL_POSITION
        """)
        
        result = await conn.execute(query, {
            "database_name": database_name, 
            "schema_name": schema_name, 
            "table_name": table_name
        })
        return [dict(row._mapping) for row in result.fetchall()]
    
    async def _get_primary_keys(self, conn: AsyncConnection, database_name: str, schema_name: str, table_name: str) -> List[str]:
        """Get primary key columns for a table."""
        query = text("""
            SELECT kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_CATALOG = kcu.TABLE_CATALOG
                AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                AND tc.TABLE_NAME = kcu.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                AND tc.TABLE_CATALOG = :database_name 
                AND tc.TABLE_SCHEMA = :schema_name
                AND tc.TABLE_NAME = :table_name
            ORDER BY kcu.ORDINAL_POSITION
        """)
        
        result = await conn.execute(query, {
            "database_name": database_name, 
            "schema_name": schema_name, 
            "table_name": table_name
        })
        return [row[0] for row in result.fetchall()]
    
    async def _get_foreign_keys(self, conn: AsyncConnection, database_name: str, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get foreign key information for a table."""
        query = text("""
            SELECT 
                kcu.COLUMN_NAME as column_name,
                kcu.REFERENCED_TABLE_SCHEMA as referenced_schema_name,
                kcu.REFERENCED_TABLE_NAME as referenced_table_name,
                kcu.REFERENCED_COLUMN_NAME as referenced_column_name,
                kcu.CONSTRAINT_NAME as constraint_name,
                rc.UPDATE_RULE as update_rule,
                rc.DELETE_RULE as delete_rule
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc 
                ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                AND kcu.TABLE_CATALOG = rc.CONSTRAINT_CATALOG
                AND kcu.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
            WHERE kcu.TABLE_CATALOG = :database_name 
                AND kcu.TABLE_SCHEMA = :schema_name 
                AND kcu.TABLE_NAME = :table_name 
                AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
        """)
        
        result = await conn.execute(query, {
            "database_name": database_name, 
            "schema_name": schema_name, 
            "table_name": table_name
        })
        return [dict(row._mapping) for row in result.fetchall()]
    
    async def _get_indexes(self, conn: AsyncConnection, database_name: str, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get index information for a table."""
        query = text("""
            SELECT 
                i.name as index_name,
                i.is_unique,
                i.is_primary_key,
                i.type_desc as index_type,
                i.is_disabled,
                i.fill_factor,
                STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) as columns
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN sys.tables t ON i.object_id = t.object_id
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = :schema_name 
                AND t.name = :table_name
                AND i.name IS NOT NULL
            GROUP BY i.name, i.is_unique, i.is_primary_key, i.type_desc, i.is_disabled, i.fill_factor
            ORDER BY i.name
        """)
        
        try:
            result = await conn.execute(query, {"schema_name": schema_name, "table_name": table_name})
            indexes = []
            for row in result.fetchall():
                indexes.append({
                    'index_name': row[0],
                    'is_unique': bool(row[1]),
                    'is_primary_key': bool(row[2]),
                    'index_type': row[3],
                    'is_disabled': bool(row[4]),
                    'fill_factor': row[5],
                    'columns': row[6].split(', ') if row[6] else []
                })
            return indexes
        except Exception as e:
            logger.warning(f"Could not get indexes for {table_name}: {e}")
            return []
    
    async def _is_primary_key(self, conn: AsyncConnection, database_name: str, schema_name: str, table_name: str, column_name: str) -> bool:
        """Check if a column is part of the primary key."""
        query = text("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_CATALOG = kcu.TABLE_CATALOG
                AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                AND tc.TABLE_NAME = kcu.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                AND tc.TABLE_CATALOG = :database_name 
                AND tc.TABLE_SCHEMA = :schema_name
                AND tc.TABLE_NAME = :table_name
                AND kcu.COLUMN_NAME = :column_name
        """)
        
        result = await conn.execute(query, {
            "database_name": database_name, 
            "schema_name": schema_name,
            "table_name": table_name, 
            "column_name": column_name
        })
        return result.scalar() > 0
    
    async def _is_foreign_key(self, conn: AsyncConnection, database_name: str, schema_name: str, table_name: str, column_name: str) -> bool:
        """Check if a column is a foreign key."""
        query = text("""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            WHERE kcu.TABLE_CATALOG = :database_name 
                AND kcu.TABLE_SCHEMA = :schema_name 
                AND kcu.TABLE_NAME = :table_name 
                AND kcu.COLUMN_NAME = :column_name
                AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
        """)
        
        result = await conn.execute(query, {
            "database_name": database_name, 
            "schema_name": schema_name,
            "table_name": table_name, 
            "column_name": column_name
        })
        return result.scalar() > 0
    
    async def _is_unique(self, conn: AsyncConnection, database_name: str, schema_name: str, table_name: str, column_name: str) -> bool:
        """Check if a column has a unique constraint."""
        query = text("""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_CATALOG = kcu.TABLE_CATALOG
                AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                AND tc.TABLE_NAME = kcu.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'UNIQUE' 
                AND tc.TABLE_CATALOG = :database_name 
                AND tc.TABLE_SCHEMA = :schema_name
                AND tc.TABLE_NAME = :table_name
                AND kcu.COLUMN_NAME = :column_name
        """)
        
        result = await conn.execute(query, {
            "database_name": database_name, 
            "schema_name": schema_name,
            "table_name": table_name, 
            "column_name": column_name
        })
        return result.scalar() > 0
    
    async def _is_identity(self, conn: AsyncConnection, database_name: str, schema_name: str, table_name: str, column_name: str) -> bool:
        """Check if a column is an identity (auto-increment) column."""
        query = text("""
            SELECT COLUMNPROPERTY(OBJECT_ID(:full_table_name), :column_name, 'IsIdentity') as is_identity
        """)
        
        try:
            full_table_name = f"{schema_name}.{table_name}"
            result = await conn.execute(query, {
                "full_table_name": full_table_name,
                "column_name": column_name
            })
            return bool(result.scalar())
        except Exception:
            return False
    
    async def _get_sample_data(self, conn: AsyncConnection, database_name: str, schema_name: str, table_name: str, column_name: str) -> List[str]:
        """Get sample data for a column."""
        # Use square brackets for MSSQL identifier quoting
        query_str = f'''
            SELECT TOP (:limit_val) [{column_name}] 
            FROM [{schema_name}].[{table_name}] 
            WHERE [{column_name}] IS NOT NULL
        '''
        
        try:
            result = await conn.execute(text(query_str), {"limit_val": self.sample_data_limit})
            return [str(row[0]) for row in result.fetchall() if row[0] is not None]
        except Exception as e:
            logger.warning(f"Could not get sample data for {database_name}.{schema_name}.{table_name}.{column_name}: {e}")
            return []
    
    async def _get_column_statistics(self, conn: AsyncConnection, database_name: str, schema_name: str, table_name: str, column_name: str) -> Dict[str, Any]:
        """Get basic statistics for a column."""
        # Use square brackets for MSSQL identifier quoting
        query_str = f'''
            SELECT 
                COUNT(*) as total_count,
                COUNT([{column_name}]) as non_null_count,
                COUNT(DISTINCT [{column_name}]) as unique_count
            FROM [{schema_name}].[{table_name}]
        '''
        
        try:
            result = await conn.execute(text(query_str))
            row = result.fetchone()
            if row:
                total_count, non_null_count, unique_count = row
                return {
                    'total_count': total_count,
                    'non_null_count': non_null_count,
                    'null_count': total_count - non_null_count,
                    'unique_count': unique_count,
                    'null_percentage': ((total_count - non_null_count) / total_count * 100) if total_count > 0 else 0
                }
        except Exception as e:
            logger.warning(f"Could not get statistics for {database_name}.{schema_name}.{table_name}.{column_name}: {e}")
        
        return {
            'total_count': 0,
            'non_null_count': 0,
            'null_count': 0,
            'unique_count': 0,
            'null_percentage': 0
        }
    
    async def _get_row_count(self, conn: AsyncConnection, database_name: str, schema_name: str, table_name: str) -> int:
        """Get total row count for a table."""
        try:
            # Try sys.dm_db_partition_stats first (faster)
            stats_query = text("""
                SELECT SUM(row_count) as row_count
                FROM sys.dm_db_partition_stats ps
                JOIN sys.objects o ON ps.object_id = o.object_id
                JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE s.name = :schema_name 
                    AND o.name = :table_name
                    AND ps.index_id IN (0, 1)
            """)
            
            result = await conn.execute(stats_query, {"schema_name": schema_name, "table_name": table_name})
            stats_result = result.fetchone()
            
            if stats_result and stats_result[0] is not None:
                return int(stats_result[0])
            
            # Fallback to actual count (slower but accurate)
            count_query = text(f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]")
            result = await conn.execute(count_query)
            return result.scalar()
            
        except Exception as e:
            logger.warning(f"Could not get row count for {schema_name}.{table_name}: {e}")
            return 0
    
    async def _get_table_info(self, conn: AsyncConnection, database_name: str, schema_name: str, table_name: str) -> Dict[str, Any]:
        """Get additional table information."""
        query = text("""
            SELECT 
                t.name as table_name,
                s.name as schema_name,
                t.create_date,
                t.modify_date,
                p.rows as estimated_rows,
                CAST(ROUND(((SUM(a.total_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS total_space_mb,
                CAST(ROUND(((SUM(a.used_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS used_space_mb,
                CAST(ROUND(((SUM(a.total_pages) - SUM(a.used_pages)) * 8) / 1024.00, 2) AS NUMERIC(36, 2)) AS unused_space_mb
            FROM sys.tables t
            INNER JOIN sys.indexes i ON t.object_id = i.object_id
            INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
            INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = :schema_name AND t.name = :table_name
                AND i.object_id > 255 AND i.index_id <= 1
            GROUP BY t.name, s.name, t.create_date, t.modify_date, p.rows
        """)
        
        try:
            result = await conn.execute(query, {"schema_name": schema_name, "table_name": table_name})
            row = result.fetchone()
            
            if row:
                return {
                    'table_name': row[0],
                    'schema_name': row[1],
                    'create_date': row[2],
                    'modify_date': row[3],
                    'estimated_rows': row[4],
                    'total_space_mb': float(row[5]) if row[5] else 0,
                    'used_space_mb': float(row[6]) if row[6] else 0,
                    'unused_space_mb': float(row[7]) if row[7] else 0
                }
        except Exception as e:
            logger.warning(f"Could not get table info for {schema_name}.{table_name}: {e}")
        
        return {}
    
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
async def main():
    """Example usage of the MSSQLSchemaExtractor."""
    # Replace with your actual connection string
    # Note: You may need to install aioodbc and ODBC drivers
    connection_string = "mssql+aioodbc://user:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"
    
    try:
        async with MSSQLSchemaExtractor(connection_string, sample_data_limit=10) as extractor:
            print("Extracting MSSQL schema information...")
            
            schema = await extractor.extract_schema(
                database_name='MyDatabase',
                schema_name='dbo',  # Default schema
                include_sample_data=True,
                include_statistics=True
            )
            
            print(f"\nFound {len(schema)} tables in the database:\n")
            
            for table in schema:
                print(f"🗃️  Table: {table['schema_name']}.{table['table_name']}")
                print(f"   Database: {table['database_name']}")
                print(f"   Rows: {table['row_count']:,}")
                print(f"   Columns: {len(table['columns'])}")
                print(f"   Primary Keys: {table['primary_keys']}")
                print(f"   Foreign Keys: {len(table['foreign_keys'])}")
                print(f"   Indexes: {len(table['indexes'])}")
                
                # Show table info
                if table['table_info']:
                    info = table['table_info']
                    print(f"   Created: {info.get('create_date', 'Unknown')}")
                    print(f"   Space Used: {info.get('used_space_mb', 0):.2f} MB")
                
                # Show column details
                print("   📊 Columns:")
                for col in table['columns'][:5]:  # Show first 5 columns
                    flags = []
                    if col.get('is_primary_key'): flags.append('PK')
                    if col.get('is_foreign_key'): flags.append('FK')
                    if col.get('is_unique'): flags.append('UNIQUE')
                    if col.get('is_identity'): flags.append('IDENTITY')
                    
                    flag_str = f" [{', '.join(flags)}]" if flags else ""
                    nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                    
                    col_type = col['full_data_type'] or col['data_type']
                    print(f"      • {col['column_name']}: {col_type} {nullable}{flag_str}")
                    
                    # Show sample data if available
                    if col.get('sample_values'):
                        sample_preview = col['sample_values'][:3]
                        print(f"        Sample: {sample_preview}")
                    
                    # Show statistics
                    if col.get('total_count'):
                        print(f"        Stats: {col['total_count']} total, {col['unique_count']} unique, {col['null_count']} nulls")
                
                if len(table['columns']) > 5:
                    print(f"      ... and {len(table['columns']) - 5} more columns")
                
                # Show foreign key relationships
                if table['foreign_keys']:
                    print("   🔗 Foreign Keys:")
                    for fk in table['foreign_keys'][:3]:  # Show first 3 foreign keys
                        ref_table = f"{fk['referenced_schema_name']}.{fk['referenced_table_name']}"
                        print(f"      • {fk['column_name']} → {ref_table}.{fk['referenced_column_name']}")
                
                print()  # Empty line between tables
                
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Note: Make sure you have aioodbc installed and proper ODBC drivers configured")
        return 1
    
    return 0


