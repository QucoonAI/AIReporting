from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection
from sqlalchemy import text
from typing import Dict, List, Any, Optional
import urllib.parse
from app.core.utils import logger


class MySQLSchemaExtractor:
    """
    Extracts schema information from MySQL databases using SQLAlchemy async.
    """
    
    def __init__(self, connection_string: str, sample_data_limit: int = 100):
        """
        Initialize the MySQL schema extractor.
        
        Args:
            connection_string: MySQL connection string with mysql+aiomysql:// scheme
            sample_data_limit: Maximum number of sample values to extract per column
        """
        self.connection_string = self._convert_to_async_connection_string(connection_string)
        self.sample_data_limit = sample_data_limit
        self.engine = create_async_engine(self.connection_string)
    
    def _convert_to_async_connection_string(self, connection_string: str) -> str:
        """Convert MySQL connection string to async SQLAlchemy format"""
        if connection_string.startswith('mysql://'):
            # Convert mysql:// to mysql+aiomysql://
            return connection_string.replace('mysql://', 'mysql+aiomysql://', 1)
        elif connection_string.startswith('mysql+aiomysql://'):
            return connection_string
        else:
            # Assume it needs the async driver prefix
            return f"mysql+aiomysql://{connection_string}"
    
    async def extract_schema(self, database_name: str = None, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract complete schema information for all tables in the specified database.
        
        Args:
            database_name: Database name to analyze (extracted from connection string if not provided)
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
                table_names = await self._get_table_names(conn, database_name)
                
                for table_name in table_names:
                    logger.info(f"Analyzing table: {database_name}.{table_name}")
                    table_schema = await self._analyze_table(conn, database_name, table_name, **kwargs)
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
    
    async def _get_table_names(self, conn: AsyncConnection, database_name: str) -> List[str]:
        """Get all table names in the specified database."""
        query = text("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = :database_name AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """)
        
        result = await conn.execute(query, {"database_name": database_name})
        return [row[0] for row in result.fetchall()]
    
    async def _analyze_table(self, conn: AsyncConnection, database_name: str, table_name: str, **kwargs) -> Dict[str, Any]:
        """
        Analyze a single table and extract its complete schema information.
        
        Args:
            conn: Database connection
            database_name: Database name
            table_name: Table name
            **kwargs: Additional analysis options
            
        Returns:
            Dictionary containing table schema information
        """
        try:
            # Get basic table info
            columns = await self._get_column_info(conn, database_name, table_name)
            
            # Get constraints and relationships
            primary_keys = await self._get_primary_keys(conn, database_name, table_name)
            foreign_keys = await self._get_foreign_keys(conn, database_name, table_name)
            indexes = await self._get_indexes(conn, database_name, table_name)
            
            # Enhance columns with additional metadata
            for column in columns:
                column_name = column['column_name']
                
                # Check if column is primary key
                column['is_primary_key'] = await self._is_primary_key(conn, database_name, table_name, column_name)
                
                # Check if column is foreign key
                column['is_foreign_key'] = await self._is_foreign_key(conn, database_name, table_name, column_name)
                
                # Check if column has unique constraint
                column['is_unique'] = await self._is_unique(conn, database_name, table_name, column_name)
                
                # Get sample data if requested
                if kwargs.get('include_sample_data', True):
                    column['sample_values'] = await self._get_sample_data(conn, database_name, table_name, column_name)
                
                # Get column statistics
                if kwargs.get('include_statistics', True):
                    stats = await self._get_column_statistics(conn, database_name, table_name, column_name)
                    column.update(stats)
            
            return {
                'database_name': database_name,
                'table_name': table_name,
                'columns': columns,
                'primary_keys': primary_keys,
                'foreign_keys': foreign_keys,
                'indexes': indexes,
                'row_count': await self._get_row_count(conn, database_name, table_name),
                'table_info': await self._get_table_info(conn, database_name, table_name)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing table {database_name}.{table_name}: {e}")
            return None
    
    async def _get_column_info(self, conn: AsyncConnection, database_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get detailed column information for a table."""
        query = text("""
            SELECT 
                c.COLUMN_NAME as column_name,
                c.DATA_TYPE as data_type,
                c.COLUMN_TYPE as column_type,
                c.IS_NULLABLE as is_nullable,
                c.COLUMN_DEFAULT as column_default,
                c.CHARACTER_MAXIMUM_LENGTH as character_maximum_length,
                c.NUMERIC_PRECISION as numeric_precision,
                c.NUMERIC_SCALE as numeric_scale,
                c.ORDINAL_POSITION as ordinal_position,
                c.COLUMN_KEY as column_key,
                c.EXTRA as extra,
                c.COLUMN_COMMENT as column_comment
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_SCHEMA = :database_name AND c.TABLE_NAME = :table_name
            ORDER BY c.ORDINAL_POSITION
        """)
        
        result = await conn.execute(query, {"database_name": database_name, "table_name": table_name})
        return [dict(row._mapping) for row in result.fetchall()]
    
    async def _get_primary_keys(self, conn: AsyncConnection, database_name: str, table_name: str) -> List[str]:
        """Get primary key columns for a table."""
        query = text("""
            SELECT kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                AND tc.TABLE_SCHEMA = :database_name 
                AND tc.TABLE_NAME = :table_name
            ORDER BY kcu.ORDINAL_POSITION
        """)
        
        result = await conn.execute(query, {"database_name": database_name, "table_name": table_name})
        return [row[0] for row in result.fetchall()]
    
    async def _get_foreign_keys(self, conn: AsyncConnection, database_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get foreign key information for a table."""
        query = text("""
            SELECT 
                kcu.COLUMN_NAME as column_name,
                kcu.REFERENCED_TABLE_NAME as referenced_table_name,
                kcu.REFERENCED_COLUMN_NAME as referenced_column_name,
                kcu.CONSTRAINT_NAME as constraint_name,
                rc.UPDATE_RULE as update_rule,
                rc.DELETE_RULE as delete_rule
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc 
                ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                AND kcu.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
            WHERE kcu.TABLE_SCHEMA = :database_name 
                AND kcu.TABLE_NAME = :table_name 
                AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
        """)
        
        result = await conn.execute(query, {"database_name": database_name, "table_name": table_name})
        return [dict(row._mapping) for row in result.fetchall()]
    
    async def _get_indexes(self, conn: AsyncConnection, database_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get index information for a table."""
        query = text(f"SHOW INDEX FROM `{database_name}`.`{table_name}`")
        
        try:
            result = await conn.execute(query)
            index_rows = result.fetchall()
            
            # Group indexes by name
            indexes_dict = {}
            for row in index_rows:
                # Convert row to dict-like access
                if hasattr(row, '_mapping'):
                    row_dict = dict(row._mapping)
                else:
                    # Fallback for different SQLAlchemy versions
                    row_dict = {
                        'Key_name': row[2],
                        'Non_unique': row[1],
                        'Column_name': row[4],
                        'Seq_in_index': row[3],
                        'Collation': row[5] if len(row) > 5 else None,
                        'Cardinality': row[6] if len(row) > 6 else None,
                        'Index_type': row[10] if len(row) > 10 else 'BTREE'
                    }
                
                index_name = row_dict['Key_name']
                if index_name not in indexes_dict:
                    indexes_dict[index_name] = {
                        'index_name': index_name,
                        'is_unique': row_dict['Non_unique'] == 0,
                        'columns': [],
                        'index_type': row_dict.get('Index_type', 'BTREE')
                    }
                
                indexes_dict[index_name]['columns'].append({
                    'column_name': row_dict['Column_name'],
                    'sequence_in_index': row_dict['Seq_in_index'],
                    'collation': row_dict.get('Collation'),
                    'cardinality': row_dict.get('Cardinality')
                })
            
            # Sort columns by sequence and convert to list
            indexes = []
            for index_info in indexes_dict.values():
                index_info['columns'].sort(key=lambda x: x['sequence_in_index'])
                indexes.append(index_info)
            
            return indexes
            
        except Exception as e:
            logger.warning(f"Could not get indexes for {table_name}: {e}")
            return []
    
    async def _is_primary_key(self, conn: AsyncConnection, database_name: str, table_name: str, column_name: str) -> bool:
        """Check if a column is part of the primary key."""
        query = text("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                AND tc.TABLE_SCHEMA = :database_name 
                AND tc.TABLE_NAME = :table_name
                AND kcu.COLUMN_NAME = :column_name
        """)
        
        result = await conn.execute(query, {
            "database_name": database_name, 
            "table_name": table_name, 
            "column_name": column_name
        })
        return result.scalar() > 0
    
    async def _is_foreign_key(self, conn: AsyncConnection, database_name: str, table_name: str, column_name: str) -> bool:
        """Check if a column is a foreign key."""
        query = text("""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            WHERE kcu.TABLE_SCHEMA = :database_name 
                AND kcu.TABLE_NAME = :table_name 
                AND kcu.COLUMN_NAME = :column_name
                AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
        """)
        
        result = await conn.execute(query, {
            "database_name": database_name, 
            "table_name": table_name, 
            "column_name": column_name
        })
        return result.scalar() > 0
    
    async def _is_unique(self, conn: AsyncConnection, database_name: str, table_name: str, column_name: str) -> bool:
        """Check if a column has a unique constraint."""
        query = text("""
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            WHERE tc.CONSTRAINT_TYPE = 'UNIQUE' 
                AND tc.TABLE_SCHEMA = :database_name 
                AND tc.TABLE_NAME = :table_name
                AND kcu.COLUMN_NAME = :column_name
        """)
        
        result = await conn.execute(query, {
            "database_name": database_name, 
            "table_name": table_name, 
            "column_name": column_name
        })
        return result.scalar() > 0
    
    async def _get_sample_data(self, conn: AsyncConnection, database_name: str, table_name: str, column_name: str) -> List[str]:
        """Get sample data for a column."""
        # Use backticks for MySQL identifier quoting
        query_str = f'''
            SELECT `{column_name}` 
            FROM `{database_name}`.`{table_name}` 
            WHERE `{column_name}` IS NOT NULL 
            LIMIT :limit_val
        '''
        
        try:
            result = await conn.execute(text(query_str), {"limit_val": self.sample_data_limit})
            return [str(row[0]) for row in result.fetchall() if row[0] is not None]
        except Exception as e:
            logger.warning(f"Could not get sample data for {database_name}.{table_name}.{column_name}: {e}")
            return []
    
    async def _get_column_statistics(self, conn: AsyncConnection, database_name: str, table_name: str, column_name: str) -> Dict[str, Any]:
        """Get basic statistics for a column."""
        # Use backticks for MySQL identifier quoting
        query_str = f'''
            SELECT 
                COUNT(*) as total_count,
                COUNT(`{column_name}`) as non_null_count,
                COUNT(DISTINCT `{column_name}`) as unique_count
            FROM `{database_name}`.`{table_name}`
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
            logger.warning(f"Could not get statistics for {database_name}.{table_name}.{column_name}: {e}")
        
        return {
            'total_count': 0,
            'non_null_count': 0,
            'null_count': 0,
            'unique_count': 0,
            'null_percentage': 0
        }
    
    async def _get_row_count(self, conn: AsyncConnection, database_name: str, table_name: str) -> int:
        """Get total row count for a table."""
        try:
            # Try information_schema first (faster but approximate)
            info_query = text("""
                SELECT TABLE_ROWS 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = :database_name AND TABLE_NAME = :table_name
            """)
            
            result = await conn.execute(info_query, {"database_name": database_name, "table_name": table_name})
            info_result = result.fetchone()
            
            if info_result and info_result[0] is not None and info_result[0] > 0:
                return int(info_result[0])
            
            # Fallback to actual count (slower but accurate)
            count_query = text(f"SELECT COUNT(*) FROM `{database_name}`.`{table_name}`")
            result = await conn.execute(count_query)
            return result.scalar()
            
        except Exception as e:
            logger.warning(f"Could not get row count for {database_name}.{table_name}: {e}")
            return 0
    
    async def _get_table_info(self, conn: AsyncConnection, database_name: str, table_name: str) -> Dict[str, Any]:
        """Get additional table information."""
        query = text("""
            SELECT 
                ENGINE,
                TABLE_COLLATION,
                TABLE_COMMENT,
                CREATE_TIME,
                UPDATE_TIME,
                TABLE_ROWS,
                DATA_LENGTH,
                INDEX_LENGTH
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = :database_name AND TABLE_NAME = :table_name
        """)
        
        try:
            result = await conn.execute(query, {"database_name": database_name, "table_name": table_name})
            row = result.fetchone()
            
            if row:
                return {
                    'engine': row[0],
                    'collation': row[1],
                    'comment': row[2],
                    'create_time': row[3],
                    'update_time': row[4],
                    'estimated_rows': row[5],
                    'data_length': row[6],
                    'index_length': row[7]
                }
        except Exception as e:
            logger.warning(f"Could not get table info for {database_name}.{table_name}: {e}")
        
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
    """Example usage of the MySQLSchemaExtractor."""
    # Replace with your actual connection string
    connection_string = "mysql://user:password@localhost:3306/ecommerce_db"
    
    try:
        async with MySQLSchemaExtractor(connection_string, sample_data_limit=10) as extractor:
            print("Extracting MySQL schema information...")
            
            schema = await extractor.extract_schema(
                database_name='ecommerce_db',  # Optional if in connection string
                include_sample_data=True,
                include_statistics=True
            )
            
            print(f"\nFound {len(schema)} tables in the database:\n")
            
            for table in schema:
                print(f"🗃️  Table: {table['table_name']}")
                print(f"   Database: {table['database_name']}")
                print(f"   Rows: {table['row_count']:,}")
                print(f"   Columns: {len(table['columns'])}")
                print(f"   Primary Keys: {table['primary_keys']}")
                print(f"   Foreign Keys: {len(table['foreign_keys'])}")
                print(f"   Indexes: {len(table['indexes'])}")
                
                # Show table info
                if table['table_info']:
                    info = table['table_info']
                    print(f"   Engine: {info.get('engine', 'Unknown')}")
                    if info.get('comment'):
                        print(f"   Comment: {info['comment']}")
                
                # Show column details
                print("   📊 Columns:")
                for col in table['columns'][:5]:  # Show first 5 columns
                    flags = []
                    if col.get('is_primary_key'): flags.append('PK')
                    if col.get('is_foreign_key'): flags.append('FK')
                    if col.get('is_unique'): flags.append('UNIQUE')
                    if col.get('extra') and 'auto_increment' in col.get('extra', '').lower(): 
                        flags.append('AUTO_INCREMENT')
                    
                    flag_str = f" [{', '.join(flags)}]" if flags else ""
                    nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                    
                    col_type = col['column_type'] or col['data_type']
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
                        print(f"      • {fk['column_name']} → {fk['referenced_table_name']}.{fk['referenced_column_name']}")
                
                print()  # Empty line between tables
                
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0

