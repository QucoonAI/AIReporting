from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection
from sqlalchemy import text
from typing import Dict, List, Any
from app.core.utils import logger


class OracleSchemaExtractor:
    """
    Extracts schema information from Oracle Database using SQLAlchemy async.
    Supports Oracle-specific features like sequences, packages, synonyms, and tablespaces.
    """
    
    def __init__(self, connection_string: str, sample_data_limit: int = 100):
        """
        Initialize the Oracle schema extractor.
        
        Args:
            connection_string: Oracle connection string with oracle+oracledb:// scheme
            sample_data_limit: Maximum number of sample values to extract per column
        """
        self.connection_string = self._convert_to_async_connection_string(connection_string)
        self.sample_data_limit = sample_data_limit
        self.engine = create_async_engine(self.connection_string)
    
    def _convert_to_async_connection_string(self, connection_string: str) -> str:
        """Convert Oracle connection string to async SQLAlchemy format"""
        if connection_string.startswith('oracle://'):
            # Convert oracle:// to oracle+oracledb://
            return connection_string.replace('oracle://', 'oracle+oracledb://', 1)
        elif connection_string.startswith('oracle+oracledb://'):
            return connection_string
        elif connection_string.startswith('oracle+cx_oracle://'):
            # Convert sync cx_Oracle to async oracledb
            return connection_string.replace('oracle+cx_oracle://', 'oracle+oracledb://', 1)
        else:
            # Assume it needs the async driver prefix
            return f"oracle+oracledb://{connection_string}"
    
    async def extract_schema(self, schema_name: str = None, **kwargs) -> List[Dict[str, Any]]:
        """
        Extract complete schema information for all tables in the specified schema.
        
        Args:
            schema_name: Schema name to analyze (defaults to current user schema)
            **kwargs: Additional options for schema extraction
            
        Returns:
            List of table schema dictionaries
        """
        tables = []
        
        try:
            async with self.engine.connect() as conn:
                # Get current schema if not specified
                if not schema_name:
                    schema_name = await self._get_current_schema(conn)
                
                # Get Oracle version for feature compatibility
                oracle_version = await self._get_oracle_version(conn)
                logger.info(f"Connected to Oracle version: {oracle_version}")
                
                table_names = await self._get_table_names(conn, schema_name)
                
                for table_name in table_names:
                    logger.info(f"Analyzing table: {schema_name}.{table_name}")
                    table_schema = await self._analyze_table(conn, schema_name, table_name, oracle_version, **kwargs)
                    if table_schema:
                        tables.append(table_schema)
                        
        except Exception as e:
            logger.error(f"Error extracting schema: {e}")
            raise
            
        return tables
    
    async def _get_current_schema(self, conn: AsyncConnection) -> str:
        """Get the current schema name"""
        try:
            result = await conn.execute(text("SELECT USER FROM DUAL"))
            return result.fetchone()[0]
        except Exception:
            return "UNKNOWN"
    
    async def _get_oracle_version(self, conn: AsyncConnection) -> str:
        """Get Oracle version information"""
        try:
            result = await conn.execute(text("SELECT BANNER FROM V$VERSION WHERE ROWNUM = 1"))
            version_info = result.fetchone()
            return version_info[0] if version_info else "Unknown"
        except Exception:
            return "Unknown"
    
    async def _get_table_names(self, conn: AsyncConnection, schema_name: str) -> List[str]:
        """Get all table names in the specified schema."""
        query = text("""
            SELECT TABLE_NAME 
            FROM ALL_TABLES 
            WHERE OWNER = :schema_name 
            ORDER BY TABLE_NAME
        """)
        
        result = await conn.execute(query, {"schema_name": schema_name.upper()})
        return [row[0] for row in result.fetchall()]
    
    async def _analyze_table(self, conn: AsyncConnection, schema_name: str, table_name: str, oracle_version: str, **kwargs) -> Dict[str, Any]:
        """
        Analyze a single table and extract its complete schema information.
        
        Args:
            conn: Database connection
            schema_name: Schema name
            table_name: Table name
            oracle_version: Oracle version for feature detection
            **kwargs: Additional analysis options
            
        Returns:
            Dictionary containing table schema information
        """
        try:
            # Get basic table info
            columns = await self._get_column_info(conn, schema_name, table_name, oracle_version)
            
            # Get constraints and relationships
            primary_keys = await self._get_primary_keys(conn, schema_name, table_name)
            foreign_keys = await self._get_foreign_keys(conn, schema_name, table_name)
            indexes = await self._get_indexes(conn, schema_name, table_name)
            check_constraints = await self._get_check_constraints(conn, schema_name, table_name)
            
            # Get Oracle-specific features
            triggers = await self._get_triggers(conn, schema_name, table_name)
            sequences = await self._get_sequences(conn, schema_name, table_name)
            partitions = await self._get_partitions(conn, schema_name, table_name)
            
            # Enhance columns with additional metadata
            for column in columns:
                column_name = column['column_name']
                
                # Check if column is primary key
                column['is_primary_key'] = await self._is_primary_key(conn, schema_name, table_name, column_name)
                
                # Check if column is foreign key
                column['is_foreign_key'] = await self._is_foreign_key(conn, schema_name, table_name, column_name)
                
                # Check if column has unique constraint
                column['is_unique'] = await self._is_unique(conn, schema_name, table_name, column_name)
                
                # Check for virtual columns (Oracle 11g+)
                column['is_virtual'] = await self._is_virtual_column(conn, schema_name, table_name, column_name, oracle_version)
                
                # Get sample data if requested
                if kwargs.get('include_sample_data', True):
                    column['sample_values'] = await self._get_sample_data(conn, schema_name, table_name, column_name)
                
                # Get column statistics
                if kwargs.get('include_statistics', True):
                    stats = await self._get_column_statistics(conn, schema_name, table_name, column_name)
                    column.update(stats)
            
            return {
                'schema_name': schema_name,
                'table_name': table_name,
                'columns': columns,
                'primary_keys': primary_keys,
                'foreign_keys': foreign_keys,
                'indexes': indexes,
                'check_constraints': check_constraints,
                'triggers': triggers,
                'sequences': sequences,
                'partitions': partitions,
                'row_count': await self._get_row_count(conn, schema_name, table_name),
                'table_info': await self._get_table_info(conn, schema_name, table_name),
                'oracle_version': oracle_version
            }
            
        except Exception as e:
            logger.error(f"Error analyzing table {schema_name}.{table_name}: {e}")
            return None
    
    async def _get_column_info(self, conn: AsyncConnection, schema_name: str, table_name: str, oracle_version: str) -> List[Dict[str, Any]]:
        """Get detailed column information for a table with Oracle-specific features."""
        
        # Base query for all Oracle versions
        base_query = """
            SELECT 
                c.COLUMN_NAME as column_name,
                c.DATA_TYPE as data_type,
                c.DATA_LENGTH as data_length,
                c.DATA_PRECISION as data_precision,
                c.DATA_SCALE as data_scale,
                c.NULLABLE as is_nullable,
                c.DATA_DEFAULT as column_default,
                c.COLUMN_ID as ordinal_position,
                c.CHAR_LENGTH as char_length,
                c.CHAR_USED as char_used,
                CASE 
                    WHEN c.DATA_TYPE IN ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR') 
                    THEN c.DATA_TYPE || '(' || c.CHAR_LENGTH || 
                         CASE WHEN c.CHAR_USED = 'C' THEN ' CHAR' ELSE ' BYTE' END || ')'
                    WHEN c.DATA_TYPE = 'NUMBER' AND c.DATA_PRECISION IS NOT NULL
                    THEN c.DATA_TYPE || '(' || c.DATA_PRECISION || 
                         CASE WHEN c.DATA_SCALE > 0 THEN ',' || c.DATA_SCALE ELSE '' END || ')'
                    ELSE c.DATA_TYPE
                END as full_data_type
        """
        
        # Add Oracle 11g+ virtual column support
        if "11." in oracle_version or "12." in oracle_version or "18." in oracle_version or "19." in oracle_version or "21." in oracle_version:
            extended_query = base_query + """,
                c.VIRTUAL_COLUMN as is_virtual,
                c.DATA_DEFAULT as generation_expression
            """
        else:
            extended_query = base_query + """,
                'NO' as is_virtual,
                NULL as generation_expression
            """
        
        query = text(extended_query + """
            FROM ALL_TAB_COLUMNS c
            WHERE c.OWNER = :schema_name AND c.TABLE_NAME = :table_name
            ORDER BY c.COLUMN_ID
        """)
        
        result = await conn.execute(query, {
            "schema_name": schema_name.upper(), 
            "table_name": table_name.upper()
        })
        return [dict(row._mapping) for row in result.fetchall()]
    
    async def _get_primary_keys(self, conn: AsyncConnection, schema_name: str, table_name: str) -> List[str]:
        """Get primary key columns for a table."""
        query = text("""
            SELECT cc.COLUMN_NAME
            FROM ALL_CONSTRAINTS c
            JOIN ALL_CONS_COLUMNS cc ON c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME 
                AND c.OWNER = cc.OWNER
            WHERE c.CONSTRAINT_TYPE = 'P' 
                AND c.OWNER = :schema_name 
                AND c.TABLE_NAME = :table_name
            ORDER BY cc.POSITION
        """)
        
        result = await conn.execute(query, {
            "schema_name": schema_name.upper(), 
            "table_name": table_name.upper()
        })
        return [row[0] for row in result.fetchall()]
    
    async def _get_foreign_keys(self, conn: AsyncConnection, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get foreign key information for a table."""
        query = text("""
            SELECT 
                cc.COLUMN_NAME as column_name,
                rc.OWNER as referenced_schema_name,
                rc.TABLE_NAME as referenced_table_name,
                rcc.COLUMN_NAME as referenced_column_name,
                c.CONSTRAINT_NAME as constraint_name,
                c.DELETE_RULE as delete_rule,
                c.STATUS as status
            FROM ALL_CONSTRAINTS c
            JOIN ALL_CONS_COLUMNS cc ON c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME 
                AND c.OWNER = cc.OWNER
            JOIN ALL_CONSTRAINTS rc ON c.R_CONSTRAINT_NAME = rc.CONSTRAINT_NAME 
                AND c.R_OWNER = rc.OWNER
            JOIN ALL_CONS_COLUMNS rcc ON rc.CONSTRAINT_NAME = rcc.CONSTRAINT_NAME 
                AND rc.OWNER = rcc.OWNER
                AND cc.POSITION = rcc.POSITION
            WHERE c.CONSTRAINT_TYPE = 'R' 
                AND c.OWNER = :schema_name 
                AND c.TABLE_NAME = :table_name
        """)
        
        result = await conn.execute(query, {
            "schema_name": schema_name.upper(), 
            "table_name": table_name.upper()
        })
        return [dict(row._mapping) for row in result.fetchall()]
    
    async def _get_indexes(self, conn: AsyncConnection, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get index information for a table."""
        query = text("""
            SELECT 
                i.INDEX_NAME as index_name,
                i.INDEX_TYPE as index_type,
                i.UNIQUENESS as uniqueness,
                i.STATUS as status,
                i.TABLESPACE_NAME as tablespace_name,
                i.LOGGING as logging,
                i.COMPRESSION as compression,
                LISTAGG(ic.COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY ic.COLUMN_POSITION) as columns
            FROM ALL_INDEXES i
            JOIN ALL_IND_COLUMNS ic ON i.INDEX_NAME = ic.INDEX_NAME 
                AND i.OWNER = ic.INDEX_OWNER
            WHERE i.TABLE_OWNER = :schema_name 
                AND i.TABLE_NAME = :table_name
            GROUP BY i.INDEX_NAME, i.INDEX_TYPE, i.UNIQUENESS, i.STATUS, 
                     i.TABLESPACE_NAME, i.LOGGING, i.COMPRESSION
            ORDER BY i.INDEX_NAME
        """)
        
        try:
            result = await conn.execute(query, {
                "schema_name": schema_name.upper(), 
                "table_name": table_name.upper()
            })
            indexes = []
            for row in result.fetchall():
                indexes.append({
                    'index_name': row[0],
                    'index_type': row[1],
                    'is_unique': row[2] == 'UNIQUE',
                    'status': row[3],
                    'tablespace_name': row[4],
                    'logging': row[5],
                    'compression': row[6],
                    'columns': row[7].split(', ') if row[7] else []
                })
            return indexes
        except Exception as e:
            logger.warning(f"Could not get indexes for {table_name}: {e}")
            return []
    
    async def _get_check_constraints(self, conn: AsyncConnection, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get check constraints for a table."""
        query = text("""
            SELECT 
                c.CONSTRAINT_NAME as constraint_name,
                c.SEARCH_CONDITION as check_clause,
                c.STATUS as status
            FROM ALL_CONSTRAINTS c
            WHERE c.CONSTRAINT_TYPE = 'C' 
                AND c.OWNER = :schema_name 
                AND c.TABLE_NAME = :table_name
                AND c.SEARCH_CONDITION IS NOT NULL
                AND c.CONSTRAINT_NAME NOT LIKE 'SYS_%'
        """)
        
        try:
            result = await conn.execute(query, {
                "schema_name": schema_name.upper(), 
                "table_name": table_name.upper()
            })
            return [dict(row._mapping) for row in result.fetchall()]
        except Exception as e:
            logger.warning(f"Could not get check constraints for {table_name}: {e}")
            return []
    
    async def _get_triggers(self, conn: AsyncConnection, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get triggers for a table."""
        query = text("""
            SELECT 
                t.TRIGGER_NAME as trigger_name,
                t.TRIGGER_TYPE as trigger_type,
                t.TRIGGERING_EVENT as triggering_event,
                t.STATUS as status,
                t.DESCRIPTION as description
            FROM ALL_TRIGGERS t
            WHERE t.OWNER = :schema_name 
                AND t.TABLE_NAME = :table_name
            ORDER BY t.TRIGGER_NAME
        """)
        
        try:
            result = await conn.execute(query, {
                "schema_name": schema_name.upper(), 
                "table_name": table_name.upper()
            })
            return [dict(row._mapping) for row in result.fetchall()]
        except Exception as e:
            logger.warning(f"Could not get triggers for {table_name}: {e}")
            return []
    
    async def _get_sequences(self, conn: AsyncConnection, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get sequences associated with a table (for auto-increment columns)."""
        query = text("""
            SELECT 
                s.SEQUENCE_NAME as sequence_name,
                s.MIN_VALUE as min_value,
                s.MAX_VALUE as max_value,
                s.INCREMENT_BY as increment_by,
                s.LAST_NUMBER as last_number,
                s.CACHE_SIZE as cache_size,
                s.CYCLE_FLAG as cycle_flag
            FROM ALL_SEQUENCES s
            WHERE s.SEQUENCE_OWNER = :schema_name
                AND (s.SEQUENCE_NAME LIKE :table_pattern1 
                     OR s.SEQUENCE_NAME LIKE :table_pattern2)
        """)
        
        try:
            result = await conn.execute(query, {
                "schema_name": schema_name.upper(),
                "table_pattern1": f"{table_name.upper()}_%",
                "table_pattern2": f"%{table_name.upper()}%"
            })
            return [dict(row._mapping) for row in result.fetchall()]
        except Exception as e:
            logger.warning(f"Could not get sequences for {table_name}: {e}")
            return []
    
    async def _get_partitions(self, conn: AsyncConnection, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get partition information for a table."""
        query = text("""
            SELECT 
                p.PARTITION_NAME as partition_name,
                p.PARTITION_POSITION as partition_position,
                p.TABLESPACE_NAME as tablespace_name,
                p.HIGH_VALUE as high_value,
                p.NUM_ROWS as num_rows,
                p.COMPRESSION as compression
            FROM ALL_TAB_PARTITIONS p
            WHERE p.TABLE_OWNER = :schema_name 
                AND p.TABLE_NAME = :table_name
            ORDER BY p.PARTITION_POSITION
        """)
        
        try:
            result = await conn.execute(query, {
                "schema_name": schema_name.upper(), 
                "table_name": table_name.upper()
            })
            return [dict(row._mapping) for row in result.fetchall()]
        except Exception as e:
            logger.warning(f"Could not get partitions for {table_name}: {e}")
            return []
    
    async def _is_primary_key(self, conn: AsyncConnection, schema_name: str, table_name: str, column_name: str) -> bool:
        """Check if a column is part of the primary key."""
        query = text("""
            SELECT COUNT(*) 
            FROM ALL_CONSTRAINTS c
            JOIN ALL_CONS_COLUMNS cc ON c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME 
                AND c.OWNER = cc.OWNER
            WHERE c.CONSTRAINT_TYPE = 'P' 
                AND c.OWNER = :schema_name 
                AND c.TABLE_NAME = :table_name
                AND cc.COLUMN_NAME = :column_name
        """)
        
        result = await conn.execute(query, {
            "schema_name": schema_name.upper(), 
            "table_name": table_name.upper(), 
            "column_name": column_name.upper()
        })
        return result.scalar() > 0
    
    async def _is_foreign_key(self, conn: AsyncConnection, schema_name: str, table_name: str, column_name: str) -> bool:
        """Check if a column is a foreign key."""
        query = text("""
            SELECT COUNT(*)
            FROM ALL_CONSTRAINTS c
            JOIN ALL_CONS_COLUMNS cc ON c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME 
                AND c.OWNER = cc.OWNER
            WHERE c.CONSTRAINT_TYPE = 'R' 
                AND c.OWNER = :schema_name 
                AND c.TABLE_NAME = :table_name
                AND cc.COLUMN_NAME = :column_name
        """)
        
        result = await conn.execute(query, {
            "schema_name": schema_name.upper(), 
            "table_name": table_name.upper(), 
            "column_name": column_name.upper()
        })
        return result.scalar() > 0
    
    async def _is_unique(self, conn: AsyncConnection, schema_name: str, table_name: str, column_name: str) -> bool:
        """Check if a column has a unique constraint."""
        query = text("""
            SELECT COUNT(*)
            FROM ALL_CONSTRAINTS c
            JOIN ALL_CONS_COLUMNS cc ON c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME 
                AND c.OWNER = cc.OWNER
            WHERE c.CONSTRAINT_TYPE = 'U' 
                AND c.OWNER = :schema_name 
                AND c.TABLE_NAME = :table_name
                AND cc.COLUMN_NAME = :column_name
        """)
        
        result = await conn.execute(query, {
            "schema_name": schema_name.upper(), 
            "table_name": table_name.upper(), 
            "column_name": column_name.upper()
        })
        return result.scalar() > 0
    
    async def _is_virtual_column(self, conn: AsyncConnection, schema_name: str, table_name: str, column_name: str, oracle_version: str) -> bool:
        """Check if a column is virtual/computed (Oracle 11g+)."""
        # Virtual columns supported from Oracle 11g+
        if not any(v in oracle_version for v in ["11.", "12.", "18.", "19.", "21."]):
            return False
        
        try:
            query = text("""
                SELECT VIRTUAL_COLUMN
                FROM ALL_TAB_COLUMNS
                WHERE OWNER = :schema_name 
                    AND TABLE_NAME = :table_name 
                    AND COLUMN_NAME = :column_name
            """)
            
            result = await conn.execute(query, {
                "schema_name": schema_name.upper(), 
                "table_name": table_name.upper(), 
                "column_name": column_name.upper()
            })
            row = result.fetchone()
            return row and row[0] == 'YES'
        except Exception:
            return False
    
    async def _get_sample_data(self, conn: AsyncConnection, schema_name: str, table_name: str, column_name: str) -> List[str]:
        """Get sample data for a column."""
        # Use double quotes for Oracle identifier quoting
        query_str = f'''
            SELECT "{column_name}" 
            FROM "{schema_name}"."{table_name}" 
            WHERE "{column_name}" IS NOT NULL 
            AND ROWNUM <= :limit_val
        '''
        
        try:
            result = await conn.execute(text(query_str), {"limit_val": self.sample_data_limit})
            return [str(row[0]) for row in result.fetchall() if row[0] is not None]
        except Exception as e:
            logger.warning(f"Could not get sample data for {schema_name}.{table_name}.{column_name}: {e}")
            return []
    
    async def _get_column_statistics(self, conn: AsyncConnection, schema_name: str, table_name: str, column_name: str) -> Dict[str, Any]:
        """Get basic statistics for a column."""
        # Use double quotes for Oracle identifier quoting
        query_str = f'''
            SELECT 
                COUNT(*) as total_count,
                COUNT("{column_name}") as non_null_count,
                COUNT(DISTINCT "{column_name}") as unique_count
            FROM "{schema_name}"."{table_name}"
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
            logger.warning(f"Could not get statistics for {schema_name}.{table_name}.{column_name}: {e}")
        
        return {
            'total_count': 0,
            'non_null_count': 0,
            'null_count': 0,
            'unique_count': 0,
            'null_percentage': 0
        }
    
    async def _get_row_count(self, conn: AsyncConnection, schema_name: str, table_name: str) -> int:
        """Get total row count for a table."""
        try:
            # Try USER_TABLES statistics first (faster but may be stale)
            stats_query = text("""
                SELECT NUM_ROWS 
                FROM ALL_TABLES 
                WHERE OWNER = :schema_name AND TABLE_NAME = :table_name
            """)
            
            result = await conn.execute(stats_query, {
                "schema_name": schema_name.upper(), 
                "table_name": table_name.upper()
            })
            stats_result = result.fetchone()
            
            if stats_result and stats_result[0] is not None and stats_result[0] > 0:
                return int(stats_result[0])
            
            # Fallback to actual count (slower but accurate)
            count_query = text(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"')
            result = await conn.execute(count_query)
            return result.scalar()
            
        except Exception as e:
            logger.warning(f"Could not get row count for {schema_name}.{table_name}: {e}")
            return 0
    
    async def _get_table_info(self, conn: AsyncConnection, schema_name: str, table_name: str) -> Dict[str, Any]:
        """Get additional table information with Oracle-specific features."""
        query = text("""
            SELECT 
                t.TABLESPACE_NAME as tablespace_name,
                t.STATUS as status,
                t.LOGGING as logging,
                t.COMPRESSION as compression,
                t.COMPRESS_FOR as compress_for,
                t.NUM_ROWS as estimated_rows,
                t.BLOCKS as blocks,
                t.EMPTY_BLOCKS as empty_blocks,
                t.LAST_ANALYZED as last_analyzed,
                t.PARTITIONED as is_partitioned,
                t.TEMPORARY as is_temporary,
                t.CLUSTER_NAME as cluster_name,
                tc.COMMENTS as table_comment
            FROM ALL_TABLES t
            LEFT JOIN ALL_TAB_COMMENTS tc ON t.OWNER = tc.OWNER AND t.TABLE_NAME = tc.TABLE_NAME
            WHERE t.OWNER = :schema_name AND t.TABLE_NAME = :table_name
        """)
        
        try:
            result = await conn.execute(query, {
                "schema_name": schema_name.upper(), 
                "table_name": table_name.upper()
            })
            row = result.fetchone()
            
            if row:
                return {
                    'tablespace_name': row[0],
                    'status': row[1],
                    'logging': row[2],
                    'compression': row[3],
                    'compress_for': row[4],
                    'estimated_rows': row[5],
                    'blocks': row[6],
                    'empty_blocks': row[7],
                    'last_analyzed': row[8],
                    'is_partitioned': row[9] == 'YES',
                    'is_temporary': row[10] == 'Y',
                    'cluster_name': row[11],
                    'table_comment': row[12]
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
    """Example usage of the OracleSchemaExtractor."""
    # Replace with your actual connection string
    # Note: You may need to install oracledb
    connection_string = "oracle+oracledb://user:password@hostname:1521/service_name"
    
    try:
        async with OracleSchemaExtractor(connection_string, sample_data_limit=10) as extractor:
            print("Extracting Oracle schema information...")
            
            schema = await extractor.extract_schema(
                schema_name='HR',  # Optional, defaults to current user
                include_sample_data=True,
                include_statistics=True
            )
            
            print(f"\nFound {len(schema)} tables in the schema:\n")
            
            for table in schema:
                print(f"🗃️  Table: {table['schema_name']}.{table['table_name']}")
                print(f"   Oracle Version: {table['oracle_version']}")
                print(f"   Rows: {table['row_count']:,}")
                print(f"   Columns: {len(table['columns'])}")
                print(f"   Primary Keys: {table['primary_keys']}")
                print(f"   Foreign Keys: {len(table['foreign_keys'])}")
                print(f"   Indexes: {len(table['indexes'])}")
                print(f"   Check Constraints: {len(table['check_constraints'])}")
                print(f"   Triggers: {len(table['triggers'])}")
                print(f"   Sequences: {len(table['sequences'])}")
                print(f"   Partitions: {len(table['partitions'])}")
                
                # Show table info
                if table['table_info']:
                    info = table['table_info']
                    print(f"   Tablespace: {info.get('tablespace_name', 'Unknown')}")
                    print(f"   Status: {info.get('status', 'Unknown')}")
                    print(f"   Partitioned: {info.get('is_partitioned', False)}")
                    print(f"   Compression: {info.get('compression', 'None')}")
                    if info.get('table_comment'):
                        print(f"   Comment: {info['table_comment']}")
                
                # Show column details
                print("   📊 Columns:")
                for col in table['columns'][:5]:  # Show first 5 columns
                    flags = []
                    if col.get('is_primary_key'): flags.append('PK')
                    if col.get('is_foreign_key'): flags.append('FK')
                    if col.get('is_unique'): flags.append('UNIQUE')
                    if col.get('is_virtual'): flags.append('VIRTUAL')
                    
                    flag_str = f" [{', '.join(flags)}]" if flags else ""
                    nullable = "NULL" if col['is_nullable'] == 'Y' else "NOT NULL"
                    
                    col_type = col['full_data_type'] or col['data_type']
                    print(f"      • {col['column_name']}: {col_type} {nullable}{flag_str}")
                    
                    # Show generation expression for virtual columns
                    if col.get('is_virtual') and col.get('generation_expression'):
                        print(f"        Generated: {col['generation_expression']}")
                    
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
                
                # Show check constraints if any
                if table['check_constraints']:
                    print("   ✅ Check Constraints:")
                    for cc in table['check_constraints'][:2]:  # Show first 2 check constraints
                        print(f"      • {cc['constraint_name']}: {cc['check_clause'][:50]}...")
                
                # Show triggers if any
                if table['triggers']:
                    print("   🔥 Triggers:")
                    for trigger in table['triggers'][:2]:  # Show first 2 triggers
                        print(f"      • {trigger['trigger_name']}: {trigger['trigger_type']} {trigger['triggering_event']}")
                
                # Show sequences if any
                if table['sequences']:
                    print("   🔢 Sequences:")
                    for seq in table['sequences'][:2]:  # Show first 2 sequences
                        print(f"      • {seq['sequence_name']}: Last={seq['last_number']}, Increment={seq['increment_by']}")
                
                # Show partitions if any
                if table['partitions']:
                    print("   📂 Partitions:")
                    for part in table['partitions'][:3]:  # Show first 3 partitions
                        print(f"      • {part['partition_name']}: Rows={part['num_rows']}, Tablespace={part['tablespace_name']}")
                
                print()  # Empty line between tables
                
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Note: Make sure you have oracledb installed and proper Oracle client configured")
        return 1
    
    return 0


