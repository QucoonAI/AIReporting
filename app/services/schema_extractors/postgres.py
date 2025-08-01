import asyncio
import psycopg
from typing import Dict, List, Any, Optional, Tuple
from . import BaseSchemaExtractor, DataSourceSchema, DataType, ColumnSchema, TableSchema


class PostgresSchemaExtractor(BaseSchemaExtractor):
    """PostgreSQL-specific schema extractor with full functionality"""
    
    def __init__(self, sample_data_limit: int = 100):
        self.sample_data_limit = sample_data_limit
    
    async def extract_schema(self, connection_string: str, schema_name: str = 'public', **kwargs) -> DataSourceSchema:
        """Extract unified schema from PostgreSQL database with full details"""
        tables = []
        
        try:
            async with await psycopg.AsyncConnection.connect(connection_string) as conn:
                async with conn.cursor() as cursor:
                    # Get all tables in the schema
                    table_names = await self._get_table_names(cursor, schema_name)
                    
                    for table_name in table_names:
                        table_schema = await self._analyze_table(cursor, schema_name, table_name, **kwargs)
                        if table_schema:
                            tables.append(table_schema)
            
            if not tables:
                raise ValueError(f"No tables found in schema '{schema_name}'")
            
            # Extract database name from connection string
            database_name = self._extract_database_name(connection_string)
            
            # Calculate metadata
            total_rows = sum(table.row_count or 0 for table in tables)
            business_context = self._infer_business_context(tables)
            
            return DataSourceSchema(
                source_name=f"postgres_{database_name}_{schema_name}",
                source_type="postgres",
                tables=tables,
                metadata={
                    "database_name": database_name,
                    "schema_name": schema_name,
                    "total_tables": len(tables),
                    "business_context": business_context,
                    "database_engine": "PostgreSQL",
                    "supports_transactions": True,
                    "supports_schemas": True,
                    "supports_json": True
                }
            )
            
        except Exception as e:
            raise Exception(f"Error extracting PostgreSQL schema: {e}")
    
    def _extract_database_name(self, connection_string: str) -> str:
        """Extract database name from PostgreSQL connection string"""
        try:
            import urllib.parse
            parsed = urllib.parse.urlparse(connection_string)
            return parsed.path.lstrip('/') if parsed.path else "unknown_database"
        except Exception:
            return "unknown_database"
    
    async def _get_table_names(self, cursor, schema_name: str) -> List[str]:
        """Get list of all tables in the schema"""
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        await cursor.execute(query, (schema_name,))
        results = await cursor.fetchall()
        return [row[0] for row in results]
    
    async def _analyze_table(self, cursor, schema_name: str, table_name: str, **kwargs) -> Optional[TableSchema]:
        """Analyze individual PostgreSQL table with full details"""
        try:
            # Get table columns with detailed information
            columns = await self._get_table_columns(cursor, schema_name, table_name, **kwargs)
            
            if not columns:
                return None
            
            # Get table row count
            row_count = await self._get_table_row_count(cursor, schema_name, table_name)
            
            # Get table metadata
            table_info = await self._get_table_info(cursor, schema_name, table_name)
            
            # Determine table type
            table_type = self._determine_table_type(table_name, columns)
            
            return TableSchema(
                name=table_name,
                columns=columns,
                row_count=row_count,
                table_type=table_type,
                primary_keys=self._extract_primary_keys(columns),
                foreign_keys=await self._get_foreign_keys(cursor, schema_name, table_name),
                indexes=await self._get_table_indexes(cursor, schema_name, table_name),
                description=self._generate_table_description(table_name, columns, row_count, table_info)
            )
            
        except Exception as e:
            print(f"Warning: Failed to analyze table '{table_name}': {e}")
            return None
    
    async def _get_table_columns(self, cursor, schema_name: str, table_name: str, **kwargs) -> List[ColumnSchema]:
        """Get detailed column information with sample data and statistics"""
        query = """
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
                WHEN c.column_default LIKE 'nextval%' THEN true
                ELSE false
            END as is_serial
        FROM information_schema.columns c
        WHERE c.table_schema = %s AND c.table_name = %s
        ORDER BY c.ordinal_position
        """
        
        await cursor.execute(query, (schema_name, table_name))
        column_rows = await cursor.fetchall()
        
        columns = []
        include_sample_data = kwargs.get('include_sample_data', True)
        
        for row in column_rows:
            column = await self._create_column_schema(cursor, schema_name, table_name, row, include_sample_data)
            columns.append(column)
        
        return columns
    
    async def _create_column_schema(self, cursor, schema_name: str, table_name: str, column_info, include_sample_data: bool) -> ColumnSchema:
        """Create ColumnSchema from PostgreSQL column information"""
        col_name = column_info[0]  # column_name
        data_type = column_info[1]  # data_type
        udt_name = column_info[8]   # udt_name
        
        # Map PostgreSQL type to unified type
        unified_type = self._map_postgres_type(data_type, udt_name)
        
        # Get sample data if requested
        sample_values = []
        value_stats = {}
        
        if include_sample_data:
            sample_values, value_stats = await self._get_column_sample_data(
                cursor, schema_name, table_name, col_name, unified_type
            )
        
        # Infer semantic type
        semantic_type = self._infer_semantic_type(col_name, unified_type, sample_values)
        
        # Create type description
        type_desc = self._create_type_description(data_type, column_info)
        
        # Create column schema
        column = ColumnSchema(
            name=col_name,
            data_type=semantic_type,
            original_type=type_desc,
            is_nullable=column_info[5] == 'YES',  # is_nullable
            sample_values=sample_values[:3],
            description=self._generate_column_description(col_name, semantic_type, column_info),
            constraints=self._extract_column_constraints(column_info)
        )

        column.is_foreign_key = await self._detect_foreign_key_status(cursor, schema_name, table_name, col_name)
        
        # Add statistics from sample data
        if value_stats:
            column.value_count = value_stats.get('total_count', 0)
            column.null_count = value_stats.get('null_count', 0)
            column.unique_count = value_stats.get('unique_count', 0)
            column.min_value = value_stats.get('min_value')
            column.max_value = value_stats.get('max_value')
            column.avg_value = value_stats.get('avg_value')
            column.min_length = value_stats.get('min_length')
            column.max_length = value_stats.get('max_length')
            column.avg_length = value_stats.get('avg_length')
        
        # Detect primary key and unique constraints
        column.is_primary_key = await self._is_primary_key_column(cursor, schema_name, table_name, col_name)
        column.is_unique = column.is_primary_key or await self._is_unique_column(cursor, schema_name, table_name, col_name)
        
        return column
    
    def _map_postgres_type(self, data_type: str, udt_name: str) -> DataType:
        """Map PostgreSQL types to unified DataType enum"""
        type_mapping = {
            'integer': DataType.INTEGER,
            'bigint': DataType.INTEGER,
            'smallint': DataType.INTEGER,
            'numeric': DataType.DECIMAL,
            'real': DataType.DECIMAL,
            'double precision': DataType.DECIMAL,
            'money': DataType.CURRENCY,
            'character varying': DataType.TEXT,
            'text': DataType.TEXT,
            'character': DataType.TEXT,
            'boolean': DataType.BOOLEAN,
            'date': DataType.DATE,
            'timestamp without time zone': DataType.DATETIME,
            'timestamp with time zone': DataType.DATETIME,
            'time': DataType.TIME,
            'json': DataType.JSON,
            'jsonb': DataType.JSON,
            'uuid': DataType.IDENTIFIER,
            'bytea': DataType.BINARY,
        }
        
        return type_mapping.get(data_type, DataType.UNKNOWN)
    
    def _create_type_description(self, data_type: str, column_info) -> str:
        """Create detailed type description"""
        type_parts = [data_type]
        
        char_max_len = column_info[2]  # character_maximum_length
        numeric_precision = column_info[3]  # numeric_precision
        numeric_scale = column_info[4]  # numeric_scale
        
        if char_max_len:
            type_parts.append(f"({char_max_len})")
        elif numeric_precision:
            if numeric_scale:
                type_parts.append(f"({numeric_precision},{numeric_scale})")
            else:
                type_parts.append(f"({numeric_precision})")
        
        return "".join(type_parts)
    
    async def _get_column_sample_data(self, cursor, schema_name: str, table_name: str, column_name: str, data_type: DataType) -> Tuple[List[str], Dict[str, Any]]:
        """Get sample data and statistics for a column"""
        try:
            # Get sample data
            sample_query = f'''
            SELECT "{column_name}" 
            FROM "{schema_name}"."{table_name}" 
            WHERE "{column_name}" IS NOT NULL 
            LIMIT {self.sample_data_limit}
            '''
            await cursor.execute(sample_query)
            sample_results = await cursor.fetchall()
            sample_values = [str(row[0]) for row in sample_results if row[0] is not None]
            
            # Get basic statistics
            stats_query = f'''
            SELECT 
                COUNT(*) as total_count,
                COUNT("{column_name}") as non_null_count,
                COUNT(DISTINCT "{column_name}") as unique_count
            FROM "{schema_name}"."{table_name}"
            '''
            await cursor.execute(stats_query)
            stats_result = await cursor.fetchone()
            
            value_stats = {
                'total_count': stats_result[0],
                'null_count': stats_result[0] - stats_result[1],
                'unique_count': stats_result[2]
            }
            
            # Add type-specific statistics
            if data_type in [DataType.INTEGER, DataType.DECIMAL, DataType.CURRENCY] and sample_values:
                try:
                    numeric_stats_query = f'''
                    SELECT 
                        MIN("{column_name}") as min_val,
                        MAX("{column_name}") as max_val,
                        AVG("{column_name}"::numeric) as avg_val
                    FROM "{schema_name}"."{table_name}"
                    WHERE "{column_name}" IS NOT NULL
                    '''
                    await cursor.execute(numeric_stats_query)
                    numeric_result = await cursor.fetchone()
                    if numeric_result:
                        value_stats.update({
                            'min_value': float(numeric_result[0]) if numeric_result[0] is not None else None,
                            'max_value': float(numeric_result[1]) if numeric_result[1] is not None else None,
                            'avg_value': float(numeric_result[2]) if numeric_result[2] is not None else None
                        })
                except Exception:
                    pass  # Skip numeric stats if query fails
            
            elif data_type == DataType.TEXT and sample_values:
                try:
                    text_stats_query = f'''
                    SELECT 
                        MIN(LENGTH("{column_name}")) as min_len,
                        MAX(LENGTH("{column_name}")) as max_len,
                        AVG(LENGTH("{column_name}")) as avg_len
                    FROM "{schema_name}"."{table_name}"
                    WHERE "{column_name}" IS NOT NULL
                    '''
                    await cursor.execute(text_stats_query)
                    text_result = await cursor.fetchone()
                    if text_result:
                        value_stats.update({
                            'min_length': int(text_result[0]) if text_result[0] is not None else None,
                            'max_length': int(text_result[1]) if text_result[1] is not None else None,
                            'avg_length': float(text_result[2]) if text_result[2] is not None else None
                        })
                except Exception:
                    pass  # Skip text stats if query fails
            
            return sample_values, value_stats
            
        except Exception as e:
            print(f"Warning: Could not get sample data for {table_name}.{column_name}: {e}")
            return [], {}
    
    async def _get_table_row_count(self, cursor, schema_name: str, table_name: str) -> Optional[int]:
        """Get row count for table"""
        try:
            count_query = f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"'
            await cursor.execute(count_query)
            result = await cursor.fetchone()
            return int(result[0]) if result else 0
        except Exception as e:
            print(f"Warning: Could not get row count for {table_name}: {e}")
            return None
    
    async def _get_table_info(self, cursor, schema_name: str, table_name: str) -> Dict[str, Any]:
        """Get additional table information"""
        try:
            query = """
            SELECT 
                obj_description(c.oid) as table_comment
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = %s AND c.relname = %s
            """
            await cursor.execute(query, (schema_name, table_name))
            result = await cursor.fetchone()
            
            return {
                'comment': result[0] if result and result[0] else None
            }
        except Exception:
            return {}
    
    async def _is_primary_key_column(self, cursor, schema_name: str, table_name: str, column_name: str) -> bool:
        """Check if column is part of primary key"""
        try:
            query = """
            SELECT COUNT(*) 
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY' 
                AND tc.table_schema = %s 
                AND tc.table_name = %s
                AND kcu.column_name = %s
            """
            await cursor.execute(query, (schema_name, table_name, column_name))
            result = await cursor.fetchone()
            return result[0] > 0 if result else False
        except Exception:
            return False
    
    async def _is_unique_column(self, cursor, schema_name: str, table_name: str, column_name: str) -> bool:
        """Check if column has unique constraint"""
        try:
            query = """
            SELECT COUNT(*)
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'UNIQUE' 
                AND tc.table_schema = %s 
                AND tc.table_name = %s
                AND kcu.column_name = %s
            """
            await cursor.execute(query, (schema_name, table_name, column_name))
            result = await cursor.fetchone()
            return result[0] > 0 if result else False
        except Exception:
            return False
    
    async def _get_foreign_keys(self, cursor, schema_name: str, table_name: str) -> List[Dict[str, str]]:
        """Get foreign key relationships for the table"""
        try:
            query = """
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
                AND tc.table_schema = %s
                AND tc.table_name = %s
            """
            
            await cursor.execute(query, (schema_name, table_name))
            results = await cursor.fetchall()
            
            foreign_keys = []
            for row in results:
                foreign_keys.append({
                    'column': row[0],
                    'references_table': row[1],
                    'references_column': row[2],
                    'constraint_name': row[3],
                    'update_rule': row[4],
                    'delete_rule': row[5]
                })
            
            return foreign_keys
            
        except Exception as e:
            print(f"Warning: Could not get foreign keys for {table_name}: {e}")
            return []
    
    async def _get_table_indexes(self, cursor, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get indexes for the table"""
        try:
            query = """
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
            WHERE n.nspname = %s AND t.relname = %s
            GROUP BY i.relname, ix.indisunique, ix.indisprimary, am.amname
            """
            
            await cursor.execute(query, (schema_name, table_name))
            results = await cursor.fetchall()
            
            indexes = []
            for row in results:
                indexes.append({
                    'name': row[0],
                    'is_unique': bool(row[1]),
                    'is_primary': bool(row[2]),
                    'access_method': row[3],
                    'columns': list(row[4]) if row[4] else []
                })
            
            return indexes
            
        except Exception as e:
            print(f"Warning: Could not get indexes for {table_name}: {e}")
            return []
    
    def _extract_primary_keys(self, columns: List[ColumnSchema]) -> List[str]:
        """Extract primary key column names"""
        return [col.name for col in columns if col.is_primary_key]
    
    def _extract_column_constraints(self, column_info) -> List[str]:
        """Extract column constraints"""
        constraints = []
        
        if column_info[5] == 'NO':  # is_nullable
            constraints.append('NOT_NULL')
        
        if column_info[9]:  # is_serial (auto-increment)
            constraints.append('SERIAL')
        
        if column_info[6]:  # column_default
            constraints.append('DEFAULT_VALUE')
        
        return constraints if constraints else None
    
    def _determine_table_type(self, table_name: str, columns: List[ColumnSchema]) -> str:
        """Determine the business type of the table"""
        name_lower = table_name.lower()
        
        # Common business table patterns
        if any(pattern in name_lower for pattern in ['user', 'customer', 'client']):
            return 'customer_table'
        elif any(pattern in name_lower for pattern in ['order', 'transaction', 'purchase', 'sale']):
            return 'transaction_table'
        elif any(pattern in name_lower for pattern in ['product', 'item', 'inventory']):
            return 'product_table'
        elif any(pattern in name_lower for pattern in ['employee', 'staff']):
            return 'employee_table'
        elif any(pattern in name_lower for pattern in ['log', 'audit', 'history']):
            return 'audit_table'
        elif any(pattern in name_lower for pattern in ['config', 'setting', 'parameter']):
            return 'configuration_table'
        elif any(pattern in name_lower for pattern in ['lookup', 'reference', 'master']):
            return 'reference_table'
        elif len(columns) > 20:
            return 'complex_data_table'
        else:
            return 'database_table'
    
    def _generate_table_description(self, table_name: str, columns: List[ColumnSchema], row_count: Optional[int], table_info: Dict) -> str:
        """Generate comprehensive table description"""
        desc_parts = [f"PostgreSQL table '{table_name}' with {len(columns)} columns"]
        
        if row_count:
            desc_parts.append(f"and {row_count:,} rows")
        
        # Add business context
        pk_cols = [col for col in columns if col.is_primary_key]
        if pk_cols:
            desc_parts.append(f"Primary key: {', '.join(col.name for col in pk_cols)}")
        
        fk_cols = [col for col in columns if col.is_foreign_key]
        if fk_cols:
            desc_parts.append(f"Has {len(fk_cols)} foreign key relationship(s)")
        
        # Add metadata
        if table_info.get('comment'):
            desc_parts.append(f"Comment: {table_info['comment']}")
        
        return " | ".join(desc_parts)
    
    def _generate_column_description(self, col_name: str, data_type: DataType, column_info) -> str:
        """Generate business-focused column description"""
        name_lower = col_name.lower()
        
        # Business context based on column name and type
        if data_type == DataType.IDENTIFIER:
            if 'customer' in name_lower:
                return "Customer identifier for tracking and relationships"
            elif 'order' in name_lower:
                return "Order identifier for transaction tracking"
            elif 'product' in name_lower:
                return "Product identifier for catalog references"
            else:
                return "Unique identifier field for business operations"
        
        elif data_type == DataType.EMAIL:
            return "Email address field for customer communication"
        
        elif data_type == DataType.CURRENCY:
            if 'price' in name_lower:
                return "Price information in monetary format"
            elif 'total' in name_lower or 'amount' in name_lower:
                return "Total monetary amount for calculations"
            else:
                return "Financial data in monetary format"
        
        elif data_type == DataType.CATEGORICAL:
            return "Categorical field for classification and grouping"
        
        elif data_type in [DataType.DATE, DataType.DATETIME]:
            if 'created' in name_lower:
                return "Creation timestamp for audit trail"
            elif 'updated' in name_lower or 'modified' in name_lower:
                return "Last modification timestamp"
            else:
                return "Temporal data for chronological analysis"
        
        elif data_type == DataType.JSON:
            return "JSON data field for structured information storage"
        
        else:
            return f"{data_type.value.title()} field for business operations"
    
    def _infer_business_context(self, tables: List[TableSchema]) -> str:
        """Infer overall business context of the database"""
        table_names = [table.name.lower() for table in tables]
        
        # E-commerce patterns
        if any('order' in name for name in table_names) and any('customer' in name for name in table_names):
            return "ecommerce_database"
        
        # CRM patterns
        elif any('customer' in name or 'contact' in name for name in table_names):
            return "crm_database"
        
        # HR patterns
        elif any('employee' in name or 'staff' in name for name in table_names):
            return "hr_database"
        
        # Financial patterns
        elif any('account' in name or 'transaction' in name for name in table_names):
            return "financial_database"
        
        # Content management
        elif any('post' in name or 'article' in name or 'content' in name for name in table_names):
            return "cms_database"
        
        # Analytics/Data warehouse
        elif any('fact' in name or 'dim' in name or 'analytics' in name for name in table_names):
            return "analytics_database"
        
        else:
            return "postgresql_database"
    
    async def _detect_foreign_key_status(self, cursor, schema_name: str, table_name: str, column_name: str) -> bool:
        """Check if column is a foreign key"""
        try:
            query = """
            SELECT COUNT(*)
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' 
                AND tc.table_schema = %s 
                AND tc.table_name = %s
                AND kcu.column_name = %s
            """
            await cursor.execute(query, (schema_name, table_name, column_name))
            result = await cursor.fetchone()
            return result[0] > 0 if result else False
        except Exception:
            return False

    def get_source_type(self) -> str:
        return "postgres"


# Helper function for schema extraction
def extract_postgres_schema_async(connection_string: str, schema_name: str = 'public') -> DataSourceSchema:
    """Extract schema from PostgreSQL using connection string"""
    import asyncio
    
    async def _extract():
        extractor = PostgresSchemaExtractor()
        return await extractor.extract_schema(connection_string, schema_name=schema_name)
    
    return asyncio.run(_extract())

