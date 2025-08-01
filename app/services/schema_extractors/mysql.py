import mysql.connector
import urllib.parse
from typing import Dict, List, Any, Optional, Tuple
from . import BaseSchemaExtractor, DataSourceSchema, TableSchema, ColumnSchema, DataType


class MySQLSchemaExtractor(BaseSchemaExtractor):
    """MySQL-specific schema extractor using unified architecture"""
    
    def __init__(self, sample_data_limit: int = 100):
        self.sample_data_limit = sample_data_limit
    
    async def extract_schema(self, connection_string: str, **kwargs) -> DataSourceSchema:
        """
        Extract unified schema from MySQL database.
        
        Args:
            connection_string: MySQL connection string or dict
            **kwargs: Additional options (database_name, include_sample_data, etc.)
            
        Returns:
            DataSourceSchema: Unified schema representation
        """
        try:
            # Parse connection parameters
            if isinstance(connection_string, str):
                connection_config, database_name = self._parse_connection_string(connection_string)
            else:
                connection_config = connection_string
                database_name = kwargs.get('database_name') or connection_config.get('database')
            
            if not database_name:
                raise ValueError("Database name must be specified")
            
            # Extract schema
            tables = await self._extract_tables_schema(connection_config, database_name, **kwargs)
            
            if not tables:
                raise ValueError(f"No tables found in database '{database_name}'")
            
            # Calculate metadata
            total_rows = sum(table.row_count or 0 for table in tables)
            business_context = self._infer_business_context(tables)
            
            return DataSourceSchema(
                source_name=f"mysql_{database_name}",
                source_type="mysql",
                tables=tables,
                metadata={
                    "database_name": database_name,
                    "total_tables": len(tables),
                    "business_context": business_context,
                    "database_engine": "MySQL",
                    "supports_transactions": True,
                    "supports_foreign_keys": True
                }
            )
            
        except Exception as e:
            raise Exception(f"Error extracting MySQL schema: {e}")
    
    def _parse_connection_string(self, connection_string: str) -> Tuple[Dict[str, Any], str]:
        """Parse MySQL connection string"""
        try:
            parsed = urllib.parse.urlparse(connection_string)
            
            connection_config = {
                'host': parsed.hostname or 'localhost',
                'port': parsed.port or 3306,
                'user': parsed.username,
                'password': parsed.password,
            }
            
            # Extract database name from path
            database_name = parsed.path.lstrip('/') if parsed.path else None
            
            # Add database to config if present
            if database_name:
                connection_config['database'] = database_name
            
            return connection_config, database_name
            
        except Exception as e:
            raise ValueError(f"Invalid MySQL connection string: {e}")
    
    async def _extract_tables_schema(self, connection_config: Dict[str, Any], database_name: str, **kwargs) -> List[TableSchema]:
        """Extract schema for all tables in the database"""
        tables = []
        
        try:
            # Connect to MySQL
            conn = mysql.connector.connect(**connection_config)
            cursor = conn.cursor(dictionary=True)
            
            # Get list of tables
            table_names = self._get_table_names(cursor, database_name)
            
            for table_name in table_names:
                table_schema = self._analyze_table(cursor, database_name, table_name, **kwargs)
                if table_schema:
                    tables.append(table_schema)
            
            cursor.close()
            conn.close()
            
            return tables
            
        except mysql.connector.Error as e:
            raise Exception(f"MySQL database error: {e}")
    
    def _get_table_names(self, cursor, database_name: str) -> List[str]:
        """Get list of all tables in the database"""
        query = """
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        cursor.execute(query, (database_name,))
        return [row['TABLE_NAME'] for row in cursor.fetchall()]
    
    def _analyze_table(self, cursor, database_name: str, table_name: str, **kwargs) -> Optional[TableSchema]:
        """Analyze individual MySQL table"""
        try:
            # Get table columns
            columns = self._get_table_columns(cursor, database_name, table_name, **kwargs)
            
            if not columns:
                return None
            
            # Get table row count
            row_count = self._get_table_row_count(cursor, database_name, table_name)
            
            # Get table metadata
            table_info = self._get_table_info(cursor, database_name, table_name)
            
            # Determine table type and relationships
            table_type = self._determine_table_type(table_name, columns)
            
            return TableSchema(
                name=table_name,
                columns=columns,
                row_count=row_count,
                table_type=table_type,
                primary_keys=self._extract_primary_keys(columns),
                foreign_keys=self._get_foreign_keys(cursor, database_name, table_name),
                indexes=self._get_table_indexes(cursor, database_name, table_name),
                description=self._generate_table_description(table_name, columns, row_count, table_info)
            )
            
        except Exception as e:
            print(f"Warning: Failed to analyze table '{table_name}': {e}")
            return None
    
    def _get_table_columns(self, cursor, database_name: str, table_name: str, **kwargs) -> List[ColumnSchema]:
        """Get detailed column information for a table"""
        query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            COLUMN_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            ORDINAL_POSITION,
            COLUMN_KEY,
            EXTRA,
            COLUMN_COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """
        
        cursor.execute(query, (database_name, table_name))
        column_rows = cursor.fetchall()
        
        columns = []
        include_sample_data = kwargs.get('include_sample_data', True)
        
        for row in column_rows:
            column = self._create_column_schema(cursor, database_name, table_name, row, include_sample_data)
            columns.append(column)
        
        return columns
    
    def _create_column_schema(self, cursor, database_name: str, table_name: str, column_info: Dict, include_sample_data: bool) -> ColumnSchema:
        """Create ColumnSchema from MySQL column information"""
        col_name = column_info['COLUMN_NAME']
        mysql_type = column_info['DATA_TYPE']
        column_type = column_info['COLUMN_TYPE']
        
        # Map MySQL type to unified type
        data_type = self._map_mysql_type_to_unified(mysql_type, column_type)
        
        # Get sample data if requested
        sample_values = []
        value_stats = {}
        
        if include_sample_data:
            sample_values, value_stats = self._get_column_sample_data(
                cursor, database_name, table_name, col_name, data_type
            )
        
        # Infer semantic type
        semantic_type = self._infer_semantic_type(col_name, data_type, sample_values)
        
        # Create column schema
        column = ColumnSchema(
            name=col_name,
            data_type=semantic_type,
            original_type=f"{mysql_type}({column_type})",
            is_nullable=column_info['IS_NULLABLE'] == 'YES',
            is_primary_key=column_info['COLUMN_KEY'] == 'PRI',
            is_unique=column_info['COLUMN_KEY'] in ('PRI', 'UNI'),
            sample_values=sample_values[:3],  # Limit to 3 samples
            description=self._generate_column_description(col_name, semantic_type, column_info),
            constraints=self._extract_column_constraints(column_info)
        )

        column.is_foreign_key = self._detect_foreign_key_status(cursor, database_name, table_name, col_name)
        
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
        
        return column
    
    def _map_mysql_type_to_unified(self, mysql_type: str, column_type: str) -> DataType:
        """Map MySQL data types to unified DataType enum"""
        mysql_type_lower = mysql_type.lower()
        
        # Integer types
        if mysql_type_lower in ('tinyint', 'smallint', 'mediumint', 'int', 'integer', 'bigint'):
            # Check for boolean (tinyint(1))
            if mysql_type_lower == 'tinyint' and '(1)' in column_type:
                return DataType.BOOLEAN
            return DataType.INTEGER
        
        # Decimal types
        elif mysql_type_lower in ('decimal', 'numeric', 'float', 'double', 'real'):
            return DataType.DECIMAL
        
        # String types
        elif mysql_type_lower in ('char', 'varchar', 'text', 'tinytext', 'mediumtext', 'longtext'):
            return DataType.TEXT
        
        # Date/Time types
        elif mysql_type_lower == 'date':
            return DataType.DATE
        elif mysql_type_lower in ('datetime', 'timestamp'):
            return DataType.DATETIME
        elif mysql_type_lower == 'time':
            return DataType.TIME
        
        # JSON type (MySQL 5.7+)
        elif mysql_type_lower == 'json':
            return DataType.JSON
        
        # Binary types
        elif mysql_type_lower in ('binary', 'varbinary', 'blob', 'tinyblob', 'mediumblob', 'longblob'):
            return DataType.BINARY
        
        # Enum/Set as categorical
        elif mysql_type_lower in ('enum', 'set'):
            return DataType.CATEGORICAL
        
        else:
            return DataType.UNKNOWN
    
    def _get_column_sample_data(self, cursor, database_name: str, table_name: str, column_name: str, data_type: DataType) -> Tuple[List[str], Dict[str, Any]]:
        """Get sample data and statistics for a column"""
        try:
            # Get sample data
            sample_query = f"""
            SELECT `{column_name}` 
            FROM `{database_name}`.`{table_name}` 
            WHERE `{column_name}` IS NOT NULL 
            LIMIT {self.sample_data_limit}
            """
            cursor.execute(sample_query)
            sample_results = cursor.fetchall()
            sample_values = [str(row[column_name]) for row in sample_results if row[column_name] is not None]
            
            # Get basic statistics
            stats_query = f"""
            SELECT 
                COUNT(*) as total_count,
                COUNT(`{column_name}`) as non_null_count,
                COUNT(DISTINCT `{column_name}`) as unique_count
            FROM `{database_name}`.`{table_name}`
            """
            cursor.execute(stats_query)
            stats_result = cursor.fetchone()
            
            value_stats = {
                'total_count': stats_result['total_count'],
                'null_count': stats_result['total_count'] - stats_result['non_null_count'],
                'unique_count': stats_result['unique_count']
            }
            
            # Add type-specific statistics
            if data_type in [DataType.INTEGER, DataType.DECIMAL, DataType.CURRENCY] and sample_values:
                try:
                    numeric_stats_query = f"""
                    SELECT 
                        MIN(`{column_name}`) as min_val,
                        MAX(`{column_name}`) as max_val,
                        AVG(`{column_name}`) as avg_val
                    FROM `{database_name}`.`{table_name}`
                    WHERE `{column_name}` IS NOT NULL
                    """
                    cursor.execute(numeric_stats_query)
                    numeric_result = cursor.fetchone()
                    if numeric_result:
                        value_stats.update({
                            'min_value': float(numeric_result['min_val']) if numeric_result['min_val'] is not None else None,
                            'max_value': float(numeric_result['max_val']) if numeric_result['max_val'] is not None else None,
                            'avg_value': float(numeric_result['avg_val']) if numeric_result['avg_val'] is not None else None
                        })
                except Exception:
                    pass  # Skip numeric stats if query fails
            
            elif data_type == DataType.TEXT and sample_values:
                try:
                    text_stats_query = f"""
                    SELECT 
                        MIN(LENGTH(`{column_name}`)) as min_len,
                        MAX(LENGTH(`{column_name}`)) as max_len,
                        AVG(LENGTH(`{column_name}`)) as avg_len
                    FROM `{database_name}`.`{table_name}`
                    WHERE `{column_name}` IS NOT NULL
                    """
                    cursor.execute(text_stats_query)
                    text_result = cursor.fetchone()
                    if text_result:
                        value_stats.update({
                            'min_length': int(text_result['min_len']) if text_result['min_len'] is not None else None,
                            'max_length': int(text_result['max_len']) if text_result['max_len'] is not None else None,
                            'avg_length': float(text_result['avg_len']) if text_result['avg_len'] is not None else None
                        })
                except Exception:
                    pass  # Skip text stats if query fails
            
            return sample_values, value_stats
            
        except Exception as e:
            print(f"Warning: Could not get sample data for {table_name}.{column_name}: {e}")
            return [], {}
    
    def _get_table_row_count(self, cursor, database_name: str, table_name: str) -> Optional[int]:
        """Get approximate row count for table"""
        try:
            # Try to get from information_schema first (faster but approximate)
            query = """
            SELECT TABLE_ROWS 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """
            cursor.execute(query, (database_name, table_name))
            result = cursor.fetchone()
            
            if result and result['TABLE_ROWS']:
                return int(result['TABLE_ROWS'])
            
            # Fallback to actual count (slower but accurate)
            count_query = f"SELECT COUNT(*) as row_count FROM `{database_name}`.`{table_name}`"
            cursor.execute(count_query)
            count_result = cursor.fetchone()
            return int(count_result['row_count']) if count_result else 0
            
        except Exception as e:
            print(f"Warning: Could not get row count for {table_name}: {e}")
            return None
    
    def _get_table_info(self, cursor, database_name: str, table_name: str) -> Dict[str, Any]:
        """Get additional table information"""
        try:
            query = """
            SELECT 
                ENGINE,
                TABLE_COLLATION,
                TABLE_COMMENT,
                CREATE_TIME,
                UPDATE_TIME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """
            cursor.execute(query, (database_name, table_name))
            result = cursor.fetchone()
            
            if result:
                return {
                    'engine': result.get('ENGINE'),
                    'collation': result.get('TABLE_COLLATION'),
                    'comment': result.get('TABLE_COMMENT'),
                    'created': result.get('CREATE_TIME'),
                    'updated': result.get('UPDATE_TIME')
                }
            
            return {}
            
        except Exception:
            return {}
    
    def _get_foreign_keys(self, cursor, database_name: str, table_name: str) -> List[Dict[str, str]]:
        """Get foreign key relationships for the table"""
        try:
            query = """
            SELECT 
                kcu.COLUMN_NAME,
                kcu.REFERENCED_TABLE_NAME,
                kcu.REFERENCED_COLUMN_NAME,
                kcu.CONSTRAINT_NAME,
                rc.UPDATE_RULE,
                rc.DELETE_RULE
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc 
                ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                AND kcu.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
            WHERE kcu.TABLE_SCHEMA = %s 
                AND kcu.TABLE_NAME = %s 
                AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
            """
            
            cursor.execute(query, (database_name, table_name))
            results = cursor.fetchall()
            
            foreign_keys = []
            for row in results:
                foreign_keys.append({
                    'column': row['COLUMN_NAME'],
                    'references_table': row['REFERENCED_TABLE_NAME'],
                    'references_column': row['REFERENCED_COLUMN_NAME'],
                    'constraint_name': row['CONSTRAINT_NAME'],
                    'update_rule': row['UPDATE_RULE'],
                    'delete_rule': row['DELETE_RULE']
                })
            
            return foreign_keys
            
        except Exception as e:
            print(f"Warning: Could not get foreign keys for {table_name}: {e}")
            return []
    
    def _get_table_indexes(self, cursor, database_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get indexes for the table"""
        try:
            query = f"SHOW INDEX FROM `{database_name}`.`{table_name}`"
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Group indexes by name
            indexes_dict = {}
            for row in results:
                index_name = row['Key_name']
                if index_name not in indexes_dict:
                    indexes_dict[index_name] = {
                        'name': index_name,
                        'is_unique': row['Non_unique'] == 0,
                        'columns': [],
                        'type': row.get('Index_type', 'BTREE')
                    }
                
                indexes_dict[index_name]['columns'].append({
                    'column_name': row['Column_name'],
                    'sequence': row['Seq_in_index'],
                    'collation': row.get('Collation'),
                    'cardinality': row.get('Cardinality')
                })
            
            # Sort columns by sequence and convert to list
            indexes = []
            for index_info in indexes_dict.values():
                index_info['columns'].sort(key=lambda x: x['sequence'])
                indexes.append(index_info)
            
            return indexes
            
        except Exception as e:
            print(f"Warning: Could not get indexes for {table_name}: {e}")
            return []
    
    def _extract_primary_keys(self, columns: List[ColumnSchema]) -> List[str]:
        """Extract primary key column names"""
        return [col.name for col in columns if col.is_primary_key]
    
    def _extract_column_constraints(self, column_info: Dict) -> List[str]:
        """Extract column constraints"""
        constraints = []
        
        if column_info['COLUMN_KEY'] == 'PRI':
            constraints.append('PRIMARY_KEY')
        elif column_info['COLUMN_KEY'] == 'UNI':
            constraints.append('UNIQUE')
        elif column_info['COLUMN_KEY'] == 'MUL':
            constraints.append('INDEX')
        
        if column_info['IS_NULLABLE'] == 'NO':
            constraints.append('NOT_NULL')
        
        if 'auto_increment' in (column_info.get('EXTRA', '') or '').lower():
            constraints.append('AUTO_INCREMENT')
        
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
            return 'business_table'
    
    def _generate_table_description(self, table_name: str, columns: List[ColumnSchema], row_count: Optional[int], table_info: Dict) -> str:
        """Generate comprehensive table description"""
        desc_parts = [f"MySQL table '{table_name}' with {len(columns)} columns"]
        
        if row_count:
            desc_parts.append(f"and {row_count:,} rows")
        
        # Add business context
        pk_cols = [col for col in columns if col.is_primary_key]
        if pk_cols:
            desc_parts.append(f"Primary key: {', '.join(col.name for col in pk_cols)}")
        
        fk_cols = [col for col in columns if col.is_foreign_key]
        if fk_cols:
            desc_parts.append(f"Has {len(fk_cols)} foreign key relationship(s)")
        
        # Add technical details
        if table_info.get('engine'):
            desc_parts.append(f"Engine: {table_info['engine']}")
        
        if table_info.get('comment'):
            desc_parts.append(f"Comment: {table_info['comment']}")
        
        return " | ".join(desc_parts)
    
    def _generate_column_description(self, col_name: str, data_type: DataType, column_info: Dict) -> str:
        """Generate business-focused column description"""
        name_lower = col_name.lower()
        
        # Business context based on column name and type
        if data_type == DataType.IDENTIFIER:
            if 'customer' in name_lower:
                return f"Customer identifier - {column_info.get('COLUMN_COMMENT', 'references customer entity')}"
            elif 'order' in name_lower:
                return f"Order identifier - {column_info.get('COLUMN_COMMENT', 'tracks order transactions')}"
            elif 'product' in name_lower:
                return f"Product identifier - {column_info.get('COLUMN_COMMENT', 'references product catalog')}"
            else:
                return f"Unique identifier - {column_info.get('COLUMN_COMMENT', 'primary business key')}"
        
        elif data_type == DataType.EMAIL:
            return f"Email address field - {column_info.get('COLUMN_COMMENT', 'for communication')}"
        
        elif data_type == DataType.CURRENCY:
            if 'price' in name_lower:
                return f"Price information - {column_info.get('COLUMN_COMMENT', 'monetary pricing data')}"
            elif 'total' in name_lower or 'amount' in name_lower:
                return f"Total amount - {column_info.get('COLUMN_COMMENT', 'calculated monetary value')}"
            else:
                return f"Financial data - {column_info.get('COLUMN_COMMENT', 'monetary values')}"
        
        elif data_type == DataType.CATEGORICAL:
            return f"Categorical field - {column_info.get('COLUMN_COMMENT', 'classification/status data')}"
        
        elif data_type in [DataType.DATE, DataType.DATETIME]:
            if 'created' in name_lower:
                return f"Creation timestamp - {column_info.get('COLUMN_COMMENT', 'record creation time')}"
            elif 'updated' in name_lower or 'modified' in name_lower:
                return f"Update timestamp - {column_info.get('COLUMN_COMMENT', 'last modification time')}"
            else:
                return f"Temporal data - {column_info.get('COLUMN_COMMENT', 'date/time information')}"
        
        else:
            comment = column_info.get('COLUMN_COMMENT', f"{data_type.value} data field")
            return f"{data_type.value.title()} field - {comment}"
    
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
        
        # Inventory patterns
        elif any('product' in name or 'inventory' in name for name in table_names):
            return "inventory_database"
        
        # Content management
        elif any('post' in name or 'article' in name or 'content' in name for name in table_names):
            return "cms_database"
        
        else:
            return "business_database"
    
    def _detect_foreign_key_status(self, cursor, database_name: str, table_name: str, column_name: str) -> bool:
        """Check if column is a foreign key"""
        try:
            query = """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            WHERE kcu.TABLE_SCHEMA = %s 
                AND kcu.TABLE_NAME = %s 
                AND kcu.COLUMN_NAME = %s
                AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
            """
            cursor.execute(query, (database_name, table_name, column_name))
            result = cursor.fetchone()
            return result['COUNT(*)'] > 0 if result else False
        except Exception:
            return False

    def get_source_type(self) -> str:
        return "mysql"


# Helper function for connection string extraction
def extract_mysql_schema_from_string(connection_string: str, database_name: str = None) -> DataSourceSchema:
    """Extract schema from MySQL using connection string"""
    import asyncio
    
    async def _extract():
        extractor = MySQLSchemaExtractor()
        return await extractor.extract_schema(connection_string, database_name=database_name)
    
    return asyncio.run(_extract())


# Example usage
if __name__ == "__main__":
    import asyncio
    import json
    
    async def test_mysql_extractor():
        """Test the MySQL extractor"""
        print("MySQL Schema Extractor Test")
        print("=" * 50)
        
        # Example connection string
        connection_string = "mysql://user:password@localhost:3306/ecommerce_db"
        
        try:
            extractor = MySQLSchemaExtractor()
            # schema = await extractor.extract_schema(connection_string)
            
            # Mock example output
            example_output = {
                "source_name": "mysql_ecommerce_db",
                "source_type": "mysql",
                "total_tables": 5,
                "total_columns": 45,
                "tables": [
                    {
                        "name": "customers",
                        "table_type": "customer_table",
                        "row_count": 10000,
                        "columns": [
                            {
                                "name": "customer_id",
                                "data_type": "identifier",
                                "is_primary_key": True,
                                "is_unique": True,
                                "description": "Customer identifier - primary business key"
                            },
                            {
                                "name": "email",
                                "data_type": "email",
                                "is_nullable": False,
                                "sample_values": ["john@example.com", "jane@company.com"],
                                "description": "Email address field - for communication"
                            }
                        ],
                        "primary_keys": ["customer_id"],
                        "indexes": [
                            {
                                "name": "idx_email",
                                "is_unique": True,
                                "columns": [{"column_name": "email"}]
                            }
                        ]
                    }
                ],
                "metadata": {
                    "database_name": "ecommerce_db",
                    "business_context": "ecommerce_database",
                    "database_engine": "MySQL"
                }
            }
            
            print("Example MySQL schema extraction output:")
            print(json.dumps(example_output, indent=2))
            
            print("\nKey Features:")
            print("- Semantic type inference (email, currency, identifier)")
            print("- Business context detection (ecommerce, CRM, etc.)")
            print("- Relationship mapping (foreign keys, indexes)")
            print("- Sample data collection for LLM context")
            print("- Performance statistics and constraints")
            
        except Exception as e:
            print(f"Error: {e}")
    
    asyncio.run(test_mysql_extractor())
