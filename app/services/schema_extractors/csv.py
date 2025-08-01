from . import BaseSchemaExtractor, DataSourceSchema, DataType, ColumnSchema, TableSchema
import pandas as pd
import io
from typing import List, Optional, Tuple, Dict, Any


class CSVSchemaExtractor(BaseSchemaExtractor):
    """CSV-specific schema extractor"""
    
    def __init__(self, sample_data_limit: int = 100):
        self.sample_data_limit = sample_data_limit
    
    async def extract_schema(self, upload_file, **kwargs) -> DataSourceSchema:
        """
        Extract unified schema from CSV file.
        
        Args:
            upload_file: FastAPI UploadFile containing CSV data
            **kwargs: Additional options (include_sample_data, etc.)
            
        Returns:
            DataSourceSchema: Unified schema representation
        """
        try:
            # Read file content
            file_content = await upload_file.read()
            await upload_file.seek(0)
            
            if not file_content:
                raise ValueError("CSV file is empty")
            
            # Decode content with proper encoding detection
            try:
                csv_content = file_content.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    csv_content = file_content.decode('latin1')
                except UnicodeDecodeError:
                    csv_content = file_content.decode('utf-8', errors='ignore')
            
            if not csv_content.strip():
                raise ValueError("CSV file contains no data")
            
            # Parse CSV with robust settings
            try:
                df = pd.read_csv(
                    io.StringIO(csv_content),
                    encoding=None,  # Let pandas auto-detect
                    sep=None,       # Let pandas auto-detect separator
                    engine='python', # More flexible parser
                    skip_blank_lines=True,
                    na_values=['', ' ', 'NULL', 'null', 'None', 'N/A', 'n/a']
                )
            except Exception as e:
                # Try with explicit comma separator as fallback
                try:
                    df = pd.read_csv(
                        io.StringIO(csv_content),
                        sep=',',
                        encoding='utf-8',
                        skip_blank_lines=True,
                        na_values=['', ' ', 'NULL', 'null', 'None', 'N/A', 'n/a']
                    )
                except Exception:
                    raise ValueError(f"Could not parse CSV file: {e}")
            
            if df.empty:
                raise ValueError("CSV file contains no data rows")
            
            if len(df.columns) == 0:
                raise ValueError("CSV file contains no columns")
            
            # Clean column names (strip whitespace)
            df.columns = df.columns.str.strip()
            
            # Analyze each column
            columns = []
            include_sample_data = kwargs.get('include_sample_data', True)
            
            for col_name in df.columns:
                column = self._create_column_schema(df, col_name, include_sample_data)
                columns.append(column)
            
            # Determine table type
            table_type = self._determine_table_type(upload_file.filename or "csv_data", columns)
            
            # Create table schema
            table = TableSchema(
                name=upload_file.filename or "csv_data",
                columns=columns,
                row_count=len(df),
                table_type=table_type,
                description=self._generate_table_description(upload_file.filename or "csv_data", columns, len(df))
            )
            
            # Calculate metadata
            file_size_mb = len(file_content) / (1024 * 1024)
            business_context = self._infer_business_context([table])
            
            return DataSourceSchema(
                source_name=upload_file.filename or "uploaded_csv",
                source_type="csv",
                tables=[table],
                metadata={
                    "file_size_mb": round(file_size_mb, 2),
                    "business_context": business_context,
                    "encoding_used": "utf-8",
                    "separator_detected": ",",
                    "total_rows": len(df),
                    "has_header": True
                }
            )
            
        except Exception as e:
            raise Exception(f"Error extracting CSV schema: {e}")
    
    def _create_column_schema(self, df: pd.DataFrame, col_name: str, include_sample_data: bool) -> ColumnSchema:
        """Create ColumnSchema from pandas column"""
        col_data = df[col_name]
        
        # Map pandas type to unified type
        data_type = self._map_pandas_type_to_unified(col_data)
        
        # Get sample data and statistics
        sample_values = []
        value_stats = {}
        
        if include_sample_data:
            sample_values, value_stats = self._get_column_sample_data(col_data, data_type)
        
        # Infer semantic type
        semantic_type = self._infer_semantic_type(col_name, data_type, sample_values)
        
        # Create column schema
        column = ColumnSchema(
            name=col_name,
            data_type=semantic_type,
            original_type=str(col_data.dtype),
            is_nullable=col_data.isnull().any(),
            sample_values=sample_values[:3],  # Limit to 3 samples
            value_count=len(col_data),
            null_count=int(col_data.isnull().sum()),
            unique_count=int(col_data.nunique()),
            description=self._generate_column_description(col_name, semantic_type, col_data)
        )
        
        # Set constraints
        constraints = []
        if not column.is_nullable:
            constraints.append('NOT_NULL')
        
        # Check uniqueness
        if column.unique_count == column.value_count and column.null_count == 0:
            column.is_unique = True
            constraints.append('UNIQUE')
        
        # Detect potential primary keys
        if column.is_unique and any(keyword in col_name.lower() for keyword in ['id', 'key', 'pk']):
            column.is_primary_key = True
            constraints.append('PRIMARY_KEY_CANDIDATE')
        
        # Set constraints
        column.constraints = constraints if constraints else None
        
        # Add type-specific statistics from sample data
        if value_stats:
            column.min_value = value_stats.get('min_value')
            column.max_value = value_stats.get('max_value')
            column.avg_value = value_stats.get('avg_value')
            column.min_length = value_stats.get('min_length')
            column.max_length = value_stats.get('max_length')
            column.avg_length = value_stats.get('avg_length')
        
        return column
    
    def _map_pandas_type_to_unified(self, col_data: pd.Series) -> DataType:
        """Map pandas data types to unified DataType enum"""
        dtype_str = str(col_data.dtype).lower()
        
        # Integer types
        if pd.api.types.is_integer_dtype(col_data):
            return DataType.INTEGER
        
        # Float types
        elif pd.api.types.is_float_dtype(col_data):
            return DataType.DECIMAL
        
        # Boolean types
        elif pd.api.types.is_bool_dtype(col_data):
            return DataType.BOOLEAN
        
        # Datetime types
        elif pd.api.types.is_datetime64_any_dtype(col_data):
            return DataType.DATETIME
        
        # String/Object types
        elif pd.api.types.is_string_dtype(col_data) or pd.api.types.is_object_dtype(col_data):
            return DataType.TEXT
        
        else:
            return DataType.UNKNOWN
    
    def _get_column_sample_data(self, col_data: pd.Series, data_type: DataType) -> Tuple[List[str], Dict[str, Any]]:
        """Get sample data and statistics for a column"""
        try:
            # Get non-null values for sampling
            non_null_data = col_data.dropna()
            
            # Sample values (limit to avoid memory issues)
            sample_size = min(self.sample_data_limit, len(non_null_data))
            sample_values = [str(val) for val in non_null_data.head(sample_size).tolist() if val is not None]
            
            # Basic statistics
            value_stats = {
                'total_count': len(col_data),
                'null_count': int(col_data.isnull().sum()),
                'unique_count': int(col_data.nunique())
            }
            
            # Add type-specific statistics
            if data_type in [DataType.INTEGER, DataType.DECIMAL, DataType.CURRENCY] and not non_null_data.empty:
                try:
                    numeric_data = pd.to_numeric(non_null_data, errors='coerce').dropna()
                    if not numeric_data.empty:
                        value_stats.update({
                            'min_value': float(numeric_data.min()),
                            'max_value': float(numeric_data.max()),
                            'avg_value': float(numeric_data.mean())
                        })
                except Exception:
                    pass  # Skip numeric stats if conversion fails
            
            elif data_type == DataType.TEXT and not non_null_data.empty:
                try:
                    str_lengths = non_null_data.astype(str).str.len()
                    value_stats.update({
                        'min_length': int(str_lengths.min()),
                        'max_length': int(str_lengths.max()),
                        'avg_length': float(str_lengths.mean())
                    })
                except Exception:
                    pass  # Skip text stats if calculation fails
            
            return sample_values, value_stats
            
        except Exception as e:
            print(f"Warning: Could not get sample data for column: {e}")
            return [], {}
    
    def _determine_table_type(self, filename: str, columns: List[ColumnSchema]) -> str:
        """Determine the business type of the table"""
        filename_lower = filename.lower() if filename else ""
        
        # Check filename patterns
        if any(pattern in filename_lower for pattern in ['customer', 'client']):
            return 'customer_data'
        elif any(pattern in filename_lower for pattern in ['order', 'transaction', 'sales']):
            return 'transaction_data'
        elif any(pattern in filename_lower for pattern in ['product', 'inventory', 'catalog']):
            return 'product_data'
        elif any(pattern in filename_lower for pattern in ['employee', 'staff']):
            return 'employee_data'
        elif any(pattern in filename_lower for pattern in ['export', 'report']):
            return 'report_data'
        
        # Check column patterns
        column_names = [col.name.lower() for col in columns]
        if any('customer' in name for name in column_names):
            return 'customer_data'
        elif any('order' in name or 'transaction' in name for name in column_names):
            return 'transaction_data'
        elif any('product' in name for name in column_names):
            return 'product_data'
        elif len(columns) > 20:
            return 'detailed_data'
        else:
            return 'csv_data'
    
    def _generate_table_description(self, filename: str, columns: List[ColumnSchema], row_count: int) -> str:
        """Generate comprehensive table description"""
        desc_parts = [f"CSV file '{filename}' with {len(columns)} columns and {row_count:,} rows"]
        
        # Add business context
        id_cols = [col for col in columns if col.data_type == DataType.IDENTIFIER]
        if id_cols:
            desc_parts.append(f"Contains identifiers: {', '.join(col.name for col in id_cols)}")
        
        currency_cols = [col for col in columns if col.data_type == DataType.CURRENCY]
        if currency_cols:
            desc_parts.append(f"Contains financial data: {', '.join(col.name for col in currency_cols)}")
        
        date_cols = [col for col in columns if col.data_type in [DataType.DATE, DataType.DATETIME]]
        if date_cols:
            desc_parts.append(f"Contains temporal data: {', '.join(col.name for col in date_cols)}")
        
        return " | ".join(desc_parts)
    
    def _generate_column_description(self, col_name: str, data_type: DataType, col_data: pd.Series) -> str:
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
            unique_count = col_data.nunique()
            return f"Categorical field with {unique_count} distinct values for classification"
        
        elif data_type in [DataType.DATE, DataType.DATETIME]:
            if 'created' in name_lower:
                return "Creation timestamp for audit trail"
            elif 'updated' in name_lower or 'modified' in name_lower:
                return "Last modification timestamp"
            else:
                return "Temporal data for chronological analysis"
        
        else:
            return f"{data_type.value.title()} field for business operations"
    
    def _infer_business_context(self, tables: List[TableSchema]) -> str:
        """Infer overall business context of the CSV file"""
        if not tables:
            return "csv_data"
        
        table = tables[0]  # CSV has only one table
        column_names = [col.name.lower() for col in table.columns]
        
        # E-commerce patterns
        if any('customer' in name for name in column_names) and any('order' in name for name in column_names):
            return "ecommerce_data"
        
        # Customer data
        elif any('customer' in name or 'client' in name for name in column_names):
            return "customer_data"
        
        # Sales/Transaction data
        elif any('sale' in name or 'transaction' in name or 'order' in name for name in column_names):
            return "sales_transaction_data"
        
        # Product data
        elif any('product' in name or 'item' in name for name in column_names):
            return "product_inventory_data"
        
        # Financial data
        elif any('amount' in name or 'price' in name or 'cost' in name for name in column_names):
            return "financial_data"
        
        # Employee data
        elif any('employee' in name or 'staff' in name for name in column_names):
            return "employee_data"
        
        else:
            return "business_data"
    
    def get_source_type(self) -> str:
        return "csv"


# Helper function for testing
async def extract_csv_schema_from_content(file_content: bytes, filename: str = "data.csv") -> DataSourceSchema:
    """Extract schema from CSV file content bytes"""
    
    class MockUploadFile:
        def __init__(self, content: bytes, filename: str):
            self.content = content
            self.filename = filename
            self.content_type = "text/csv"
        
        async def read(self) -> bytes:
            return self.content
        
        async def seek(self, position: int = 0) -> None:
            pass
    
    mock_file = MockUploadFile(file_content, filename)
    extractor = CSVSchemaExtractor()
    return await extractor.extract_schema(mock_file)

