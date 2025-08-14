import pandas as pd
import io
from datetime import datetime
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse, urlunparse
from sqlalchemy import text
from fastapi import UploadFile, HTTPException, status
from app.core.utils import logger
from .db_classes.postgres.main import PostgresSchemaExtractor
from .db_classes.mysql import MySQLSchemaExtractor
from .db_classes.mssql import MSSQLSchemaExtractor
from .db_classes.mariadb import MariaDBSchemaExtractor
from .db_classes.oracle import OracleSchemaExtractor


class ExtactorService:
    """Service class for handling DataSource business logic."""
    
    # Update supported database types
    def __init__(self):
        self.FILE_BASED_TYPES = {'csv', 'xlsx'}
        self.DATABASE_TYPES = {'postgres', 'mysql', 'mariadb', 'mssql', 'oracle'}

    async def _extract_schema_from_database(
        self,
        data_source_type: str, 
        connection_string: str
    ) -> Dict[str, Any]:
        """
        Extract schema from database connections using unified extractors.
        Enhanced to support all new database types.
        
        Args:
            data_source_type: Type of database
            connection_string: Database connection string
            
        Returns:
            Extracted schema as dictionary
            
        Raises:
            HTTPException: If extraction fails
        """
        try:
            
            if data_source_type == 'postgres':
                async with PostgresSchemaExtractor(connection_string, sample_data_limit=10) as extractor:
                    print("Extracting schema information...")
                    
                    schema = await extractor.extract_schema(
                        schema_name='public',
                        include_sample_data=True,
                        include_statistics=True
                    )
                    
            elif data_source_type == 'mysql':
                async with MySQLSchemaExtractor(connection_string, sample_data_limit=10) as extractor:
                    print("Extracting MySQL schema information...")
                    
                    schema = await extractor.extract_schema(
                        database_name='public',  # Optional if in connection string
                        include_sample_data=True,
                        include_statistics=True
                    )
                    
            elif data_source_type == 'mariadb':
                async with MariaDBSchemaExtractor(connection_string, sample_data_limit=10) as extractor:
                    print("Extracting MariaDB schema information...")
                    
                    schema = await extractor.extract_schema(
                        database_name='public',  # Optional if in connection string
                        include_sample_data=True,
                        include_statistics=True
                    )
                    
            elif data_source_type == 'mssql':
                async with MSSQLSchemaExtractor(connection_string, sample_data_limit=10) as extractor:
                    print("Extracting MSSQL schema information...")
                    
                    schema = await extractor.extract_schema(
                        database_name='MyDatabase',
                        schema_name='dbo',  # Default schema
                        include_sample_data=True,
                        include_statistics=True
                    )
                    
            elif data_source_type == 'oracle':
                async with OracleSchemaExtractor(connection_string, sample_data_limit=10) as extractor:
                    print("Extracting Oracle schema information...")
                    
                    schema = await extractor.extract_schema(
                        schema_name='public',  # Optional, defaults to current user
                        include_sample_data=True,
                        include_statistics=True
                    )

            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Unsupported database type: {data_source_type}"
                )
            
            # Convert list of table schemas to DataSourceSchema format
            return self._convert_db_tables_to_schema_dict(schema, data_source_type)
            
        except Exception as e:
            logger.error(f"Database schema extraction failed for {data_source_type}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to extract schema from {data_source_type} database: {str(e)}"
            )

    def _convert_db_tables_to_schema_dict(
        self, 
        db_tables: List[Dict[str, Any]], 
        data_source_type: str
    ) -> Dict[str, Any]:
        """
        Convert database extractor output to standardized schema dictionary.
        
        Args:
            db_tables: List of table dictionaries from database extractors
            data_source_type: Type of database
            
        Returns:
            Standardized schema dictionary
        """
        try:
            # Build standardized schema structure
            schema_dict = {
                "data_source_type": data_source_type,
                "extraction_timestamp": datetime.now().isoformat(),
                "metadata": {
                    "total_tables": len(db_tables),
                    "extraction_method": "database_connection",
                    "database_specific_info": {}
                },
                "tables": []
            }
            
            for table_info in db_tables:
                # Extract common table information
                table_dict = {
                    "name": table_info.get("table_name"),
                    "row_count": table_info.get("row_count", 0),
                    "table_type": "table",
                    "description": "",
                    "primary_keys": table_info.get("primary_keys", []),
                    "foreign_keys": table_info.get("foreign_keys", []),
                    "indexes": table_info.get("indexes", []),
                    "columns": []
                }
                
                # Add database-specific metadata
                if data_source_type == "postgres":
                    table_dict["schema_name"] = table_info.get("schema_name")
                elif data_source_type in ["mysql", "mariadb"]:
                    table_dict["database_name"] = table_info.get("database_name")
                    table_dict["engine"] = table_info.get("table_info", {}).get("engine")
                elif data_source_type == "mssql":
                    table_dict["database_name"] = table_info.get("database_name")
                    table_dict["schema_name"] = table_info.get("schema_name")
                elif data_source_type == "oracle":
                    table_dict["schema_name"] = table_info.get("schema_name")
                    table_dict["tablespace_name"] = table_info.get("table_info", {}).get("tablespace_name")
                    table_dict["partitioned"] = table_info.get("table_info", {}).get("is_partitioned", False)
                
                # Process columns
                for col_info in table_info.get("columns", []):
                    column_dict = {
                        "name": col_info.get("column_name"),
                        "data_type": self._standardize_data_type(
                            col_info.get("data_type"), 
                            data_source_type
                        ),
                        "original_type": col_info.get("full_data_type") or col_info.get("data_type"),
                        "is_nullable": col_info.get("is_nullable") in ["YES", "Y", True],
                        "is_primary_key": col_info.get("is_primary_key", False),
                        "is_foreign_key": col_info.get("is_foreign_key", False),
                        "is_unique": col_info.get("is_unique", False),
                        "description": "",
                        "sample_values": col_info.get("sample_values", []),
                        "constraints": [],
                        "value_count": col_info.get("total_count", 0),
                        "null_count": col_info.get("null_count", 0),
                        "unique_count": col_info.get("unique_count", 0)
                    }
                    
                    # Add database-specific column attributes
                    if data_source_type == "postgres":
                        if col_info.get("is_serial"):
                            column_dict["constraints"].append("SERIAL")
                    elif data_source_type in ["mysql", "mariadb"]:
                        if "auto_increment" in col_info.get("extra", "").lower():
                            column_dict["constraints"].append("AUTO_INCREMENT")
                        if col_info.get("is_virtual"):
                            column_dict["constraints"].append("VIRTUAL")
                    elif data_source_type == "mssql":
                        if col_info.get("is_identity"):
                            column_dict["constraints"].append("IDENTITY")
                    elif data_source_type == "oracle":
                        if col_info.get("is_virtual"):
                            column_dict["constraints"].append("VIRTUAL")
                    
                    # Add foreign key reference information
                    fk_info = self._find_foreign_key_reference(
                        col_info.get("column_name"), 
                        table_info.get("foreign_keys", [])
                    )
                    if fk_info:
                        column_dict["references_table"] = fk_info.get("referenced_table_name")
                        column_dict["references_column"] = fk_info.get("referenced_column_name")
                    
                    table_dict["columns"].append(column_dict)
                
                # Add additional database-specific table information
                if data_source_type == "oracle":
                    # Add Oracle-specific features
                    table_dict["triggers"] = table_info.get("triggers", [])
                    table_dict["sequences"] = table_info.get("sequences", [])
                    table_dict["partitions"] = table_info.get("partitions", [])
                    table_dict["check_constraints"] = table_info.get("check_constraints", [])
                elif data_source_type in ["mysql", "mariadb"]:
                    table_dict["check_constraints"] = table_info.get("check_constraints", [])
                
                schema_dict["tables"].append(table_dict)
            
            # Add database-specific metadata
            if db_tables:
                first_table = db_tables[0]
                if data_source_type == "oracle":
                    schema_dict["metadata"]["database_specific_info"]["oracle_version"] = first_table.get("oracle_version")
                elif data_source_type == "mariadb":
                    schema_dict["metadata"]["database_specific_info"]["mariadb_version"] = first_table.get("mariadb_version")
            
            return schema_dict
            
        except Exception as e:
            logger.error(f"Error converting database tables to schema dict: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to process database schema"
            )

    def _standardize_data_type(self, original_type: str, data_source_type: str) -> str:
        """
        Standardize data types across different databases.
        
        Args:
            original_type: Original database-specific type
            data_source_type: Source database type
            
        Returns:
            Standardized data type
        """
        if not original_type:
            return "unknown"
        
        original_lower = original_type.lower()
        
        # Text types
        if any(t in original_lower for t in ["varchar", "char", "text", "string", "nvarchar", "nchar"]):
            return "text"
        
        # Integer types
        if any(t in original_lower for t in ["int", "integer", "bigint", "smallint", "tinyint"]):
            return "integer"
        
        # Decimal/Float types
        if any(t in original_lower for t in ["decimal", "numeric", "float", "double", "real", "number"]):
            return "decimal"
        
        # Boolean types
        if any(t in original_lower for t in ["bool", "boolean", "bit"]):
            return "boolean"
        
        # Date/Time types
        if any(t in original_lower for t in ["date", "time", "timestamp", "datetime"]):
            return "datetime"
        
        # JSON types
        if "json" in original_lower:
            return "json"
        
        # Binary types
        if any(t in original_lower for t in ["blob", "binary", "varbinary", "image"]):
            return "binary"
        
        # UUID types
        if "uuid" in original_lower:
            return "uuid"
        
        # Default to text for unknown types
        return "text"

    def _find_foreign_key_reference(
        self, 
        column_name: str, 
        foreign_keys: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """
        Find foreign key reference information for a column.
        
        Args:
            column_name: Name of the column
            foreign_keys: List of foreign key definitions
            
        Returns:
            Foreign key reference info or None
        """
        for fk in foreign_keys:
            if fk.get("column_name") == column_name:
                return fk
        return None

    async def _extract_schema_from_file(
        self, 
        data_source_type: str, 
        file_content: bytes = None,
        file: UploadFile = None
    ) -> Dict[str, Any]:
        """
        Extract schema from file content or UploadFile
        """
        try:
            if data_source_type == 'csv':
                return await self._extract_csv_schema(file_content, file)
            elif data_source_type == 'xlsx':
                return await self._extract_xlsx_schema(file_content, file)
            elif data_source_type == 'pdf':
                return await self._extract_pdf_schema(file_content, file)
            else:
                raise HTTPException(
                    status_code=status.HTTP_501_NOT_IMPLEMENTED,
                    detail=f"Schema extraction for {data_source_type} files is not implemented yet"
                )
                
        except Exception as e:
            logger.error(f"Error extracting schema from {data_source_type} file: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to extract schema from {data_source_type} file"
            )

    async def _extract_csv_schema(self, file_content: bytes = None, file: UploadFile = None) -> Dict[str, Any]:
        """Extract schema from CSV file"""
        try:
            # Read CSV content
            if file_content:
                csv_data = pd.read_csv(io.BytesIO(file_content))
            elif file:
                content = await file.read()
                csv_data = pd.read_csv(io.BytesIO(content))
            else:
                raise ValueError("Either file_content or file must be provided")
            
            # Extract column information
            columns = []
            for col_name in csv_data.columns:
                col_data = csv_data[col_name]
                
                # Determine data type
                if pd.api.types.is_numeric_dtype(col_data):
                    if pd.api.types.is_integer_dtype(col_data):
                        data_type = "INTEGER"
                    else:
                        data_type = "FLOAT"
                elif pd.api.types.is_datetime64_any_dtype(col_data):
                    data_type = "DATETIME"
                elif pd.api.types.is_bool_dtype(col_data):
                    data_type = "BOOLEAN"
                else:
                    data_type = "VARCHAR"
                
                # Get sample values
                sample_values = col_data.dropna().head(5).tolist()
                
                # Calculate statistics
                null_count = col_data.isnull().sum()
                unique_count = col_data.nunique()
                
                column_info = {
                    "name": col_name,
                    "data_type": data_type,
                    "original_type": str(col_data.dtype),
                    "is_nullable": null_count > 0,
                    "is_primary_key": False,
                    "is_foreign_key": False,
                    "is_unique": unique_count == len(col_data),
                    "sample_values": sample_values,
                    "value_count": len(col_data),
                    "null_count": int(null_count),
                    "unique_count": int(unique_count)
                }
                
                # Add numeric statistics if applicable
                if pd.api.types.is_numeric_dtype(col_data):
                    column_info.update({
                        "min_value": float(col_data.min()) if not pd.isna(col_data.min()) else None,
                        "max_value": float(col_data.max()) if not pd.isna(col_data.max()) else None,
                        "avg_value": float(col_data.mean()) if not pd.isna(col_data.mean()) else None
                    })
                
                # Add text statistics if applicable
                if data_type == "VARCHAR":
                    text_lengths = col_data.astype(str).str.len()
                    column_info.update({
                        "min_length": int(text_lengths.min()) if not pd.isna(text_lengths.min()) else None,
                        "max_length": int(text_lengths.max()) if not pd.isna(text_lengths.max()) else None,
                        "avg_length": float(text_lengths.mean()) if not pd.isna(text_lengths.mean()) else None
                    })
                
                columns.append(column_info)
            
            # Create table schema
            table_schema = {
                "name": "csv_data",
                "table_type": "table",
                "row_count": len(csv_data),
                "columns": columns,
                "description": "",
                "primary_keys": [],
                "foreign_keys": [],
                "indexes": []
            }
            
            # Create full schema
            schema = {
                "metadata": {
                    "extraction_timestamp": datetime.now().isoformat(),
                    "data_source_type": "csv",
                    "total_tables": 1,
                    "total_columns": len(columns),
                    "total_rows": len(csv_data)
                },
                "tables": [table_schema]
            }
            
            return schema
            
        except Exception as e:
            logger.error(f"Error extracting CSV schema: {e}")
            raise

    async def _extract_xlsx_schema(self, file_content: bytes = None, file: UploadFile = None) -> Dict[str, Any]:
        """Extract schema from Excel file"""
        try:
            # Read Excel content
            if file_content:
                excel_file = pd.ExcelFile(io.BytesIO(file_content))
            elif file:
                content = await file.read()
                excel_file = pd.ExcelFile(io.BytesIO(content))
            else:
                raise ValueError("Either file_content or file must be provided")
            
            tables = []
            
            # Process each sheet
            for sheet_name in excel_file.sheet_names:
                sheet_data = pd.read_excel(excel_file, sheet_name=sheet_name)
                
                # Extract column information (similar to CSV)
                columns = []
                for col_name in sheet_data.columns:
                    col_data = sheet_data[col_name]
                    
                    # Determine data type
                    if pd.api.types.is_numeric_dtype(col_data):
                        if pd.api.types.is_integer_dtype(col_data):
                            data_type = "INTEGER"
                        else:
                            data_type = "FLOAT"
                    elif pd.api.types.is_datetime64_any_dtype(col_data):
                        data_type = "DATETIME"
                    elif pd.api.types.is_bool_dtype(col_data):
                        data_type = "BOOLEAN"
                    else:
                        data_type = "VARCHAR"
                    
                    # Get sample values
                    sample_values = col_data.dropna().head(5).tolist()
                    
                    # Calculate statistics
                    null_count = col_data.isnull().sum()
                    unique_count = col_data.nunique()
                    
                    column_info = {
                        "name": col_name,
                        "data_type": data_type,
                        "original_type": str(col_data.dtype),
                        "is_nullable": null_count > 0,
                        "is_primary_key": False,
                        "is_foreign_key": False,
                        "is_unique": unique_count == len(col_data),
                        "sample_values": sample_values,
                        "value_count": len(col_data),
                        "null_count": int(null_count),
                        "unique_count": int(unique_count)
                    }
                    
                    columns.append(column_info)
                
                # Create table schema for this sheet
                table_schema = {
                    "name": sheet_name,
                    "table_type": "sheet",
                    "row_count": len(sheet_data),
                    "columns": columns,
                    "description": "",
                    "primary_keys": [],
                    "foreign_keys": [],
                    "indexes": []
                }
                
                tables.append(table_schema)
            
            # Create full schema
            total_rows = sum(table["row_count"] for table in tables)
            total_columns = sum(len(table["columns"]) for table in tables)
            
            schema = {
                "metadata": {
                    "extraction_timestamp": datetime.now().isoformat(),
                    "data_source_type": "xlsx",
                    "total_tables": len(tables),
                    "total_columns": total_columns,
                    "total_rows": total_rows
                },
                "tables": tables
            }
            
            return schema
            
        except Exception as e:
            logger.error(f"Error extracting Excel schema: {e}")
            raise

    async def _extract_pdf_schema(self, file_content: bytes = None, file: UploadFile = None) -> Dict[str, Any]:
        """Extract schema from PDF file (basic implementation)"""
        try:
            # For PDF files, we'll create a basic schema since PDFs don't have structured data
            # This is a placeholder implementation
            
            if file_content:
                content_size = len(file_content)
            elif file:
                content = await file.read()
                content_size = len(content)
            else:
                raise ValueError("Either file_content or file must be provided")
            
            # Create a basic table representing the PDF content
            table_schema = {
                "name": "pdf_content",
                "table_type": "document",
                "row_count": 1,  # PDFs are treated as single documents
                "columns": [
                    {
                        "name": "content",
                        "data_type": "TEXT",
                        "original_type": "pdf_text",
                        "is_nullable": False,
                        "is_primary_key": False,
                        "is_foreign_key": False,
                        "is_unique": True,
                        "sample_values": ["PDF document content"],
                        "value_count": 1,
                        "null_count": 0,
                        "unique_count": 1,
                        "description": "Full text content of the PDF document"
                    },
                    {
                        "name": "page_count",
                        "data_type": "INTEGER",
                        "original_type": "int",
                        "is_nullable": True,
                        "is_primary_key": False,
                        "is_foreign_key": False,
                        "is_unique": False,
                        "sample_values": [None],  # Would need PDF parsing library to get actual count
                        "value_count": 1,
                        "null_count": 1,
                        "unique_count": 1,
                        "description": "Number of pages in the PDF"
                    },
                    {
                        "name": "file_size",
                        "data_type": "INTEGER",
                        "original_type": "int",
                        "is_nullable": False,
                        "is_primary_key": False,
                        "is_foreign_key": False,
                        "is_unique": True,
                        "sample_values": [content_size],
                        "value_count": 1,
                        "null_count": 0,
                        "unique_count": 1,
                        "description": "Size of the PDF file in bytes"
                    }
                ],
                "description": "PDF document metadata and content",
                "primary_keys": [],
                "foreign_keys": [],
                "indexes": []
            }
            
            # Create full schema
            schema = {
                "metadata": {
                    "extraction_timestamp": datetime.now().isoformat(),
                    "data_source_type": "pdf",
                    "total_tables": 1,
                    "total_columns": 3,
                    "total_rows": 1,
                    "file_size_bytes": content_size,
                    "note": "PDF schema extraction is basic. For structured data extraction, consider converting PDF to structured format first."
                },
                "tables": [table_schema]
            }
            
            return schema
            
        except Exception as e:
            logger.error(f"Error extracting PDF schema: {e}")
            raise

    async def execute_database_query(
        self,
        query: str,
        connection_string: str,
        data_source_type: str
    ) -> pd.DataFrame:
        """
        Execute a read-only query against any supported database and return results as DataFrame.
        
        Args:
            query: SQL query to execute (must be SELECT statement)
            connection_string: Database connection string
            data_source_type: Type of database ('postgres', 'mysql', 'mariadb', 'mssql', 'oracle')
            
        Returns:
            pandas DataFrame with query results
            
        Raises:
            HTTPException: If query is not SELECT or execution fails
            ValueError: If data_source_type is not supported
        """
        # Validate that this is a read-only query
        if not query.strip().upper().startswith('SELECT'):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only SELECT queries are allowed for security reasons"
            )
        
        # Validate data source type
        if data_source_type not in self.DATABASE_TYPES:
            raise ValueError(f"Unsupported database type: {data_source_type}. Supported types: {self.DATABASE_TYPES}")
        
        try:
            if data_source_type == 'postgres':
                # Normalize connection string for psycopg (not psycopg2)
                normalized_conn = self._normalize_postgres_connection_string(connection_string)
                async with PostgresSchemaExtractor(normalized_conn) as extractor:
                    async with extractor._extractor.engine.connect() as conn:
                        result = await conn.execute(text(query))
                        
                        # Convert result to DataFrame
                        columns = list(result.keys())
                        rows = result.fetchall()
                        return pd.DataFrame(rows, columns=columns)
            
            elif data_source_type == 'mysql':
                async with MySQLSchemaExtractor(connection_string) as extractor:
                    async with extractor.engine.connect() as conn:
                        result = await conn.execute(text(query))
                        
                        columns = list(result.keys())
                        rows = result.fetchall()
                        return pd.DataFrame(rows, columns=columns)
            
            elif data_source_type == 'mariadb':
                async with MariaDBSchemaExtractor(connection_string) as extractor:
                    async with extractor.engine.connect() as conn:
                        result = await conn.execute(text(query))
                        
                        columns = list(result.keys())
                        rows = result.fetchall()
                        return pd.DataFrame(rows, columns=columns)
            
            elif data_source_type == 'mssql':
                async with MSSQLSchemaExtractor(connection_string) as extractor:
                    async with extractor.engine.connect() as conn:
                        result = await conn.execute(text(query))
                        
                        columns = list(result.keys())
                        rows = result.fetchall()
                        return pd.DataFrame(rows, columns=columns)
            
            elif data_source_type == 'oracle':
                async with OracleSchemaExtractor(connection_string) as extractor:
                    async with extractor.engine.connect() as conn:
                        result = await conn.execute(text(query))
                        
                        columns = list(result.keys())
                        rows = result.fetchall()
                        return pd.DataFrame(rows, columns=columns)
            
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Database type {data_source_type} is not supported for query execution"
                )
                
        except HTTPException:
            # Re-raise HTTP exceptions as-is
            raise
        except Exception as e:
            logger.error(f"Database query execution failed for {data_source_type}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to execute query on {data_source_type} database: {str(e)}"
            )

    def _normalize_postgres_connection_string(self, connection_string: str) -> str:
        """
        Normalize PostgreSQL connection string to use psycopg (not psycopg2) for async operations.
        
        Args:
            connection_string: Original PostgreSQL connection string
            
        Returns:
            Normalized connection string with asyncpg driver
        """
        
        try:
            parsed = urlparse(connection_string)
            
            # Handle different PostgreSQL scheme formats
            if parsed.scheme in ['postgres', 'postgresql']:
                # Use asyncpg for async operations
                new_scheme = 'postgresql+asyncpg'
            elif parsed.scheme.startswith('postgresql+psycopg2'):
                # Replace psycopg2 with asyncpg
                new_scheme = 'postgresql+asyncpg'
            elif parsed.scheme.startswith('postgresql+psycopg'):
                # Replace psycopg with asyncpg (psycopg is sync, we need async)
                new_scheme = 'postgresql+asyncpg'
            elif parsed.scheme.startswith('postgresql+asyncpg'):
                # Already correct
                return connection_string
            else:
                # Default to asyncpg
                new_scheme = 'postgresql+asyncpg'
            
            # Rebuild the connection string
            normalized_parsed = parsed._replace(scheme=new_scheme)
            return urlunparse(normalized_parsed)
            
        except Exception as e:
            logger.warning(f"Could not parse PostgreSQL connection string, using as-is: {e}")
            return connection_string


