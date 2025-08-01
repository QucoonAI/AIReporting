from datetime import datetime
from typing import Optional, Dict, Any, List
from fastapi import UploadFile, HTTPException, status
from .schema_extractors.factory import SchemaExtractorFactory
from app.core.utils import logger
from .schema_extractors.postgres import PostgresSchemaExtractor
from .schema_extractors.mysql import MySQLSchemaExtractor
from .schema_extractors.mssql import MSSQLSchemaExtractor
from .schema_extractors.mariadb import MariaDBSchemaExtractor
from .schema_extractors.oracle import OracleSchemaExtractor


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
        file: Optional[UploadFile] = None,
        file_content: Optional[bytes] = None
    ) -> Dict[str, Any]:
        """
        Extract schema from file-based data sources using unified extractors.
        
        Args:
            data_source_type: Type of data source
            file: Upload file object (for creation)
            file_content: File content bytes (for refresh)
            
        Returns:
            Extracted schema as dictionary
            
        Raises:
            HTTPException: If extraction fails
        """
        try:
            extractor = SchemaExtractorFactory.get_extractor(data_source_type)
            
            if data_source_type == 'csv':
                if file:
                    schema = await extractor.extract_schema(file)
                elif file_content:
                    # Create a mock UploadFile from bytes for CSV
                    import io
                    from fastapi import UploadFile
                    mock_file = UploadFile(
                        filename="data.csv",
                        file=io.BytesIO(file_content),
                        content_type="text/csv"
                    )
                    schema = await extractor.extract_schema(mock_file)
                else:
                    raise ValueError("Either file or file_content must be provided")
                    
            elif data_source_type == 'xlsx':
                if file:
                    schema = await extractor.extract_schema(file)
                elif file_content:
                    # Create a mock UploadFile from bytes for XLSX
                    import io
                    from fastapi import UploadFile
                    mock_file = UploadFile(
                        filename="data.xlsx",
                        file=io.BytesIO(file_content),
                        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    schema = await extractor.extract_schema(mock_file)
                else:
                    raise ValueError("Either file or file_content must be provided")
                    
            elif data_source_type == 'pdf':
                raise HTTPException(
                    status_code=status.HTTP_501_NOT_IMPLEMENTED,
                    detail="PDF processing is not implemented yet"
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Unsupported file-based data source type: {data_source_type}"
                )
            
            # Convert DataSourceSchema to dictionary
            return schema.to_dict()
            
        except Exception as e:
            logger.error(f"Schema extraction failed for {data_source_type}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to extract schema from {data_source_type} file"
            )



