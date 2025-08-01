# Modified methods in data_source.py service class

class DataSourceService:
    """Service class for handling DataSource business logic."""
    
    # Update supported database types
    def __init__(
        self,
        data_source_repo: DataSourceRepository,
        redis_factory: RedisServiceFactory
    ):
        self.data_source_repo = data_source_repo
        self.redis_factory = redis_factory
        self.temp_data_service = redis_factory.temp_data_service
        self.s3_client = boto3.client('s3')
        self.s3_bucket = settings.S3_BUCKET_NAME
        
        self.FILE_BASED_TYPES = {'csv', 'xlsx'}
        # Expanded database support with new extractors
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
            extractor = SchemaExtractorFactory.get_extractor(data_source_type)
            
            if data_source_type == 'postgres':
                # Use async context manager for proper connection handling
                async with extractor:
                    schema = await extractor.extract_schema(
                        schema_name='public',
                        include_sample_data=True,
                        include_statistics=True
                    )
                    
            elif data_source_type == 'mysql':
                async with extractor:
                    # Extract database name from connection string if not provided
                    database_name = extractor._extract_database_name_from_connection()
                    schema = await extractor.extract_schema(
                        database_name=database_name,
                        include_sample_data=True,
                        include_statistics=True
                    )
                    
            elif data_source_type == 'mariadb':
                async with extractor:
                    database_name = extractor._extract_database_name_from_connection()
                    schema = await extractor.extract_schema(
                        database_name=database_name,
                        include_sample_data=True,
                        include_statistics=True
                    )
                    
            elif data_source_type == 'mssql':
                async with extractor:
                    database_name = extractor._extract_database_name_from_connection()
                    schema = await extractor.extract_schema(
                        database_name=database_name,
                        schema_name='dbo',  # Default SQL Server schema
                        include_sample_data=True,
                        include_statistics=True
                    )
                    
            elif data_source_type == 'oracle':
                async with extractor:
                    # Oracle will use current user schema by default
                    schema = await extractor.extract_schema(
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

    async def refresh_data_source_schema(self, data_source_id: int) -> DataSource:
        """
        Enhanced refresh method with support for all database types.
        """
        try:
            # Get the existing data source
            existing_data_source = await self.data_source_repo.get_data_source_by_id(data_source_id)
            if not existing_data_source:
                raise DataSourceNotFoundError(data_source_id)
            
            data_source_type = existing_data_source.data_source_type.value
            data_source_url = existing_data_source.data_source_url
            
            # Extract schema based on data source type
            json_schema = None
            
            if data_source_type in self.FILE_BASED_TYPES:
                if not data_source_url.startswith('https://'):
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid {data_source_type.upper()} file URL"
                    )
                
                # Download file from S3 and extract schema
                s3_key = self._extract_s3_key_from_url(data_source_url)
                file_content = await self.download_file_from_s3(s3_key)
                json_schema = await self._extract_schema_from_file(
                    data_source_type, 
                    file_content=file_content
                )
                
            elif data_source_type in self.DATABASE_TYPES:
                # Use enhanced database extraction with proper connection management
                json_schema = await self._extract_schema_from_database(data_source_type, data_source_url)
                
            else:
                raise HTTPException(
                    status_code=status.HTTP_501_NOT_IMPLEMENTED,
                    detail=f"Schema refresh for {data_source_type} is not supported yet"
                )
            
            if json_schema is None:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to extract schema from data source"
                )
            
            # Preserve user-added descriptions from existing schema
            enhanced_schema = self._preserve_user_descriptions(
                new_schema=json_schema,
                existing_schema=existing_data_source.data_source_schema
            )
            
            # Update the data source with new schema
            updated_data_source = await self.data_source_repo.refresh_data_source_schema(
                data_source_id=data_source_id,
                new_schema=enhanced_schema
            )
            
            logger.info(f"Data source schema refreshed successfully: {data_source_id}")
            return updated_data_source
            
        except (DataSourceNotFoundError, HTTPException):
            raise
        except Exception as e:
            logger.error(f"Unexpected error refreshing schema for data source {data_source_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to refresh data source schema"
            )

    def _preserve_user_descriptions(
        self, 
        new_schema: Dict[str, Any], 
        existing_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Preserve user-added descriptions when refreshing schema.
        
        Args:
            new_schema: Newly extracted schema
            existing_schema: Current schema with user descriptions
            
        Returns:
            New schema with preserved user descriptions
        """
        try:
            import copy
            enhanced_schema = copy.deepcopy(new_schema)
            
            if not existing_schema or "tables" not in existing_schema:
                return enhanced_schema
            
            # Create lookup maps for existing descriptions
            existing_table_descriptions = {}
            existing_column_descriptions = {}
            
            for table in existing_schema.get("tables", []):
                table_name = table.get("name")
                if table_name and table.get("description"):
                    existing_table_descriptions[table_name] = table["description"]
                
                existing_column_descriptions[table_name] = {}
                for column in table.get("columns", []):
                    col_name = column.get("name")
                    if col_name and column.get("description"):
                        existing_column_descriptions[table_name][col_name] = column["description"]
            
            # Apply preserved descriptions to new schema
            for table in enhanced_schema.get("tables", []):
                table_name = table.get("name")
                if table_name in existing_table_descriptions:
                    table["description"] = existing_table_descriptions[table_name]
                
                if table_name in existing_column_descriptions:
                    for column in table.get("columns", []):
                        col_name = column.get("name")
                        if col_name in existing_column_descriptions[table_name]:
                            column["description"] = existing_column_descriptions[table_name][col_name]
            
            # Update metadata
            if "metadata" not in enhanced_schema:
                enhanced_schema["metadata"] = {}
            
            enhanced_schema["metadata"]["descriptions_preserved"] = True
            enhanced_schema["metadata"]["refresh_timestamp"] = datetime.now().isoformat()
            
            return enhanced_schema
            
        except Exception as e:
            logger.error(f"Error preserving user descriptions: {e}")
            return new_schema

    async def upload_and_extract_schema(
        self, 
        user_id: int, 
        data_source_name: str,
        data_source_type: str,
        data_source_url: Optional[str] = None,
        file: Optional[UploadFile] = None
    ) -> Dict[str, Any]:
        """
        Enhanced schema extraction with support for all database types.
        """
        try:
            # Validate user limits and name uniqueness
            await self._validate_user_limits(user_id)
            await self._validate_unique_name(user_id, data_source_name)
            
            json_schema = None
            final_url = data_source_url
            temp_file_identifier = None
            file_content = None
            connection_test_result = None
            
            # Handle file-based data sources
            if data_source_type in self.FILE_BASED_TYPES:
                if not file:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"File is required for {data_source_type} data source"
                    )
                
                # Validate file without uploading to S3
                file_content = await file.read()
                self._validate_file(file, file_content)
                
                # Store file temporarily in Redis
                if self.temp_data_service:
                    temp_file_identifier = f"{user_id}_{data_source_name}_{uuid.uuid4().hex}"
                    
                    temp_file_data = {
                        "filename": file.filename,
                        "content_type": file.content_type,
                        "content": base64.b64encode(file_content).decode('utf-8'),
                        "user_id": user_id,
                        "data_source_name": data_source_name,
                        "data_source_type": data_source_type,
                        "size": len(file_content)
                    }
                    
                    await self.temp_data_service.store_temp_data(
                        operation="file_upload_extract",
                        identifier=temp_file_identifier,
                        data=temp_file_data,
                        expiry_minutes=30
                    )
                
                # Reset file pointer for schema extraction
                await file.seek(0)
                json_schema = await self._extract_schema_from_file(data_source_type, file=file)
                
                # Generate placeholder URL
                file_extension = self._get_file_extension(file.filename)
                final_url = f"pending_upload://{data_source_name}_{uuid.uuid4().hex}.{file_extension}"
                
            # Handle database connections with enhanced support
            elif data_source_type in self.DATABASE_TYPES:
                # Test connection before extracting schema
                connection_test_result = await self._test_database_connection(data_source_type, data_source_url)
                
                if not connection_test_result.get("success"):
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Database connection failed: {connection_test_result.get('error', 'Unknown error')}"
                    )
                
                json_schema = await self._extract_schema_from_database(data_source_type, data_source_url)
                final_url = data_source_url
                
            else:
                raise HTTPException(
                    status_code=status.HTTP_501_NOT_IMPLEMENTED,
                    detail=f"{data_source_type.title()} integration is not implemented yet"
                )
            
            # Generate LLM description
            llm_description = self._get_llm_prompt_from_schema(json_schema)
            
            # Convert to UI-friendly format
            tables_for_ui = self._convert_schema_for_ui(json_schema)
            
            # Prepare response data
            result = {
                "data_source_name": data_source_name,
                "data_source_type": data_source_type,
                "data_source_url": final_url,
                "extracted_schema": json_schema,
                "tables": tables_for_ui,
                "llm_description": llm_description
            }
            
            # Add file metadata and temp identifier for file-based sources
            if file and temp_file_identifier:
                result.update({
                    "file_metadata": {
                        "filename": file.filename,
                        "content_type": file.content_type,
                        "size": len(file_content)
                    },
                    "temp_file_identifier": temp_file_identifier
                })
            
            # Add connection test result for database sources
            if connection_test_result:
                result["connection_info"] = connection_test_result
            
            return result
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error during schema extraction: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to extract schema from data source"
            )

    async def _test_database_connection(
        self, 
        data_source_type: str, 
        connection_string: str
    ) -> Dict[str, Any]:
        """
        Test database connection before schema extraction.
        
        Args:
            data_source_type: Type of database
            connection_string: Database connection string
            
        Returns:
            Connection test result with success status and metadata
        """
        try:
            extractor = SchemaExtractorFactory.get_extractor(data_source_type)
            
            # Quick connection test
            async with extractor:
                # Try a simple query to test connection
                if data_source_type == 'postgres':
                    async with extractor.engine.connect() as conn:
                        result = await conn.execute(text("SELECT version()"))
                        version_info = result.fetchone()[0]
                        return {
                            "success": True,
                            "database_type": data_source_type,
                            "version": version_info,
                            "message": "Connection successful"
                        }
                        
                elif data_source_type in ['mysql', 'mariadb']:
                    async with extractor.engine.connect() as conn:
                        result = await conn.execute(text("SELECT VERSION()"))
                        version_info = result.fetchone()[0]
                        return {
                            "success": True,
                            "database_type": data_source_type,
                            "version": version_info,
                            "message": "Connection successful"
                        }
                        
                elif data_source_type == 'mssql':
                    async with extractor.engine.connect() as conn:
                        result = await conn.execute(text("SELECT @@VERSION"))
                        version_info = result.fetchone()[0]
                        return {
                            "success": True,
                            "database_type": data_source_type,
                            "version": version_info,
                            "message": "Connection successful"
                        }
                        
                elif data_source_type == 'oracle':
                    async with extractor.engine.connect() as conn:
                        result = await conn.execute(text("SELECT BANNER FROM V$VERSION WHERE ROWNUM = 1"))
                        version_info = result.fetchone()[0]
                        return {
                            "success": True,
                            "database_type": data_source_type,
                            "version": version_info,
                            "message": "Connection successful"
                        }
                        
        except Exception as e:
            logger.error(f"Database connection test failed for {data_source_type}: {e}")
            return {
                "success": False,
                "database_type": data_source_type,
                "error": str(e),
                "message": "Connection failed"
            }

