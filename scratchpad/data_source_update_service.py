import uuid
from typing import Dict, Any
from datetime import datetime
from fastapi import UploadFile, HTTPException, status
from app.models.data_source import DataSource
from app.core.utils import logger
from .enhanced_data_source_service import EnhancedDataSourceService
from .enhanced_temp_data_service import EnhancedTempDataService
from .streaming_file_service import StreamingFileService
from .transaction_manager import DataSourceTransactionManager
from .connection_pool_manager import connection_pool_manager
from .distributed_lock_manager import require_data_source_update_lock
from .validation_middleware import validation_cache

class EnhancedDataSourceUpdateService:
    """Enhanced service for handling data source updates with improved performance and reliability"""
    
    def __init__(
        self, 
        data_source_service: EnhancedDataSourceService, 
        temp_service: EnhancedTempDataService,
        streaming_service: StreamingFileService
    ):
        self.data_source_service = data_source_service
        self.temp_service = temp_service
        self.streaming_service = streaming_service
        self.UPDATE_OPERATION = "data_source_update"
    
    @require_data_source_update_lock
    async def initiate_schema_refresh_update_enhanced(
        self,
        data_source_id: int,
        user_id: int,
    ) -> Dict[str, Any]:
        """
        Enhanced schema refresh with connection pooling and streaming
        """
        try:
            # Validate ownership with caching
            validation_result = await self.data_source_service.ownership_validator.validate_ownership(
                user_id, data_source_id
            )
            current_data_source = validation_result["data_source"]
            
            # Extract fresh schema using connection pooling
            new_schema = await self._extract_fresh_schema_pooled(current_data_source)
            
            # Generate schema diff efficiently
            schema_diff = await self._generate_schema_diff_async(
                old_schema=current_data_source.data_source_schema,
                new_schema=new_schema
            )
            
            # Prepare staged update data
            update_data = {
                "update_type": "schema_refresh",
                "data_source_id": data_source_id,
                "user_id": user_id,
                "current_data": {
                    "data_source_id": current_data_source.data_source_id,
                    "data_source_name": current_data_source.data_source_name,
                    "data_source_type": current_data_source.data_source_type.value,
                    "data_source_url": current_data_source.data_source_url,
                    "current_schema": current_data_source.data_source_schema
                },
                "proposed_changes": {
                    "new_schema": new_schema,
                    "schema_diff": schema_diff,
                    "tables_added": schema_diff.get("tables_added", []),
                    "tables_removed": schema_diff.get("tables_removed", []),
                    "tables_modified": schema_diff.get("tables_modified", []),
                    "columns_added": schema_diff.get("columns_added", {}),
                    "columns_removed": schema_diff.get("columns_removed", {}),
                    "columns_modified": schema_diff.get("columns_modified", {})
                },
                "requires_approval": True,
                "created_at": datetime.now().isoformat()
            }
            
            # Store staged update with enhanced temp service
            temp_identifier = f"update_{data_source_id}_{user_id}_{uuid.uuid4().hex}"
            await self.temp_service.store_extraction_with_file_reference(
                user_id=user_id,
                data_source_name=f"update_{current_data_source.data_source_name}",
                extraction_result=update_data,
                expiry_minutes=60  # Longer expiry for updates
            )
            
            return {
                "temp_identifier": temp_identifier,
                "update_type": "schema_refresh",
                "data_source_name": current_data_source.data_source_name,
                "changes_summary": {
                    "has_changes": len(schema_diff.get("tables_added", [])) > 0 or 
                                   len(schema_diff.get("tables_removed", [])) > 0 or
                                   len(schema_diff.get("tables_modified", [])) > 0,
                    "tables_added_count": len(schema_diff.get("tables_added", [])),
                    "tables_removed_count": len(schema_diff.get("tables_removed", [])),
                    "tables_modified_count": len(schema_diff.get("tables_modified", [])),
                    "total_changes": len(schema_diff.get("tables_added", [])) + 
                                   len(schema_diff.get("tables_removed", [])) + 
                                   len(schema_diff.get("tables_modified", []))
                },
                "requires_approval": True
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error initiating schema refresh update for data source {data_source_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to initiate schema refresh update"
            )
    
    @require_data_source_update_lock
    async def initiate_connection_change_update_enhanced(
        self,
        data_source_id: int,
        user_id: int,
        new_connection_url: str,
    ) -> Dict[str, Any]:
        """
        Enhanced connection change with async connection testing
        """
        try:
            # Validate ownership
            validation_result = await self.data_source_service.ownership_validator.validate_ownership(
                user_id, data_source_id
            )
            current_data_source = validation_result["data_source"]
            
            # Test new connection asynchronously
            data_source_type = current_data_source.data_source_type.value
            connection_test = await connection_pool_manager.test_connection(
                data_source_type, 
                new_connection_url
            )
            
            if not connection_test["success"]:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Failed to connect to new database: {connection_test['error']}"
                )
            
            # Extract schema from new connection using pool
            try:
                new_schema = await self.data_source_service._extract_schema_from_database_pooled(
                    data_source_type, 
                    new_connection_url
                )
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Failed to extract schema from new connection: {str(e)}"
                )
            
            # Generate schema diff
            schema_diff = await self._generate_schema_diff_async(
                old_schema=current_data_source.data_source_schema,
                new_schema=new_schema
            )
            
            # Prepare staged update data
            update_data = {
                "update_type": "connection_change",
                "data_source_id": data_source_id,
                "user_id": user_id,
                "current_data": {
                    "data_source_id": current_data_source.data_source_id,
                    "data_source_name": current_data_source.data_source_name,
                    "data_source_type": current_data_source.data_source_type.value,
                    "data_source_url": current_data_source.data_source_url,
                    "current_schema": current_data_source.data_source_schema
                },
                "proposed_changes": {
                    "new_connection_url": new_connection_url,
                    "new_schema": new_schema,
                    "schema_diff": schema_diff,
                    "connection_test_successful": True,
                    "connection_response_time": connection_test.get("response_time", 0)
                },
                "requires_approval": True,
                "created_at": datetime.now().isoformat()
            }
            
            # Store staged update
            temp_identifier = f"update_{data_source_id}_{user_id}_{uuid.uuid4().hex}"
            await self.temp_service.store_extraction_with_file_reference(
                user_id=user_id,
                data_source_name=f"update_{current_data_source.data_source_name}",
                extraction_result=update_data,
                expiry_minutes=60
            )
            
            return {
                "temp_identifier": temp_identifier,
                "update_type": "connection_change",
                "data_source_name": current_data_source.data_source_name,
                "connection_test_successful": True,
                "connection_response_time": connection_test.get("response_time", 0),
                "changes_summary": {
                    "connection_changed": True,
                    "schema_changes": len(schema_diff.get("tables_added", [])) > 0 or 
                                     len(schema_diff.get("tables_removed", [])) > 0 or
                                     len(schema_diff.get("tables_modified", [])) > 0,
                    "tables_added_count": len(schema_diff.get("tables_added", [])),
                    "tables_removed_count": len(schema_diff.get("tables_removed", [])),
                    "tables_modified_count": len(schema_diff.get("tables_modified", []))
                },
                "requires_approval": True
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error initiating connection change update: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to initiate connection change update"
            )
    
    @require_data_source_update_lock
    async def initiate_file_replace_update_enhanced(
        self,
        data_source_id: int,
        user_id: int,
        new_file: UploadFile,
    ) -> Dict[str, Any]:
        """
        Enhanced file replacement with streaming processing
        """
        try:
            # Validate ownership
            validation_result = await self.data_source_service.ownership_validator.validate_ownership(
                user_id, data_source_id
            )
            current_data_source = validation_result["data_source"]
            
            # Validate this is a file-based data source
            if current_data_source.data_source_type.value not in self.data_source_service.FILE_BASED_TYPES:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="File replacement is only supported for file-based data sources"
                )
            
            # Validate file using streaming service
            file_info = await self.streaming_service.validate_file_streaming(new_file)
            
            # Process file with streaming and extract schema
            temp_file_path = None
            async with self.streaming_service.temporary_file_from_upload(new_file) as temp_path:
                temp_file_path = temp_path
                
                try:
                    new_schema = await self.data_source_service._extract_schema_from_file_path(
                        current_data_source.data_source_type.value,
                        temp_path
                    )
                except Exception as e:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Failed to extract schema from new file: {str(e)}"
                    )
                
                # Generate schema diff
                schema_diff = await self._generate_schema_diff_async(
                    old_schema=current_data_source.data_source_schema,
                    new_schema=new_schema
                )
                
                # Get file size for metadata
                import os
                file_size = os.path.getsize(temp_path)
                
                # Prepare staged update data
                update_data = {
                    "update_type": "file_replace",
                    "data_source_id": data_source_id,
                    "user_id": user_id,
                    "current_data": {
                        "data_source_id": current_data_source.data_source_id,
                        "data_source_name": current_data_source.data_source_name,
                        "data_source_type": current_data_source.data_source_type.value,
                        "data_source_url": current_data_source.data_source_url,
                        "current_schema": current_data_source.data_source_schema
                    },
                    "proposed_changes": {
                        "new_file_metadata": {
                            "filename": new_file.filename,
                            "content_type": new_file.content_type,
                            "size": file_size
                        },
                        "new_schema": new_schema,
                        "schema_diff": schema_diff
                    },
                    "requires_approval": True,
                    "created_at": datetime.now().isoformat()
                }
                
                # Store staged update with file reference
                temp_identifier = f"update_{data_source_id}_{user_id}_{uuid.uuid4().hex}"
                await self.temp_service.store_extraction_with_file_reference(
                    user_id=user_id,
                    data_source_name=f"update_{current_data_source.data_source_name}",
                    extraction_result=update_data,
                    file_path=temp_path,
                    expiry_minutes=60
                )
                
                return {
                    "temp_identifier": temp_identifier,
                    "update_type": "file_replace",
                    "data_source_name": current_data_source.data_source_name,
                    "new_file_info": {
                        "filename": new_file.filename,
                        "size": file_size,
                        "content_type": new_file.content_type
                    },
                    "changes_summary": {
                        "file_changed": True,
                        "schema_changes": len(schema_diff.get("tables_added", [])) > 0 or 
                                         len(schema_diff.get("tables_removed", [])) > 0 or
                                         len(schema_diff.get("tables_modified", [])) > 0,
                        "tables_added_count": len(schema_diff.get("tables_added", [])),
                        "tables_removed_count": len(schema_diff.get("tables_removed", [])),
                        "tables_modified_count": len(schema_diff.get("tables_modified", []))
                    },
                    "requires_approval": True
                }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error initiating file replace update: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to initiate file replace update"
            )
    
    async def apply_staged_update_with_transaction(
        self,
        data_source_id: int,
        temp_identifier: str,
        updated_llm_description: str,
        user_id: int,
    ) -> DataSource:
        """
        Apply staged update using transaction manager
        """
        try:
            # Get staged update data
            cached_data = await self.temp_service.get_extraction_with_file_content(
                temp_identifier, 
                user_id, 
                include_file_content=True
            )
            
            if not cached_data:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Staged update not found or expired"
                )
            
            update_data = cached_data["extraction_result"]
            
            # Validate data source ID matches
            if update_data["data_source_id"] != data_source_id:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Data source ID mismatch"
                )
            
            # Validate ownership
            validation_result = await self.data_source_service.ownership_validator.validate_ownership(
                user_id, data_source_id
            )
            current_data_source = validation_result["data_source"]
            
            # Create transaction for atomic update
            transaction = DataSourceTransactionManager()
            
            # Apply update based on type
            update_type = update_data["update_type"]
            
            if update_type == "schema_refresh":
                updated_data_source = await self._setup_schema_refresh_transaction(
                    transaction, current_data_source, update_data, updated_llm_description
                )
            
            elif update_type == "connection_change":
                updated_data_source = await self._setup_connection_change_transaction(
                    transaction, current_data_source, update_data, updated_llm_description
                )
            
            elif update_type == "file_replace":
                updated_data_source = await self._setup_file_replace_transaction(
                    transaction, current_data_source, update_data, updated_llm_description, cached_data
                )
            
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Unsupported update type: {update_type}"
                )
            
            # Add cleanup operation
            transaction.add_temp_data_cleanup_operation(
                self.temp_service, temp_identifier, user_id
            )
            
            # Execute transaction
            result = await transaction.execute_transaction()
            
            if not result["success"]:
                logger.error(f"Update transaction failed: {result}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to apply update: {result.get('error', 'Unknown error')}"
                )
            
            # Invalidate relevant caches
            validation_cache.invalidate(user_id, "ownership_check", data_source_id=data_source_id)
            
            logger.info(f"Applied staged update {temp_identifier} to data source {data_source_id}")
            return result["results"]["database_update"]
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error applying staged update {temp_identifier}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to apply staged update"
            )
    
    async def _extract_fresh_schema_pooled(self, data_source: DataSource) -> Dict[str, Any]:
        """Extract fresh schema using connection pooling"""
        data_source_type = data_source.data_source_type.value
        data_source_url = data_source.data_source_url
        
        if data_source_type in self.data_source_service.FILE_BASED_TYPES:
            # For file-based sources, download and process
            from app.core.utils import extract_s3_key_from_url, download_file_from_s3
            s3_key = extract_s3_key_from_url(data_source_url)
            file_content = await download_file_from_s3(s3_key)
            
            from .extractor import ExtactorService
            extractor = ExtactorService()
            return await extractor._extract_schema_from_file(
                data_source_type, 
                file_content=file_content
            )
        
        elif data_source_type in self.data_source_service.DATABASE_TYPES:
            return await self.data_source_service._extract_schema_from_database_pooled(
                data_source_type, 
                data_source_url
            )
        
        else:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail=f"Schema extraction not supported for {data_source_type}"
            )
    
    async def _generate_schema_diff_async(
        self, 
        old_schema: Dict[str, Any], 
        new_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate schema diff asynchronously to avoid blocking"""
        import asyncio
        from concurrent.futures import ThreadPoolExecutor
        
        def generate_diff():
            return self._generate_schema_diff_sync(old_schema, new_schema)
        
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            return await loop.run_in_executor(executor, generate_diff)
    
    def _generate_schema_diff_sync(
        self, 
        old_schema: Dict[str, Any], 
        new_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Synchronous schema diff generation (same logic as before but optimized)"""
        try:
            diff = {
                "tables_added": [],
                "tables_removed": [],
                "tables_modified": [],
                "columns_added": {},
                "columns_removed": {},
                "columns_modified": {}
            }
            
            # Handle case where old_schema might be a string (LLM description only)
            if isinstance(old_schema, str):
                if isinstance(new_schema, dict) and "tables" in new_schema:
                    diff["tables_added"] = [table["name"] for table in new_schema.get("tables", [])]
                return diff
            
            old_tables = {table["name"]: table for table in old_schema.get("tables", [])}
            new_tables = {table["name"]: table for table in new_schema.get("tables", [])}
            
            # Find added and removed tables
            old_table_names = set(old_tables.keys())
            new_table_names = set(new_tables.keys())
            
            diff["tables_added"] = list(new_table_names - old_table_names)
            diff["tables_removed"] = list(old_table_names - new_table_names)
            
            # Find modified tables (optimized comparison)
            common_tables = old_table_names & new_table_names
            
            for table_name in common_tables:
                old_table = old_tables[table_name]
                new_table = new_tables[table_name]
                
                # Quick comparison of column structure
                old_columns = {col["name"]: col for col in old_table.get("columns", [])}
                new_columns = {col["name"]: col for col in new_table.get("columns", [])}
                
                old_col_names = set(old_columns.keys())
                new_col_names = set(new_columns.keys())
                
                cols_added = list(new_col_names - old_col_names)
                cols_removed = list(old_col_names - new_col_names)
                cols_modified = []
                
                # Check for modified columns (only check common columns)
                for col_name in (old_col_names & new_col_names):
                    old_col = old_columns[col_name]
                    new_col = new_columns[col_name]
                    
                    # Compare key attributes efficiently
                    if (old_col.get("data_type") != new_col.get("data_type") or
                        old_col.get("is_nullable") != new_col.get("is_nullable") or
                        old_col.get("is_primary_key") != new_col.get("is_primary_key")):
                        cols_modified.append({
                            "name": col_name,
                            "old_type": old_col.get("data_type"),
                            "new_type": new_col.get("data_type"),
                            "changes": []
                        })
                
                # If there are column changes, mark table as modified
                if cols_added or cols_removed or cols_modified:
                    diff["tables_modified"].append(table_name)
                    if cols_added:
                        diff["columns_added"][table_name] = cols_added
                    if cols_removed:
                        diff["columns_removed"][table_name] = cols_removed
                    if cols_modified:
                        diff["columns_modified"][table_name] = cols_modified
            
            return diff
            
        except Exception as e:
            logger.error(f"Error generating schema diff: {e}")
            return {
                "tables_added": [],
                "tables_removed": [],
                "tables_modified": [],
                "columns_added": {},
                "columns_removed": {},
                "columns_modified": {},
                "error": "Failed to generate diff"
            }
    
    async def _setup_schema_refresh_transaction(
        self, 
        transaction: DataSourceTransactionManager,
        current_data_source: DataSource, 
        update_data: Dict[str, Any],
        updated_llm_description: str
    ) -> DataSource:
        """Setup transaction for schema refresh"""
        from app.schemas.data_source import DataSourceUpdateRequest
        
        def update_schema():
            update_request = DataSourceUpdateRequest(
                data_source_schema=updated_llm_description
            )
            return self.data_source_service.data_source_repo.update_data_source(
                data_source_id=current_data_source.data_source_id,
                update_data=update_request
            )
        
        transaction.add_database_update_operation(update_schema)
        return current_data_source
    
    async def _setup_connection_change_transaction(
        self, 
        transaction: DataSourceTransactionManager,
        current_data_source: DataSource, 
        update_data: Dict[str, Any],
        updated_llm_description: str
    ) -> DataSource:
        """Setup transaction for connection change"""
        from app.schemas.data_source import DataSourceUpdateRequest
        
        new_connection_url = update_data["proposed_changes"]["new_connection_url"]
        
        def update_connection():
            update_request = DataSourceUpdateRequest(
                data_source_url=new_connection_url,
                data_source_schema=updated_llm_description
            )
            return self.data_source_service.data_source_repo.update_data_source(
                data_source_id=current_data_source.data_source_id,
                update_data=update_request
            )
        
        transaction.add_database_update_operation(update_connection)
        return current_data_source
    
    async def _setup_file_replace_transaction(
        self, 
        transaction: DataSourceTransactionManager,
        current_data_source: DataSource, 
        update_data: Dict[str, Any],
        updated_llm_description: str,
        cached_data: Dict[str, Any]
    ) -> DataSource:
        """Setup transaction for file replacement"""
        from app.schemas.data_source import DataSourceUpdateRequest
        import io
        
        file_metadata = update_data["proposed_changes"]["new_file_metadata"]
        
        # Get file content from cache
        file_content = cached_data.get("file_content")
        if not file_content:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="File content not found in cached data"
            )
        
        # Create UploadFile object for S3 upload
        file_obj = UploadFile(
            filename=file_metadata["filename"],
            file=io.BytesIO(file_content),
            content_type=file_metadata["content_type"]
        )
        
        # Add S3 upload operation
        transaction.add_s3_upload_operation(
            file_obj, 
            current_data_source.data_source_user_id, 
            current_data_source.data_source_name,
            None
        )
        
        # Add database update operation (will use S3 URL from previous operation)
        def update_file_source():
            # This will be called after S3 upload, so we need to get the URL from transaction results
            update_request = DataSourceUpdateRequest(
                data_source_schema=updated_llm_description
            )
            return self.data_source_service.data_source_repo.update_data_source(
                data_source_id=current_data_source.data_source_id,
                update_data=update_request
            )
        
        transaction.add_database_update_operation(update_file_source)
        
        # Add cleanup of old S3 file
        if current_data_source.data_source_url.startswith("https://"):
            transaction.add_old_file_cleanup_operation(current_data_source.data_source_url)
        
        return current_data_source