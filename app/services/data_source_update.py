import io
import copy
import base64
from typing import Dict, Any
from datetime import datetime
import uuid
from fastapi import UploadFile, HTTPException, status
from app.models.data_source import DataSource
from app.schemas.data_source import DataSourceUpdateRequest
from app.core.utils import logger
from app.core.utils.s3_functions import extract_s3_key_from_url, upload_file_to_s3, validate_file, download_file_from_s3, delete_file_from_s3
from .data_source import DataSourceService
from app.core.utils.extractor import ExtactorService
from .redis_managers.data_source import TempDataSourceService


extractor = ExtactorService()

class DataSourceUpdateService:
    """Specialized service for handling data source updates with staging"""
    
    def __init__(self, data_source_service: DataSourceService, temp_service: TempDataSourceService):
        self.data_source_service = data_source_service
        self.UPDATE_OPERATION = "data_source_update"
        self.temp_service = temp_service
    
    async def initiate_schema_refresh_update(
        self,
        data_source_id: int,
        user_id: int,
    ) -> Dict[str, Any]:
        """
        Initiate a schema refresh update by re-extracting schema from the current source
        """
        try:
            # Get current data source
            current_data_source = await self.data_source_service.get_data_source_by_id(data_source_id)
            
            # Validate ownership
            if current_data_source.data_source_user_id != user_id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You don't have permission to update this data source"
                )
            
            # Extract fresh schema
            new_schema = await self._extract_fresh_schema(current_data_source)
            
            # Generate schema diff
            schema_diff = self._generate_schema_diff(
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
            
            # Store staged update
            temp_identifier = f"update_{data_source_id}_{user_id}_{uuid.uuid4().hex}"
            await self.temp_service.store_extraction(
                user_id=user_id,
                data_source_name=f"update_{current_data_source.data_source_name}",
                extraction_result=update_data,
                expiry_minutes=60  # Longer expiry for updates
            )
            
            # Return summary for user review
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
    
    async def initiate_connection_change_update(
        self,
        data_source_id: int,
        user_id: int,
        new_connection_url: str,
    ) -> Dict[str, Any]:
        """
        Initiate a connection URL change update
        """
        try:
            # Get current data source
            current_data_source = await self.data_source_service.get_data_source_by_id(data_source_id)
            
            # Validate ownership
            if current_data_source.data_source_user_id != user_id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You don't have permission to update this data source"
                )
            
            # Test new connection and extract schema
            data_source_type = current_data_source.data_source_type.value
            try:
                new_schema = await extractor._extract_schema_from_database(
                    data_source_type, 
                    new_connection_url
                )
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Failed to connect to new database: {str(e)}"
                )
            
            # Generate schema diff
            schema_diff = self._generate_schema_diff(
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
                    "connection_test_successful": True
                },
                "requires_approval": True,
                "created_at": datetime.now().isoformat()
            }
            
            # Store staged update
            temp_identifier = f"update_{data_source_id}_{user_id}_{uuid.uuid4().hex}"
            await self.temp_service.store_extraction(
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
                "changes_summary": {
                    "connection_changed": True,
                    "schema_changes": len(schema_diff.get("tables_added", [])) > 0 or 
                                     len(schema_diff.get("tables_removed", [])) > 0 or
                                     len(schema_diff.get("tables_modified", [])) > 0
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
    
    async def initiate_file_replace_update(
        self,
        data_source_id: int,
        user_id: int,
        new_file: UploadFile,
    ) -> Dict[str, Any]:
        """
        Initiate a file replacement update
        """
        try:
            # Get current data source
            current_data_source = await self.data_source_service.get_data_source_by_id(data_source_id)
            
            # Validate ownership
            if current_data_source.data_source_user_id != user_id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You don't have permission to update this data source"
                )
            
            # Validate this is a file-based data source
            if current_data_source.data_source_type.value not in self.data_source_service.FILE_BASED_TYPES:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="File replacement is only supported for file-based data sources"
                )
            
            # Validate and read new file
            file_content = await new_file.read()
            validate_file(new_file, file_content)
            
            # Extract schema from new file
            try:
                new_schema = await extractor._extract_schema_from_file(
                    current_data_source.data_source_type.value,
                    file_content=file_content
                )
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Failed to extract schema from new file: {str(e)}"
                )
            
            # Generate schema diff
            schema_diff = self._generate_schema_diff(
                old_schema=current_data_source.data_source_schema,
                new_schema=new_schema
            )
            
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
                        "size": len(file_content)
                    },
                    "new_schema": new_schema,
                    "schema_diff": schema_diff
                },
                "requires_approval": True,
                "created_at": datetime.now().isoformat()
            }
            
            # Store staged update with file content
            temp_identifier = f"update_{data_source_id}_{user_id}_{uuid.uuid4().hex}"
            await self.temp_service.store_extraction(
                user_id=user_id,
                data_source_name=f"update_{current_data_source.data_source_name}",
                extraction_result=update_data,
                file_content=file_content,
                expiry_minutes=60
            )
            
            return {
                "temp_identifier": temp_identifier,
                "update_type": "file_replace",
                "data_source_name": current_data_source.data_source_name,
                "new_file_info": {
                    "filename": new_file.filename,
                    "size": len(file_content),
                    "content_type": new_file.content_type
                },
                "changes_summary": {
                    "file_changed": True,
                    "schema_changes": len(schema_diff.get("tables_added", [])) > 0 or 
                                     len(schema_diff.get("tables_removed", [])) > 0 or
                                     len(schema_diff.get("tables_modified", [])) > 0
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
    
    async def get_staged_update(
        self,
        temp_identifier: str,
        user_id: int,
    ) -> Dict[str, Any]:
        """
        Get details of a staged update for review
        """
        try:
            # Retrieve staged update data
            cached_data = await self.temp_service.get_extraction(temp_identifier, user_id)
            
            if not cached_data:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Staged update not found or expired"
                )
            
            update_data = cached_data["extraction_result"]
            
            # Validate this is an update operation
            if update_data.get("update_type") not in ["schema_refresh", "connection_change", "file_replace"]:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid update type"
                )
            
            return {
                "temp_identifier": temp_identifier,
                "update_type": update_data["update_type"],
                "data_source_id": update_data["data_source_id"],
                "current_data": update_data["current_data"],
                "proposed_changes": update_data["proposed_changes"],
                "created_at": update_data["created_at"],
                "expires_at": cached_data.get("expires_at"),
                "has_file": cached_data.get("has_file", False)
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting staged update {temp_identifier}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve staged update"
            )
    
    async def apply_staged_update(
        self,
        data_source_id: int,
        temp_identifier: str,
        updated_llm_description: str,
        user_id: int,
    ) -> DataSource:
        """
        Apply a staged update to the data source
        """
        try:
            # Get staged update data
            cached_data = await self.temp_service.get_extraction(temp_identifier, user_id)
            
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
            
            # Get current data source
            current_data_source = await self.data_source_service.get_data_source_by_id(data_source_id)
            
            # Validate ownership
            if current_data_source.data_source_user_id != user_id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You don't have permission to update this data source"
                )
            
            # Apply update based on type
            update_type = update_data["update_type"]
            
            if update_type == "schema_refresh":
                updated_data_source = await self._apply_schema_refresh(
                    current_data_source, update_data, updated_llm_description
                )
            
            elif update_type == "connection_change":
                updated_data_source = await self._apply_connection_change(
                    current_data_source, update_data, updated_llm_description
                )
            
            elif update_type == "file_replace":
                updated_data_source = await self._apply_file_replace(
                    current_data_source, update_data, updated_llm_description, cached_data
                )
            
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Unsupported update type: {update_type}"
                )
            
            # Clean up staged update
            await self.temp_service.delete_extraction(temp_identifier, user_id)
            
            logger.info(f"Applied staged update {temp_identifier} to data source {data_source_id}")
            return updated_data_source
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error applying staged update {temp_identifier}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to apply staged update"
            )
    
    async def cancel_staged_update(
        self,
        temp_identifier: str,
        user_id: int,
    ) -> bool:
        """
        Cancel a staged update
        """
        try:
            success = await self.temp_service.delete_extraction(temp_identifier, user_id)
            if success:
                logger.info(f"Cancelled staged update {temp_identifier}")
            return success
            
        except Exception as e:
            logger.error(f"Error cancelling staged update {temp_identifier}: {e}")
            return False
    
    # Helper methods  
    async def _extract_fresh_schema(self, data_source: DataSource) -> Dict[str, Any]:
        """Extract fresh schema from data source"""
        data_source_type = data_source.data_source_type.value
        data_source_url = data_source.data_source_url
        
        if data_source_type in self.data_source_service.FILE_BASED_TYPES:
            # Download file from S3 and extract schema
            s3_key = extract_s3_key_from_url(data_source_url)
            file_content = await download_file_from_s3(s3_key)
            return await extractor._extract_schema_from_file(
                data_source_type, 
                file_content=file_content
            )
        
        elif data_source_type in self.data_source_service.DATABASE_TYPES:
            return await extractor._extract_schema_from_database(data_source_type, data_source_url)
        
        else:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail=f"Schema extraction not supported for {data_source_type}"
            )
    
    def _generate_schema_diff(
        self, 
        old_schema: Dict[str, Any], 
        new_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate a detailed diff between two schemas"""
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
                # If old schema is just a string, treat everything as new
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
            
            # Find modified tables
            common_tables = old_table_names & new_table_names
            
            for table_name in common_tables:
                old_table = old_tables[table_name]
                new_table = new_tables[table_name]
                
                # Compare columns
                old_columns = {col["name"]: col for col in old_table.get("columns", [])}
                new_columns = {col["name"]: col for col in new_table.get("columns", [])}
                
                old_col_names = set(old_columns.keys())
                new_col_names = set(new_columns.keys())
                
                cols_added = list(new_col_names - old_col_names)
                cols_removed = list(old_col_names - new_col_names)
                cols_modified = []
                
                # Check for modified columns
                for col_name in (old_col_names & new_col_names):
                    old_col = old_columns[col_name]
                    new_col = new_columns[col_name]
                    
                    # Compare key attributes
                    if (old_col.get("data_type") != new_col.get("data_type") or
                        old_col.get("is_nullable") != new_col.get("is_nullable") or
                        old_col.get("is_primary_key") != new_col.get("is_primary_key")):
                        cols_modified.append({
                            "name": col_name,
                            "old": old_col,
                            "new": new_col
                        })
                
                # If there are column changes, mark table as modified
                if cols_added or cols_removed or cols_modified:
                    diff["tables_modified"].append(table_name)
                    diff["columns_added"][table_name] = cols_added
                    diff["columns_removed"][table_name] = cols_removed
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
    
    async def _apply_schema_refresh(
        self, 
        current_data_source: DataSource, 
        update_data: Dict[str, Any],
        updated_llm_description: str
    ) -> DataSource:
        """Apply schema refresh update"""
        new_schema = update_data["proposed_changes"]["new_schema"]
        
        # Preserve user descriptions
        enhanced_schema = await self._preserve_user_descriptions(
            new_schema=new_schema,
            existing_schema=current_data_source.data_source_schema if isinstance(current_data_source.data_source_schema, dict) else {}
        )
        
        # Add metadata
        if "metadata" not in enhanced_schema:
            enhanced_schema["metadata"] = {}
        
        enhanced_schema["metadata"]["llm_description"] = updated_llm_description
        enhanced_schema["metadata"]["last_refresh"] = datetime.now().isoformat()
        enhanced_schema["metadata"]["refresh_type"] = "user_initiated"
        
        # Update the data source
        update_request = DataSourceUpdateRequest(
            data_source_schema=enhanced_schema["metadata"]["llm_description"]  # For now, just save LLM description
        )
        
        return await self.data_source_service.data_source_repo.update_data_source(
            data_source_id=current_data_source.data_source_id,
            update_data=update_request
        )
    
    async def _apply_connection_change(
        self, 
        current_data_source: DataSource, 
        update_data: Dict[str, Any],
        updated_llm_description: str
    ) -> DataSource:
        """Apply connection change update"""
        new_connection_url = update_data["proposed_changes"]["new_connection_url"]
        new_schema = update_data["proposed_changes"]["new_schema"]
        
        # Preserve user descriptions
        enhanced_schema = await self._preserve_user_descriptions(
            new_schema=new_schema,
            existing_schema=current_data_source.data_source_schema if isinstance(current_data_source.data_source_schema, dict) else {}
        )
        
        # Add metadata
        if "metadata" not in enhanced_schema:
            enhanced_schema["metadata"] = {}
        
        enhanced_schema["metadata"]["llm_description"] = updated_llm_description
        enhanced_schema["metadata"]["connection_updated"] = datetime.now().isoformat()
        enhanced_schema["metadata"]["previous_connection"] = current_data_source.data_source_url
        
        update_request = DataSourceUpdateRequest(
            data_source_url=new_connection_url,
            data_source_schema=enhanced_schema["metadata"]["llm_description"]  # For now, just save LLM description
        )
        
        return await self.data_source_service.data_source_repo.update_data_source(
            data_source_id=current_data_source.data_source_id,
            update_data=update_request
        )
    
    async def _apply_file_replace(
        self, 
        current_data_source: DataSource, 
        update_data: Dict[str, Any],
        updated_llm_description: str,
        cached_data: Dict[str, Any]
    ) -> DataSource:
        """Apply file replacement update"""
        new_schema = update_data["proposed_changes"]["new_schema"]
        file_metadata = update_data["proposed_changes"]["new_file_metadata"]
        
        # Get file content from cache
        file_content = base64.b64decode(cached_data["file_content"])
        
        # Create UploadFile object for S3 upload
        file_obj = UploadFile(
            filename=file_metadata["filename"],
            file=io.BytesIO(file_content),
            content_type=file_metadata["content_type"]
        )
        
        # Upload new file to S3
        new_s3_url = await upload_file_to_s3(
            file=file_obj,
            user_id=current_data_source.data_source_user_id,
            data_source_name=current_data_source.data_source_name
        )
        
        # Preserve user descriptions
        enhanced_schema = await self._preserve_user_descriptions(
            new_schema=new_schema,
            existing_schema=current_data_source.data_source_schema if isinstance(current_data_source.data_source_schema, dict) else {}
        )
        
        # Add metadata
        if "metadata" not in enhanced_schema:
            enhanced_schema["metadata"] = {}
        
        enhanced_schema["metadata"]["llm_description"] = updated_llm_description
        enhanced_schema["metadata"]["file_replaced"] = datetime.now().isoformat()
        enhanced_schema["metadata"]["previous_file_url"] = current_data_source.data_source_url
        enhanced_schema["metadata"]["new_file_metadata"] = file_metadata
        
        update_request = DataSourceUpdateRequest(
            data_source_url=new_s3_url,
            data_source_schema=enhanced_schema["metadata"]["llm_description"]  # For now, just save LLM description
        )
        
        updated_data_source = await self.data_source_service.data_source_repo.update_data_source(
            data_source_id=current_data_source.data_source_id,
            update_data=update_request
        )
        
        # Clean up old S3 file
        try:
            old_s3_key = extract_s3_key_from_url(current_data_source.data_source_url)
            await delete_file_from_s3(old_s3_key)
        except Exception as cleanup_error:
            logger.warning(f"Failed to cleanup old S3 file: {cleanup_error}")
            # Don't fail the update if cleanup fails
        
        return updated_data_source

    async def _preserve_user_descriptions(
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

