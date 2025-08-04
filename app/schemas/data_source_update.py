from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
from datetime import datetime
from .data_source import DataSourceResponse

# Request Models
class ConnectionChangeUpdateRequest(BaseModel):
    """Request to initiate a connection change update"""
    new_connection_url: str = Field(..., description="New database connection URL")


class MetadataOnlyUpdateRequest(BaseModel):
    """Request for simple metadata updates that don't require staging"""
    data_source_name: Optional[str] = Field(None, min_length=1, max_length=255)
    llm_description: Optional[str] = Field(None, description="Updated LLM description")


class ApplyUpdateRequest(BaseModel):
    """Request to apply a staged update"""
    updated_llm_description: str = Field(..., description="User's final LLM description")


# Response Models

class UpdateInitiationResponse(BaseModel):
    """Response when an update is initiated and staged"""
    message: str
    temp_identifier: str
    update_type: str
    data_source_name: str
    changes_summary: Dict[str, Any]
    requires_approval: bool


class SchemaChangesSummary(BaseModel):
    """Summary of schema changes"""
    has_changes: bool
    tables_added_count: int
    tables_removed_count: int
    tables_modified_count: int
    total_changes: int


class SchemaDiff(BaseModel):
    """Detailed schema differences"""
    tables_added: List[str]
    tables_removed: List[str]
    tables_modified: List[str]
    columns_added: Dict[str, List[str]]
    columns_removed: Dict[str, List[str]]
    columns_modified: Dict[str, List[Dict[str, Any]]]


class CurrentDataSourceInfo(BaseModel):
    """Current data source information"""
    data_source_id: int
    data_source_name: str
    data_source_type: str
    data_source_url: str
    current_schema: Optional[Dict[str, Any]] = None


class ProposedChanges(BaseModel):
    """Proposed changes for an update"""
    new_schema: Optional[Dict[str, Any]] = None
    new_connection_url: Optional[str] = None
    new_file_metadata: Optional[Dict[str, Any]] = None
    schema_diff: Optional[SchemaDiff] = None
    connection_test_successful: Optional[bool] = None


class StagedUpdateDetails(BaseModel):
    """Detailed information about a staged update"""
    temp_identifier: str
    update_type: str
    data_source_id: int
    current_data: CurrentDataSourceInfo
    proposed_changes: ProposedChanges
    created_at: str
    expires_at: str
    has_file: bool = False


class StagedUpdateResponse(BaseModel):
    """Response for getting staged update details"""
    message: str
    staged_update: StagedUpdateDetails


class UpdateApplicationResponse(BaseModel):
    """Response when an update is successfully applied"""
    message: str
    data_source: 'DataSourceResponse'  # Forward reference to avoid circular import


class UpdateCancellationResponse(BaseModel):
    """Response when an update is cancelled"""
    message: str
    temp_identifier: str


class PendingUpdatesListResponse(BaseModel):
    """Response for listing pending updates"""
    message: str
    pending_updates: List[Dict[str, Any]]
    total_count: int


class SchemaDiffResponse(BaseModel):
    """Response for getting schema diff details"""
    message: str
    temp_identifier: str
    data_source_name: str
    schema_diff: SchemaDiff
    summary: Dict[str, Any]


# Additional utility models

class FileUpdateInfo(BaseModel):
    """Information about a file being replaced"""
    filename: str
    size: int
    content_type: str


class ConnectionTestResult(BaseModel):
    """Result of testing a new connection"""
    successful: bool
    error_message: Optional[str] = None
    schema_extracted: bool = False


class UpdateValidationResult(BaseModel):
    """Result of validating an update"""
    valid: bool
    errors: List[str] = []
    warnings: List[str] = []

