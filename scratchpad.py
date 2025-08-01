from typing import Optional, List, Union, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
from .enum import DataSourceType


class DataSourceResponse(BaseModel):
    """Schema for data source response"""
    data_source_id: int
    data_source_user_id: int
    data_source_name: str = Field(..., min_length=1, max_length=255, description="Name of the data source")
    data_source_type: DataSourceType = Field(..., description="Type of the data source")
    data_source_url: str = Field(..., description="URL of the data source")
    data_source_created_at: datetime
    data_source_updated_at: datetime
    data_source_schema: Optional[Dict[str, Any]] = Field(None, description="Schema of the data source")

    class Config:
        from_attributes = True


class DataSourceSchemaExtractionResponse(BaseModel):
    """Response model for schema extraction endpoint"""
    message: str
    data_source_name: str
    data_source_type: str
    data_source_url: str
    extracted_schema: Dict[str, Any] = Field(..., description="Extracted schema from the data source")
    llm_description: Any = Field(..., description="LLM-optimized description of the schema")
    file_metadata: Optional[Dict[str, Any]] = Field(None, description="Metadata about uploaded file")
    temp_file_identifier: Optional[str] = Field(None, description="Redis-based temporary file identifier")

    class Config:
        from_attributes = True


class DataSourceCreateResponse(BaseModel):
    """Response schema for data source creation"""
    message: str
    data_source: DataSourceResponse


class DataSourceUpdateResponse(BaseModel):
    """Response schema for data source update"""
    message: str
    data_source: DataSourceResponse


class DataSourceDeleteResponse(BaseModel):
    """Response schema for data source deletion"""
    message: str


class DataSourceSchemaRefreshResponse(BaseModel):
    """Response schema for data source schema refresh"""
    message: str
    data_source: DataSourceResponse


class DataSourceListResponse(BaseModel):
    """Response schema for data source list"""
    message: str
    data_sources: List[DataSourceResponse]
    total_count: int


class PaginationMetadata(BaseModel):
    """Pagination metadata"""
    page: int
    per_page: int
    total: int
    total_pages: int
    has_next: bool
    has_prev: bool


class DataSourcePaginatedListResponse(BaseModel):
    """Response schema for paginated data source list"""
    message: str
    data_sources: List[DataSourceResponse]
    pagination: PaginationMetadata


class DataSourceCreateWithSchemaRequest(BaseModel):
    """Request model for creating data source with approved schema"""
    data_source_name: str
    data_source_type: DataSourceType
    data_source_url: str
    final_schema: Dict[str, Any]  # User-approved schema with descriptions
    temp_file_identifier: Optional[str] = None  # Reference to Redis-stored file

    class Config:
        from_attributes = True