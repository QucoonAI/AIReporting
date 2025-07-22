import re
from datetime import datetime
from typing import Optional, List, Union
from pydantic import BaseModel, Field, field_validator, model_validator, FileUrl, PostgresDsn, MySQLDsn, MongoDsn
from schemas.enum import DataSourceType


class DataSourceUrlValidator:
    """Utility class for validating data source URLs based on type"""
    
    @staticmethod
    def validate_and_convert_url(data_type: 'DataSourceType', url: Union[str, 'FileUrl', 'PostgresDsn', 'MySQLDsn', 'MongoDsn']) -> Union[str, 'FileUrl', 'PostgresDsn', 'MySQLDsn', 'MongoDsn']:
        """Validate and convert URL based on data source type"""
        if not isinstance(url, str):
            return url
        
        if data_type in [DataSourceType.CSV, DataSourceType.XLSX, DataSourceType.GOOGLE, DataSourceType.PDF]:
            try:
                return FileUrl(url)
            except Exception as e:
                raise ValueError(f'Invalid file URL for {data_type.value}: {str(e)}')
        
        elif data_type == DataSourceType.POSTGRES:
            try:
                return PostgresDsn(url)
            except Exception as e:
                raise ValueError(f'Invalid PostgreSQL DSN: {str(e)}')
        
        elif data_type == DataSourceType.MYSQL:
            try:
                return MySQLDsn(url)
            except Exception as e:
                raise ValueError(f'Invalid MySQL DSN: {str(e)}')
        
        elif data_type == DataSourceType.MONGODB:
            try:
                return MongoDsn(url)
            except Exception as e:
                raise ValueError(f'Invalid MongoDB DSN: {str(e)}')
        
        elif data_type == DataSourceType.MSSQL:
            mssql_pattern = r'^(?:mssql|sqlserver):\/\/(?:[^:\/\s]+(?::[^@\/\s]*)?@)?[^:\/\s]+(?::\d+)?(?:\/[^?\s]*)?(?:\?[^#\s]*)?(?:#[^\s]*)?$|^Server=.+;Database=.+;.*$'
            if not re.match(mssql_pattern, url, re.IGNORECASE):
                raise ValueError('Invalid MSSQL connection string format')
            return url
        
        elif data_type == DataSourceType.ORACLE:
            oracle_pattern = r'^(?:oracle:\/\/[^:\/\s]+(?::\d+)?(?:\/[^?\s]*)?(?:\?[^#\s]*)?|[^:\/\s]+:\d+:[^\/\s]+|[^:\/\s]+:\d+\/[^\/\s]+|\([^)]+\))$'
            if not re.match(oracle_pattern, url, re.IGNORECASE):
                raise ValueError('Invalid Oracle connection string format')
            return url
        
        return url

    @staticmethod
    def validate_name(name: Optional[str]) -> Optional[str]:
        """Validate and clean data source name"""
        if name is not None and not name.strip():
            raise ValueError('Data source name cannot be empty')
        return name.strip() if name else name


class DataSourceBase(BaseModel):
    data_source_name: str = Field(..., min_length=1, max_length=255, description="Name of the data source")
    data_source_type: DataSourceType = Field(..., description="Type of the data source")
    data_source_url: Union[str, FileUrl, PostgresDsn, MySQLDsn, MongoDsn] = Field(
        ..., description="URL of the data source"
    )

    @field_validator('data_source_name')
    @classmethod
    def validate_name(cls, v):
        return DataSourceUrlValidator.validate_name(v)

    @model_validator(mode='after')
    def validate_url_based_on_type(self):
        self.data_source_url = DataSourceUrlValidator.validate_and_convert_url(
            self.data_source_type, self.data_source_url
        )
        return self


# Request schemas
class DataSourceCreateRequest(DataSourceBase):
    """Schema for creating a new data source"""
    pass


class DataSourceUpdateRequest(BaseModel):
    """Schema for updating a data source"""
    data_source_name: Optional[str] = Field(None, min_length=1, max_length=255)
    data_source_type: Optional[DataSourceType] = None
    data_source_url: Optional[Union[str, FileUrl, PostgresDsn, MySQLDsn, MongoDsn]] = Field(
        None, description="URL of the data source"
    )

    @field_validator('data_source_name')
    @classmethod
    def validate_name(cls, v):
        return DataSourceUrlValidator.validate_name(v)

    @model_validator(mode='after')
    def validate_url_based_on_type(self):
        # Skip validation if either field is None
        if self.data_source_type is None or self.data_source_url is None:
            return self
        
        self.data_source_url = DataSourceUrlValidator.validate_and_convert_url(
            self.data_source_type, self.data_source_url
        )
        return self


# Response schemas
class DataSourceResponse(DataSourceBase):
    """Schema for data source response"""
    data_source_id: int
    data_source_user_id: int
    data_source_created_at: datetime
    data_source_updated_at: datetime

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

