import re
from datetime import datetime
from typing import Optional, List, Union, Dict, Any
from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator,
    FileUrl,
    PostgresDsn,
    MySQLDsn,
    MongoDsn,
)
from .enum import DataSourceType


class DataSourceUrlValidator:
    """Utility class for validating data source URLs based on type"""

    @staticmethod
    def _validate_file_type_match(data_type: "DataSourceType", url: str) -> None:
        """Validate that the URL matches the expected file type"""
        url_lower = url.lower()

        if data_type == DataSourceType.CSV:
            if not url_lower.endswith(".csv"):
                raise ValueError(
                    "URL must point to a CSV file (.csv extension required)"
                )

        elif data_type == DataSourceType.XLSX:
            if not (url_lower.endswith(".xlsx") or url_lower.endswith(".xls")):
                raise ValueError(
                    "URL must point to an Excel file (.xlsx or .xls extension required)"
                )

        elif data_type == DataSourceType.PDF:
            if not url_lower.endswith(".pdf"):
                raise ValueError(
                    "URL must point to a PDF file (.pdf extension required)"
                )

        elif data_type == DataSourceType.GOOGLE:
            # Google Sheets URL patterns
            google_patterns = [
                r"docs\.google\.com/spreadsheets",
                r"drive\.google\.com/file/d/[a-zA-Z0-9-_]+.*",
                r"sheets\.googleapis\.com",
            ]

            if not any(
                re.search(pattern, url, re.IGNORECASE) for pattern in google_patterns
            ):
                raise ValueError(
                    "URL must be a valid Google Sheets link (docs.google.com/spreadsheets, drive.google.com, or sheets.googleapis.com)"
                )

    @staticmethod
    def validate_and_convert_url(
        data_type: "DataSourceType",
        url: Union[str, "FileUrl", "PostgresDsn", "MySQLDsn", "MongoDsn"],
    ) -> Union[str, "FileUrl", "PostgresDsn", "MySQLDsn", "MongoDsn"]:
        """Validate and convert URL based on data source type"""
        if not isinstance(url, str):
            return url

        if data_type in [
            DataSourceType.CSV,
            DataSourceType.XLSX,
            DataSourceType.GOOGLE,
            DataSourceType.PDF,
        ]:
            DataSourceUrlValidator._validate_file_type_match(data_type, url)
            try:
                return FileUrl(url)
            except Exception as e:
                raise ValueError(f"Invalid file URL for {data_type.value}: {str(e)}")

        elif data_type == DataSourceType.POSTGRES:
            try:
                return PostgresDsn(url)
            except Exception as e:
                raise ValueError(f"Invalid PostgreSQL DSN: {str(e)}")

        elif data_type == DataSourceType.MYSQL:
            try:
                return MySQLDsn(url)
            except Exception as e:
                raise ValueError(f"Invalid MySQL DSN: {str(e)}")

        elif data_type == DataSourceType.MONGODB:
            try:
                return MongoDsn(url)
            except Exception as e:
                raise ValueError(f"Invalid MongoDB DSN: {str(e)}")

        elif data_type == DataSourceType.MSSQL:
            mssql_pattern = r"^(?:mssql|sqlserver):\/\/(?:[^:\/\s]+(?::[^@\/\s]*)?@)?[^:\/\s]+(?::\d+)?(?:\/[^?\s]*)?(?:\?[^#\s]*)?(?:#[^\s]*)?$|^Server=.+;Database=.+;.*$"
            if not re.match(mssql_pattern, url, re.IGNORECASE):
                raise ValueError("Invalid MSSQL connection string format")
            return url

        elif data_type == DataSourceType.ORACLE:
            oracle_pattern = r"^(?:oracle:\/\/[^:\/\s]+(?::\d+)?(?:\/[^?\s]*)?(?:\?[^#\s]*)?|[^:\/\s]+:\d+:[^\/\s]+|[^:\/\s]+:\d+\/[^\/\s]+|\([^)]+\))$"
            if not re.match(oracle_pattern, url, re.IGNORECASE):
                raise ValueError("Invalid Oracle connection string format")
            return url

        return url

    @staticmethod
    def validate_name(name: Optional[str]) -> Optional[str]:
        """Validate and clean data source name"""
        if name is not None and not name.strip():
            raise ValueError("Data source name cannot be empty")
        return name.strip() if name else name


class DataSourceBase(BaseModel):
    data_source_name: str = Field(
        ..., min_length=1, max_length=255, description="Name of the data source"
    )
    data_source_type: DataSourceType = Field(..., description="Type of the data source")
    data_source_url: Union[str, FileUrl, PostgresDsn, MySQLDsn, MongoDsn] = Field(
        ..., description="URL of the data source"
    )

    @field_validator("data_source_name")
    @classmethod
    def validate_name(cls, v):
        return DataSourceUrlValidator.validate_name(v)

    @model_validator(mode="after")
    def validate_url_based_on_type(self):
        self.data_source_url = DataSourceUrlValidator.validate_and_convert_url(
            self.data_source_type, self.data_source_url
        )
        return self


# Request schemas
class DataSourceCreateRequest(BaseModel):
    """Schema for creating a new data source"""

    data_source_name: str = Field(
        ..., min_length=1, max_length=255, description="Name of the data source"
    )
    data_source_type: DataSourceType = Field(..., description="Type of the data source")
    data_source_url: Union[str, FileUrl, PostgresDsn, MySQLDsn, MongoDsn] = Field(
        ..., description="URL of the data source"
    )

    @field_validator("data_source_name")
    @classmethod
    def validate_name(cls, v):
        return DataSourceUrlValidator.validate_name(v)

    @model_validator(mode="after")
    def validate_url_based_on_type(self):
        # For file-based types, we'll skip URL validation since it will be replaced with S3 URL
        file_based_types = [DataSourceType.CSV, DataSourceType.XLSX, DataSourceType.PDF]

        if self.data_source_type not in file_based_types:
            self.data_source_url = DataSourceUrlValidator.validate_and_convert_url(
                self.data_source_type, self.data_source_url
            )

        return self


#
class DataSourceUpdateRequest(BaseModel):
    """Schema for updating a data source"""

    data_source_name: Optional[str] = Field(None, min_length=1, max_length=255)
    data_source_type: Optional[DataSourceType] = None
    data_source_url: Optional[Union[str, FileUrl, PostgresDsn, MySQLDsn, MongoDsn]] = (
        Field(None, description="URL of the data source")
    )
    data_source_schema: Optional[Union[str, Dict[str, Any]]] = Field(
        None,
        description="Schema of the data source (can be string description or full schema dict)",
    )

    @field_validator("data_source_name")
    @classmethod
    def validate_name(cls, v):
        return DataSourceUrlValidator.validate_name(v)

    @model_validator(mode="after")
    def validate_url_based_on_type(self):
        # Skip validation if either field is None
        if self.data_source_type is None or self.data_source_url is None:
            return self

        self.data_source_url = DataSourceUrlValidator.validate_and_convert_url(
            self.data_source_type, self.data_source_url
        )
        return self


# Response schemas
class DataSourceResponse(BaseModel):
    """Schema for data source response"""

    data_source_id: int
    data_source_user_id: int
    data_source_name: str = Field(
        ..., min_length=1, max_length=255, description="Name of the data source"
    )
    data_source_type: DataSourceType = Field(..., description="Type of the data source")
    data_source_url: str = Field(..., description="URL of the data source")
    data_source_created_at: datetime
    data_source_updated_at: datetime
    data_source_schema: Optional[Union[str, Dict[str, Any]]] = Field(
        None, description="Schema of the data source"
    )

    class Config:
        from_attributes = True


#
class DataSourceCreateResponse(BaseModel):
    """Response schema for data source creation"""

    message: str
    data_source: DataSourceResponse


#
class DataSourceUpdateResponse(BaseModel):
    """Response schema for data source update"""

    message: str
    data_source: DataSourceResponse


#
class DataSourceDeleteResponse(BaseModel):
    """Response schema for data source deletion"""

    message: str


#
class DataSourceSchemaRefreshResponse(BaseModel):
    """Response schema for data source schema refresh"""

    message: str
    data_source: DataSourceResponse


# class DataSourceListResponse(BaseModel):
#     """Response schema for data source list"""
#     message: str
#     data_sources: List[DataSourceResponse]
#     total_count: int


#
class PaginationMetadata(BaseModel):
    """Pagination metadata"""

    page: int
    per_page: int
    total: int
    total_pages: int
    has_next: bool
    has_prev: bool


#
class DataSourcePaginatedListResponse(BaseModel):
    """Response schema for paginated data source list"""

    message: str
    data_sources: List[DataSourceResponse]
    pagination: PaginationMetadata


class TableSchemaResponse(BaseModel):
    """Schema representation for a single table/sheet"""

    name: str
    columns: List[Dict[str, Any]]
    row_count: Optional[int] = None
    table_type: str = "table"
    description: Optional[str] = None


#
class DataSourceSchemaExtractionResponse(BaseModel):
    """Response model for schema extraction endpoint"""

    message: str
    data_source_name: str
    data_source_type: str
    data_source_url: str
    tables: Dict[str, Any] = Field(
        ..., description="Table schemas for UI display"
    )
    llm_description: str = Field(
        ..., description="LLM-optimized description of the schema"
    )
    file_metadata: Optional[Dict[str, Any]] = Field(
        None, description="Metadata about uploaded file"
    )
    temp_identifier: str = Field(
        ..., description="Temporary identifier for this extraction"
    )

    class Config:
        from_attributes = True


#
class DataSourceCreateWithSchemaRequest(BaseModel):
    """Request model for creating data source with approved schema"""

    llm_description: str = Field(
        ..., description="User's final/edited LLM description"
    )

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "updated_llm_description": "This database contains customer information with tables for users, orders, and products. The users table stores customer demographics, orders table tracks purchase history, and products table contains inventory data."
            }
        }


class PendingExtractionSummary(BaseModel):
    """Summary of a pending extraction"""

    temp_identifier: str
    data_source_name: str
    data_source_type: str
    has_file: bool
    table_count: int
    created_at: str
    expires_at: str
    status: str = "extracted"

    class Config:
        from_attributes = True


#
class PendingExtractionListResponse(BaseModel):
    """Response for listing pending extractions"""

    message: str
    pending_extractions: List[PendingExtractionSummary]
    total_count: int

    class Config:
        from_attributes = True


#
class PendingExtractionResponse(BaseModel):
    """Detailed response for a specific pending extraction"""

    message: str
    temp_identifier: str
    extraction_result: Dict[str, Any]  # Full extraction result
    created_at: str
    expires_at: str
    has_file: bool

    class Config:
        from_attributes = True
