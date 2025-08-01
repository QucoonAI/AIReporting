from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
from enum import Enum
import json

class DataType(Enum):
    """Standardized data types across all sources"""
    INTEGER = "integer"
    DECIMAL = "decimal"
    TEXT = "text"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    TIME = "time"
    JSON = "json"
    BINARY = "binary"
    UNKNOWN = "unknown"
    
    # Special semantic types for LLM understanding
    EMAIL = "email"
    PHONE = "phone"
    URL = "url"
    CURRENCY = "currency"
    PERCENTAGE = "percentage"
    CATEGORICAL = "categorical"
    IDENTIFIER = "identifier"  # IDs, keys, etc.


@dataclass
class ColumnSchema:
    """Standardized column representation across all data sources"""
    name: str
    data_type: DataType
    is_nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_unique: bool = False
    
    # Statistical information (for LLM context)
    sample_values: Optional[List[str]] = None
    value_count: Optional[int] = None
    null_count: Optional[int] = None
    unique_count: Optional[int] = None
    
    # Numeric statistics
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    avg_value: Optional[float] = None
    
    # Text statistics
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None
    
    # Relationships
    references_table: Optional[str] = None
    references_column: Optional[str] = None
    
    # Metadata
    description: Optional[str] = None
    constraints: Optional[List[str]] = None
    original_type: Optional[str] = None  # Store original DB/file type
    
    def to_llm_description(self) -> str:
        """Generate LLM-friendly description of this column"""
        desc_parts = [f"Column '{self.name}' ({self.data_type.value})"]
        
        if self.is_primary_key:
            desc_parts.append("PRIMARY KEY")
        if self.is_foreign_key:
            desc_parts.append(f"FOREIGN KEY -> {self.references_table}.{self.references_column}")
        if self.is_unique:
            desc_parts.append("UNIQUE")
        if not self.is_nullable:
            desc_parts.append("NOT NULL")
            
        if self.sample_values:
            desc_parts.append(f"Sample values: {', '.join(self.sample_values[:3])}")
            
        if self.value_count:
            desc_parts.append(f"{self.value_count} total values")
            if self.null_count:
                desc_parts.append(f"{self.null_count} nulls")
                
        return " | ".join(desc_parts)


@dataclass
class TableSchema:
    """Standardized table/sheet representation"""
    name: str
    columns: List[ColumnSchema]
    row_count: Optional[int] = None
    
    # Relationships
    primary_keys: Optional[List[str]] = None
    foreign_keys: Optional[List[Dict[str, str]]] = None
    indexes: Optional[List[Dict[str, Any]]] = None
    
    # Metadata
    description: Optional[str] = None
    table_type: str = "table"  # table, view, sheet, etc.
    
    def get_column(self, name: str) -> Optional[ColumnSchema]:
        """Get column by name"""
        return next((col for col in self.columns if col.name == name), None)
    
    def to_llm_summary(self) -> str:
        """Generate LLM-friendly summary of this table"""
        summary = f"Table '{self.name}' with {len(self.columns)} columns"
        if self.row_count:
            summary += f" and ~{self.row_count} rows"
            
        key_cols = [col.name for col in self.columns if col.is_primary_key or col.is_unique]
        if key_cols:
            summary += f". Key columns: {', '.join(key_cols)}"
            
        return summary


@dataclass
class DataSourceSchema:
    """Unified schema representation for all data source types"""
    source_name: str
    source_type: str  # csv, xlsx, postgres, etc.
    tables: List[TableSchema]
    
    # Global metadata
    total_tables: int = 0
    total_columns: int = 0
    total_rows: Optional[int] = None
    
    # Source-specific metadata
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        self.total_tables = len(self.tables)
        self.total_columns = sum(len(table.columns) for table in self.tables)
        self.total_rows = sum(table.row_count or 0 for table in self.tables if table.row_count)
    
    def get_table(self, name: str) -> Optional[TableSchema]:
        """Get table by name"""
        return next((table for table in self.tables if table.name == name), None)
    
    def to_llm_prompt(self) -> str:
        """Generate comprehensive LLM prompt describing the data structure"""
        prompt_parts = [
            f"Data Source: {self.source_name} ({self.source_type})",
            f"Contains {self.total_tables} tables with {self.total_columns} total columns"
        ]
        
        if self.total_rows:
            prompt_parts.append(f"and approximately {self.total_rows} total rows")
            
        prompt_parts.append("\n\nTABLE STRUCTURES:")
        
        for table in self.tables:
            prompt_parts.append(f"\n{table.to_llm_summary()}")
            prompt_parts.append("Columns:")
            
            for col in table.columns:
                prompt_parts.append(f"  - {col.to_llm_description()}")
                
        return "\n".join(prompt_parts)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "source_name": self.source_name,
            "source_type": self.source_type,
            "total_tables": self.total_tables,
            "total_columns": self.total_columns,
            "total_rows": self.total_rows,
            "tables": [asdict(table) for table in self.tables],
            "metadata": self.metadata
        }
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), indent=2, default=str)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataSourceSchema':
        """Create DataSourceSchema from dictionary"""
        # Convert table dictionaries back to TableSchema objects
        tables = []
        for table_data in data.get('tables', []):
            # Convert column dictionaries back to ColumnSchema objects
            columns = []
            for col_data in table_data.get('columns', []):
                # Convert data_type string back to enum
                data_type = DataType(col_data['data_type']) if isinstance(col_data['data_type'], str) else col_data['data_type']
                
                column = ColumnSchema(
                    name=col_data['name'],
                    data_type=data_type,
                    is_nullable=col_data.get('is_nullable', True),
                    is_primary_key=col_data.get('is_primary_key', False),
                    is_foreign_key=col_data.get('is_foreign_key', False),
                    is_unique=col_data.get('is_unique', False),
                    sample_values=col_data.get('sample_values'),
                    value_count=col_data.get('value_count'),
                    null_count=col_data.get('null_count'),
                    unique_count=col_data.get('unique_count'),
                    min_value=col_data.get('min_value'),
                    max_value=col_data.get('max_value'),
                    avg_value=col_data.get('avg_value'),
                    min_length=col_data.get('min_length'),
                    max_length=col_data.get('max_length'),
                    avg_length=col_data.get('avg_length'),
                    references_table=col_data.get('references_table'),
                    references_column=col_data.get('references_column'),
                    description=col_data.get('description'),
                    constraints=col_data.get('constraints'),
                    original_type=col_data.get('original_type')
                )
                columns.append(column)
            
            table = TableSchema(
                name=table_data['name'],
                columns=columns,
                row_count=table_data.get('row_count'),
                primary_keys=table_data.get('primary_keys'),
                foreign_keys=table_data.get('foreign_keys'),
                indexes=table_data.get('indexes'),
                description=table_data.get('description'),
                table_type=table_data.get('table_type', 'table')
            )
            tables.append(table)
        
        return cls(
            source_name=data['source_name'],
            source_type=data['source_type'],
            tables=tables,
            total_tables=data.get('total_tables', len(tables)),
            total_columns=data.get('total_columns', sum(len(t.columns) for t in tables)),
            total_rows=data.get('total_rows'),
            metadata=data.get('metadata')
        )


class BaseSchemaExtractor(ABC):
    """Base class for all schema extractors"""
    
    @abstractmethod
    async def extract_schema(self, source: Any, **kwargs) -> DataSourceSchema:
        """Extract schema from data source"""
        pass
    
    @abstractmethod
    def get_source_type(self) -> str:
        """Return the source type this extractor handles"""
        pass
    
    def _infer_semantic_type(self, column_name: str, data_type: DataType, sample_values: List[str]) -> DataType:
        """Infer semantic data type from column name and sample values"""
        # Convert to lowercase for matching
        col_name_lower = column_name.lower()
        
        # Check column name patterns
        if any(pattern in col_name_lower for pattern in ['email', 'mail']):
            return DataType.EMAIL
        elif any(pattern in col_name_lower for pattern in ['phone', 'tel', 'mobile']):
            return DataType.PHONE
        elif any(pattern in col_name_lower for pattern in ['url', 'link', 'website']):
            return DataType.URL
        elif any(pattern in col_name_lower for pattern in ['id', 'key', 'uuid']):
            return DataType.IDENTIFIER
        elif any(pattern in col_name_lower for pattern in ['price', 'cost', 'amount', 'salary']):
            return DataType.CURRENCY
        elif 'percent' in col_name_lower or col_name_lower.endswith('_pct'):
            return DataType.PERCENTAGE
        
        # Check sample values if available
        if sample_values and len(sample_values) > 0:
            # Email pattern
            import re
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            if all(re.match(email_pattern, str(val)) for val in sample_values[:5] if val):
                return DataType.EMAIL
            
            # Phone pattern
            phone_pattern = r'^[\+]?[1-9]?[\d\s\-\(\)]{10,}$'
            if all(re.match(phone_pattern, str(val)) for val in sample_values[:5] if val):
                return DataType.PHONE
            
            # URL pattern
            url_pattern = r'^https?://[^\s/$.?#].[^\s]*$'
            if all(re.match(url_pattern, str(val)) for val in sample_values[:5] if val):
                return DataType.URL
        
        return data_type

