from typing import List
from . import BaseSchemaExtractor
# from .csv import CSVSchemaExtractor
# from .xlsx import XLSXSchemaExtractor
from .postgres import PostgresSchemaExtractor
from .mysql import MySQLSchemaExtractor
from .mssql import MSSQLSchemaExtractor
from .mariadb import MariaDBSchemaExtractor
from .oracle import OracleSchemaExtractor


class SchemaExtractorFactory:
    """Factory for creating appropriate schema extractors"""
    
    _extractors = {
        # 'csv': CSVSchemaExtractor,
        # 'xlsx': XLSXSchemaExtractor,
        'postgres': PostgresSchemaExtractor,
        'mysql': MySQLSchemaExtractor,
        'mssql': MSSQLSchemaExtractor,
        'mariadb': MariaDBSchemaExtractor,
        'oracle': OracleSchemaExtractor,
    }
    
    @classmethod
    def get_extractor(cls, source_type: str) -> BaseSchemaExtractor:
        """Get appropriate extractor for source type"""
        extractor_class = cls._extractors.get(source_type)
        if not extractor_class:
            raise ValueError(f"No extractor available for source type: {source_type}")
        
        if isinstance(extractor_class, str):
            raise NotImplementedError(f"Extractor {extractor_class} not yet implemented")
        
        return extractor_class()
    
    @classmethod
    def get_supported_types(cls) -> List[str]:
        """Get list of supported data source types"""
        return list(cls._extractors.keys())
    
    @classmethod
    def is_supported(cls, source_type: str) -> bool:
        """Check if a data source type is supported"""
        return source_type in cls._extractors
    
    @classmethod
    def register_extractor(cls, source_type: str, extractor_class: type):
        """Register a new extractor for a data source type"""
        if not issubclass(extractor_class, BaseSchemaExtractor):
            raise ValueError("Extractor must inherit from BaseSchemaExtractor")
        cls._extractors[source_type] = extractor_class

