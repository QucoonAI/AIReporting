from enum import Enum


class DataSourceType(str, Enum):
    CSV = "csv"
    XLSX = "xlsx"
    GOOGLE = "google"
    PDF = "pdf"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    MSSQL = "mssql"
    ORACLE = "oracle"
    MONGODB = "mongodb"

class MessageRole(str, Enum):
    USER = "user"
    ASSISTANT = "assistant"

class ChatStatus(str, Enum):
    ACTIVE = "active"
    ARCHIVED = "archived"
    DELETED = "deleted"