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
