from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # App settings
    SECRET_KEY: str
    DATABASE_URL: str
    REDIS_URL: str
    SENDGRID_AUTH_KEY: str
    
    # AWS settings
    REGION: str
    ACCESS_KEY_ID: str
    SECRET_ACCESS_KEY: str
    AWS_ACCOUNT_ID: str
    DYNAMODB_CHAT_SESSIONS_TABLE: str
    DYNAMODB_MESSAGES_TABLE: str
    S3_BUCKET_NAME: str
    S3_PROFILE_AVATAR_BUCKET: str

    # Chat settings
    DEFAULT_MAX_TOKENS: int

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache()
def get_settings() -> Settings:
    return Settings()
