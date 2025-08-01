from typing import Optional
import boto3
from botocore.exceptions import ClientError
from .settings import get_settings
from app.core.utils import logger


settings = get_settings()

class DynamoDBConnection:
    """
    Handles DynamoDB connection and health checks only
    No table creation - that's handled by infrastructure
    """
    
    def __init__(self, region_name: Optional[str] = None):
        self.region_name = region_name or settings.REGION
        
        try:
            session = boto3.Session(
                aws_access_key_id=settings.ACCESS_KEY_ID,
                aws_secret_access_key=settings.SECRET_ACCESS_KEY
            )
            self.dynamodb = session.client('dynamodb', region_name=self.region_name)
            self.dynamodb_resource = session.resource('dynamodb', region_name=self.region_name)
            logger.info(f"Connected to DynamoDB in region: {self.region_name}")
            
        except Exception as e:
            logger.error(f"Failed to connect to DynamoDB: {e}")
            raise

    def health_check(self) -> bool:
        """
        Verify that required tables exist and are accessible
        This is a health check, not table creation
        """
        required_tables = [settings.DYNAMODB_CHAT_SESSIONS_TABLE, settings.DYNAMODB_MESSAGES_TABLE]
        
        try:
            for table_name in required_tables:
                response = self.dynamodb.describe_table(TableName=table_name)
                status = response['Table']['TableStatus']
                
                if status != 'ACTIVE':
                    logger.error(f"Table '{table_name}' is not active (status: {status})")
                    return False
                    
                logger.info(f"✓ Table '{table_name}' is healthy")
                
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.error(f"Required table not found. Ensure infrastructure is deployed.")
                return False
            else:
                logger.error(f"Error checking table health: {e}")
                return False
        except Exception as e:
            logger.error(f"Unexpected error during health check: {e}")
            return False
        
        logger.info("All required tables are healthy")
        return True

    def get_table(self, table_name: str):
        """Get a DynamoDB table resource"""
        return self.dynamodb_resource.Table(table_name)


dynamodb_connection: Optional[DynamoDBConnection] = None

def get_dynamodb_connection() -> DynamoDBConnection:
    """Get the global database connection"""
    global dynamodb_connection
    if dynamodb_connection is None:
        dynamodb_connection = DynamoDBConnection()
    return dynamodb_connection

