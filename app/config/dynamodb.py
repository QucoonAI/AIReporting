import importlib
from pathlib import Path
from typing import List, Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError
from config.settings import get_settings
from core.utils import logger


settings = get_settings()

class DatabaseSetup:
    """
    Handles DynamoDB table setup for FastAPI application
    """
    
    def __init__(self, 
        region_name: Optional[str] = None,
        environment: str = 'dev'
    ):
        """
        Initialize database setup
        
        Args:
            region_name: AWS region (defaults to AWS_REGION env var or us-east-1)
            profile_name: AWS profile (defaults to AWS_PROFILE env var)
            environment: Environment name for tagging
        """
        self.region_name = region_name or settings.AWS_REGION
        self.environment = environment
        
        # Initialize DynamoDB client
        try:
            session = boto3.Session(aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY, aws_account_id=settings.AWS_ACCOUNT_ID)
            self.dynamodb = session.client('dynamodb', region_name=self.region_name)
            logger.info(f"Connected to DynamoDB in region: {self.region_name}")
            
        except Exception as e:
            logger.error(f"Failed to connect to DynamoDB: {e}")
            raise

    def discover_table_definitions(self, tables_dir: str = 'tables') -> List[Dict[str, Any]]:
        """
        Automatically discover and load table definitions from the tables directory
        
        Args:
            tables_dir: Directory containing table definition files
            
        Returns:
            List of table definitions
        """
        table_definitions = []
        tables_path = Path(tables_dir)
        
        if not tables_path.exists():
            logger.warning(f"Tables directory '{tables_dir}' not found")
            return table_definitions
        
        # Find all Python files in tables directory
        for file_path in tables_path.glob('*.py'):
            if file_path.name.startswith('__'):
                continue
                
            module_name = f"{tables_dir}.{file_path.stem}"
            
            try:
                # Import the module
                module = importlib.import_module(module_name)
                
                # Try to get table definition using function
                if hasattr(module, 'get_table_definition'):
                    table_def = module.get_table_definition(self.environment)
                    table_definitions.append(table_def)
                    logger.info(f"Loaded table definition from {module_name}")
                
                else:
                    logger.warning(f"No table definition found in {module_name}")
                    
            except Exception as e:
                logger.error(f"Failed to load table definition from {module_name}: {e}")
        
        return table_definitions

    def get_specific_table_definitions(self, table_modules: List[str]) -> List[Dict[str, Any]]:
        """
        Get table definitions from specific modules
        
        Args:
            table_modules: List of module names (e.g., ['tables.chat_sessions', 'tables.messages'])
            
        Returns:
            List of table definitions
        """
        table_definitions = []
        
        for module_name in table_modules:
            try:
                module = importlib.import_module(module_name)
                
                if hasattr(module, 'get_table_definition'):
                    table_def = module.get_table_definition(self.environment)
                    table_definitions.append(table_def)
                    logger.info(f"Loaded table definition from {module_name}")
                else:
                    logger.warning(f"No get_table_definition function in {module_name}")
                    
            except Exception as e:
                logger.error(f"Failed to load {module_name}: {e}")
        
        return table_definitions

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists"""
        try:
            self.dynamodb.describe_table(TableName=table_name)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return False
            raise

    def create_table(self, table_definition: Dict[str, Any]) -> bool:
        """
        Create a single DynamoDB table
        
        Args:
            table_definition: Table definition dictionary
            
        Returns:
            bool: True if successful, False otherwise
        """
        table_name = table_definition['TableName']
        
        try:
            if self.table_exists(table_name):
                logger.info(f"Table '{table_name}' already exists")
                return True
            
            logger.info(f"Creating table '{table_name}'...")
            self.dynamodb.create_table(**table_definition)
            
            # Wait for table to become active
            logger.info(f"Waiting for table '{table_name}' to become active...")
            waiter = self.dynamodb.get_waiter('table_exists')
            waiter.wait(
                TableName=table_name,
                WaiterConfig={'Delay': 2, 'MaxAttempts': 30}
            )
            
            logger.info(f"Table '{table_name}' created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table '{table_name}': {e}")
            return False

    def create_all_tables(self, table_definitions: Optional[List[Dict[str, Any]]] = None) -> bool:
        """
        Create all tables for the application
        
        Args:
            table_definitions: Optional list of table definitions. 
                             If None, will auto-discover from tables directory
            
        Returns:
            bool: True if all tables created successfully
        """
        if table_definitions is None:
            table_definitions = self.discover_table_definitions()
        
        if not table_definitions:
            logger.warning("No table definitions found")
            return True
        
        logger.info(f"Creating {len(table_definitions)} tables...")
        
        success_count = 0
        for table_def in table_definitions:
            if self.create_table(table_def):
                success_count += 1
        
        total_tables = len(table_definitions)
        if success_count == total_tables:
            logger.info(f"All {total_tables} tables created successfully")
            return True
        else:
            logger.error(f"Only {success_count}/{total_tables} tables created successfully")
            return False

    def verify_tables(self, expected_tables: Optional[List[str]] = None) -> bool:
        """
        Verify that expected tables exist and are active
        
        Args:
            expected_tables: List of expected table names. If None, checks discovered tables
            
        Returns:
            bool: True if all expected tables exist and are active
        """
        if expected_tables is None:
            # Get table names from discovered definitions
            table_definitions = self.discover_table_definitions()
            expected_tables = [table_def['TableName'] for table_def in table_definitions]
        
        if not expected_tables:
            logger.info("No tables to verify")
            return True
        
        logger.info(f"Verifying {len(expected_tables)} tables...")
        
        for table_name in expected_tables:
            try:
                response = self.dynamodb.describe_table(TableName=table_name)
                status = response['Table']['TableStatus']
                
                if status == 'ACTIVE':
                    logger.info(f"✓ Table '{table_name}' is active")
                else:
                    logger.warning(f"⚠ Table '{table_name}' status: {status}")
                    return False
                    
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    logger.error(f"✗ Table '{table_name}' does not exist")
                    return False
                else:
                    logger.error(f"✗ Error checking table '{table_name}': {e}")
                    return False
            except Exception as e:
                logger.error(f"✗ Unexpected error checking table '{table_name}': {e}")
                return False
        
        logger.info("All tables verified successfully")
        return True

    def _update_environment_tags(self, table_definition: Dict[str, Any]):
        """Update environment placeholder in table tags"""
        if 'Tags' in table_definition:
            for tag in table_definition['Tags']:
                if tag['Value'] == '${environment}':
                    tag['Value'] = self.environment


# Convenience functions for FastAPI lifespan
def initialize_database() -> bool:
    """
    Initialize database tables for FastAPI application
    
    Args:
        environment: Environment name
        
    Returns:
        bool: True if successful
    """
    try:
        db_setup = DatabaseSetup()
        return db_setup.create_all_tables()
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        return False


def verify_database() -> bool:
    """
    Verify database tables exist and are ready
    
    Args:
        environment: Environment name
        
    Returns:
        bool: True if all tables are ready
    """
    try:
        db_setup = DatabaseSetup()
        return db_setup.verify_tables()
    except Exception as e:
        logger.error(f"Database verification failed: {e}")
        return False

