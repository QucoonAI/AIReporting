from models.chat_session import ChatSessionModel
from models.message import MessageModel
from .settings import get_settings


settings = get_settings()

def initialize_dynamodb_tables():
    """Initialize DynamoDB tables if they don't exist"""
    
    # Set AWS region (configure as needed)
    region = settings.AWS_REGION
    
    # Create tables if they don't exist
    if not ChatSessionModel.exists():
        ChatSessionModel.create_table(
            read_capacity_units=5,
            write_capacity_units=5,
            wait=True
        )
        print("Created ChatSessions table")
    
    if not MessageModel.exists():
        MessageModel.create_table(
            read_capacity_units=5,
            write_capacity_units=5,
            wait=True
        )
        print("Created Messages table")


