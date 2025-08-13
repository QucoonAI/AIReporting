import uuid
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from app.core.utils import logger
from app.config.dynamodb import get_dynamodb_connection
from app.config.settings import get_settings


settings = get_settings()

class ChatRepository:
    """Repository class for handling Chat DynamoDB operations."""
    
    def __init__(self):
        """Initialize the chat repository with DynamoDB connection."""
        self.db_connection = get_dynamodb_connection()
        self.chat_sessions_table = self.db_connection.get_table(settings.DYNAMODB_CHAT_SESSIONS_TABLE)

    async def create_chat_session(
        self,
        user_id: int,
        data_source_id: int,
        title: str,
    ) -> Dict[str, Any]:
        """
        Create a new chat session.
        
        Args:
            user_id: ID of the user creating the session
            data_source_id: ID of the associated data source
            title: Session title
            
        Returns:
            Created chat session dict
            
        Raises:
            Exception: If session creation fails
        """
        try:
            session_id = str(uuid.uuid4())
            now = datetime.now(timezone.utc).isoformat()
            
            item = {
                'pk': f"USER#{user_id}",
                'sk': f"SESSION#{session_id}",
                'gsi1_pk': f"DATASOURCE#{data_source_id}",
                'gsi1_sk': f"SESSION#{session_id}",
                'session_id': session_id,
                'user_id': user_id,
                'data_source_id': data_source_id,
                'title': title,
                'created_at': now,
                'updated_at': now
            }
            
            self.chat_sessions_table.put_item(Item=item)
            
            logger.info(f"Chat session created successfully: {session_id}")
            return item
            
        except ClientError as e:
            logger.error(f"Error creating chat session: {e}")
            raise Exception(f"Failed to create chat session: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error creating chat session: {e}")
            raise
    
    async def get_chat_session(self, user_id: int, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a chat session by user ID and session ID.
        
        Args:
            user_id: ID of the user
            session_id: ID of the chat session
            
        Returns:
            Chat session dict if found, None otherwise
        """
        try:
            response = self.chat_sessions_table.get_item(
                Key={
                    'pk': f"USER#{user_id}",
                    'sk': f"SESSION#{session_id}"
                }
            )
            
            return response.get('Item')
            
        except ClientError as e:
            logger.error(f"Error getting chat session {session_id}: {e}")
            raise
    
    async def update_chat_session(
        self,
        user_id: int,
        session_id: str,
        **updates
    ) -> Optional[Dict[str, Any]]:
        """
        Update a chat session.
        
        Args:
            user_id: ID of the user
            session_id: ID of the chat session
            **updates: Fields to update
            
        Returns:
            Updated chat session dict if successful, None if not found
        """
        try:
            # Build update expression dynamically
            update_expression = "SET updated_at = :updated_at"
            expression_values = {':updated_at': datetime.now(timezone.utc).isoformat()}
            
            if 'title' in updates:
                update_expression += ", title = :title"
                expression_values[':title'] = updates['title']
            
            response = self.chat_sessions_table.update_item(
                Key={
                    'pk': f"USER#{user_id}",
                    'sk': f"SESSION#{session_id}"
                },
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values,
                ReturnValues='ALL_NEW'
            )
            
            updated_session = response.get('Attributes')
            if updated_session:
                logger.info(f"Chat session updated successfully: {session_id}")
            
            return updated_session
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                return None
            logger.error(f"Error updating chat session {session_id}: {e}")
            raise Exception(f"Failed to update chat session: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error updating chat session {session_id}: {e}")
            raise
    
    async def delete_chat_session(self, user_id: int, session_id: str) -> bool:
        """
        Delete a chat session.
        
        Args:
            user_id: ID of the user
            session_id: ID of the chat session
            
        Returns:
            True if deleted successfully, False if not found
        """
        try:
            response = self.chat_sessions_table.delete_item(
                Key={
                    'pk': f"USER#{user_id}",
                    'sk': f"SESSION#{session_id}"
                },
                ReturnValues='ALL_OLD'
            )
            
            deleted = 'Attributes' in response
            if deleted:
                logger.info(f"Chat session deleted successfully: {session_id}")
            
            return deleted
            
        except ClientError as e:
            logger.error(f"Error deleting chat session {session_id}: {e}")
            raise Exception(f"Failed to delete chat session: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error deleting chat session {session_id}: {e}")
            raise
    
    async def get_user_chat_sessions(
        self,
        user_id: int,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get all chat sessions for a user.
        
        Args:
            user_id: ID of the user
            limit: Maximum number of sessions to return
            
        Returns:
            List of chat session dicts
        """
        try:
            response = self.chat_sessions_table.query(
                KeyConditionExpression=Key('pk').eq(f"USER#{user_id}") & Key('sk').begins_with('SESSION#'),
                Limit=limit,
                ScanIndexForward=False  # Most recent first
            )
            
            return response.get('Items', [])
            
        except ClientError as e:
            logger.error(f"Error getting user chat sessions for user {user_id}: {e}")
            raise
    
    async def get_user_chat_sessions_paginated(
        self,
        user_id: int,
        limit: int = 10,
        last_evaluated_key: Optional[Dict[str, Any]] = None
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        Get paginated chat sessions for a user.
        
        Args:
            user_id: ID of the user
            limit: Number of sessions per page
            last_evaluated_key: Key for pagination
            
        Returns:
            Tuple of (sessions_list, next_page_key)
        """
        try:
            query_params = {
                'KeyConditionExpression': Key('pk').eq(f"USER#{user_id}") & Key('sk').begins_with('SESSION#'),
                'Limit': limit,
                'ScanIndexForward': False
            }
            
            if last_evaluated_key:
                query_params['ExclusiveStartKey'] = last_evaluated_key
            
            response = self.chat_sessions_table.query(**query_params)
            
            sessions = response.get('Items', [])
            next_key = response.get('LastEvaluatedKey')
            
            return sessions, next_key
            
        except ClientError as e:
            logger.error(f"Error getting paginated chat sessions for user {user_id}: {e}")
            raise

    async def get_data_source_sessions(
        self,
        data_source_id: int,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get all chat sessions for a specific data source using GSI1.
        
        Args:
            data_source_id: ID of the data source
            limit: Maximum number of sessions to return
            
        Returns:
            List of chat session dicts
        """
        try:
            response = self.chat_sessions_table.query(
                IndexName='GSI1-DataSource-Sessions',
                KeyConditionExpression=Key('gsi1_pk').eq(f"DATASOURCE#{data_source_id}") & Key('gsi1_sk').begins_with('SESSION#'),
                Limit=limit,
                ScanIndexForward=False  # Most recent first
            )
            
            return response.get('Items', [])
            
        except ClientError as e:
            logger.error(f"Error getting data source sessions for data source {data_source_id}: {e}")
            raise

    async def get_data_source_id_by_session_id(
        self,
        session_id: str
    ) -> Optional[int]: 
        """
        Get the data source ID associated with a chat session.
        
        Args:
            session_id: ID of the chat session
            
        Returns:
            Data source ID if found, None otherwise
        """
        try:
            response = self.dynamodb.get_item(
                TableName=self.chat_table_name,
                Key={
                    'sk': {'S': f"SESSION#{session_id}"}
                },
                ProjectionExpression='data_source_id'
            )
            
            if 'Item' not in response:
                return None
                
            return int(response['Item']['data_source_id']['N'])
            
        except ClientError as e:
            logger.error(f"Error getting data source ID for session {session_id}: {e}")
            raise

