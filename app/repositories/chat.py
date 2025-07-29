import uuid
import boto3
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from core.utils import logger


class ChatRepository:
    """Repository class for handling Chat and Message DynamoDB operations."""
    
    def __init__(self, dynamodb_client=None):
        self.dynamodb = dynamodb_client or boto3.client('dynamodb')
        self.chat_table_name = 'ChatSessions'

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
            max_tokens: Maximum tokens allowed for this session
            
        Returns:
            Created chat session dict
            
        Raises:
            Exception: If session creation fails
        """
        try:
            session_id = str(uuid.uuid4())
            now = datetime.now(timezone.utc).isoformat()
            
            item = {
                'pk': {'S': f"USER#{user_id}"},
                'sk': {'S': f"SESSION#{session_id}"},
                'gsi1_pk': {'S': f"DATASOURCE#{data_source_id}"},
                'gsi1_sk': {'S': f"SESSION#{session_id}"},
                'session_id': {'S': session_id},
                'user_id': {'N': str(user_id)},
                'data_source_id': {'N': str(data_source_id)},
                'title': {'S': title},
                'created_at': {'S': now},
                'updated_at': {'S': now}
            }
            
            self.dynamodb.put_item(
                TableName=self.chat_table_name,
                Item=item
            )
            
            # Convert back to dict for return
            session = self._deserialize_chat_session(item)
            logger.info(f"Chat session created successfully: {session_id}")
            return session
            
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
            response = self.dynamodb.get_item(
                TableName=self.chat_table_name,
                Key={
                    'pk': {'S': f"USER#{user_id}"},
                    'sk': {'S': f"SESSION#{session_id}"}
                }
            )
            
            if 'Item' not in response:
                return None
                
            return self._deserialize_chat_session(response['Item'])
            
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
            # Build update expression
            update_expression = "SET updated_at = :updated_at"
            expression_values = {':updated_at': {'S': datetime.utcnow().isoformat()}}
            
            if 'title' in updates:
                update_expression += ", title = :title"
                expression_values[':title'] = {'S': updates['title']}
            if 'message_count' in updates:
                update_expression += ", message_count = :message_count"
                expression_values[':message_count'] = {'N': str(updates['message_count'])}
            if 'total_tokens_all_branches' in updates:
                update_expression += ", total_tokens_all_branches = :total_tokens"
                expression_values[':total_tokens'] = {'N': str(updates['total_tokens_all_branches'])}
            if 'active_branch_tokens' in updates:
                update_expression += ", active_branch_tokens = :active_tokens"
                expression_values[':active_tokens'] = {'N': str(updates['active_branch_tokens'])}
            
            response = self.dynamodb.update_item(
                TableName=self.chat_table_name,
                Key={
                    'pk': {'S': f"USER#{user_id}"},
                    'sk': {'S': f"SESSION#{session_id}"}
                },
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values,
                ReturnValues='ALL_NEW'
            )
            
            if 'Attributes' not in response:
                return None
                
            updated_session = self._deserialize_chat_session(response['Attributes'])
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
        Delete a chat session and all its messages.
        
        Args:
            user_id: ID of the user
            session_id: ID of the chat session
            
        Returns:
            True if deleted successfully, False if not found
        """
        try:
            # First delete all messages in the session
            await self.delete_all_session_messages(session_id)
            
            # Then delete the session
            response = self.dynamodb.delete_item(
                TableName=self.chat_table_name,
                Key={
                    'pk': {'S': f"USER#{user_id}"},
                    'sk': {'S': f"SESSION#{session_id}"}
                },
                ReturnValues='ALL_OLD'
            )
            
            if 'Attributes' not in response:
                return False
            
            logger.info(f"Chat session deleted successfully: {session_id}")
            return True
            
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
            response = self.dynamodb.query(
                TableName=self.chat_table_name,
                KeyConditionExpression='pk = :pk AND begins_with(sk, :sk_prefix)',
                ExpressionAttributeValues={
                    ':pk': {'S': f"USER#{user_id}"},
                    ':sk_prefix': {'S': 'SESSION#'}
                },
                Limit=limit,
                ScanIndexForward=False  # Most recent first
            )
            
            sessions = []
            for item in response.get('Items', []):
                sessions.append(self._deserialize_chat_session(item))
            
            return sessions
            
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
                'TableName': self.chat_table_name,
                'KeyConditionExpression': 'pk = :pk AND begins_with(sk, :sk_prefix)',
                'ExpressionAttributeValues': {
                    ':pk': {'S': f"USER#{user_id}"},
                    ':sk_prefix': {'S': 'SESSION#'}
                },
                'Limit': limit,
                'ScanIndexForward': False
            }
            
            if last_evaluated_key:
                query_params['ExclusiveStartKey'] = last_evaluated_key
            
            response = self.dynamodb.query(**query_params)
            
            sessions = []
            for item in response.get('Items', []):
                sessions.append(self._deserialize_chat_session(item))
            
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
            response = self.dynamodb.query(
                TableName=self.chat_table_name,
                IndexName='GSI1-DataSource-Sessions',
                KeyConditionExpression='gsi1_pk = :gsi1_pk AND begins_with(gsi1_sk, :gsi1_sk_prefix)',
                ExpressionAttributeValues={
                    ':gsi1_pk': {'S': f"DATASOURCE#{data_source_id}"},
                    ':gsi1_sk_prefix': {'S': 'SESSION#'}
                },
                Limit=limit,
                ScanIndexForward=False  # Most recent first
            )
            
            sessions = []
            for item in response.get('Items', []):
                sessions.append(self._deserialize_chat_session(item))
            
            return sessions
            
        except ClientError as e:
            logger.error(f"Error getting data source sessions for data source {data_source_id}: {e}")
            raise

    # ===============================
    # Helper Methods
    # ===============================
    
    def _deserialize_chat_session(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Convert DynamoDB item to chat session dict."""
        return {
            'session_id': item['session_id']['S'],
            'user_id': int(item['user_id']['N']),
            'data_source_id': int(item['data_source_id']['N']),
            'title': item['title']['S'],
            'message_count': int(item['message_count']['N']),
            'total_tokens_all_branches': int(item['total_tokens_all_branches']['N']),
            'active_branch_tokens': int(item['active_branch_tokens']['N']),
            'max_tokens': int(item['max_tokens']['N']),
            'created_at': item['created_at']['S'],
            'updated_at': item['updated_at']['S']
        }


