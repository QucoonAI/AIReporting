import uuid
import boto3
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from schemas.chat import MessageRole
from core.utils import logger


class MessageRepository:
    """Repository class for handling Message DynamoDB operations."""

    def __init__(self, dynamodb_client=None):
        self.dynamodb = dynamodb_client or boto3.client('dynamodb')
        self.message_table_name = 'Messages'
    
    async def create_message(
        self,
        session_id: str,
        user_id: int,
        role: MessageRole,
        content: str,
        token_count: int,
        message_index: int,
        parent_message_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new message in a chat session.
        
        Args:
            session_id: ID of the chat session
            user_id: ID of the user
            role: Message role (user or assistant)
            content: Message content
            token_count: Number of tokens in the message
            message_index: Index of the message in the session
            parent_message_id: ID of the parent message (for branching)
            
        Returns:
            Created message dict
        """
        try:
            message_id = str(uuid.uuid4())
            now = datetime.now(timezone.utc).isoformat()
            
            item = {
                'pk': {'S': f"SESSION#{session_id}"},
                'sk': {'S': f"MSG#{message_index:06d}#{message_id}"},
                'gsi1_pk': {'S': f"USER#{user_id}"},
                'gsi1_sk': {'S': f"MSG#{now}#{message_id}"},
                'message_id': {'S': message_id},
                'session_id': {'S': session_id},
                'user_id': {'N': str(user_id)},
                'role': {'S': role.value},
                'content': {'S': content},
                'token_count': {'N': str(token_count)},
                'message_index': {'N': str(message_index)},
                'is_active': {'BOOL': True},
                'created_at': {'S': now}
            }
            
            if parent_message_id:
                item['parent_message_id'] = {'S': parent_message_id}
            
            self.dynamodb.put_item(
                TableName=self.message_table_name,
                Item=item
            )
            
            message = self._deserialize_message(item)
            logger.info(f"Message created successfully: {message_id}")
            return message
            
        except ClientError as e:
            logger.error(f"Error creating message: {e}")
            raise Exception(f"Failed to create message: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error creating message: {e}")
            raise
    
    async def get_message(self, session_id: str, message_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific message by session ID and message ID.
        
        Args:
            session_id: ID of the chat session
            message_id: ID of the message
            
        Returns:
            Message dict if found, None otherwise
        """
        try:
            # Query all messages in the session to find the one with matching message_id
            response = self.dynamodb.query(
                TableName=self.message_table_name,
                KeyConditionExpression='pk = :pk AND begins_with(sk, :sk_prefix)',
                ExpressionAttributeValues={
                    ':pk': {'S': f"SESSION#{session_id}"},
                    ':sk_prefix': {'S': 'MSG#'}
                }
            )
            
            for item in response.get('Items', []):
                message = self._deserialize_message(item)
                if message['message_id'] == message_id:
                    return message
            
            return None
            
        except ClientError as e:
            logger.error(f"Error getting message {message_id}: {e}")
            raise
    
    async def get_session_messages(
        self,
        session_id: str,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all messages for a chat session.
        
        Args:
            session_id: ID of the chat session
            limit: Maximum number of messages to return
            
        Returns:
            List of message dicts ordered by message_index
        """
        try:
            query_params = {
                'TableName': self.message_table_name,
                'KeyConditionExpression': 'pk = :pk AND begins_with(sk, :sk_prefix)',
                'ExpressionAttributeValues': {
                    ':pk': {'S': f"SESSION#{session_id}"},
                    ':sk_prefix': {'S': 'MSG#'}
                },
                'ScanIndexForward': True  # Oldest first (by message_index)
            }
            
            if limit:
                query_params['Limit'] = limit
            
            response = self.dynamodb.query(**query_params)
            
            messages = []
            for item in response.get('Items', []):
                messages.append(self._deserialize_message(item))
            
            return messages
            
        except ClientError as e:
            logger.error(f"Error getting session messages for session {session_id}: {e}")
            raise
    
    async def update_message(
        self,
        session_id: str,
        message_id: str,
        **updates
    ) -> Optional[Dict[str, Any]]:
        """
        Update a message.
        
        Args:
            session_id: ID of the chat session
            message_id: ID of the message
            **updates: Fields to update
            
        Returns:
            Updated message dict if successful, None if not found
        """
        try:
            # First find the message to get its SK
            message = await self.get_message(session_id, message_id)
            if not message:
                return None
            
            sk = f"MSG#{message['message_index']:06d}#{message_id}"
            
            # Build update expression
            update_expression = "SET"
            expression_values = {}
            
            update_parts = []
            if 'content' in updates:
                update_parts.append("content = :content")
                expression_values[':content'] = {'S': updates['content']}
            if 'token_count' in updates:
                update_parts.append("token_count = :token_count")
                expression_values[':token_count'] = {'N': str(updates['token_count'])}
            if 'is_active' in updates:
                update_parts.append("is_active = :is_active")
                expression_values[':is_active'] = {'BOOL': updates['is_active']}
            
            if not update_parts:
                return message
            
            update_expression += " " + ", ".join(update_parts)
            
            response = self.dynamodb.update_item(
                TableName=self.message_table_name,
                Key={
                    'pk': {'S': f"SESSION#{session_id}"},
                    'sk': {'S': sk}
                },
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values,
                ReturnValues='ALL_NEW'
            )
            
            if 'Attributes' not in response:
                return None
                
            updated_message = self._deserialize_message(response['Attributes'])
            logger.info(f"Message updated successfully: {message_id}")
            return updated_message
            
        except ClientError as e:
            logger.error(f"Error updating message {message_id}: {e}")
            raise Exception(f"Failed to update message: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error updating message {message_id}: {e}")
            raise
    
    async def deactivate_branch_messages(
        self,
        session_id: str,
        parent_message_id: str
    ) -> int:
        """
        Deactivate all messages in a branch (used when editing messages).
        
        Args:
            session_id: ID of the chat session
            parent_message_id: ID of the parent message
            
        Returns:
            Number of messages deactivated
        """
        try:
            messages = await self.get_session_messages(session_id)
            deactivated_count = 0
            
            # Find all messages that are descendants of the parent
            def get_descendants(parent_id: str) -> List[Dict[str, Any]]:
                descendants = []
                for message in messages:
                    if message.get('parent_message_id') == parent_id:
                        descendants.append(message)
                        # Recursively get descendants of this message
                        descendants.extend(get_descendants(message['message_id']))
                return descendants
            
            # Get all descendant messages
            descendants = get_descendants(parent_message_id)
            
            # Deactivate all descendants
            for message in descendants:
                if message.get('is_active', True):
                    await self.update_message(
                        session_id=session_id,
                        message_id=message['message_id'],
                        is_active=False
                    )
                    deactivated_count += 1
            
            logger.info(f"Deactivated {deactivated_count} messages in branch")
            return deactivated_count
            
        except Exception as e:
            logger.error(f"Error deactivating branch messages: {e}")
            raise
    
    async def delete_all_session_messages(self, session_id: str) -> int:
        """
        Delete all messages for a chat session.
        
        Args:
            session_id: ID of the chat session
            
        Returns:
            Number of messages deleted
        """
        try:
            messages = await self.get_session_messages(session_id)
            deleted_count = 0
            
            for message in messages:
                sk = f"MSG#{message['message_index']:06d}#{message['message_id']}"
                self.dynamodb.delete_item(
                    TableName=self.message_table_name,
                    Key={
                        'pk': {'S': f"SESSION#{session_id}"},
                        'sk': {'S': sk}
                    }
                )
                deleted_count += 1
            
            logger.info(f"Deleted {deleted_count} messages for session {session_id}")
            return deleted_count
            
        except ClientError as e:
            logger.error(f"Error deleting session messages for session {session_id}: {e}")
            raise
    
    async def get_active_conversation_path(
        self,
        session_id: str,
        leaf_message_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get the active conversation path (no branching) from root to a specific message.
        
        Args:
            session_id: ID of the chat session
            leaf_message_id: ID of the leaf message (if None, gets the latest active path)
            
        Returns:
            List of message dicts representing the conversation path
        """
        try:
            all_messages = await self.get_session_messages(session_id)
            active_messages = [msg for msg in all_messages if msg.get('is_active', True)]
            
            if not active_messages:
                return []
            
            # If no leaf specified, find the latest active message
            if not leaf_message_id:
                latest_message = max(active_messages, key=lambda x: x['created_at'])
                leaf_message_id = latest_message['message_id']
            
            # Build the path from leaf to root
            path = []
            current_id = leaf_message_id
            message_map = {msg['message_id']: msg for msg in active_messages}
            
            while current_id and current_id in message_map:
                current_msg = message_map[current_id]
                path.append(current_msg)
                current_id = current_msg.get('parent_message_id')
            
            # Reverse to get root-to-leaf order
            path.reverse()
            return path
            
        except Exception as e:
            logger.error(f"Error getting active conversation path: {e}")
            raise
    
    async def calculate_active_branch_tokens(self, session_id: str) -> int:
        """
        Calculate total tokens in the current active conversation branch.
        
        Args:
            session_id: ID of the chat session
            
        Returns:
            Total token count for active branch
        """
        try:
            active_path = await self.get_active_conversation_path(session_id)
            return sum(msg['token_count'] for msg in active_path)
            
        except Exception as e:
            logger.error(f"Error calculating active branch tokens: {e}")
            raise
    
    async def calculate_total_session_tokens(self, session_id: str) -> int:
        """
        Calculate total tokens across all branches in the session.
        
        Args:
            session_id: ID of the chat session
            
        Returns:
            Total token count for all messages
        """
        try:
            all_messages = await self.get_session_messages(session_id)
            return sum(msg['token_count'] for msg in all_messages)
            
        except Exception as e:
            logger.error(f"Error calculating total session tokens: {e}")
            raise

    # ===============================
    # Helper Methods
    # ===============================
    
    def _deserialize_message(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Convert DynamoDB item to message dict."""
        message = {
            'message_id': item['message_id']['S'],
            'session_id': item['session_id']['S'],
            'user_id': int(item['user_id']['N']),
            'role': item['role']['S'],
            'content': item['content']['S'],
            'token_count': int(item['token_count']['N']),
            'message_index': int(item['message_index']['N']),
            'is_active': item['is_active']['BOOL'],
            'created_at': item['created_at']['S']
        }
        
        if 'parent_message_id' in item:
            message['parent_message_id'] = item['parent_message_id']['S']
        else:
            message['parent_message_id'] = None
            
        return message

