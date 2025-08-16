import uuid
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from app.schemas.chat import MessageRole
from app.core.utils import logger
from app.config.dynamodb import get_dynamodb_connection
from app.config.settings import get_settings


settings = get_settings()

class MessageRepository:
    """Repository class for handling Message DynamoDB operations."""

    def __init__(self):
        """Initialize the message repository with DynamoDB connection."""
        self.db_connection = get_dynamodb_connection()
        self.messages_table = self.db_connection.get_table(settings.DYNAMODB_MESSAGES_TABLE)

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
                'pk': f"SESSION#{session_id}",
                'sk': f"MSG#{message_index:06d}#{message_id}",
                'gsi1_pk': f"USER#{user_id}",
                'gsi1_sk': f"MSG#{now}#{message_id}",
                'message_id': message_id,
                'session_id': session_id,
                'user_id': user_id,
                'role': role.value,
                'content': content,
                'token_count': token_count,
                'message_index': message_index,
                'is_active': True,
                'created_at': now
            }

            if parent_message_id:
                item['parent_message_id'] = parent_message_id

            self.messages_table.put_item(Item=item)

            logger.info(f"Message created successfully: {message_id}")
            return item

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
            response = self.messages_table.query(
                KeyConditionExpression=Key('pk').eq(f"SESSION#{session_id}") & Key('sk').begins_with('MSG#'),
                FilterExpression=Attr('message_id').eq(message_id)
            )

            items = response.get('Items', [])
            return items[0] if items else None

        except ClientError as e:
            logger.error(f"Error getting message {message_id}: {e}")
            raise

    async def get_session_messages_active(
        self,
        session_id: str,
        limit: Optional[int] = None,
        include_inactive: bool = False,
    ) -> List[Dict[str, Any]]:
        """Get session messages with proper active filtering"""
        try:
            query_params = {
                "KeyConditionExpression": Key("pk").eq(f"SESSION#{session_id}")
                & Key("sk").begins_with("MSG#"),
                "ScanIndexForward": True,
            }

            if not include_inactive:
                query_params["FilterExpression"] = Attr("is_active").eq(True)

            if limit:
                query_params["Limit"] = limit

            response = self.messages_table.query(**query_params)
            return response.get("Items", [])

        except ClientError as e:
            logger.error(
                f"Error getting session messages for session {session_id}: {e}"
            )
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

            # Build update expression dynamically
            update_parts = []
            expression_values = {}

            if 'content' in updates:
                update_parts.append("content = :content")
                expression_values[':content'] = updates['content']
            if 'token_count' in updates:
                update_parts.append("token_count = :token_count")
                expression_values[':token_count'] = updates['token_count']
            if 'is_active' in updates:
                update_parts.append("is_active = :is_active")
                expression_values[':is_active'] = updates['is_active']

            if not update_parts:
                return message

            update_expression = "SET " + ", ".join(update_parts)

            response = self.messages_table.update_item(
                Key={
                    'pk': f"SESSION#{session_id}",
                    'sk': sk
                },
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values,
                ReturnValues='ALL_NEW'
            )

            updated_message = response.get('Attributes')
            if updated_message:
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
            messages = await self.get_session_messages_active(session_id)
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
            messages = await self.get_session_messages_active(session_id)
            deleted_count = 0

            for message in messages:
                index = int(message['message_index'])
                sk = f"MSG#{index:06d}#{message['message_id']}"
                self.messages_table.delete_item(
                    Key={
                        'pk': f"SESSION#{session_id}",
                        'sk': sk
                    }
                )
                deleted_count += 1

            logger.info(f"Deleted {deleted_count} messages for session {session_id}")
            return deleted_count

        except ClientError as e:
            logger.error(f"Error deleting session messages for session {session_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error deleting chat session messages : {e}")
            raise
    
    async def get_active_conversation_path(
        self,
        session_id: str,
        leaf_message_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get active conversation path with proper validation"""
        try:
            active_messages = await self.get_session_messages_active(session_id)
            
            if not active_messages:
                return []
            
            # Create lookup maps for efficiency
            message_map = {msg['message_id']: msg for msg in active_messages}
            
            # If no leaf specified, find the latest by message_index
            if not leaf_message_id:
                latest_message = max(active_messages, key=lambda x: x['message_index'])
                leaf_message_id = latest_message['message_id']
            
            # Validate leaf message exists and is active
            if leaf_message_id not in message_map:
                logger.warning(f"Leaf message {leaf_message_id} not found in active messages")
                return []
            
            # Build path with cycle detection
            path = []
            current_id = leaf_message_id
            visited = set()
            
            while current_id and current_id in message_map:
                if current_id in visited:
                    logger.error(f"Circular reference detected in session {session_id} at message {current_id}")
                    raise ValueError("Circular reference in message tree")
                
                visited.add(current_id)
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
            all_messages = await self.get_session_messages_active(session_id)
            return sum(msg['token_count'] for msg in all_messages)

        except Exception as e:
            logger.error(f"Error calculating total session tokens: {e}")
            raise

    async def get_user_recent_messages(
        self,
        user_id: int,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get recent messages for a user across all sessions using GSI1.
        
        Args:
            user_id: ID of the user
            limit: Maximum number of messages to return
            
        Returns:
            List of message dicts ordered by creation time (most recent first)
        """
        try:
            response = self.messages_table.query(
                IndexName='GSI1-User-Messages',
                KeyConditionExpression=Key('gsi1_pk').eq(f"USER#{user_id}") & Key('gsi1_sk').begins_with('MSG#'),
                Limit=limit,
                ScanIndexForward=False  # Most recent first
            )

            return response.get('Items', [])

        except ClientError as e:
            logger.error(f"Error getting user recent messages for user {user_id}: {e}")
            raise

