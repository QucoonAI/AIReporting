import uuid
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
from pynamodb.exceptions import DoesNotExist, PutError, UpdateError, DeleteError
from dynamodb.tables.chat import ChatSessionModel
from dynamodb.tables.message import MessageModel
from schemas.chat import MessageRole
from core.utils import logger


class ChatRepository:
    """Repository class for handling Chat and Message DynamoDB operations."""
    
    def __init__(self):
        pass
    
    # ===============================
    # Chat Session Operations
    # ===============================

    async def create_chat_session(
        self,
        user_id: int,
        data_source_id: int,
        title: str,
        max_tokens: int = 50000
    ) -> ChatSessionModel:
        """
        Create a new chat session.
        
        Args:
            user_id: ID of the user creating the session
            data_source_id: ID of the associated data source
            title: Session title
            max_tokens: Maximum tokens allowed for this session
            
        Returns:
            Created ChatSessionModel instance
            
        Raises:
            Exception: If session creation fails
        """
        try:
            session_id = str(uuid.uuid4())
            now = datetime.utcnow()
            
            session = ChatSessionModel(
                pk=f"USER#{user_id}",
                sk=f"SESSION#{session_id}",
                gsi1_pk=f"DATASOURCE#{data_source_id}",
                gsi1_sk=f"SESSION#{session_id}",
                session_id=session_id,
                user_id=user_id,
                data_source_id=data_source_id,
                title=title,
                message_count=0,
                total_tokens_all_branches=0,
                active_branch_tokens=0,
                max_tokens=max_tokens,
                created_at=now,
                updated_at=now
            )
            
            session.save()
            logger.info(f"Chat session created successfully: {session_id}")
            return session
            
        except PutError as e:
            logger.error(f"Error creating chat session: {e}")
            raise Exception(f"Failed to create chat session: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error creating chat session: {e}")
            raise
    
    async def get_chat_session(self, user_id: int, session_id: str) -> Optional[ChatSessionModel]:
        """
        Get a chat session by user ID and session ID.
        
        Args:
            user_id: ID of the user
            session_id: ID of the chat session
            
        Returns:
            ChatSessionModel instance if found, None otherwise
        """
        try:
            session = ChatSessionModel.get(
                hash_key=f"USER#{user_id}",
                range_key=f"SESSION#{session_id}"
            )
            return session
        except DoesNotExist:
            return None
        except Exception as e:
            logger.error(f"Error getting chat session {session_id}: {e}")
            raise
    
    async def update_chat_session(
        self,
        user_id: int,
        session_id: str,
        **updates
    ) -> Optional[ChatSessionModel]:
        """
        Update a chat session.
        
        Args:
            user_id: ID of the user
            session_id: ID of the chat session
            **updates: Fields to update
            
        Returns:
            Updated ChatSessionModel instance if successful, None if not found
        """
        try:
            session = await self.get_chat_session(user_id, session_id)
            if not session:
                return None
            
            # Update allowed fields
            if 'title' in updates:
                session.title = updates['title']
            if 'message_count' in updates:
                session.message_count = updates['message_count']
            if 'total_tokens_all_branches' in updates:
                session.total_tokens_all_branches = updates['total_tokens_all_branches']
            if 'active_branch_tokens' in updates:
                session.active_branch_tokens = updates['active_branch_tokens']
            
            session.updated_at = datetime.utcnow()
            session.save()
            
            logger.info(f"Chat session updated successfully: {session_id}")
            return session
            
        except UpdateError as e:
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
            session = await self.get_chat_session(user_id, session_id)
            if not session:
                return False
            
            # Delete all messages in the session first
            await self.delete_all_session_messages(session_id)
            
            # Delete the session
            session.delete()
            
            logger.info(f"Chat session deleted successfully: {session_id}")
            return True
            
        except DeleteError as e:
            logger.error(f"Error deleting chat session {session_id}: {e}")
            raise Exception(f"Failed to delete chat session: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error deleting chat session {session_id}: {e}")
            raise
    
    async def get_user_chat_sessions(
        self,
        user_id: int,
        limit: int = 50
    ) -> List[ChatSessionModel]:
        """
        Get all chat sessions for a user.
        
        Args:
            user_id: ID of the user
            limit: Maximum number of sessions to return
            
        Returns:
            List of ChatSessionModel instances
        """
        try:
            sessions = []
            for session in ChatSessionModel.query(
                hash_key=f"USER#{user_id}",
                range_key_condition=ChatSessionModel.sk.startswith("SESSION#"),
                limit=limit,
                scan_index_forward=False  # Most recent first
            ):
                sessions.append(session)
            
            return sessions
            
        except Exception as e:
            logger.error(f"Error getting user chat sessions for user {user_id}: {e}")
            raise
    
    async def get_user_chat_sessions_paginated(
        self,
        user_id: int,
        limit: int = 10,
        last_evaluated_key: Optional[str] = None
    ) -> Tuple[List[ChatSessionModel], Optional[str]]:
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
            query_kwargs = {
                'hash_key': f"USER#{user_id}",
                'range_key_condition': ChatSessionModel.sk.startswith("SESSION#"),
                'limit': limit,
                'scan_index_forward': False
            }
            
            if last_evaluated_key:
                query_kwargs['last_evaluated_key'] = {
                    'pk': f"USER#{user_id}",
                    'sk': last_evaluated_key
                }
            
            result = ChatSessionModel.query(**query_kwargs)
            sessions = list(result)
            
            # Get next page key if available
            next_key = None
            if hasattr(result, 'last_evaluated_key') and result.last_evaluated_key:
                next_key = result.last_evaluated_key.get('sk')
            
            return sessions, next_key
            
        except Exception as e:
            logger.error(f"Error getting paginated chat sessions for user {user_id}: {e}")
            raise
    
    # ===============================
    # Message Operations
    # ===============================
    
    async def create_message(
        self,
        session_id: str,
        user_id: int,
        role: MessageRole,
        content: str,
        token_count: int,
        message_index: int,
        parent_message_id: Optional[str] = None
    ) -> MessageModel:
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
            Created MessageModel instance
        """
        try:
            message_id = str(uuid.uuid4())
            now = datetime.utcnow()
            
            message = MessageModel(
                pk=f"SESSION#{session_id}",
                sk=f"MSG#{message_index:06d}#{message_id}",
                gsi1_pk=f"USER#{user_id}",
                gsi1_sk=f"MSG#{now.isoformat()}#{message_id}",
                message_id=message_id,
                session_id=session_id,
                user_id=user_id,
                role=role.value,
                content=content,
                token_count=token_count,
                message_index=message_index,
                parent_message_id=parent_message_id,
                is_active=True,
                created_at=now
            )
            
            message.save()
            logger.info(f"Message created successfully: {message_id}")
            return message
            
        except PutError as e:
            logger.error(f"Error creating message: {e}")
            raise Exception(f"Failed to create message: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error creating message: {e}")
            raise
    
    async def get_message(self, session_id: str, message_id: str) -> Optional[MessageModel]:
        """
        Get a specific message by session ID and message ID.
        
        Args:
            session_id: ID of the chat session
            message_id: ID of the message
            
        Returns:
            MessageModel instance if found, None otherwise
        """
        try:
            # Query all messages in the session to find the one with matching message_id
            for message in MessageModel.query(
                hash_key=f"SESSION#{session_id}",
                range_key_condition=MessageModel.sk.startswith("MSG#")
            ):
                if message.message_id == message_id:
                    return message
            return None
            
        except Exception as e:
            logger.error(f"Error getting message {message_id}: {e}")
            raise
    
    async def get_session_messages(
        self,
        session_id: str,
        limit: Optional[int] = None
    ) -> List[MessageModel]:
        """
        Get all messages for a chat session.
        
        Args:
            session_id: ID of the chat session
            limit: Maximum number of messages to return
            
        Returns:
            List of MessageModel instances ordered by message_index
        """
        try:
            messages = []
            query_kwargs = {
                'hash_key': f"SESSION#{session_id}",
                'range_key_condition': MessageModel.sk.startswith("MSG#"),
                'scan_index_forward': True  # Oldest first
            }
            
            if limit:
                query_kwargs['limit'] = limit
            
            for message in MessageModel.query(**query_kwargs):
                messages.append(message)
            
            return messages
            
        except Exception as e:
            logger.error(f"Error getting session messages for session {session_id}: {e}")
            raise
    
    async def update_message(
        self,
        session_id: str,
        message_id: str,
        **updates
    ) -> Optional[MessageModel]:
        """
        Update a message.
        
        Args:
            session_id: ID of the chat session
            message_id: ID of the message
            **updates: Fields to update
            
        Returns:
            Updated MessageModel instance if successful, None if not found
        """
        try:
            message = await self.get_message(session_id, message_id)
            if not message:
                return None
            
            # Update allowed fields
            if 'content' in updates:
                message.content = updates['content']
            if 'token_count' in updates:
                message.token_count = updates['token_count']
            if 'is_active' in updates:
                message.is_active = updates['is_active']
            
            message.save()
            
            logger.info(f"Message updated successfully: {message_id}")
            return message
            
        except UpdateError as e:
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
            def is_descendant(msg, parent_id):
                if msg.parent_message_id == parent_id:
                    return True
                # Check if any of its ancestors is the parent
                for other_msg in messages:
                    if other_msg.message_id == msg.parent_message_id:
                        return is_descendant(other_msg, parent_id)
                return False
            
            for message in messages:
                if message.parent_message_id == parent_message_id or is_descendant(message, parent_message_id):
                    message.is_active = False
                    message.save()
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
                message.delete()
                deleted_count += 1
            
            logger.info(f"Deleted {deleted_count} messages for session {session_id}")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error deleting session messages for session {session_id}: {e}")
            raise
    
    async def get_active_conversation_path(
        self,
        session_id: str,
        leaf_message_id: Optional[str] = None
    ) -> List[MessageModel]:
        """
        Get the active conversation path (no branching) from root to a specific message.
        
        Args:
            session_id: ID of the chat session
            leaf_message_id: ID of the leaf message (if None, gets the latest active path)
            
        Returns:
            List of MessageModel instances representing the conversation path
        """
        try:
            all_messages = await self.get_session_messages(session_id)
            active_messages = [msg for msg in all_messages if msg.is_active]
            
            if not active_messages:
                return []
            
            # If no leaf specified, find the latest message
            if not leaf_message_id:
                latest_message = max(active_messages, key=lambda x: x.message_index)
                leaf_message_id = latest_message.message_id
            
            # Build the path from leaf to root
            path = []
            current_id = leaf_message_id
            
            while current_id:
                current_msg = next((msg for msg in active_messages if msg.message_id == current_id), None)
                if not current_msg:
                    break
                path.append(current_msg)
                current_id = current_msg.parent_message_id
            
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
            return sum(msg.token_count for msg in active_path)
            
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
            return sum(msg.token_count for msg in all_messages)
            
        except Exception as e:
            logger.error(f"Error calculating total session tokens: {e}")
            raise



