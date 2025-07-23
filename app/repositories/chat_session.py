from datetime import datetime, timezone
from typing import List, Optional, Tuple, Dict, Any
import uuid
from pynamodb.exceptions import DoesNotExist
from models.chat_session import ChatSessionModel, MessageModel
from schemas.chat_session import ChatSession, SessionLimitConfig, ConversationTreeResponse
from schemas.message import Message, MessageCreate, MessageEdit, MessageEditResponse
from schemas.enum import MessageRole


class ChatRepository:
    
    def _estimate_token_count(self, text: str, chars_per_token: float = 4.0) -> int:
        """Estimate token count from text length"""
        return max(1, int(len(text) / chars_per_token))
    
    def _should_use_token_limiting(self, config: SessionLimitConfig) -> bool:
        """Determine if token-based limiting should be used"""
        return config.limiting_strategy == "token_based"
    
    async def _check_and_enforce_limits(
        self, 
        session: ChatSessionModel, 
        new_message_tokens: int,
        config: SessionLimitConfig
    ) -> bool:
        """Check limits and archive messages if needed. Returns True if archiving occurred."""
        
        if self._should_use_token_limiting(config):
            return await self._enforce_token_limits(session, new_message_tokens, config)
        else:
            return await self._enforce_message_limits(session, config)
    
    async def _enforce_token_limits(
        self, 
        session: ChatSessionModel, 
        new_message_tokens: int,
        config: SessionLimitConfig
    ) -> bool:
        """Enforce token-based limits"""
        
        current_tokens = session.active_tokens
        total_after_new = current_tokens + new_message_tokens
        token_limit = session.max_tokens
        threshold = int(token_limit * config.token_archive_threshold)
        
        if total_after_new > threshold:
            # Calculate target tokens after archiving
            target_tokens = int(token_limit * 0.6)  # Archive to 60% of limit
            await self._archive_messages_by_tokens(
                session.session_id, 
                target_tokens,
                config.preserve_conversation_pairs
            )
            return True
        
        return False
    
    async def _enforce_message_limits(self, session: ChatSessionModel, config: SessionLimitConfig) -> bool:
        """Enforce message-based limits (fallback)"""
        
        if session.active_message_count >= session.max_messages:
            keep_count = int(session.max_messages * 0.6)  # Keep 60% of messages
            await self._archive_old_messages(session.session_id, keep_count)
            return True
        
        return False
    
    async def _archive_messages_by_tokens(
        self, 
        session_id: str, 
        target_tokens: int,
        preserve_pairs: bool = True
    ):
        """Archive messages to reach target token count"""
        
        # Get all active messages, oldest first
        messages_query = MessageModel.query(
            f'SESSION#{session_id}',
            scan_index_forward=True  # Oldest first
        )
        
        active_messages = [m for m in messages_query if m.is_active]
        
        if not active_messages:
            return
        
        current_tokens = sum(m.token_count or 0 for m in active_messages)
        
        if current_tokens <= target_tokens:
            return
        
        tokens_to_remove = current_tokens - target_tokens
        removed_tokens = 0
        messages_to_archive = []
        
        if preserve_pairs:
            # Archive in conversation pairs (user + assistant)
            i = 0
            while i < len(active_messages) and removed_tokens < tokens_to_remove:
                msg = active_messages[i]
                messages_to_archive.append(msg)
                removed_tokens += (msg.token_count or 0)
                
                # If this is a user message, try to include the next assistant message
                if (msg.role == 'user' and 
                    i + 1 < len(active_messages) and 
                    active_messages[i + 1].role == 'assistant'):
                    assistant_msg = active_messages[i + 1]
                    messages_to_archive.append(assistant_msg)
                    removed_tokens += (assistant_msg.token_count or 0)
                    i += 2
                else:
                    i += 1
        else:
            # Archive individual messages
            for msg in active_messages:
                if removed_tokens >= tokens_to_remove:
                    break
                messages_to_archive.append(msg)
                removed_tokens += (msg.token_count or 0)
        
        # Archive the selected messages
        archive_time = datetime.now(timezone.utc)
        for msg in messages_to_archive:
            msg.update(actions=[
                MessageModel.is_active.set(False),
                MessageModel.archived_at.set(archive_time),
                MessageModel.archive_reason.set('token_limit')
            ])
    
    async def _get_context_messages_by_tokens(
        self, 
        session_id: str, 
        max_tokens: int,
        preserve_pairs: bool = True
    ) -> List[Message]:
        """Get recent messages that fit within token limit for LLM context"""
        
        # Get all active messages, most recent first
        messages_query = MessageModel.query(
            f'SESSION#{session_id}',
            scan_index_forward=False  # Most recent first
        )
        
        active_messages = [m for m in messages_query if m.is_active]
        
        if not active_messages:
            return []
        
        # Reverse to get chronological order, then select from end
        chronological_messages = list(reversed(active_messages))
        
        selected_messages = []
        current_tokens = 0
        
        if preserve_pairs:
            # Work backwards in pairs
            i = len(chronological_messages) - 1
            temp_selection = []
            
            while i >= 0 and current_tokens < max_tokens:
                msg = chronological_messages[i]
                msg_tokens = msg.token_count or 0
                
                # If this is an assistant message, try to include the preceding user message
                if (msg.role == 'assistant' and 
                    i > 0 and 
                    chronological_messages[i-1].role == 'user'):
                    user_msg = chronological_messages[i-1]
                    user_tokens = user_msg.token_count or 0
                    pair_tokens = msg_tokens + user_tokens
                    
                    if current_tokens + pair_tokens <= max_tokens:
                        temp_selection.extend([user_msg, msg])
                        current_tokens += pair_tokens
                        i -= 2
                    else:
                        break
                else:
                    if current_tokens + msg_tokens <= max_tokens:
                        temp_selection.append(msg)
                        current_tokens += msg_tokens
                        i -= 1
                    else:
                        break
            
            # Reverse to get chronological order
            selected_messages = list(reversed(temp_selection))
        else:
            # Simple token-based selection from the end
            for msg in reversed(chronological_messages):
                msg_tokens = msg.token_count or 0
                if current_tokens + msg_tokens <= max_tokens:
                    selected_messages.insert(0, msg)
                    current_tokens += msg_tokens
                else:
                    break
        
        return [self._message_model_to_schema(msg) for msg in selected_messages]
    
    async def create_chat_session(
        self, 
        user_id: int, 
        data_source_id: int,
        data_source_name: str, 
        data_source_type: str,
        title: Optional[str] = None,
        config: Optional[SessionLimitConfig] = None
    ) -> ChatSession:
        """Create a new chat session"""
        
        session_id = f"chat_{uuid.uuid4().hex[:8]}"
        now = datetime.now(timezone.utc)
        
        if not title:
            title = f"Chat with {data_source_name}"
        
        if not config:
            config = SessionLimitConfig()
        
        # Create session model
        session = ChatSessionModel(
            pk=f'USER#{user_id}',
            sk=f'SESSION#{session_id}',
            gsi1_pk=f'DATASOURCE#{data_source_id}',
            gsi1_sk=f'SESSION#{session_id}',
            session_id=session_id,
            user_id=user_id,
            data_source_id=data_source_id,
            data_source_name=data_source_name,
            data_source_type=data_source_type,
            title=title,
            is_active=True,
            status='active',
            message_count=0,
            active_message_count=0,
            total_tokens=0,
            active_tokens=0,
            max_messages=config.max_messages,
            max_tokens=config.max_tokens,
            created_at=now,
            updated_at=now,
            settings={
                'context_window_tokens': config.context_window_tokens,
                'context_window_messages': config.context_window_messages,
                'token_archive_threshold': config.token_archive_threshold,
                'limiting_strategy': config.limiting_strategy,
                'archive_strategy': config.archive_strategy,
                'preserve_conversation_pairs': config.preserve_conversation_pairs,
                'allow_editing': config.allow_editing,
                'regenerate_on_edit': config.regenerate_on_edit,
                'estimate_tokens': config.estimate_tokens,
                'chars_per_token': config.chars_per_token
            }
        )
        
        session.save()
        
        return self._session_model_to_schema(session)
    
    async def list_user_sessions(
        self, 
        user_id: int, 
        page: int = 1, 
        per_page: int = 20
    ) -> Tuple[List[ChatSession], int, bool]:
        """List chat sessions for a user with pagination"""
        
        try:
            # Query sessions for user (most recent first)
            sessions_query = ChatSessionModel.query(
                f'USER#{user_id}',
                scan_index_forward=False,  # Most recent first
                limit=per_page + 1  # Get one extra to check if there's more
            )
            
            sessions_list = list(sessions_query)
            has_more = len(sessions_list) > per_page
            
            if has_more:
                sessions_list = sessions_list[:-1]  # Remove the extra item
            
            # Convert to schema objects
            sessions = [self._session_model_to_schema(session) for session in sessions_list]
            
            return sessions, len(sessions), has_more
            
        except Exception as e:
            raise Exception(f"Error listing user sessions: {str(e)}")
    
    async def get_conversation_tree(self, session_id: str, user_id: int) -> ConversationTreeResponse:
        """Get complete conversation tree for a session"""
        
        try:
            # Verify session belongs to user
            session = ChatSessionModel.get(f'USER#{user_id}', f'SESSION#{session_id}')
            
            # Get all messages for the session
            messages_query = MessageModel.query(f'SESSION#{session_id}')
            messages_list = list(messages_query)
            
            # Build tree structure
            tree = self._build_conversation_tree(messages_list)
            
            # Get active conversation path
            active_path = self._get_active_path(tree)
            
            # Calculate analytics
            analytics = self._calculate_session_analytics(messages_list)
            
            return ConversationTreeResponse(
                session_id=session_id,
                tree=tree,
                active_path=active_path,
                analytics=analytics
            )
            
        except DoesNotExist:
            raise ValueError(f"Session {session_id} not found for user {user_id}")
        except Exception as e:
            raise Exception(f"Error getting conversation tree: {str(e)}")
    
    async def add_message(
        self, 
        session_id: str, 
        user_id: int, 
        message_data: MessageCreate,
        **kwargs
    ) -> Message:
        """Add a message to a chat session with token-based limiting"""
        
        try:
            # Get session to check limits
            session = ChatSessionModel.get(f'USER#{user_id}', f'SESSION#{session_id}')
            config = SessionLimitConfig(**session.settings)
            
            # Calculate or estimate token count
            message_tokens = kwargs.get('token_count')
            if not message_tokens and config.estimate_tokens:
                message_tokens = self._estimate_token_count(
                    message_data.content, 
                    config.chars_per_token
                )
            message_tokens = message_tokens or 1  # Minimum 1 token
            
            # Check and enforce limits before adding
            archiving_occurred = await self._check_and_enforce_limits(
                session, message_tokens, config
            )
            
            # Create message
            message_id = f"msg_{uuid.uuid4().hex[:8]}"
            now = datetime.now(timezone.utc)
            timestamp_str = now.isoformat()
            message_index = session.message_count + 1
            
            message = MessageModel(
                pk=f'SESSION#{session_id}',
                sk=f'MSG#{timestamp_str}#{message_id}',
                gsi1_pk=f'USER#{user_id}',
                gsi1_sk=f'MSG#{timestamp_str}#{message_id}',
                message_id=message_id,
                session_id=session_id,
                user_id=user_id,
                role=message_data.role.value,
                content=message_data.content,
                message_index=message_index,
                is_active=True,
                is_edited=False,
                version=1,
                token_count=message_tokens,
                created_at=now,
                **{k: v for k, v in kwargs.items() if k != 'token_count'}
            )
            
            message.save()
            
            # Update session counts (recalculate if archiving occurred)
            if archiving_occurred:
                # Recalculate active counts after archiving
                active_messages = list(MessageModel.query(
                    f'SESSION#{session_id}'
                ))
                active_count = len([m for m in active_messages if m.is_active])
                active_token_sum = sum(m.token_count or 0 for m in active_messages if m.is_active)
                
                session.update(actions=[
                    ChatSessionModel.message_count.add(1),
                    ChatSessionModel.total_tokens.add(message_tokens),
                    ChatSessionModel.active_message_count.set(active_count),
                    ChatSessionModel.active_tokens.set(active_token_sum),
                    ChatSessionModel.updated_at.set(now)
                ])
            else:
                session.update(actions=[
                    ChatSessionModel.message_count.add(1),
                    ChatSessionModel.active_message_count.add(1),
                    ChatSessionModel.total_tokens.add(message_tokens),
                    ChatSessionModel.active_tokens.add(message_tokens),
                    ChatSessionModel.updated_at.set(now)
                ])
            
            return self._message_model_to_schema(message)
            
        except DoesNotExist:
            raise ValueError(f"Session {session_id} not found for user {user_id}")
        except Exception as e:
            raise Exception(f"Error adding message: {str(e)}")
    
    async def edit_message(
        self, 
        user_id: int, 
        message_id: str, 
        edit_data: MessageEdit
    ) -> MessageEditResponse:
        """Edit a message using cascade regeneration pattern"""
        
        try:
            # Find the original message
            original_messages = list(MessageModel.user_message_index.query(f'USER#{user_id}'))
            original_message = None
            
            for msg in original_messages:
                if msg.message_id == message_id:
                    original_message = msg
                    break
            
            if not original_message:
                raise ValueError(f"Message {message_id} not found for user {user_id}")
            
            session_id = original_message.session_id
            
            # Create edited version
            now = datetime.now(timezone.utc)
            edited_message_id = f"msg_{uuid.uuid4().hex[:8]}"
            
            edited_message = MessageModel(
                pk=f'SESSION#{session_id}',
                sk=f'MSG#{now.isoformat()}#{edited_message_id}',
                gsi1_pk=f'USER#{user_id}',
                gsi1_sk=f'MSG#{now.isoformat()}#{edited_message_id}',
                message_id=edited_message_id,
                session_id=session_id,
                user_id=user_id,
                role=original_message.role,
                content=edit_data.content,
                message_index=original_message.message_index,
                is_active=True,
                is_edited=True,
                parent_message_id=message_id,
                version=original_message.version + 1,
                created_at=now,
                edited_at=now
            )
            
            edited_message.save()
            
            # Archive original message and subsequent messages (cascade)
            await self._archive_message_chain_after(session_id, original_message.message_index)
            
            # Convert to schema objects
            original_msg = self._message_model_to_schema(original_message)
            edited_msg = self._message_model_to_schema(edited_message)
            
            return MessageEditResponse(
                original_message=original_msg,
                edited_message=edited_msg,
                regenerated_responses=[]  # Will be populated by LLM service
            )
            
        except Exception as e:
            raise Exception(f"Error editing message: {str(e)}")
    
    # Helper methods
    
    def _session_model_to_schema(self, session: ChatSessionModel) -> ChatSession:
        """Convert DynamoDB model to Pydantic schema"""
        return ChatSession(
            session_id=session.session_id,
            user_id=session.user_id,
            data_source_id=session.data_source_id,
            data_source_name=session.data_source_name,
            data_source_type=session.data_source_type,
            title=session.title,
            is_active=session.is_active,
            status=session.status,
            message_count=session.message_count,
            active_message_count=session.active_message_count,
            total_tokens=session.total_tokens,
            active_tokens=session.active_tokens,
            max_messages=session.max_messages,
            max_tokens=session.max_tokens,
            created_at=session.created_at,
            updated_at=session.updated_at,
            settings=session.settings
        )
    
    def _message_model_to_schema(self, message: MessageModel) -> Message:
        """Convert DynamoDB model to Pydantic schema"""
        return Message(
            message_id=message.message_id,
            session_id=message.session_id,
            user_id=message.user_id,
            role=MessageRole(message.role),
            content=message.content,
            message_index=message.message_index,
            is_active=message.is_active,
            is_edited=message.is_edited,
            parent_message_id=message.parent_message_id,
            version=message.version,
            token_count=message.token_count,
            model_used=message.model_used,
            processing_time_ms=message.processing_time_ms,
            created_at=message.created_at,
            edited_at=message.edited_at,
            archived_at=message.archived_at,
            archive_reason=message.archive_reason,
            metadata=message.metadata
        )
    
    def _build_conversation_tree(self, messages: List[MessageModel]) -> Dict[str, Any]:
        """Build conversation tree from messages"""
        tree = {}
        
        for msg in messages:
            parent_id = msg.parent_message_id
            if parent_id:
                if parent_id not in tree:
                    tree[parent_id] = []
                tree[parent_id].append(self._message_model_to_schema(msg).dict())
            else:
                # Root messages
                if 'root' not in tree:
                    tree['root'] = []
                tree['root'].append(self._message_model_to_schema(msg).dict())
        
        return tree
    
    def _get_active_path(self, tree: Dict[str, Any]) -> List[Message]:
        """Extract active conversation path from tree"""
        active_path = []
        
        def traverse_active(node_id="root"):
            if node_id in tree:
                for message_dict in tree[node_id]:
                    if message_dict.get("is_active", True):
                        active_path.append(Message(**message_dict))
                        traverse_active(message_dict["message_id"])
                        break  # Only follow one active path
        
        traverse_active()
        return sorted(active_path, key=lambda m: m.message_index)
    
    def _calculate_session_analytics(self, messages: List[MessageModel]) -> Dict[str, Any]:
        """Calculate session analytics"""
        analytics = {
            'total_messages': len(messages),
            'active_messages': len([m for m in messages if m.is_active]),
            'archived_messages': len([m for m in messages if not m.is_active]),
            'user_messages': len([m for m in messages if m.role == 'user']),
            'assistant_messages': len([m for m in messages if m.role == 'assistant']),
            'edited_messages': len([m for m in messages if m.is_edited]),
            'total_tokens': sum(m.token_count or 0 for m in messages),
            'avg_response_time': 0,
            'conversation_duration': None
        }
        
        # Calculate average response time
        assistant_messages = [m for m in messages if m.role == 'assistant' and m.processing_time_ms]
        if assistant_messages:
            analytics['avg_response_time'] = sum(m.processing_time_ms for m in assistant_messages) / len(assistant_messages)
        
        # Calculate conversation duration
        if messages:
            start_time = min(m.created_at for m in messages)
            end_time = max(m.created_at for m in messages)
            analytics['conversation_duration'] = (end_time - start_time).total_seconds()
        
        return analytics
    
    async def _archive_old_messages(self, session_id: str, keep_count: int):
        """Archive old messages to stay within limits"""
        # Get all active messages, oldest first
        messages_query = MessageModel.query(
            f'SESSION#{session_id}',
            scan_index_forward=True  # Oldest first
        )
        
        active_messages = [m for m in messages_query if m.is_active]
        
        if len(active_messages) <= keep_count:
            return
        
        # Archive messages beyond the keep_count
        messages_to_archive = active_messages[:-keep_count]
        
        for msg in messages_to_archive:
            msg.update(actions=[
                MessageModel.is_active.set(False),
                MessageModel.archived_at.set(datetime.now(timezone.utc)),
                MessageModel.archive_reason.set('message_limit')
            ])
    
    async def _archive_message_chain_after(self, session_id: str, from_index: int):
        """Archive all messages after a given index (for editing scenarios)"""
        messages_query = MessageModel.query(f'SESSION#{session_id}')
        
        for msg in messages_query:
            if msg.message_index >= from_index and msg.is_active:
                msg.update(actions=[
                    MessageModel.is_active.set(False),
                    MessageModel.archived_at.set(datetime.now(timezone.utc)),
                    MessageModel.archive_reason.set('cascade_regeneration')
                ])