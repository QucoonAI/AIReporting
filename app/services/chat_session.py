from repositories.chat_session import ChatRepository
from schemas.chat_session import ChatSession, ChatSessionCreate, SessionLimitConfig, ConversationTreeResponse
from schemas.message import Message, MessageCreate, MessageEdit, MessageEditResponse
from schemas.enum import MessageRole


class ChatService:
    def __init__(self, chat_repository: ChatRepository):
        self.chat_repository = chat_repository
    
    async def create_session(
        self, 
        user_id: int, 
        create_data: ChatSessionCreate,
        data_source_name: str,
        data_source_type: str
    ) -> ChatSession:
        """Create a new chat session"""
        return await self.chat_repository.create_chat_session(
            user_id=user_id,
            data_source_id=create_data.data_source_id,
            data_source_name=data_source_name,
            data_source_type=data_source_type,
            title=create_data.title,
            config=create_data.config
        )
    
    async def list_user_sessions(
        self, 
        user_id: int, 
        page: int = 1, 
        per_page: int = 20
    ) -> dict:
        """List user's chat sessions"""
        sessions, total, has_more = await self.chat_repository.list_user_sessions(
            user_id, page, per_page
        )
        
        return {
            "sessions": sessions,
            "total_sessions": total,
            "page": page,
            "per_page": per_page,
            "has_more": has_more
        }
    
    async def get_conversation_tree(self, session_id: str, user_id: int) -> ConversationTreeResponse:
        """Get conversation tree for a session"""
        return await self.chat_repository.get_conversation_tree(session_id, user_id)
    
    async def add_message(
        self, 
        session_id: str, 
        user_id: int, 
        message_data: MessageCreate,
        **kwargs
    ) -> Message:
        """Add a message to a session"""
        return await self.chat_repository.add_message(session_id, user_id, message_data, **kwargs)
    
    async def edit_message(
        self, 
        user_id: int, 
        message_id: str, 
        edit_data: MessageEdit
    ) -> MessageEditResponse:
        """Edit a message with cascade regeneration"""
        return await self.chat_repository.edit_message(user_id, message_id, edit_data)

