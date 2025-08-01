from app.services.llm_services.ai_function import AIQuery
from fastapi import HTTPException, status
import uuid
from datetime import datetime, timezone
from app.schemas import ai_request, ai_response
from app.repositories.chat import ChatRepository
from app.repositories.message import MessageRepository
from app.repositories.data_source import DataSourceRepository


call_query = AIQuery()

class ChatService:
    def __init__(self, chat_repository: ChatRepository, message_repository: MessageRepository, data_source_repository: DataSourceRepository):
        self.chat_repository = chat_repository
        self.msg_repository = message_repository
        self.data_source_repository = data_source_repository

    async def create_chat_session(self, user_id: str,  session_data: ai_request.CreateSessionRequest):
        # Logic to create a chat session in the database
        session_id = str(uuid.uuid4())
        session = await self.chat_repository.create_chat_session(
                user_id=user_id,
                session_id=session_id,
                data_source_id=session_data.sessionRequest.dataSourceId,
                session_name=session_data.sessionName,
                created_at=datetime.now(timezone.utc).isoformat(),
        )
        if not session:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create chat session"
            )
        else:
            return ai_response.SessionIdResponse(
                    sessionId=session_id,
                    status='created',
                    message='Session created successfully'
                )

    async def send_message(self, user_id: str, session_id: str, message: str):
        session_id = session_id
        data_source_id = await self.chat_repository.get_data_source_id_by_session_id(session_id)
        if not data_source_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source not found"
            )
        
        data_cred = await self.data_source_repository.get_data_source_by_id(data_source_id)
        if not data_cred:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source credentials not found"
            )
        initial_response = await call_query.initial_processor(message, "memory")
        json_extractor = await call_query.agentic_call(initial_response, data_cred)
        if json_extractor:
            message = await self.msg_repository.create_message(
                session_id=session_id,
                user_id=user_id,
                content=json_extractor
            )
            if not message:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to save message"
                )
        final_response = await call_query.final_processor(message, json_extractor)
        return ai_response.AIResponse(
            data=ai_response.response_data(
                requestType=json_extractor[0],
                response=final_response[1]
            )
        )

