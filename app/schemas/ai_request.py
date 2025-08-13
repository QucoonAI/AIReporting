from pydantic import BaseModel


class User(BaseModel):
    userName: str

class DataSourceCred(BaseModel):
    dataSourceId: str   # Points to the connection string and the database schema

class MessageRequest(BaseModel):
    userMessage: str 

class CreateSessionRequest(BaseModel):
    sessionName: str
    sessionRequest: DataSourceCred

class AIRequest(BaseModel):
    userInformation: User
    sessionId: str
    messageRequest: MessageRequest

