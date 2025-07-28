from pydantic import BaseModel

class User(BaseModel):
    userName: str

class DataSourceCred(BaseModel):
    dataSourceId: str   # Points to the connection string and the database schema

class MessageRequest(BaseModel):
    userMessage: str 

class AIRequest(BaseModel):
    userInformation: User
    dataSourceCred: DataSourceCred
    messageRequest: MessageRequest
