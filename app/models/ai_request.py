from pydantic import BaseModel
from typing import List, Union


class User(BaseModel):
    userName: str

class DataSourceCred(BaseModel):
    dataSourceId: str
    dataSourceName: str
    dataSourceType: str

class MessageRequest(BaseModel):
    userMessage: str

class AIRequest(BaseModel):
    userInformation: User
    dataSourceCred: DataSourceCred
    messageRequest: MessageRequest
