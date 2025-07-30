from pydantic import BaseModel
from typing import Union, Literal


class TextResponse(BaseModel):
    requestType: Literal["text"]
    response: str

class ExcelResponse(BaseModel):
    requestType: Literal["excel"]
    response: str

class CsvResponse(BaseModel):
    requestType: Literal["csv"]
    response: str

class MySQLResponse(BaseModel):
    requestType: Literal["mysql"]
    response: str

class MongoDBResponse(BaseModel):
    requestType: Literal["mongodb"]
    response: str

class PostgreSQLResponse(BaseModel):
    requestType: Literal["postgresql"]
    response: str

response_data = Union[TextResponse, ExcelResponse, CsvResponse, MySQLResponse, MongoDBResponse, PostgreSQLResponse]

class SessionIdResponse(BaseModel):
    sessionId: str
    status: str
    message: str

class AIResponse(BaseModel):
     data : response_data


