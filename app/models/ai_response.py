from pydantic import BaseModel
from typing import Union, Literal


class TextResponse(BaseModel):
    requestType: Literal["text"]
    response: str

class ExcelResponse(BaseModel):
    requestType: Literal["excel"]
    fileUrl: str
    fileName: str

class CsvResponse(BaseModel):
    requestType: Literal["csv"]
    fileUrl: str
    fileName: str

response_data = Union[TextResponse, ExcelResponse, CsvResponse]

class AIResponse(BaseModel):
     data : response_data


