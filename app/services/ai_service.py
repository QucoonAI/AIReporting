import yaml
import anthropic
from app.core.utils import bedrock, read_from_sql_db, read_from_mongo_db


modelId = "anthropic.claude-3-5-sonnet-20240620-v1:0"

with open("app/config/prompts.yaml", "r") as file:
    prompts = yaml.safe_load(file)

DEFAULT_SYSTEM_PROMPT = prompts["system_prompt"]
SCHEMA_SYSTEM_PROMPT = prompts["schema_prompt"]
INITIAL_SYSTEM_PROMPT = prompts["initial_system_prompt"]
FINAL_SYSTEM_PROMPT = prompts["final_system_prompt"]


tool_list = [
    {
        "toolSpec": {
            "name": "generic_response",
            "description": "Conversationally engage the user on information different from a direct request for data source query",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "requestType": {
                            "type": "string",
                            "description": "The request type, in this case it's always <generic_response>.",
                            "maxLength": 19,
                            "nullable": False,
                            "enum": ["generic_response"],
                        },
                        "queryType": {
                            "type": "string",
                            "description": "The type of query, in this case it's always <text>.",
                            "maxLength": 4,
                            "nullable": False,
                            "enum": ["text"],
                        },
                        "response": {
                            "type": "string",
                            "description": "The response to the generic request in one sentence only. It should be concise and to the point. This is a generic response to a user's query that does not require a specific data source query.",
                            "maxLength": 1000,
                            "nullable": False,
                        },
                    },
                    "required": ["requestType", "queryType", "response"],
                }
            },
        }
    },
    {
        "toolSpec": {
            "name": "query_response",
            "description": "This is the query response tool. It is used to generate a code snippet or query based on the user's request depending on the data source type and structure.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "requestType": {
                            "type": "string",
                            "description": "The request type, in this case it's always <query_response>.",
                            "maxLength": 19,
                            "nullable": False,
                            "enum": ["query_response"],
                        },
                        "dataSource": {
                            "type": "string",
                            "description": "The type of data source to be queried, such as 'csv' or 'MySQL'.",
                            "maxLength": 1000,
                            "nullable": False,
                            "enum": ["csv", "excel", "PostgreSQL", "MySQL", "MongoDB"],
                        },
                        "queryType": {
                            "type": "string",
                            "description": "The type of data source query to be executed. Python is used for CSV and Excel files, while SQL is used for MySQL databases, PostgreSQL is used for PostgreSQL databases. While for MongoDB, it uses a MongoDB query.",
                            "maxLength": 1000,
                            "nullable": False,
                            "enum": ["python", "sql", "mongodb", "postgresql"],
                        },
                        "query": {
                            "type": "string",
                            "description": "The query to be executed on the data source. e.g. a Python code snippet for CSV or Excel files, or a SQL query for databases.",
                            "maxLength": 1000,
                            "nullable": False,
                        },
                    },
                    "required": ["requestType", "dataSource", "queryType", "query"],
                }
            },
        }
    },
]

initial_tool_list = [
    {
        "toolSpec": {
            "name": "generic_response",
            "description": "Conversationally engage the user on information different from a direct request for data source query",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "requestType": {
                            "type": "string",
                            "description": "The request type, in this case it's always <generic_response>.",
                            "maxLength": 19,
                            "nullable": False,
                            "enum": ["generic_response"],
                        },
                        "response": {
                            "type": "string",
                            "description": "The response to the generic request in one sentence only. It should be concise and to the point. This is a generic response to a user's query that does not require a specific data source query.",
                            "maxLength": 1000,
                            "nullable": False,
                        },
                    },
                    "required": ["requestType", "response"],
                }
            },
        }
    },
    {
        "toolSpec": {
            "name": "query_response",
            "description": "This is the query response tool. It is used to generate a code snippet or query based on the user's request depending on the data source type and structure.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "requestType": {
                            "type": "string",
                            "description": "The request type, in this case it's always <query_response>.",
                            "maxLength": 19,
                            "nullable": False,
                            "enum": ["query_response"],
                        },
                        "response": {
                            "type": "string",
                            "description": "The response to the query that analyses what needs to be executed on the data source",
                            "maxLength": 1000,
                            "nullable": False,
                        },
                    },
                    "required": ["requestType", "response"],
                }
            },
        }
    },
]


class AIQuery:
    def __init__(
        self,
        tools=tool_list,
        initial_tool_list=initial_tool_list,
        system_prompt=DEFAULT_SYSTEM_PROMPT,
        schema_system_prompt=SCHEMA_SYSTEM_PROMPT,
        initial_system_prompt=INITIAL_SYSTEM_PROMPT,
        final_system_prompt=FINAL_SYSTEM_PROMPT,
    ):
        self.tools = tools
        self.sys_prompts = system_prompt
        self.schema = schema_system_prompt
        self.initial_prompt = initial_system_prompt
        self.final_prompt = final_system_prompt
        self.initial_tools = initial_tool_list

    def initial_processor(self, context, memory):
        try:
            message = {
                "role": "user",
                "content": [
                    {
                        "text": f"<context>{context}</context> \n. Be accurate and precise with your response."
                    },
                    {
                        "text": f"Do not generate a ficticious analysis of the user's query and context, only generate a response based on the user's query and the context {memory} provided."
                    },
                ],
            }

            response = bedrock.converse(
                modelId=modelId,
                messages=[message],
                system=[
                    {"text": self.initial_prompt},
                ],
                inferenceConfig={
                    "temperature": 0,
                },
                toolConfig={
                    "tools": self.initial_tools  # Use initial tools for the first interaction
                },
            )

            response_message = response["output"]["message"]
            response_content_blocks = response_message["content"]
            content_block = next(
                (block for block in response_content_blocks if "toolUse" in block), None
            )

            if content_block is None:
                # Handle case where no toolUse block is found
                exception = True
                return exception, "No toolUse block found in response"

            tool_use_block = content_block["toolUse"]
            tool_result_dict = tool_use_block["input"]

            # Replace all "<UNKNOWN>" with None in the dictionary
            for key, value in tool_result_dict.items():
                if value == "<UNKNOWN>":
                    tool_result_dict[key] = None

            exception = False
            return exception, tool_result_dict

        except Exception as e:
            exception = True
            return exception, str(e)

    def extract_json(self, context, schema, schema_type):  # Added 'self' parameter
        try:
            message = {
                "role": "user",
                "content": [
                    {
                        "text": f"<context>{context}</context> \n. Please use the appropriate tool to generate the right response based on the context. Use this {schema} to generate the perfect and accurate query. The type of database is: {schema_type}."  # Added schema_type
                    },
                    {
                        "text": "Do not generate a wrong query using a fictitious schema, if you can't find the required details for generating a query, use the generic_response tool to provide a response to the user's query."
                    },
                ],
            }
            response = bedrock.converse(  # Note: bedrock needs to be defined/imported
                modelId=modelId,  # Note: modelId needs to be defined
                messages=[message],
                system=[
                    {
                        "text": self.sys_prompts
                    }, 
                ],
                inferenceConfig={"maxTokens": 2000, "temperature": 0},
                toolConfig={"tools": self.tools},  # Note: tool_list needs to be defined
            )
            
            tool_use_block = response['output']['message']['content'][1]['toolUse']
            tool_result_dict = tool_use_block["input"]

            # Replace all "<UNKNOWN>" with None in the dictionary
            for key, value in tool_result_dict.items():
                if value == "<UNKNOWN>":
                    tool_result_dict[key] = None

            exception = False
            return exception, tool_result_dict

        except Exception as e:
            exception = True
            raise
            # return exception, str(e)

    def final_processor(self, user_message, context):
        try:
            message = {
                "role": "user",
                "content": [
                    {
                        "text": f"<context>{context}</context> \n. Please be precise and concise with your final response to a user query."
                    },
                    {
                        "text": f"Do not generate a fictitious analysis of the user's query: {user_message} and answer: {context}, only generate a response based on the context provided."
                    },
                ],
            }
            response = bedrock.converse(
                modelId=modelId,
                messages=[message],
                system=[
                    {"text": self.final_prompt},
                ],
                inferenceConfig={"temperature": 1},
            )

            response_message = response["output"]["message"]
            response_content_blocks = response_message["content"]
            return response_content_blocks

        except Exception as e:
            exception = True
            return exception, str(e)

    def schema_refactor(self, context):
        try:
            message = {
                "role": "user",
                "content": [
                    {
                        "text": f"<context>{context}</context> \n. Please be precise and concise with refactoring of the schema."
                    },
                    {
                        "text": "Do not modify the names of the schema fields, only give the schema in a json format."
                    },
                ],
            }
            response = bedrock.converse(
                modelId=modelId,
                messages=[message],
                system=[
                    {"text": self.schema},
                ],
                inferenceConfig={"temperature": 0},
            )

            response_message = response["output"]["message"]
            response_content_blocks = response_message["content"]
            return response_content_blocks
        except Exception as e:
            exception = True
            return exception, str(e)
    
    def agentic_call(self, message, db_creds):
        data_source_schema = db_creds["schema"]
        data_source_type = db_creds["type"]
        data_source_url = db_creds["url"]

        exception, agent_call = self.extract_json(message, data_source_schema, data_source_type)

        agent_type = agent_call.get("requestType")
        
        if agent_type == "query_response":
            data_source = agent_call.get("dataSource")
            query_type = agent_call.get("queryType")
            if data_source == "MySQL" and query_type == "sql":
                query = agent_call.get("query")
                query_result = read_from_sql_db(query, data_source_url)
                return query_result, query_type
            elif data_source == "MongoDB" and query_type == "mongodb":
                query = agent_call.get("query")
                query_result = read_from_mongo_db(query, data_source_url)
                return query_result, query_type
            elif data_source == "PostgreSQL" and query_type == "postgresql":
                query = agent_call.get("query")
                query_result = read_from_sql_db(query, data_source_url)
                print('ggg')
                return query_result, query_type
        elif agent_type == "generic_response":
            query_type = agent_call.get("queryType", "text")
            if query_type == "text":
                response = agent_call.get("response")
                return response, query_type
        else:
            raise ValueError("Invalid request type in agent call response")
        
        return None
    
    def token_count(self, text: str) -> int:
        return max(1, len(text) // 6)

