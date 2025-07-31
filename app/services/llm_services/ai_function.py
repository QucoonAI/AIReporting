import boto3
import yaml
from app.core.utils import read_from_sql_db, read_from_mongo_db


session = boto3.Session()
bedrock = boto3.client(service_name='bedrock-runtime', region_name = "us-east-1" )
modelId="anthropic.claude-3-5-sonnet-20240620-v1:0"

with open("app/config/prompts.yaml", "r") as file:
    prompts = yaml.safe_load(file)
DEFAULT_SYSTEM_PROMPT = prompts["system_prompt"]


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
                                "enum": ["generic_response"]  
                            },
                            "queryType": {
                                "type": "string",
                                "description": "The type of query, in this case it's always <text>.",
                                "maxLength": 4,
                                "nullable": False,
                                "enum": ["text"]  
                            },
                             "response": {
                            "type": "string",
                            "description": "The response to the generic request in one sentence only. It should be concise and to the point. This is a generic response to a user's query that does not require a specific data source query.",
                            "maxLength": 1000,
                            "nullable": False
                            },
                        },
                        "required": [
                            "requestType",
                            "queryType",  
                            "response"
                        ]
                    }
                }
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
                                "enum": ["query_response"]  
                            },
                            "dataSource": {
                                "type": "string",
                                "description": "The type of data source to be queried, such as 'csv' or 'MySQL'.",
                                "maxLength": 1000,
                                "nullable": False,
                                "enum": ["csv", "excel", "PostgreSQL", "MySQL", "MongoDB"]
                            },
                            "queryType": {
                                "type": "string",
                                "description": "The type of data source query to be executed. Python is used for CSV and Excel files, while SQL is used for databases like PostgreSQL, MySQL. While for MongoDB, it uses a MongoDB query.",
                                "maxLength": 1000,
                                "nullable": False,
                                "enum": ["python", "sql", "mongodb"]
                            },
                            "query": {
                                "type": "string",
                                "description": "The query to be executed on the data source. e.g. a Python code snippet for CSV or Excel files, or a SQL query for databases.",
                                "maxLength": 1000,
                                "nullable": False
                            }
                        },
                        "required": [
                            "requestType",  
                            "dataSource",
                            "queryType",
                            "query"
                        ]
                    }
                }
            }
        }
]

class AIQuery:
    def __init__(self, tools = tool_list, system_prompt = DEFAULT_SYSTEM_PROMPT):
        self.tools = tools
        self.sys_prompts = system_prompt

    def extract_json(self, context):  # Added 'self' parameter
        try: 
            message = {
                "role": "user",
                "content": [
                    { "text": f"<context>{context}</context> \n. Please use the appropriate tool to generate the right response based on the context." },
                    {"text": "Do not generate a wrong query using a fictitious schema, if you can't find the required details for generating a query, use the generic_response tool to provide a response to the user's query."}
                ],
            }
            response = bedrock.converse(  # Note: bedrock needs to be defined/imported
                modelId=modelId,  # Note: modelId needs to be defined
                messages=[message],
                system=[
                    { "text": self.sys_prompts},  # Note: DEFAULT_SYSTEM_PROMPT needs to be defined
                ],
                inferenceConfig={
                    "maxTokens": 2000,
                    "temperature": 0
                },
                toolConfig={
                    "tools": self.tools  # Note: tool_list needs to be defined
                }
            )

            response_message = response['output']['message']
            response_content_blocks = response_message['content']

            content_block = next((block for block in response_content_blocks if 'toolUse' in block), None)
            
            if content_block is None:
                # Handle case where no toolUse block is found
                exception = True
                return exception, "No toolUse block found in response"

            tool_use_block = content_block['toolUse']
            tool_result_dict = tool_use_block['input']

            # Replace all "<UNKNOWN>" with None in the dictionary
            for key, value in tool_result_dict.items():
                if value == "<UNKNOWN>":
                    tool_result_dict[key] = None
                
            exception = False
            return exception, tool_result_dict

        except Exception as e:
            exception = True
            return exception, str(e)
        
    def agentic_call(self, message, db_creds=None):
        agent_call = self.extract_json(message) 
        agent_type = agent_call.get('requestType')
        if agent_type == "query_response":
            data_source = agent_call.get('dataSource')
            query_type = agent_call.get('queryType')
            if data_source == 'MySQL' and query_type == 'sql':
                query = agent_call.get('query')
                query_result = read_from_sql_db(query, db_creds)
                return query_result, query_type
            elif data_source == 'MongoDB' and query_type == 'mongodb':
                query = agent_call.get('query')
                query_result = read_from_mongo_db(query, db_creds)
                return query_result, query_type
            elif data_source == 'PostgreSQL' and query_type == 'sql':
                query = agent_call.get('query')
                query_result = read_from_sql_db(query, db_creds)
                return query_result, query_type
        elif agent_type == "generic_response" and query_type == 'text':
            response = agent_call.get('response')
            return response, query_type
        else:
            raise ValueError("Invalid request type in agent call response")
        return None
            
# # Example usage with proper initialization
# if __name__ == "__main__":    
#     database_schema = """inventory_id INT PRIMARY KEY AUTO_INCREMENT,
#         product_id INT NOT NULL,
#         quantity_on_hand INT NOT NULL DEFAULT 0,
#         quantity_reserved INT DEFAULT 0, -- for pending orders
#         last_restock_date DATE,
#         expiry_date DATE,
#         location VARCHAR(50), -- aisle, shelf location
#         updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"""
    
#     context = f"Hello, I need the number of products in the inventory that are currently available for sale. schema: {database_schema}"
    
#     # Create instance and call method
#     ai_query = AIQuery(tools=tool_list, system_prompt=DEFAULT_SYSTEM_PROMPT)
#     result = ai_query.extract_json(context)
#     print(result)