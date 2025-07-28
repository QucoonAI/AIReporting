import boto3
import yaml
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
                             "response": {
                            "type": "string",
                            "description": "The response to the generic request in one sentence only. It should be concise and to the point. This is a generic response to a user's query that does not require a specific data source query.",
                            "maxLength": 1000,
                            "nullable": False
                            },
                        },
                        "required": [
                            "requestType",  
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
                                "description": "The type of data source to be queried, such as 'csv' or 'sql'.",
                                "maxLength": 1000,
                                "nullable": False,
                                "enum": ["csv", "excel", "PostgreSQL", "MySQL", "MongoDB"]
                            },
                            "queryType": {
                                "type": "string",
                                "description": "The type of data source query to be executed. Python is used for CSV and Excel files, while SQL is used for databases like PostgreSQL, MySQL, and MongoDB.",
                                "maxLength": 1000,
                                "nullable": False,
                                "enum": ["python", "sql"]
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


def extract_json(context):

    try: 
        message = {
                "role": "user",
                "content": [
                    { "text": f"<context>{context}</context> \n. Please use the apporiate tool to generate the right response based on the context." },
                    {"text": "Do not generate a wrong query using a ficticious schema, if you can't find the required details for generating a query, use the generic_response tool to provide a response to the user's query."}
                ],
            }

        response = bedrock.converse(
            modelId=modelId,
            messages=[message],
            system=[
                { "text": DEFAULT_SYSTEM_PROMPT },
            ],
            inferenceConfig={
                "maxTokens": 2000,
                "temperature": 0
            },
            toolConfig={
                "tools": tool_list
            }
        )


        response_message = response['output']['message']

        response_content_blocks = response_message['content']

        content_block = next((block for block in response_content_blocks if 'toolUse' in block), None)

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
        return exception,str(e)



database_schema = """inventory_id INT PRIMARY KEY AUTO_INCREMENT,
    product_id INT NOT NULL,
    quantity_on_hand INT NOT NULL DEFAULT 0,
    quantity_reserved INT DEFAULT 0, -- for pending orders
    last_restock_date DATE,
    expiry_date DATE,
    location VARCHAR(50), -- aisle, shelf location
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"""
context = (f"Hello, I need the number of products in the inventory that are currently available for sale. schema: {database_schema}")
print(extract_json(context))