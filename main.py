import os
import yaml
import json
import boto3
from db_files.db_agent import execute_query

# Initialize Bedrock client
session = boto3.Session()
bedrock = session.client('bedrock-runtime', region_name=os.getenv('AWS_REGION', 'us-east-1'))
model_id = os.getenv('BEDROCK_MODEL_ID', 'anthropic.claude-3-5-sonnet-20240620-v1:0')

# Load prompts
with open('prompts.yaml', 'r') as f:
    prompts = yaml.safe_load(f)
system_prompt = prompts.get('system_prompt')

# Define tool specifications
tool_specs = [
    {
        "toolSpec": {
            "name": "db_agent",
            "description": "Generate a PostgresSQL query to fetch data for analytics based on user request.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {"response": {"type": "string"}},
                    "required": ["response"]
                }
            }
        }
    },
    {
        "toolSpec": {
            "name": "generic_request",
            "description": "Generate a direct response to the user when no database call is needed.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {"response": {"type": "string"}},
                    "required": ["response"]
                }
            }
        }
    }
]


def get_coherent_response_from_model(prompt: str) -> str:
    resp = bedrock.converse(
        modelId=model_id,
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        system=[{"text": system_prompt}],
        inferenceConfig={"maxTokens": 1500, "temperature": 0}
    )
    return resp['output']['message']['content'][0]['text']


def get_response(context: str) -> str:
    """
    Main entrypoint: decide whether to call DB or give a generic answer via tool calling.
    """
    # Prepare user message
    user_message = {
        "role": "user",
        "content": [{"text": f"<context>{context}</context> Please choose the appropriate tool."}]
    }

    # Call Bedrock with toolConfig
    response = bedrock.converse(
        modelId=model_id,
        messages=[user_message],
        system=[{"text": system_prompt}],
        inferenceConfig={"maxTokens": 1500, "temperature": 0},
        toolConfig={"tools": tool_specs}
    )
    # Extract tool use
    for block in response['output']['message']['content']:
        if 'toolUse' in block:
            tool = block['toolUse']
            name = tool['name']
            user_sql = tool['input']['response']
            print(f"Tool called: {name}, SQL: {user_sql}")
            if name == 'db_agent':
                # Execute SQL and fetch results
                results = execute_query(user_sql)
                results_str = json.dumps(results, default=str)
                follow_up = (
                    f"Based on the following database results: {results_str}, "
                    f"provide a detailed answer to the original question: '{context}'"
                )
                return get_coherent_response_from_model(follow_up)
            elif name == 'generic_request':
                return tool['input']['response']
    return "I'm sorry, I couldn't process that."
