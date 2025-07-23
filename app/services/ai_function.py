import boto3
session = boto3.Session()
bedrock = boto3.client(service_name='bedrock-runtime', region_name = "us-east-1" )
modelId="anthropic.claude-3-5-sonnet-20240620-v1:0"

