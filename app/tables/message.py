from typing import Dict, Any


def get_table_definition(environment: str = 'dev') -> Dict[str, Any]:
    """
    Get the Messages table definition
    
    Args:
        environment: Environment name for tagging
        
    Returns:
        Complete table definition dictionary
    """
    return {
        'TableName': 'Messages',
        'BillingMode': 'PAY_PER_REQUEST',
        'AttributeDefinitions': [
            {
                'AttributeName': 'pk',
                'AttributeType': 'S'  # SESSION#{session_id}
            },
            {
                'AttributeName': 'sk',
                'AttributeType': 'S'  # MSG#{message_index}#{message_id}
            },
            {
                'AttributeName': 'gsi1_pk',
                'AttributeType': 'S'  # USER#{user_id}
            },
            {
                'AttributeName': 'gsi1_sk',
                'AttributeType': 'S'  # MSG#{created_at}#{message_id}
            }
        ],
        'KeySchema': [
            {
                'AttributeName': 'pk',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'sk',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        'GlobalSecondaryIndexes': [
            {
                'IndexName': 'GSI1-User-Messages',
                'KeySchema': [
                    {
                        'AttributeName': 'gsi1_pk',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'gsi1_sk',
                        'KeyType': 'RANGE'
                    }
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            }
        ],
        'PointInTimeRecoverySpecification': {
            'PointInTimeRecoveryEnabled': True
        },
        'SSESpecification': {
            'Enabled': True
        },
        'Tags': [
            {
                'Key': 'Application',
                'Value': 'chat-application'
            },
            {
                'Key': 'Environment',
                'Value': environment
            },
            {
                'Key': 'TableType',
                'Value': 'Messages'
            }
        ]
    }

