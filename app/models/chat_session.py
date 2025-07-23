from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute, NumberAttribute, BooleanAttribute, 
    UTCDateTimeAttribute, JSONAttribute
)
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection


class DataSourceIndex(GlobalSecondaryIndex):
    """
    GSI for querying sessions by data source
    """
    class Meta:
        index_name = 'GSI1-DataSource-Sessions'
        projection = AllProjection()
        
    gsi1_pk = UnicodeAttribute(hash_key=True)
    gsi1_sk = UnicodeAttribute(range_key=True)


class ChatSessionModel(Model):
    """
    DynamoDB model for chat sessions
    """
    class Meta:
        table_name = 'ChatSessions'
        region = 'us-east-1'  # Configure as needed
        
    # Primary key
    pk = UnicodeAttribute(hash_key=True)  # USER#{user_id}
    sk = UnicodeAttribute(range_key=True)  # SESSION#{session_id}
    
    # GSI for data source queries
    gsi1_pk = UnicodeAttribute()  # DATASOURCE#{data_source_id}
    gsi1_sk = UnicodeAttribute()  # SESSION#{session_id}
    data_source_index = DataSourceIndex()
    
    # Session attributes
    session_id = UnicodeAttribute()
    user_id = NumberAttribute()
    data_source_id = NumberAttribute()
    data_source_name = UnicodeAttribute()
    data_source_type = UnicodeAttribute()
    title = UnicodeAttribute()
    is_active = BooleanAttribute(default=True)
    status = UnicodeAttribute(default='active')
    message_count = NumberAttribute(default=0)
    active_message_count = NumberAttribute(default=0)
    total_tokens = NumberAttribute(default=0)
    active_tokens = NumberAttribute(default=0)
    max_messages = NumberAttribute(default=200)
    max_tokens = NumberAttribute(default=50000)
    created_at = UTCDateTimeAttribute()
    updated_at = UTCDateTimeAttribute()
    settings = JSONAttribute(null=True)
    entity_type = UnicodeAttribute(default='ChatSession')



