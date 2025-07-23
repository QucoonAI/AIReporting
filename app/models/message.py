from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute, NumberAttribute, BooleanAttribute, 
    UTCDateTimeAttribute, JSONAttribute
)
from pynamodb.indexes import GlobalSecondaryIndex, AllProjection


class UserMessageIndex(GlobalSecondaryIndex):
    """
    GSI for querying messages by user
    """
    class Meta:
        index_name = 'GSI1-User-Messages'
        projection = AllProjection()
        
    gsi1_pk = UnicodeAttribute(hash_key=True)
    gsi1_sk = UnicodeAttribute(range_key=True)


class MessageModel(Model):
    """
    DynamoDB model for chat messages
    """
    class Meta:
        table_name = 'Messages'
        region = 'us-east-1'  # Configure as needed
        
    # Primary key
    pk = UnicodeAttribute(hash_key=True)  # SESSION#{session_id}
    sk = UnicodeAttribute(range_key=True)  # MSG#{timestamp}#{message_id}
    
    # GSI for user queries
    gsi1_pk = UnicodeAttribute()  # USER#{user_id}
    gsi1_sk = UnicodeAttribute()  # MSG#{timestamp}#{message_id}
    user_message_index = UserMessageIndex()
    
    # Message attributes
    message_id = UnicodeAttribute()
    session_id = UnicodeAttribute()
    user_id = NumberAttribute()
    role = UnicodeAttribute()  # 'user' or 'assistant'
    content = UnicodeAttribute()
    message_index = NumberAttribute()
    is_active = BooleanAttribute(default=True)
    is_edited = BooleanAttribute(default=False)
    parent_message_id = UnicodeAttribute(null=True)
    version = NumberAttribute(default=1)
    token_count = NumberAttribute(null=True)
    model_used = UnicodeAttribute(null=True)
    processing_time_ms = NumberAttribute(null=True)
    created_at = UTCDateTimeAttribute()
    edited_at = UTCDateTimeAttribute(null=True)
    archived_at = UTCDateTimeAttribute(null=True)
    archive_reason = UnicodeAttribute(null=True)
    metadata = JSONAttribute(null=True)
    entity_type = UnicodeAttribute(default='Message')


