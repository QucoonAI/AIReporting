resource "aws_dynamodb_table" "chat_sessions" {
  name           = "${var.project_name}-${var.environment}-ChatSessions"
  billing_mode   = var.dynamodb_billing_mode
  hash_key       = "pk"
  range_key      = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  attribute {
    name = "gsi1_pk"
    type = "S"
  }

  attribute {
    name = "gsi1_sk"
    type = "S"
  }

  global_secondary_index {
    name            = "GSI1-DataSource-Sessions"
    hash_key        = "gsi1_pk"
    range_key       = "gsi1_sk"
    projection_type = "ALL"
  }

  server_side_encryption {
    enabled = var.dynamodb_server_side_encryption
  }

  point_in_time_recovery {
    enabled = var.dynamodb_point_in_time_recovery
  }

  tags = merge(var.tags, {
    Name        = "${var.project_name}-${var.environment}-chat-sessions"
    Application = "pedigraph"
    TableType   = "ChatSessions"
  })
}

# DynamoDB Table: Messages
resource "aws_dynamodb_table" "messages" {
  name           = "${var.project_name}-${var.environment}-Messages"
  billing_mode   = var.dynamodb_billing_mode
  hash_key       = "pk"
  range_key      = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  attribute {
    name = "gsi1_pk"
    type = "S"
  }

  attribute {
    name = "gsi1_sk"
    type = "S"
  }

  global_secondary_index {
    name            = "GSI1-User-Messages"
    hash_key        = "gsi1_pk"
    range_key       = "gsi1_sk"
    projection_type = "ALL"
  }

  server_side_encryption {
    enabled = var.dynamodb_server_side_encryption
  }

  point_in_time_recovery {
    enabled = var.dynamodb_point_in_time_recovery
  }

  tags = merge(var.tags, {
    Name        = "${var.project_name}-${var.environment}-messages"
    Application = "pedigraph"
    TableType   = "Messages"
  })
}