output "dynamodb_chat_sessions_table_name" {
  description = "DynamoDB ChatSessions table name"
  value       = aws_dynamodb_table.chat_sessions.name
}

output "dynamodb_messages_table_name" {
  description = "DynamoDB Messages table name"
  value       = aws_dynamodb_table.messages.name
}

output "dynamodb_chat_sessions_table_arn" {
  description = "DynamoDB ChatSessions table ARN"
  value       = aws_dynamodb_table.chat_sessions.arn
}

output "dynamodb_messages_table_arn" {
  description = "DynamoDB Messages table ARN"
  value       = aws_dynamodb_table.messages.arn
}

# S3 Outputs
output "s3_data_sources_bucket_name" {
  description = "S3 data sources bucket name"
  value       = aws_s3_bucket.data_sources.bucket
}

output "s3_profile_avatars_bucket_name" {
  description = "S3 profile avatars bucket name"
  value       = aws_s3_bucket.profile_avatars.bucket
}

output "s3_data_sources_bucket_arn" {
  description = "S3 data sources bucket ARN"
  value       = aws_s3_bucket.data_sources.arn
}

output "s3_profile_avatars_bucket_arn" {
  description = "S3 profile avatars bucket ARN"
  value       = aws_s3_bucket.profile_avatars.arn
}

output "s3_data_sources_bucket_domain_name" {
  description = "S3 data sources bucket domain name"
  value       = aws_s3_bucket.data_sources.bucket_domain_name
}

output "s3_profile_avatars_bucket_domain_name" {
  description = "S3 profile avatars bucket domain name"
  value       = aws_s3_bucket.profile_avatars.bucket_domain_name
}

output "s3_data_sources_bucket_regional_domain_name" {
  description = "S3 data sources bucket regional domain name"
  value       = aws_s3_bucket.data_sources.bucket_regional_domain_name
}

output "s3_profile_avatars_bucket_regional_domain_name" {
  description = "S3 profile avatars bucket regional domain name"
  value       = aws_s3_bucket.profile_avatars.bucket_regional_domain_name
}