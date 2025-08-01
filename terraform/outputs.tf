output "ecr_repository_uri" {
  description = "ECR repository URI"
  value       = aws_ecr_repository.lambda_repo.repository_url
}

output "ecr_repository_name" {
  description = "ECR repository name"
  value       = aws_ecr_repository.lambda_repo.name
}

output "lambda_function_name" {
  description = "Lambda function name"
  value       = aws_lambda_function.main.function_name
}

output "lambda_function_arn" {
  description = "Lambda function ARN"
  value       = aws_lambda_function.main.arn
}

output "api_gateway_url" {
  description = "API Gateway URL"
  value       = var.enable_api_gateway ? aws_api_gateway_stage.main[0].invoke_url : null
}

output "api_gateway_id" {
  description = "API Gateway REST API ID"
  value       = var.enable_api_gateway ? aws_api_gateway_rest_api.main[0].id : null
}

output "github_actions_access_key_id" {
  description = "GitHub Actions AWS Access Key ID"
  value       = aws_iam_access_key.github_actions.id
}

output "github_actions_secret_access_key" {
  description = "GitHub Actions AWS Secret Access Key"
  value       = aws_iam_access_key.github_actions.secret
  sensitive   = true
}

output "aws_region" {
  description = "AWS region"
  value       = var.aws_region
}

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

output "s3_bucket_name" {
  description = "S3 bucket name"
  value       = aws_s3_bucket.app_storage.bucket
}

output "s3_bucket_arn" {
  description = "S3 bucket ARN"
  value       = aws_s3_bucket.app_storage.arn
}

output "s3_bucket_domain_name" {
  description = "S3 bucket domain name"
  value       = aws_s3_bucket.app_storage.bucket_domain_name
}

output "ecr_image_uri_template" {
  description = "ECR image URI template for GitHub Actions"
  value       = "${aws_ecr_repository.lambda_repo.repository_url}:latest"
}

# Environment variables for application configuration
output "lambda_environment_variables" {
  description = "Environment variables configured for Lambda"
  value = {
    ENVIRONMENT                = var.environment
    DYNAMODB_CHAT_SESSIONS_TABLE = aws_dynamodb_table.chat_sessions.name
    DYNAMODB_MESSAGES_TABLE      = aws_dynamodb_table.messages.name
    REGION                   = data.aws_region.current.name
  }
}

output "deployment_summary" {
  description = "Summary of deployed resources"
  value = {
    project_name     = var.project_name
    environment      = var.environment
    aws_region      = var.aws_region
    lambda_function = aws_lambda_function.main.function_name
    api_gateway_enabled = var.enable_api_gateway
    dynamodb_tables = [
      aws_dynamodb_table.chat_sessions.name,
      aws_dynamodb_table.messages.name
    ]
  }
}

output "lambda_function_url" {
  description = "The public URL for the Lambda function"
  value       = var.enable_lambda_function_url ? aws_lambda_function_url.main[0].function_url : null
}


