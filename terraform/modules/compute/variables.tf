variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

# Networking Variables
variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "private_subnet_id" {
  description = "Private subnet ID for Lambda"
  type        = string
}

variable "lambda_security_group_id" {
  description = "Security group ID for Lambda"
  type        = string
}

# Database Variables
variable "rds_endpoint" {
  description = "RDS endpoint"
  type        = string
}

variable "rds_database_name" {
  description = "RDS database name"
  type        = string
}

variable "rds_username" {
  description = "RDS username"
  type        = string
}

# Storage Variables
variable "dynamodb_chat_sessions_table_name" {
  description = "DynamoDB chat sessions table name"
  type        = string
}

variable "dynamodb_messages_table_name" {
  description = "DynamoDB messages table name"
  type        = string
}

variable "dynamodb_chat_sessions_table_arn" {
  description = "DynamoDB chat sessions table ARN"
  type        = string
}

variable "dynamodb_messages_table_arn" {
  description = "DynamoDB messages table ARN"
  type        = string
}

variable "s3_data_sources_bucket_name" {
  description = "S3 data sources bucket name"
  type        = string
}

variable "s3_profile_avatars_bucket_name" {
  description = "S3 profile avatars bucket name"
  type        = string
}

variable "s3_data_sources_bucket_arn" {
  description = "S3 data sources bucket ARN"
  type        = string
}

variable "s3_profile_avatars_bucket_arn" {
  description = "S3 profile avatars bucket ARN"
  type        = string
}

# Container Registry Variables
variable "ecr_repository_url" {
  description = "ECR repository URL"
  type        = string
}

# Lambda Configuration
variable "lambda_timeout" {
  description = "Lambda function timeout in seconds"
  type        = number
  default     = 30
}

variable "lambda_memory_size" {
  description = "Lambda function memory size in MB"
  type        = number
  default     = 512
}

variable "lambda_runtime" {
  description = "Lambda runtime"
  type        = string
  default     = "python3.9"
}

variable "lambda_handler" {
  description = "Lambda handler"
  type        = string
  default     = "index.handler"
}

variable "lambda_log_level" {
  description = "Lambda log level"
  type        = string
  default     = "INFO"
  
  validation {
    condition     = contains(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], var.lambda_log_level)
    error_message = "Lambda log level must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL."
  }
}

variable "use_container_image" {
  description = "Use container image for Lambda instead of zip file"
  type        = bool
  default     = true
}

variable "ecr_image_uri" {
  description = "ECR image URI for Lambda function"
  type        = string
  default     = ""
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 14
}

# Lambda Function URL Configuration
variable "enable_lambda_function_url" {
  description = "Enable Lambda Function URL"
  type        = bool
  default     = true
}

variable "lambda_function_url_auth_type" {
  description = "Lambda Function URL authorization type"
  type        = string
  default     = "NONE"
  
  validation {
    condition     = contains(["NONE", "AWS_IAM"], var.lambda_function_url_auth_type)
    error_message = "Lambda Function URL auth type must be either NONE or AWS_IAM."
  }
}

variable "lambda_function_url_cors_allow_credentials" {
  description = "Allow credentials in CORS"
  type        = bool
  default     = false
}

variable "lambda_function_url_cors_origins" {
  description = "Allowed origins for Lambda Function URL CORS"
  type        = list(string)
  default     = ["*"]
}

variable "lambda_function_url_cors_methods" {
  description = "Allowed methods for Lambda Function URL CORS"
  type        = list(string)
  default     = ["*"]
}

variable "lambda_function_url_cors_headers" {
  description = "Allowed headers for Lambda Function URL CORS"
  type        = list(string)
  default     = ["date", "keep-alive", "content-type", "authorization", "x-api-key"]
}

variable "lambda_function_url_cors_expose_headers" {
  description = "Exposed headers for Lambda Function URL CORS"
  type        = list(string)
  default     = ["date", "keep-alive"]
}

variable "lambda_function_url_cors_max_age" {
  description = "CORS max age in seconds"
  type        = number
  default     = 86400
}

# Application Environment Variables
variable "secret_key" {
  description = "Application secret key"
  type        = string
  sensitive   = true
}

variable "database_url" {
  description = "Database connection URL"
  type        = string
  sensitive   = true
  default     = ""
}

variable "redis_url" {
  description = "Redis connection URL"
  type        = string
  sensitive   = true
}

variable "sendgrid_auth_key" {
  description = "Sendgrid auth key"
  type        = string
  sensitive   = true
}

variable "aws_account_id" {
  description = "AWS account ID"
  type        = string
  sensitive   = true
}

variable "aws_access_key_id" {
  description = "AWS access key ID"
  type        = string
  sensitive   = true
}

variable "aws_secret_access_key" {
  description = "AWS secret access key"
  type        = string
  sensitive   = true
}

variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}

