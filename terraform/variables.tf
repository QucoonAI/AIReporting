# Project Configuration
variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "pedigraph"
  
  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.project_name))
    error_message = "Project name must contain only lowercase letters, numbers, and hyphens."
  }
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
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

# Networking Configuration
variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
  
  validation {
    condition     = can(cidrhost(var.vpc_cidr, 0))
    error_message = "VPC CIDR must be a valid IPv4 CIDR block."
  }
}

variable "public_subnet_cidr" {
  description = "CIDR block for public subnet"
  type        = string
  default     = "10.0.1.0/24"
}

variable "private_subnet_cidr" {
  description = "CIDR block for private subnet"
  type        = string
  default     = "10.0.10.0/24"
}

# Database Configuration
variable "rds_username" {
  description = "RDS master username"
  type        = string
  default     = "dbadmin"
  
  validation {
    condition     = length(var.rds_username) >= 1 && length(var.rds_username) <= 16
    error_message = "RDS username must be between 1 and 16 characters."
  }
}

variable "rds_password" {
  description = "RDS master password"
  type        = string
  sensitive   = true
  
  validation {
    condition     = length(var.rds_password) >= 8
    error_message = "RDS password must be at least 8 characters long."
  }
}

# Lambda Configuration
variable "lambda_timeout" {
  description = "Lambda function timeout in seconds"
  type        = number
  default     = 30
  
  validation {
    condition     = var.lambda_timeout >= 1 && var.lambda_timeout <= 900
    error_message = "Lambda timeout must be between 1 and 900 seconds."
  }
}

variable "lambda_memory_size" {
  description = "Lambda function memory size in MB"
  type        = number
  default     = 512
  
  validation {
    condition     = var.lambda_memory_size >= 128 && var.lambda_memory_size <= 10240
    error_message = "Lambda memory size must be between 128 and 10240 MB."
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

variable "enable_lambda_function_url" {
  description = "Enable Lambda Function URL"
  type        = bool
  default     = true
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

# API Gateway Configuration
variable "enable_api_gateway" {
  description = "Whether to create API Gateway resources"
  type        = bool
  default     = false
}

# Logging Configuration
variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 14
  
  validation {
    condition = contains([
      1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653
    ], var.log_retention_days)
    error_message = "Log retention days must be a valid CloudWatch retention period."
  }
}

# Container Registry Configuration
variable "ecr_image_scan_on_push" {
  description = "Enable image scanning on push to ECR"
  type        = bool
  default     = true
}

# Storage Configuration
variable "dynamodb_point_in_time_recovery" {
  description = "Enable point-in-time recovery for DynamoDB tables"
  type        = bool
  default     = true
}

variable "s3_versioning_enabled" {
  description = "Enable S3 bucket versioning"
  type        = bool
  default     = true
}

variable "s3_cors_allowed_origins" {
  description = "CORS allowed origins for S3 bucket"
  type        = list(string)
  default     = ["*"]
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

