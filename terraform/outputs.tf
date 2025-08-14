# Root outputs.tf - Aggregates outputs from all modules

# Infrastructure Outputs
output "vpc_id" {
  description = "VPC ID"
  value       = module.networking.vpc_id
}

output "private_subnet_id" {
  description = "Private subnet ID"
  value       = module.networking.private_subnet_id
}

output "public_subnet_id" {
  description = "Public subnet ID"
  value       = module.networking.public_subnet_id
}

output "availability_zone" {
  description = "Availability zone used"
  value       = module.networking.availability_zone
}

# Database Outputs
output "rds_endpoint" {
  description = "RDS instance endpoint"
  value       = module.database.rds_endpoint
}

output "rds_database_name" {
  description = "RDS database name"
  value       = module.database.rds_database_name
}

output "rds_port" {
  description = "RDS instance port"
  value       = module.database.rds_port
}

output "database_connection_string" {
  description = "Database connection string (sensitive)"
  value       = module.database.connection_string
  sensitive   = true
}

# Storage Outputs
output "dynamodb_tables" {
  description = "DynamoDB table information"
  value = {
    chat_sessions = {
      name = module.storage.dynamodb_chat_sessions_table_name
      arn  = module.storage.dynamodb_chat_sessions_table_arn
    }
    messages = {
      name = module.storage.dynamodb_messages_table_name
      arn  = module.storage.dynamodb_messages_table_arn
    }
  }
}

output "s3_buckets" {
  description = "S3 bucket information"
  value = {
    data_sources = {
      name                  = module.storage.s3_data_sources_bucket_name
      arn                   = module.storage.s3_data_sources_bucket_arn
      domain_name          = module.storage.s3_data_sources_bucket_domain_name
      regional_domain_name = module.storage.s3_data_sources_bucket_regional_domain_name
    }
    profile_avatars = {
      name                  = module.storage.s3_profile_avatars_bucket_name
      arn                   = module.storage.s3_profile_avatars_bucket_arn
      domain_name          = module.storage.s3_profile_avatars_bucket_domain_name
      regional_domain_name = module.storage.s3_profile_avatars_bucket_regional_domain_name
    }
  }
}

# Container Registry Outputs
output "ecr_repository" {
  description = "ECR repository information"
  value = {
    arn          = module.container_registry.repository_arn
    name         = module.container_registry.repository_name
    url          = module.container_registry.repository_url
    registry_id  = module.container_registry.registry_id
    uri_templates = module.container_registry.repository_uri_template
  }
}

# Lambda Outputs
output "lambda_function" {
  description = "Lambda function information"
  value = {
    name        = module.compute.lambda_function_name
    arn         = module.compute.lambda_function_arn
    invoke_arn  = module.compute.lambda_function_invoke_arn
    url         = module.compute.lambda_function_url
    url_id      = module.compute.lambda_function_url_id
  }
}

output "lambda_function_url" {
  description = "Public Lambda function URL"
  value       = module.compute.lambda_function_url
}

output "lambda_role" {
  description = "Lambda execution role information"
  value = {
    arn  = module.compute.lambda_role_arn
    name = module.compute.lambda_role_name
  }
}

# API Gateway Outputs (if enabled)
output "api_gateway" {
  description = "API Gateway information"
  value = var.enable_api_gateway ? {
    url = module.api_gateway[0].api_url
    id  = module.api_gateway[0].api_id
    stage_name = module.api_gateway[0].stage_name
  } : null
}

# Monitoring Outputs
output "cloudwatch_log_group" {
  description = "CloudWatch log group information"
  value = {
    name = module.compute.cloudwatch_log_group_name
    arn  = module.compute.cloudwatch_log_group_arn
  }
}

# CI/CD Outputs
output "github_actions" {
  description = "GitHub Actions IAM user information"
  value = {
    user_name         = module.compute.github_actions_user_name
    access_key_id     = module.compute.github_actions_access_key_id
    secret_access_key = module.compute.github_actions_secret_access_key
  }
  sensitive = true
}

# Environment Configuration Summary
output "environment_config" {
  description = "Environment configuration summary"
  value = {
    project_name = var.project_name
    environment  = var.environment
    aws_region   = var.aws_region
    vpc_cidr     = var.vpc_cidr
    
    # Service endpoints
    lambda_url     = module.compute.lambda_function_url
    rds_endpoint   = module.database.rds_endpoint
    
    # Resource counts
    dynamodb_tables = 2
    s3_buckets     = 2
    subnets        = 2
  }
}

# Quick Access URLs
output "quick_access" {
  description = "Quick access URLs and commands"
  value = {
    lambda_function_url = module.compute.lambda_function_url
    aws_console_lambda  = "https://${var.aws_region}.console.aws.amazon.com/lambda/home?region=${var.aws_region}#/functions/${module.compute.lambda_function_name}"
    aws_console_rds     = "https://${var.aws_region}.console.aws.amazon.com/rds/home?region=${var.aws_region}#database:id=${module.database.rds_identifier};is-cluster=false"
    aws_console_s3      = "https://s3.console.aws.amazon.com/s3/home?region=${var.aws_region}"
    aws_console_dynamodb = "https://${var.aws_region}.console.aws.amazon.com/dynamodbv2/home?region=${var.aws_region}#tables"
    aws_console_ecr     = "https://${var.aws_region}.console.aws.amazon.com/ecr/repositories/${module.container_registry.repository_name}/?region=${var.aws_region}"
  }
}

# Deployment Information
output "deployment_info" {
  description = "Deployment summary and next steps"
  value = {
    message = "🚀 Infrastructure deployed successfully!"
    
    next_steps = [
      "1. Configure your application with the provided environment variables",
      "2. Build and push your container image to ECR: ${module.container_registry.repository_url}",
      "3. Update Lambda function with your container image",
      "4. Test your application using the Lambda Function URL: ${module.compute.lambda_function_url}",
      "5. Set up GitHub Actions with the provided AWS credentials",
      "6. Configure your database schema in PostgreSQL"
    ]
    
    important_notes = [
      "⚠️  RDS password is set - change it for production!",
      "🔒 GitHub Actions credentials are sensitive - store them securely",
      "🌐 Lambda Function URL is public - implement authentication if needed",
      "📊 Monitor costs in AWS Cost Explorer",
      "🔄 Set up automated backups for production environments"
    ]
  }
}