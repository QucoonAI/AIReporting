terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.1"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.2"
    }
  }
}

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = local.common_tags
  }
}

# Local values for common configurations
locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    CreatedDate = formatdate("YYYY-MM-DD", timestamp())
  }
  
  name_prefix = "${var.project_name}-${var.environment}"
}

# Data sources
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# 1. Networking Module - Creates VPC, subnets, gateways
module "networking" {
  source = "./modules/networking"
  
  project_name = var.project_name
  environment  = var.environment
  aws_region   = var.aws_region
  
  vpc_cidr             = var.vpc_cidr
  public_subnet_cidr   = var.public_subnet_cidr
  private_subnet_cidr  = var.private_subnet_cidr
  
  tags = local.common_tags
}

# 2. Database Module - Creates RDS PostgreSQL
module "database" {
  source = "./modules/database"
  
  project_name = var.project_name
  environment  = var.environment
  
  vpc_id                = module.networking.vpc_id
  private_subnet_id     = module.networking.private_subnet_id
  public_subnet_id      = module.networking.public_subnet_id
  lambda_security_group_id = module.networking.lambda_security_group_id
  
  db_username = var.rds_username
  db_password = var.rds_password
  
  tags = local.common_tags
}

# 3. Storage Module - Creates DynamoDB tables and S3 buckets
module "storage" {
  source = "./modules/storage"
  
  project_name = var.project_name
  environment  = var.environment
  
  dynamodb_point_in_time_recovery = var.dynamodb_point_in_time_recovery
  s3_versioning_enabled           = var.s3_versioning_enabled
  s3_cors_allowed_origins         = var.s3_cors_allowed_origins
  
  tags = local.common_tags
}

# 4. Container Registry Module - Creates ECR repository
module "container_registry" {
  source = "./modules/container-registry"
  
  project_name = var.project_name
  environment  = var.environment
  
  ecr_image_scan_on_push = var.ecr_image_scan_on_push
  
  tags = local.common_tags
}

# 5. Compute Module - Creates Lambda function and related resources
module "compute" {
  source = "./modules/compute"
  
  project_name = var.project_name
  environment  = var.environment
  aws_region   = var.aws_region
  
  # Networking
  vpc_id               = module.networking.vpc_id
  private_subnet_id    = module.networking.private_subnet_id
  lambda_security_group_id = module.networking.lambda_security_group_id
  
  # Database
  rds_endpoint      = module.database.rds_endpoint
  rds_database_name = module.database.rds_database_name
  rds_username      = var.rds_username
  
  # Storage
  dynamodb_chat_sessions_table_name = module.storage.dynamodb_chat_sessions_table_name
  dynamodb_messages_table_name      = module.storage.dynamodb_messages_table_name
  dynamodb_chat_sessions_table_arn  = module.storage.dynamodb_chat_sessions_table_arn
  dynamodb_messages_table_arn       = module.storage.dynamodb_messages_table_arn
  s3_data_sources_bucket_name       = module.storage.s3_data_sources_bucket_name
  s3_profile_avatars_bucket_name     = module.storage.s3_profile_avatars_bucket_name
  s3_data_sources_bucket_arn         = module.storage.s3_data_sources_bucket_arn
  s3_profile_avatars_bucket_arn      = module.storage.s3_profile_avatars_bucket_arn
  
  # Container Registry
  ecr_repository_url = module.container_registry.repository_url
  
  # Lambda Configuration
  lambda_timeout                    = var.lambda_timeout
  lambda_memory_size               = var.lambda_memory_size
  use_container_image              = var.use_container_image
  ecr_image_uri                    = var.ecr_image_uri
  enable_lambda_function_url       = var.enable_lambda_function_url
  lambda_function_url_cors_origins = var.lambda_function_url_cors_origins
  lambda_function_url_cors_methods = var.lambda_function_url_cors_methods
  log_retention_days               = var.log_retention_days
  
  # Application Environment Variables
  secret_key        = var.secret_key
  database_url      = var.database_url
  redis_url         = var.redis_url
  sendgrid_auth_key = var.sendgrid_auth_key
  aws_account_id    = var.aws_account_id
  aws_access_key_id = var.aws_access_key_id
  aws_secret_access_key = var.aws_secret_access_key
  
  tags = local.common_tags
  
  depends_on = [
    module.networking,
    module.database,
    module.storage,
    module.container_registry
  ]
}

# 6. API Gateway Module (Optional)
module "api_gateway" {
  count  = var.enable_api_gateway ? 1 : 0
  source = "./modules/api-gateway"
  
  project_name = var.project_name
  environment  = var.environment
  
  lambda_function_name       = module.compute.lambda_function_name
  lambda_function_arn        = module.compute.lambda_function_arn
  lambda_function_invoke_arn = module.compute.lambda_function_invoke_arn
  log_retention_days         = var.log_retention_days
  
  tags = local.common_tags
  
  depends_on = [module.compute]
}

