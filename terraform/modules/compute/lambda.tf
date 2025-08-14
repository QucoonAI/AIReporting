resource "aws_lambda_function" "main" {
  function_name = "${var.project_name}-${var.environment}"
  role         = aws_iam_role.lambda_execution_role.arn
  
  # Package Configuration
  package_type = var.use_container_image ? "Image" : "Zip"
  image_uri    = var.use_container_image ? (var.ecr_image_uri != "" ? var.ecr_image_uri : "${var.ecr_repository_url}:latest") : null
  filename     = var.use_container_image ? null : data.archive_file.lambda_zip.output_path
  handler      = var.use_container_image ? null : var.lambda_handler
  runtime      = var.use_container_image ? null : var.lambda_runtime
  
  # Performance Configuration
  timeout     = var.lambda_timeout
  memory_size = var.lambda_memory_size
  
  # VPC Configuration
  vpc_config {
    subnet_ids         = [var.private_subnet_id]
    security_group_ids = [var.lambda_security_group_id]
  }

  # Environment Variables
  environment {
    variables = merge(
      {
        ENVIRONMENT                  = var.environment
        LOG_LEVEL                   = var.lambda_log_level
        AWS_REGION                  = var.aws_region
        AWS_ACCOUNT_ID              = var.aws_account_id
      },
      # Database Variables
      {
        RDS_ENDPOINT                = var.rds_endpoint
        RDS_DATABASE_NAME           = var.rds_database_name
        RDS_USERNAME                = var.rds_username
        DATABASE_URL                = var.database_url != "" ? var.database_url : "postgresql://${var.rds_username}@${var.rds_endpoint}:5432/${var.rds_database_name}"
      },
      # Storage Variables
      {
        DYNAMODB_CHAT_SESSIONS_TABLE = var.dynamodb_chat_sessions_table_name
        DYNAMODB_MESSAGES_TABLE      = var.dynamodb_messages_table_name
        S3_DATA_SOURCES_BUCKET       = var.s3_data_sources_bucket_name
        S3_PROFILE_AVATARS_BUCKET    = var.s3_profile_avatars_bucket_name
      },
      # Application Variables
      {
        SECRET_KEY                  = var.secret_key
        REDIS_URL                   = var.redis_url
        SENDGRID_AUTH_KEY           = var.sendgrid_auth_key
        ACCESS_KEY_ID               = var.aws_access_key_id
        SECRET_ACCESS_KEY           = var.aws_secret_access_key
      }
    )
  }

  # Dependencies
  depends_on = [
    aws_iam_role_policy_attachment.lambda_basic_execution,
    aws_iam_role_policy_attachment.lambda_vpc_execution,
    aws_cloudwatch_log_group.lambda_logs,
    aws_iam_role_policy.lambda_application_policy
  ]

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}"
    Type = "Lambda"
  })

  lifecycle {
    ignore_changes = [image_uri, filename, last_modified]
  }
}
