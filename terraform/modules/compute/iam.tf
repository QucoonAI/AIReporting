resource "aws_iam_role" "lambda_execution_role" {
  name = "${var.project_name}-${var.environment}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-lambda-role"
    Type = "IAM"
  })
}

# Basic Lambda Execution Policy
resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role       = aws_iam_role.lambda_execution_role.name
}

# VPC Access Policy
resource "aws_iam_role_policy_attachment" "lambda_vpc_execution" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
  role       = aws_iam_role.lambda_execution_role.name
}

# Comprehensive Application Policy
resource "aws_iam_role_policy" "lambda_application_policy" {
  name = "${var.project_name}-${var.environment}-lambda-application-policy"
  role = aws_iam_role.lambda_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # CloudWatch Logs
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = [
          aws_cloudwatch_log_group.lambda_logs.arn,
          "${aws_cloudwatch_log_group.lambda_logs.arn}:*"
        ]
      },
      # DynamoDB Access
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Query",
          "dynamodb:Scan",
          "dynamodb:BatchGetItem",
          "dynamodb:BatchWriteItem"
        ]
        Resource = [
          var.dynamodb_chat_sessions_table_arn,
          var.dynamodb_messages_table_arn,
          "${var.dynamodb_chat_sessions_table_arn}/index/*",
          "${var.dynamodb_messages_table_arn}/index/*"
        ]
      },
      # S3 Access
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:GetObjectVersion",
          "s3:DeleteObjectVersion"
        ]
        Resource = [
          "${var.s3_data_sources_bucket_arn}/*",
          "${var.s3_profile_avatars_bucket_arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:GetBucketVersioning"
        ]
        Resource = [
          var.s3_data_sources_bucket_arn,
          var.s3_profile_avatars_bucket_arn
        ]
      },
      # RDS Connect (for IAM database authentication if needed)
      {
        Effect = "Allow"
        Action = [
          "rds-db:connect"
        ]
        Resource = "arn:aws:rds-db:${var.aws_region}:${data.aws_caller_identity.current.account_id}:dbuser:${var.project_name}-${var.environment}-postgres/${var.rds_username}"
      }
    ]
  })
}

# GitHub Actions IAM User and Policy
resource "aws_iam_user" "github_actions" {
  name = "${var.project_name}-${var.environment}-github-actions"

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-github-actions"
    Type = "CI/CD"
  })
}

resource "aws_iam_user_policy" "github_actions_policy" {
  name = "${var.project_name}-${var.environment}-github-actions-policy"
  user = aws_iam_user.github_actions.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # ECR Access
      {
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
          "ecr:InitiateLayerUpload",
          "ecr:UploadLayerPart",
          "ecr:CompleteLayerUpload",
          "ecr:PutImage"
        ]
        Resource = "arn:aws:ecr:${var.aws_region}:${data.aws_caller_identity.current.account_id}:repository/${var.project_name}-${var.environment}"
      },
      # Lambda Access
      {
        Effect = "Allow"
        Action = [
          "lambda:UpdateFunctionCode",
          "lambda:GetFunction",
          "lambda:GetFunctionConfiguration"
        ]
        Resource = aws_lambda_function.main.arn
      }
    ]
  })
}

resource "aws_iam_access_key" "github_actions" {
  user = aws_iam_user.github_actions.name
}
