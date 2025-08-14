terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# ECR Repository
resource "aws_ecr_repository" "lambda_repo" {
  name                 = "${var.project_name}-${var.environment}"
  image_tag_mutability = var.image_tag_mutability

  image_scanning_configuration {
    scan_on_push = var.ecr_image_scan_on_push
  }

  encryption_configuration {
    encryption_type = var.encryption_type
    kms_key        = var.encryption_type == "KMS" ? var.kms_key_arn : null
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-ecr"
    Type = "Container Registry"
  })
}