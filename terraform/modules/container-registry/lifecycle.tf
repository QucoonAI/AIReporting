resource "aws_ecr_lifecycle_policy" "lambda_repo_policy" {
  repository = aws_ecr_repository.lambda_repo.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last ${var.keep_production_images} production images"
        selection = {
          tagStatus     = "tagged"
          tagPrefixList = var.production_image_tags
          countType     = "imageCountMoreThan"
          countNumber   = var.keep_production_images
        }
        action = {
          type = "expire"
        }
      },
      {
        rulePriority = 2
        description  = "Keep last ${var.keep_development_images} development images"
        selection = {
          tagStatus     = "tagged"
          tagPrefixList = var.development_image_tags
          countType     = "imageCountMoreThan"
          countNumber   = var.keep_development_images
        }
        action = {
          type = "expire"
        }
      },
      {
        rulePriority = 3
        description  = "Delete untagged images after ${var.untagged_image_expiry_days} days"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = var.untagged_image_expiry_days
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}