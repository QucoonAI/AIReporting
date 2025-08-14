output "repository_arn" {
  description = "ECR repository ARN"
  value       = aws_ecr_repository.lambda_repo.arn
}

output "repository_name" {
  description = "ECR repository name"
  value       = aws_ecr_repository.lambda_repo.name
}

output "repository_url" {
  description = "ECR repository URL"
  value       = aws_ecr_repository.lambda_repo.repository_url
}

output "registry_id" {
  description = "ECR registry ID"
  value       = aws_ecr_repository.lambda_repo.registry_id
}

output "repository_uri_template" {
  description = "ECR repository URI template for different tags"
  value = {
    latest = "${aws_ecr_repository.lambda_repo.repository_url}:latest"
    dev    = "${aws_ecr_repository.lambda_repo.repository_url}:dev"
    prod   = "${aws_ecr_repository.lambda_repo.repository_url}:prod"
  }
}