output "lambda_function_name" {
  description = "Lambda function name"
  value       = aws_lambda_function.main.function_name
}

output "lambda_function_arn" {
  description = "Lambda function ARN"
  value       = aws_lambda_function.main.arn
}

output "lambda_function_invoke_arn" {
  description = "Lambda function invoke ARN"
  value       = aws_lambda_function.main.invoke_arn
}

output "lambda_function_url" {
  description = "Lambda function URL"
  value       = var.enable_lambda_function_url ? aws_lambda_function_url.main[0].function_url : null
}

output "lambda_function_url_id" {
  description = "Lambda function URL ID"
  value       = var.enable_lambda_function_url ? aws_lambda_function_url.main[0].url_id : null
}

output "lambda_role_arn" {
  description = "Lambda execution role ARN"
  value       = aws_iam_role.lambda_execution_role.arn
}

output "lambda_role_name" {
  description = "Lambda execution role name"
  value       = aws_iam_role.lambda_execution_role.name
}

output "cloudwatch_log_group_name" {
  description = "CloudWatch log group name"
  value       = aws_cloudwatch_log_group.lambda_logs.name
}

output "cloudwatch_log_group_arn" {
  description = "CloudWatch log group ARN"
  value       = aws_cloudwatch_log_group.lambda_logs.arn
}

# GitHub Actions Outputs
output "github_actions_user_name" {
  description = "GitHub Actions IAM user name"
  value       = aws_iam_user.github_actions.name
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

