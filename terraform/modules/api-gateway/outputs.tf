output "api_id" {
  description = "API Gateway REST API ID"
  value       = aws_api_gateway_rest_api.main.id
}

output "api_arn" {
  description = "API Gateway REST API ARN"
  value       = aws_api_gateway_rest_api.main.arn
}

output "api_execution_arn" {
  description = "API Gateway execution ARN"
  value       = aws_api_gateway_rest_api.main.execution_arn
}

output "api_url" {
  description = "API Gateway invoke URL"
  value       = aws_api_gateway_stage.main.invoke_url
}

output "stage_name" {
  description = "API Gateway stage name"
  value       = aws_api_gateway_stage.main.stage_name
}

output "stage_arn" {
  description = "API Gateway stage ARN"
  value       = aws_api_gateway_stage.main.arn
}

output "deployment_id" {
  description = "API Gateway deployment ID"
  value       = aws_api_gateway_deployment.main.id
}

output "api_key_id" {
  description = "API key ID (if API key is required)"
  value       = var.api_key_required ? aws_api_gateway_api_key.main[0].id : null
}

output "api_key_value" {
  description = "API key value (if API key is required)"
  value       = var.api_key_required ? aws_api_gateway_api_key.main[0].value : null
  sensitive   = true
}

output "usage_plan_id" {
  description = "Usage plan ID (if API key is required)"
  value       = var.api_key_required ? aws_api_gateway_usage_plan.main[0].id : null
}

output "cloudwatch_log_group_name" {
  description = "CloudWatch log group name for API Gateway"
  value       = aws_cloudwatch_log_group.api_gateway_logs.name
}

output "cloudwatch_log_group_arn" {
  description = "CloudWatch log group ARN for API Gateway"
  value       = aws_cloudwatch_log_group.api_gateway_logs.arn
}

# Quick access information
output "api_info" {
  description = "API Gateway information summary"
  value = {
    url            = aws_api_gateway_stage.main.invoke_url
    stage          = aws_api_gateway_stage.main.stage_name
    api_id         = aws_api_gateway_rest_api.main.id
    endpoint_type  = var.api_gateway_endpoint_type
    cors_enabled   = var.enable_cors
    auth_type      = var.api_authorization_type
    api_key_required = var.api_key_required
  }
}