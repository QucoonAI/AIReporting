resource "aws_api_gateway_deployment" "main" {
  rest_api_id = aws_api_gateway_rest_api.main.id

  triggers = {
    redeployment = sha1(jsonencode([
      aws_api_gateway_resource.proxy.id,
      aws_api_gateway_method.proxy.id,
      aws_api_gateway_integration.lambda.id,
      aws_api_gateway_method.proxy_root.id,
      aws_api_gateway_integration.lambda_root.id,
    ]))
  }

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [
    aws_api_gateway_method.proxy,
    aws_api_gateway_integration.lambda,
    aws_api_gateway_method.proxy_root,
    aws_api_gateway_integration.lambda_root,
    aws_lambda_permission.api_gw
  ]
}

# API Gateway Stage
resource "aws_api_gateway_stage" "main" {
  deployment_id = aws_api_gateway_deployment.main.id
  rest_api_id   = aws_api_gateway_rest_api.main.id
  stage_name    = var.stage_name

  # Access Logging
  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_gateway_logs.arn
    format = var.access_log_format
  }

  # X-Ray Tracing
  xray_tracing_enabled = var.xray_tracing_enabled

  # Throttling Settings
  throttle_settings {
    rate_limit  = var.throttle_rate_limit
    burst_limit = var.throttle_burst_limit
  }

  # Stage Variables
  variables = var.stage_variables

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-api-stage"
    Type = "API Gateway Stage"
  })
}

# Method Settings for the Stage
resource "aws_api_gateway_method_settings" "main" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  stage_name  = aws_api_gateway_stage.main.stage_name
  method_path = "*/*"

  settings {
    # Metrics and Logging
    metrics_enabled        = var.metrics_enabled
    logging_level         = var.logging_level
    data_trace_enabled    = var.data_trace_enabled
    
    # Throttling
    throttling_rate_limit  = var.throttle_rate_limit
    throttling_burst_limit = var.throttle_burst_limit
    
    # Caching
    caching_enabled      = var.caching_enabled
    cache_ttl_in_seconds = var.cache_ttl_seconds
    cache_key_parameters = var.cache_key_parameters
    
    # Request/Response Settings
    require_authorization_for_cache_control = var.require_authorization_for_cache_control
    unauthorized_cache_control_header_strategy = var.unauthorized_cache_control_header_strategy
  }
}

# API Key (if required)
resource "aws_api_gateway_api_key" "main" {
  count = var.api_key_required ? 1 : 0
  
  name        = "${var.project_name}-${var.environment}-api-key"
  description = "API key for ${var.project_name} ${var.environment}"
  enabled     = true

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-api-key"
  })
}

# Usage Plan (if API key is required)
resource "aws_api_gateway_usage_plan" "main" {
  count = var.api_key_required ? 1 : 0
  
  name         = "${var.project_name}-${var.environment}-usage-plan"
  description  = "Usage plan for ${var.project_name} ${var.environment}"

  api_stages {
    api_id = aws_api_gateway_rest_api.main.id
    stage  = aws_api_gateway_stage.main.stage_name
  }

  quota_settings {
    limit  = var.usage_plan_quota_limit
    period = var.usage_plan_quota_period
  }

  throttle_settings {
    rate_limit  = var.usage_plan_throttle_rate_limit
    burst_limit = var.usage_plan_throttle_burst_limit
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-usage-plan"
  })
}

# Usage Plan Key (associate API key with usage plan)
resource "aws_api_gateway_usage_plan_key" "main" {
  count = var.api_key_required ? 1 : 0
  
  key_id        = aws_api_gateway_api_key.main[0].id
  key_type      = "API_KEY"
  usage_plan_id = aws_api_gateway_usage_plan.main[0].id
}