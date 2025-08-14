terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# CloudWatch Log Group for API Gateway
resource "aws_cloudwatch_log_group" "api_gateway_logs" {
  name              = "/aws/apigateway/${var.project_name}-${var.environment}"
  retention_in_days = var.log_retention_days

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-api-logs"
    Type = "API Gateway Logs"
  })
}

# REST API
resource "aws_api_gateway_rest_api" "main" {
  name        = "${var.project_name}-${var.environment}-api"
  description = "API Gateway for ${var.project_name} ${var.environment} environment"

  endpoint_configuration {
    types = [var.api_gateway_endpoint_type]
  }

  binary_media_types = var.binary_media_types

  tags = merge(var.tags, {
    Name = "${var.project_name}-${var.environment}-api"
    Type = "API Gateway"
  })
}

# API Gateway Request Validator
resource "aws_api_gateway_request_validator" "main" {
  count = var.enable_request_validation ? 1 : 0
  
  name                        = "${var.project_name}-${var.environment}-validator"
  rest_api_id                = aws_api_gateway_rest_api.main.id
  validate_request_body       = var.validate_request_body
  validate_request_parameters = var.validate_request_parameters
}

# Proxy Resource
resource "aws_api_gateway_resource" "proxy" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  parent_id   = aws_api_gateway_rest_api.main.root_resource_id
  path_part   = "{proxy+}"
}

# Proxy Method
resource "aws_api_gateway_method" "proxy" {
  rest_api_id   = aws_api_gateway_rest_api.main.id
  resource_id   = aws_api_gateway_resource.proxy.id
  http_method   = "ANY"
  authorization = var.api_authorization_type
  authorizer_id = var.api_authorization_type == "CUSTOM" ? aws_api_gateway_authorizer.lambda_authorizer[0].id : null

  api_key_required = var.api_key_required

  request_validator_id = var.enable_request_validation ? aws_api_gateway_request_validator.main[0].id : null

  request_parameters = var.proxy_request_parameters
}

# Proxy Integration
resource "aws_api_gateway_integration" "lambda" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  resource_id = aws_api_gateway_method.proxy.resource_id
  http_method = aws_api_gateway_method.proxy.http_method

  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = var.lambda_function_invoke_arn

  timeout_milliseconds = var.integration_timeout_milliseconds

  request_parameters = var.integration_request_parameters
}

# Root Method (for requests to the root path)
resource "aws_api_gateway_method" "proxy_root" {
  rest_api_id   = aws_api_gateway_rest_api.main.id
  resource_id   = aws_api_gateway_rest_api.main.root_resource_id
  http_method   = "ANY"
  authorization = var.api_authorization_type
  authorizer_id = var.api_authorization_type == "CUSTOM" ? aws_api_gateway_authorizer.lambda_authorizer[0].id : null

  api_key_required = var.api_key_required

  request_validator_id = var.enable_request_validation ? aws_api_gateway_request_validator.main[0].id : null
}

# Root Integration
resource "aws_api_gateway_integration" "lambda_root" {
  rest_api_id = aws_api_gateway_rest_api.main.id
  resource_id = aws_api_gateway_method.proxy_root.resource_id
  http_method = aws_api_gateway_method.proxy_root.http_method

  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = var.lambda_function_invoke_arn

  timeout_milliseconds = var.integration_timeout_milliseconds
}

# Lambda Authorizer (if using custom authorization)
resource "aws_api_gateway_authorizer" "lambda_authorizer" {
  count = var.api_authorization_type == "CUSTOM" ? 1 : 0
  
  name                   = "${var.project_name}-${var.environment}-authorizer"
  rest_api_id           = aws_api_gateway_rest_api.main.id
  authorizer_uri        = var.authorizer_lambda_invoke_arn
  authorizer_credentials = var.authorizer_lambda_role_arn

  type                         = "TOKEN"
  authorizer_result_ttl_in_seconds = var.authorizer_result_ttl_seconds
  identity_source              = "method.request.header.Authorization"
}

# CORS Support (if enabled)
resource "aws_api_gateway_method" "options_proxy" {
  count = var.enable_cors ? 1 : 0
  
  rest_api_id   = aws_api_gateway_rest_api.main.id
  resource_id   = aws_api_gateway_resource.proxy.id
  http_method   = "OPTIONS"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "options_proxy" {
  count = var.enable_cors ? 1 : 0
  
  rest_api_id = aws_api_gateway_rest_api.main.id
  resource_id = aws_api_gateway_resource.proxy.id
  http_method = aws_api_gateway_method.options_proxy[0].http_method

  type = "MOCK"

  request_templates = {
    "application/json" = "{\"statusCode\": 200}"
  }
}

resource "aws_api_gateway_method_response" "options_proxy" {
  count = var.enable_cors ? 1 : 0
  
  rest_api_id = aws_api_gateway_rest_api.main.id
  resource_id = aws_api_gateway_resource.proxy.id
  http_method = aws_api_gateway_method.options_proxy[0].http_method
  status_code = "200"

  response_headers = {
    "Access-Control-Allow-Headers" = true
    "Access-Control-Allow-Methods" = true
    "Access-Control-Allow-Origin"  = true
  }
}

resource "aws_api_gateway_integration_response" "options_proxy" {
  count = var.enable_cors ? 1 : 0
  
  rest_api_id = aws_api_gateway_rest_api.main.id
  resource_id = aws_api_gateway_resource.proxy.id
  http_method = aws_api_gateway_method.options_proxy[0].http_method
  status_code = "200"

  response_headers = {
    "Access-Control-Allow-Headers" = "'${join(",", var.cors_allow_headers)}'"
    "Access-Control-Allow-Methods" = "'${join(",", var.cors_allow_methods)}'"
    "Access-Control-Allow-Origin"  = "'${var.cors_allow_origin}'"
  }

  depends_on = [aws_api_gateway_integration.options_proxy]
}

# Lambda Permission for API Gateway
resource "aws_lambda_permission" "api_gw" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = var.lambda_function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.main.execution_arn}/*/*"
}