variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "lambda_function_name" {
  description = "Name of the Lambda function to integrate with"
  type        = string
}

variable "lambda_function_arn" {
  description = "ARN of the Lambda function"
  type        = string
}

variable "lambda_function_invoke_arn" {
  description = "Invoke ARN of the Lambda function"
  type        = string
}

# API Gateway Configuration
variable "api_gateway_endpoint_type" {
  description = "API Gateway endpoint type"
  type        = string
  default     = "REGIONAL"
  
  validation {
    condition     = contains(["REGIONAL", "EDGE", "PRIVATE"], var.api_gateway_endpoint_type)
    error_message = "API Gateway endpoint type must be REGIONAL, EDGE, or PRIVATE."
  }
}

variable "stage_name" {
  description = "API Gateway stage name"
  type        = string
  default     = null # Will default to var.environment if not set
}

variable "binary_media_types" {
  description = "List of binary media types supported by the REST API"
  type        = list(string)
  default     = []
}

# Authorization
variable "api_authorization_type" {
  description = "Authorization type for API methods"
  type        = string
  default     = "NONE"
  
  validation {
    condition     = contains(["NONE", "AWS_IAM", "CUSTOM", "COGNITO_USER_POOLS"], var.api_authorization_type)
    error_message = "Authorization type must be NONE, AWS_IAM, CUSTOM, or COGNITO_USER_POOLS."
  }
}

variable "authorizer_lambda_invoke_arn" {
  description = "Invoke ARN of the Lambda authorizer function"
  type        = string
  default     = null
}

variable "authorizer_lambda_role_arn" {
  description = "ARN of the IAM role for Lambda authorizer"
  type        = string
  default     = null
}

variable "authorizer_result_ttl_seconds" {
  description = "TTL of cached authorizer results in seconds"
  type        = number
  default     = 300
}

# API Key
variable "api_key_required" {
  description = "Require API key for requests"
  type        = bool
  default     = false
}

# Request Validation
variable "enable_request_validation" {
  description = "Enable request validation"
  type        = bool
  default     = false
}

variable "validate_request_body" {
  description = "Validate request body"
  type        = bool
  default     = true
}

variable "validate_request_parameters" {
  description = "Validate request parameters"
  type        = bool
  default     = true
}

# Integration Settings
variable "integration_timeout_milliseconds" {
  description = "Integration timeout in milliseconds"
  type        = number
  default     = 29000
  
  validation {
    condition     = var.integration_timeout_milliseconds >= 50 && var.integration_timeout_milliseconds <= 29000
    error_message = "Integration timeout must be between 50 and 29000 milliseconds."
  }
}

variable "proxy_request_parameters" {
  description = "Request parameters for proxy method"
  type        = map(string)
  default     = {}
}

variable "integration_request_parameters" {
  description = "Request parameters for integration"
  type        = map(string)
  default     = {}
}

# CORS Configuration
variable "enable_cors" {
  description = "Enable CORS support"
  type        = bool
  default     = true
}

variable "cors_allow_origin" {
  description = "CORS allow origin"
  type        = string
  default     = "*"
}

variable "cors_allow_methods" {
  description = "CORS allow methods"
  type        = list(string)
  default     = ["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"]
}

variable "cors_allow_headers" {
  description = "CORS allow headers"
  type        = list(string)
  default     = ["Content-Type", "X-Amz-Date", "Authorization", "X-Api-Key", "X-Amz-Security-Token"]
}

# Logging and Monitoring
variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 14
}

variable "access_log_format" {
  description = "Access log format"
  type        = string
  default     = "$requestId $ip $caller $user [$requestTime] \"$httpMethod $resourcePath $protocol\" $status $error.message $error.messageString"
}

variable "xray_tracing_enabled" {
  description = "Enable X-Ray tracing"
  type        = bool
  default     = false
}

variable "metrics_enabled" {
  description = "Enable CloudWatch metrics"
  type        = bool
  default     = true
}

variable "logging_level" {
  description = "Logging level"
  type        = string
  default     = "INFO"
  
  validation {
    condition     = contains(["OFF", "ERROR", "INFO"], var.logging_level)
    error_message = "Logging level must be OFF, ERROR, or INFO."
  }
}

variable "data_trace_enabled" {
  description = "Enable data trace"
  type        = bool
  default     = false
}

# Throttling
variable "throttle_rate_limit" {
  description = "Throttle rate limit (requests per second)"
  type        = number
  default     = 10000
}

variable "throttle_burst_limit" {
  description = "Throttle burst limit"
  type        = number
  default     = 5000
}

# Caching
variable "caching_enabled" {
  description = "Enable response caching"
  type        = bool
  default     = false
}

variable "cache_ttl_seconds" {
  description = "Cache TTL in seconds"
  type        = number
  default     = 300
}

variable "cache_key_parameters" {
  description = "Cache key parameters"
  type        = list(string)
  default     = []
}

variable "require_authorization_for_cache_control" {
  description = "Require authorization for cache control"
  type        = bool
  default     = true
}

variable "unauthorized_cache_control_header_strategy" {
  description = "Strategy for unauthorized cache control header"
  type        = string
  default     = "SUCCEED_WITH_RESPONSE_HEADER"
  
  validation {
    condition = contains([
      "FAIL_WITH_403",
      "SUCCEED_WITH_RESPONSE_HEADER",
      "SUCCEED_WITHOUT_RESPONSE_HEADER"
    ], var.unauthorized_cache_control_header_strategy)
    error_message = "Strategy must be FAIL_WITH_403, SUCCEED_WITH_RESPONSE_HEADER, or SUCCEED_WITHOUT_RESPONSE_HEADER."
  }
}

# Usage Plan (if API key is required)
variable "usage_plan_quota_limit" {
  description = "Usage plan quota limit"
  type        = number
  default     = 20000
}

variable "usage_plan_quota_period" {
  description = "Usage plan quota period"
  type        = string
  default     = "MONTH"
  
  validation {
    condition     = contains(["DAY", "WEEK", "MONTH"], var.usage_plan_quota_period)
    error_message = "Usage plan quota period must be DAY, WEEK, or MONTH."
  }
}

variable "usage_plan_throttle_rate_limit" {
  description = "Usage plan throttle rate limit"
  type        = number
  default     = 10000
}

variable "usage_plan_throttle_burst_limit" {
  description = "Usage plan throttle burst limit"
  type        = number
  default     = 5000
}

# Stage Variables
variable "stage_variables" {
  description = "Stage variables"
  type        = map(string)
  default     = {}
}

variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}