resource "aws_lambda_function_url" "main" {
  count = var.enable_lambda_function_url ? 1 : 0
  
  function_name      = aws_lambda_function.main.function_name
  authorization_type = var.lambda_function_url_auth_type

  cors {
    allow_credentials = var.lambda_function_url_cors_allow_credentials
    allow_origins     = var.lambda_function_url_cors_origins
    allow_methods     = var.lambda_function_url_cors_methods
    allow_headers     = var.lambda_function_url_cors_headers
    expose_headers    = var.lambda_function_url_cors_expose_headers
    max_age          = var.lambda_function_url_cors_max_age
  }

  depends_on = [aws_lambda_function.main]
}