variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

# DynamoDB Configuration
variable "dynamodb_billing_mode" {
  description = "DynamoDB billing mode"
  type        = string
  default     = "PAY_PER_REQUEST"
  
  validation {
    condition     = contains(["PAY_PER_REQUEST", "PROVISIONED"], var.dynamodb_billing_mode)
    error_message = "DynamoDB billing mode must be either PAY_PER_REQUEST or PROVISIONED."
  }
}

variable "dynamodb_server_side_encryption" {
  description = "Enable DynamoDB server-side encryption"
  type        = bool
  default     = true
}

variable "dynamodb_point_in_time_recovery" {
  description = "Enable DynamoDB point-in-time recovery"
  type        = bool
  default     = true
}

# S3 Configuration
variable "s3_versioning_enabled" {
  description = "Enable S3 bucket versioning"
  type        = bool
  default     = true
}

variable "s3_encryption_algorithm" {
  description = "S3 server-side encryption algorithm"
  type        = string
  default     = "AES256"
  
  validation {
    condition     = contains(["AES256", "aws:kms"], var.s3_encryption_algorithm)
    error_message = "S3 encryption algorithm must be either AES256 or aws:kms."
  }
}

variable "s3_bucket_key_enabled" {
  description = "Enable S3 bucket key for KMS encryption"
  type        = bool
  default     = true
}

variable "s3_lifecycle_enabled" {
  description = "Enable S3 lifecycle management"
  type        = bool
  default     = true
}

variable "s3_ia_transition_days" {
  description = "Days after which objects transition to IA storage class"
  type        = number
  default     = 30
}

variable "s3_noncurrent_version_expiration_days" {
  description = "Days after which noncurrent versions expire"
  type        = number
  default     = 365
}

variable "s3_multipart_upload_abort_days" {
  description = "Days after which incomplete multipart uploads are aborted"
  type        = number
  default     = 7
}

variable "s3_cors_allowed_origins" {
  description = "CORS allowed origins for S3 buckets"
  type        = list(string)
  default     = ["*"]
}

variable "s3_cors_max_age_seconds" {
  description = "CORS max age in seconds"
  type        = number
  default     = 3000
}

variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}