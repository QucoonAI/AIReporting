variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "image_tag_mutability" {
  description = "Image tag mutability setting for the repository"
  type        = string
  default     = "MUTABLE"
  
  validation {
    condition     = contains(["MUTABLE", "IMMUTABLE"], var.image_tag_mutability)
    error_message = "Image tag mutability must be either MUTABLE or IMMUTABLE."
  }
}

variable "ecr_image_scan_on_push" {
  description = "Enable image scanning on push to ECR"
  type        = bool
  default     = true
}

variable "encryption_type" {
  description = "Encryption type for ECR repository"
  type        = string
  default     = "AES256"
  
  validation {
    condition     = contains(["AES256", "KMS"], var.encryption_type)
    error_message = "Encryption type must be either AES256 or KMS."
  }
}

variable "kms_key_arn" {
  description = "KMS key ARN for ECR encryption (required if encryption_type is KMS)"
  type        = string
  default     = null
}

# Lifecycle Policy Variables
variable "keep_production_images" {
  description = "Number of production images to keep"
  type        = number
  default     = 10
}

variable "keep_development_images" {
  description = "Number of development images to keep"
  type        = number
  default     = 5
}

variable "production_image_tags" {
  description = "Tag prefixes for production images"
  type        = list(string)
  default     = ["latest", "v", "prod", "release"]
}

variable "development_image_tags" {
  description = "Tag prefixes for development images"
  type        = list(string)
  default     = ["dev", "feature", "staging", "test"]
}

variable "untagged_image_expiry_days" {
  description = "Days after which untagged images expire"
  type        = number
  default     = 1
}

variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}