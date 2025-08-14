resource "aws_s3_bucket" "data_sources" {
  bucket = "${var.project_name}-${var.environment}-data-sources-${random_id.bucket_suffix.hex}"

  tags = merge(var.tags, {
    Name        = "${var.project_name}-${var.environment}-data-sources"
    Application = "pedigraph"
    Type        = "DataSources"
    AccessLevel = "Private"
  })
}

# Data Sources Bucket Configuration
resource "aws_s3_bucket_versioning" "data_sources" {
  bucket = aws_s3_bucket.data_sources.id
  versioning_configuration {
    status = var.s3_versioning_enabled ? "Enabled" : "Suspended"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_sources" {
  bucket = aws_s3_bucket.data_sources.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = var.s3_encryption_algorithm
    }
    bucket_key_enabled = var.s3_bucket_key_enabled
  }
}

resource "aws_s3_bucket_public_access_block" "data_sources" {
  bucket = aws_s3_bucket.data_sources.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Data Sources Lifecycle Configuration
resource "aws_s3_bucket_lifecycle_configuration" "data_sources" {
  count  = var.s3_lifecycle_enabled ? 1 : 0
  bucket = aws_s3_bucket.data_sources.id

  rule {
    id     = "data_sources_lifecycle"
    status = "Enabled"

    filter {
      prefix = ""
    }

    transition {
      days          = var.s3_ia_transition_days
      storage_class = "STANDARD_IA"
    }

    noncurrent_version_transition {
      noncurrent_days = var.s3_ia_transition_days
      storage_class   = "STANDARD_IA"
    }

    noncurrent_version_expiration {
      noncurrent_days = var.s3_noncurrent_version_expiration_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = var.s3_multipart_upload_abort_days
    }
  }
}