resource "aws_s3_bucket" "profile_avatars" {
  bucket = "${var.project_name}-${var.environment}-profile-avatars-${random_id.bucket_suffix.hex}"

  tags = merge(var.tags, {
    Name        = "${var.project_name}-${var.environment}-profile-avatars"
    Application = "pedigraph"
    Type        = "ProfileAvatars"
    AccessLevel = "Public"
  })
}

# Profile Avatars Bucket Configuration
resource "aws_s3_bucket_versioning" "profile_avatars" {
  bucket = aws_s3_bucket.profile_avatars.id
  versioning_configuration {
    status = var.s3_versioning_enabled ? "Enabled" : "Suspended"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "profile_avatars" {
  bucket = aws_s3_bucket.profile_avatars.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = var.s3_encryption_algorithm
    }
    bucket_key_enabled = var.s3_bucket_key_enabled
  }
}

# Profile avatars bucket allows public read access
resource "aws_s3_bucket_public_access_block" "profile_avatars" {
  bucket = aws_s3_bucket.profile_avatars.id

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

# Public read policy for profile avatars
resource "aws_s3_bucket_policy" "profile_avatars_public_read" {
  bucket = aws_s3_bucket.profile_avatars.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "PublicReadGetObject"
        Effect    = "Allow"
        Principal = "*"
        Action    = "s3:GetObject"
        Resource  = "${aws_s3_bucket.profile_avatars.arn}/*"
      }
    ]
  })

  depends_on = [aws_s3_bucket_public_access_block.profile_avatars]
}

# CORS configuration for profile avatars
resource "aws_s3_bucket_cors_configuration" "profile_avatars_cors" {
  bucket = aws_s3_bucket.profile_avatars.id

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "PUT", "POST", "DELETE", "HEAD"]
    allowed_origins = var.s3_cors_allowed_origins
    expose_headers  = ["ETag", "x-amz-meta-custom-header"]
    max_age_seconds = var.s3_cors_max_age_seconds
  }
}

# Profile Avatars Lifecycle Configuration
resource "aws_s3_bucket_lifecycle_configuration" "profile_avatars" {
  count  = var.s3_lifecycle_enabled ? 1 : 0
  bucket = aws_s3_bucket.profile_avatars.id

  rule {
    id     = "profile_avatars_lifecycle"
    status = "Enabled"

    filter {
      prefix = ""
    }

    noncurrent_version_expiration {
      noncurrent_days = var.s3_noncurrent_version_expiration_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = var.s3_multipart_upload_abort_days
    }
  }
}