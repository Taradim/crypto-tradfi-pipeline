terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Main S3 bucket for the data lake
resource "aws_s3_bucket" "data_lake" {
  bucket = var.bucket_name

  tags = {
    Name        = "Data Engineering Pipeline"
    Environment = "production"
    Project     = "crypto-tradfi-pipeline"
  }
}

# Enable versioning for security (allows recovery of previous versions)
resource "aws_s3_bucket_versioning" "data_lake_versioning" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Default encryption (best practice)
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake_encryption" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Block public access (security)
resource "aws_s3_bucket_public_access_block" "data_lake_private" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Object ownership controls (disables ACLs - best practice)
resource "aws_s3_bucket_ownership_controls" "data_lake_ownership" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Outputs
output "bucket_name" {
  description = "Name of the created S3 bucket"
  value       = aws_s3_bucket.data_lake.id
}

output "bucket_arn" {
  description = "ARN of the created S3 bucket"
  value       = aws_s3_bucket.data_lake.arn
}

output "bucket_region" {
  description = "Region of the created S3 bucket"
  value       = aws_s3_bucket.data_lake.region
}

