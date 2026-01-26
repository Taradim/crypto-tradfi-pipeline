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

# IAM User for host/server running Airflow DAGs (optional)
resource "aws_iam_user" "exec_user" {
  count = var.create_iam_user ? 1 : 0
  name  = var.iam_user_name != "" ? var.iam_user_name : "${var.bucket_name}-exec-user"

  tags = {
    Name        = "Airflow Host S3 Access"
    Environment = "production"
    Project     = "crypto-tradfi-pipeline"
  }
}

# IAM Access Key for the user
resource "aws_iam_access_key" "exec_user_key" {
  count = var.create_iam_user ? 1 : 0
  user  = aws_iam_user.exec_user[0].name
}

# AWS Glue Data Catalog Database for Iceberg tables
resource "aws_glue_catalog_database" "iceberg_database" {
  name        = var.glue_database_name != "" ? var.glue_database_name : "${var.bucket_name}-iceberg"
  description = "Glue Data Catalog database for Iceberg tables managed by DuckDB"

  location_uri = "s3://${aws_s3_bucket.data_lake.bucket}/"

  tags = {
    Name        = "Iceberg Database"
    Environment = "production"
    Project     = "crypto-tradfi-pipeline"
  }
}

# IAM Policy for S3 and Glue access (for Iceberg with DuckDB)
resource "aws_iam_user_policy" "exec_s3_access" {
  count  = var.create_iam_user ? 1 : 0
  name   = "${var.bucket_name}-s3-glue-access"
  user   = aws_iam_user.exec_user[0].name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartition",
          "glue:CreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:GetPartitions",
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition"
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:*:catalog",
          "arn:aws:glue:${var.aws_region}:*:database/${aws_glue_catalog_database.iceberg_database.name}",
          "arn:aws:glue:${var.aws_region}:*:table/${aws_glue_catalog_database.iceberg_database.name}/*"
        ]
      }
    ]
  })
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

output "iam_user_name" {
  description = "Name of the IAM user created for host/server access"
  value       = var.create_iam_user ? aws_iam_user.exec_user[0].name : null
  sensitive   = false
}

output "iam_access_key_id" {
  description = "Access Key ID for the IAM user (add this to .env.airflow)"
  value       = var.create_iam_user ? aws_iam_access_key.exec_user_key[0].id : null
  sensitive   = true
}

output "iam_secret_access_key" {
  description = "Secret Access Key for the IAM user (add this to .env.airflow - SAVE THIS SECURELY, it won't be shown again!)"
  value       = var.create_iam_user ? aws_iam_access_key.exec_user_key[0].secret : null
  sensitive   = true
}

output "glue_database_name" {
  description = "Name of the Glue Data Catalog database for Iceberg tables"
  value       = aws_glue_catalog_database.iceberg_database.name
}

output "glue_database_arn" {
  description = "ARN of the Glue Data Catalog database"
  value       = aws_glue_catalog_database.iceberg_database.arn
}