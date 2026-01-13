variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "eu-west-1"
}

variable "bucket_name" {
  description = "Name of the S3 bucket (must be globally unique)"
  type        = string
  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]*[a-z0-9]$", var.bucket_name))
    error_message = "Bucket name must be lowercase, alphanumeric with hyphens, and between 3-63 characters."
  }
}

variable "create_iam_user" {
  description = "Whether to create an IAM user with S3 access for the host/server running Airflow DAGs"
  type        = bool
  default     = true
}

variable "iam_user_name" {
  description = "Name of the IAM user to create (defaults to {bucket_name}-exec-user)"
  type        = string
  default     = ""
}

