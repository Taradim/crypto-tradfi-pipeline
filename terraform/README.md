# Terraform Configuration for AWS S3

This directory contains Terraform configuration to provision AWS S3 bucket for the data pipeline.

## Prerequisites

1. AWS CLI configured with IAM user credentials
2. Terraform installed (`brew install terraform` on macOS)

## Quick Start

1. Copy the example variables file:
```bash
cp terraform.tfvars.example terraform.tfvars
```

2. Edit `terraform.tfvars` with your bucket name (must be globally unique):
```hcl
aws_region  = "eu-west-1"
bucket_name = "your-unique-bucket-name-2025"
```

3. Initialize Terraform:
```bash
cd terraform
terraform init
```

4. Review the plan:
```bash
terraform plan
```

5. Apply the configuration (creates the bucket):
```bash
terraform apply
```

6. Note the outputs (bucket name, ARN) - you'll need these for your `.env` file

## What gets created

- S3 bucket with versioning enabled
- Server-side encryption (AES256)
- Public access blocked (security best practice)

## Cost

S3 Standard storage is free for the first 5 GB and 20,000 GET requests per month (AWS Free Tier).

## Cleanup

To destroy all resources:
```bash
terraform destroy
```

**Warning**: This will delete the bucket and all its contents!

