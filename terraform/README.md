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

6. Note the outputs (bucket name, ARN) - you'll need these for your `.env.airflow` file

## What gets created

- S3 bucket with versioning enabled
- Server-side encryption (AES256)
- Public access blocked (security best practice)
- **Glue Data Catalog database** (default name: `data_pipeline_portfolio`) for dbt external tables and Athena
- **IAM User** (optional, enabled by default) with S3 + Glue permissions for Airflow/dbt
- **IAM Access Keys** for the user (outputs are marked as sensitive)

## IAM User for Host/Server

By default, Terraform creates an IAM user with permissions limited to your S3 bucket. This is more secure than using your main AWS account credentials. Use these credentials on the host/server where you run your Airflow DAGs.

After running `terraform apply`, you'll get:
- `iam_access_key_id`: The Access Key ID
- `iam_secret_access_key`: The Secret Access Key (⚠️ **SAVE THIS IMMEDIATELY**, it won't be shown again!)

Add these to your `.env.airflow` file:
```bash
AWS_ACCESS_KEY_ID=<from terraform output>
AWS_SECRET_ACCESS_KEY=<from terraform output>
```

**If your IAM user (e.g. `data-pipeline-user`) already exists and was not created by this Terraform**, attach Glue permissions manually:

1. Edit `terraform/glue-policy-for-existing-user.json`: replace `ACCOUNT_ID` with your AWS account ID (e.g. `315435443947`).
2. Create the policy in IAM: AWS Console → IAM → Policies → Create policy → JSON → paste the content → Name it e.g. `DataPipelineGlueAccess`.
3. Attach the policy to your user: IAM → Users → data-pipeline-user → Add permissions → Attach policies → select `DataPipelineGlueAccess`.

Or via AWS CLI (replace ACCOUNT_ID and REGION):
```bash
aws iam put-user-policy --user-name data-pipeline-user --policy-name GlueCatalogAccess --policy-document file://terraform/glue-policy-for-existing-user.json
```
(Note: put-user-policy uses inline policy; for the JSON you must replace ACCOUNT_ID in the file first.)

To use an existing IAM user name with Terraform (so Terraform manages the policy), set in `terraform.tfvars`:
```hcl
iam_user_name = "data-pipeline-user"
```
Then run `terraform import aws_iam_user.exec_user[0] data-pipeline-user` before `terraform apply`.

To disable IAM user creation, set in `terraform.tfvars`:
```hcl
create_iam_user = false
```

Then use your existing AWS credentials from your main `.env` file instead.

## Cost

S3 Standard storage is free for the first 5 GB and 20,000 GET requests per month (AWS Free Tier).

## Getting the IAM Credentials

After `terraform apply`, get the credentials:

```bash
# Get the Access Key ID
terraform output -raw iam_access_key_id

# Get the Secret Access Key (SAVE THIS IMMEDIATELY!)
terraform output -raw iam_secret_access_key
```

⚠️ **Important**: The secret access key is only shown once. Save it immediately to your `.env.airflow` file!

## Cleanup

To destroy all resources:
```bash
terraform destroy
```

**Warning**: This will delete the bucket, all its contents, and the IAM user!

