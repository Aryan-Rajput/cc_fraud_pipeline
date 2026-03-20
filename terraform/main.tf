terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "ap-southeast-1"
}

# resource "aws_s3_bucket" "bronze" {
#   bucket = "cc-fraud-pipeline-ar"
# }

resource "aws_iam_role" "glue_role" {
  name = "GlueRole-CCFraud"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "s3_access" {
  name = "S3Policy-CCFraud"
  role = aws_iam_role.glue_role.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket",
        "s3:DeleteObject"
      ]
      Resource = [
        "arn:aws:s3:::cc-fraud-pipeline-ar",
        "arn:aws:s3:::cc-fraud-pipeline-ar/*"
      ]
    }]
  })
}

resource "aws_glue_job" "etl" {
  name         = "cc-fraud-etl"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  command {
    script_location = "s3://cc-fraud-pipeline-ar/glue-scripts/etl.py"
  }
}