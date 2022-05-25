resource "aws_s3_bucket" "bucket_raw" {
  count  = var.env == "dev" ? 1 : 0
  bucket = "dlr-${terraform.workspace}-bucket-rawzone"
  acl    = "private"

  tags = merge(
    var.instance_tags_raw,
    var.dlr_tags,
    {
      Description: "s3 bucket for raw data - Athena module"
      Environment: var.env_tags[var.env]
    }
  )

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm     = "aws:kms"
      }
    }
  }
  logging {
    target_bucket = var.bucket_logs
    target_prefix = "log/"
  }

}

resource "aws_athena_database" "athena-dev" {
  count  = var.env == "dev" ? 1 : 0
  name   = "dlr_${terraform.workspace}_athena_database"
  bucket = aws_s3_bucket.bucket_raw[count.index].bucket
}