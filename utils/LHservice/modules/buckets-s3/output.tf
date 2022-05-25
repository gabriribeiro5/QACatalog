output "kms_key" {
  value = aws_kms_key.bucket_new_key.arn
}
output "bucket_log" {
  value = aws_s3_bucket.bucket_log.id

}
output "bucket_transfer_id" {
  value = aws_s3_bucket.bucket_transfer.id
}

output "bucket_bronze_id" {
  value = aws_s3_bucket.bucket_bronze.id
}
output "bucket_silver_id" {
  value = aws_s3_bucket.bucket_silver.id
}

output "bucket_bronze_bucket" {
  value = aws_s3_bucket.bucket_bronze.bucket
}

output "bucket_silver_bucket" {
  value = aws_s3_bucket.bucket_silver.bucket
}
output "bucket_transfer_arn" {
  value = aws_s3_bucket.bucket_transfer.arn
}

output "bucket_bronze_arn" {
  value = aws_s3_bucket.bucket_bronze.arn
}

output "bucket_silver_arn" {
  value = aws_s3_bucket.bucket_silver.arn
}

output "bucket_stage_id" {
  value = aws_s3_bucket.bucket_stage.id
}

output "bucket_stage_arn" {
  value = aws_s3_bucket.bucket_stage.arn
}
