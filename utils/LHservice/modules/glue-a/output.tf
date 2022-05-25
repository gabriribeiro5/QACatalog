output "datamart_database" {
  value = aws_glue_catalog_database.datamart_refined_database.name
}

output "glue_validator_dialer_arn" {
  value = aws_glue_job.job_validator_dialer.arn
}

output "glue_validator_message_arn" {
  value = aws_glue_job.job_validator_message.arn
}

output "datamart_connection" {
  value = aws_glue_connection.datamart_connection.name
}