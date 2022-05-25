# This job is used to validate datamarts hash
resource "aws_glue_job" "hash_validator_datamarts" {
  count                  = length(var.table_datamart_hash)
  name                   = "dlr-job-hash-validator-${var.table_datamart_hash[count.index]}"
  role_arn               = var.glue_role
  glue_version           = "2.0"
  connections            = [var.datamart_connection]

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/hash-validator-v2.py"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description: "job to validate Datamarts data by hash - glue-b module"
      Environment: var.env_tags[var.env]
    }
  )

  default_arguments = {
    "--PATH_S3"               = "s3://dlr-${terraform.workspace}-bucket-refinedzone/pic/datamart/${var.table_datamart_hash[count.index]}/"
    "--LOG_PATH"              = "s3://dlr-${terraform.workspace}-bucket-log/hash/${var.table_datamart_hash[count.index]}/"
    "--DATABASE"              = "Datamart"
    "--TABLE_NAME"            = var.hash_datamart_table[count.index]
    "--LAST_MODIFICATION_COL" = "lastmodificationdate"
    "--DATE_COL"              = var.creation_date_col[count.index]
    "--ID_COL"                = var.hash_datamart_id[count.index]
    "--JDBC_USERNAME"         = var.jdbc_user
    "--JDBC_PASSWORD"         = var.jdbc_pass
    "--STRING_CONNECTION"     = var.string_connection[var.env]
  }
}

# This trigger calls job validate hash
resource "aws_glue_trigger" "job-trigger-datamart-hash" {
  count    = length(var.table_datamart_hash)
  name     = "dlr-trigger-job-hash-validator-${var.table_datamart_hash[count.index]}"
  schedule = var.schedule_datamart_hash[count.index]
  type     = "SCHEDULED"

  actions {
    job_name = aws_glue_job.hash_validator_datamarts[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description: "trigger to start job that validate Datamarts hash - glue-b module"
      Environment: var.env_tags[var.env]
    }
  )

}
