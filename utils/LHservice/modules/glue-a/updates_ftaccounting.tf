# This job is used to update tables
resource "aws_glue_job" "dbo-carga-update-ftaccounting" {
  count        = var.env == "dev" ? 0 : length(var.table_name_ftaccounting_update)
  name         = "dlr-job-pic-ftaccounting-${var.table_name_ftaccounting_update[count.index]}-full"
  role_arn     = var.glue_role
  connections  = [aws_glue_connection.ftaccounting_connection.name]
  glue_version = "2.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/glue_a_full_v1.py"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "job to update table ftaccounting - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  default_arguments = {
    "--PATH_S3"           = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftaccounting/${var.table_name_ftaccounting_update[count.index]}/"
    "--DATABASE"          = "FTAccounting"
    "--TABLE_NAME"        = var.table_ftaccounting_update[count.index]
    "--REFERENCE_COL"     = "${var.table_col_ftaccounting_update[count.index]}"
    "--ID_COL"            = "${var.reference_col_ftaccounting_updates[count.index]}"
    "--TRANSIENT_PATH"    = "s3://dlr-${terraform.workspace}-bucket-transient/pic/ftaccounting/${var.table_name_ftaccounting_update[count.index]}/"
    "--LOG_PATH"          = "s3://dlr-${terraform.workspace}-bucket-log/jobs/"
    "--IS_FULL_UPDATE"    = var.full_update_ftaccounting[count.index]
    "--JDBC_USERNAME"     = var.jdbc_user
    "--JDBC_PASSWORD"     = var.jdbc_pass
    "--STRING_CONNECTION" = var.string_connection[var.env]
    "--LOG_PATH"          = "s3://dlr-${terraform.workspace}-bucket-log/jobs/glue/type=ingestion/origin=pic/"
    "--PROCESS_TYPE"      = "FULL"
  }
}

# This trigger calls job updates
resource "aws_glue_trigger" "job-trigger-dbo-ftaccounting-update" {
  count    = var.env == "dev" ? 0 : length(var.table_name_ftaccounting_update)
  name     = "dlr-trigger-job-pic-ftaccounting-${var.table_name_ftaccounting_update[count.index]}-updates"
  schedule = var.schedule_ftaccounting_updates[count.index]
  type     = "SCHEDULED"

  actions {
    job_name = aws_glue_job.dbo-carga-update-ftaccounting[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start job that updates table ftaccounting - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

}
