
# This job is used to update tables
resource "aws_glue_job" "dbo-carga-update-ftcrm" {
  count        = var.env == "dev" ? 0 : length(var.tables_updates_ftcrm)
  name         = "dlr-job-pic-ftcrm-${var.tables_updates_ftcrm[count.index]}-${var.type_update_ftcrm[count.index]}"
  role_arn     = var.glue_role
  connections  = [aws_glue_connection.ftcrm_connection.name]
  glue_version = "2.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/${var.update_ftcrm_script_location[count.index]}"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "job to update table ftcrm - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  default_arguments = {
    "--PATH_S3"           = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/${var.tables_updates_ftcrm[count.index]}/"
    "--DATABASE"          = "FTCRM"
    "--TABLE_NAME"        = var.table_name_ftcrm_update[count.index]
    "--REFERENCE_COL"     = var.table_col_ftcrm_update[count.index]
    "--ID_COL"            = var.reference_col_ftcrm_update[count.index]
    "--TRANSIENT_PATH"    = "s3://dlr-${terraform.workspace}-bucket-transient/pic/ftcrm/${var.tables_updates_ftcrm[count.index]}/"
    "--LOG_PATH"          = "s3://dlr-${terraform.workspace}-bucket-log/jobs/"
    "--IS_FULL_UPDATE"    = var.full_update_ftcrm[count.index]
    "--JDBC_USERNAME"     = var.jdbc_user
    "--JDBC_PASSWORD"     = var.jdbc_pass
    "--STRING_CONNECTION" = var.string_connection[var.env]
    "--QUERY"             = var.update_query_ftcrm[count.index]
    "--LOG_PATH"          = "s3://dlr-${terraform.workspace}-bucket-log/jobs/glue/type=ingestion/origin=pic/"
    "--PROCESS_TYPE"      = "${var.process_type_ftcrm[count.index]}"
  }
}

# This trigger calls job updates
resource "aws_glue_trigger" "job-trigger-dbo-ftcrm-update" {
  count    = var.env == "dev" ? 0 : length(var.tables_updates_ftcrm)
  name     = "dlr-trigger-job-pic-ftcrm-${var.tables_updates_ftcrm[count.index]}-updates"
  schedule = var.schedule_ftcrm_updates[count.index]
  type     = "SCHEDULED"

  actions {
    job_name = aws_glue_job.dbo-carga-update-ftcrm[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start job that updates table ftcrm - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

}
