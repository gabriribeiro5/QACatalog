# This job is used to update tables
resource "aws_glue_job" "dbo-carga-update-ft5l" {
  count        = var.env == "dev" ? 0 : length(var.table_name_ft5l_update)
  name         = "dlr-job-pic-ft5l-${var.table_name_ft5l_update[count.index]}-${var.type_update_ft5l[count.index]}"
  role_arn     = var.glue_role
  connections  = [aws_glue_connection.ft5l_connection.name]
  glue_version = "2.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/${var.scripts_updates_ft5l[count.index]}"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "job to update table ft5l - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  default_arguments = {
    "--PATH_S3"           = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ft5l/${var.table_name_ft5l_update[count.index]}/"
    "--DATABASE"          = "FT5L"
    "--TABLE_NAME"        = var.table_ft5l_update[count.index]
    "--REFERENCE_COL"     = "${var.table_col_ft5l_update[count.index]}"
    "--ID_COL"            = "${var.reference_col_ft5l_updates[count.index]}"
    "--TRANSIENT_PATH"    = "s3://dlr-${terraform.workspace}-bucket-transient/pic/ft5l/${var.table_name_ft5l_update[count.index]}/"
    "--LOG_PATH"          = "s3://dlr-${terraform.workspace}-bucket-log/jobs/"
    "--IS_FULL_UPDATE"    = var.full_update_ft5l[count.index]
    "--JDBC_USERNAME"     = var.jdbc_user
    "--JDBC_PASSWORD"     = var.jdbc_pass
    "--STRING_CONNECTION" = var.string_connection[var.env]
    "--LOG_PATH"          = "s3://dlr-${terraform.workspace}-bucket-log/jobs/glue/type=ingestion/origin=pic/"
    "--PROCESS_TYPE"      = "${var.process_type_ft5l[count.index]}"
  }
}

# This trigger calls job updates
resource "aws_glue_trigger" "job-trigger-dbo-ft5l-update" {
  count    = var.env == "dev" ? 0 : length(var.table_name_ft5l_update)
  name     = "dlr-trigger-job-pic-ft5l-${var.table_name_ft5l_update[count.index]}-updates"
  schedule = var.schedule_ft5l_updates[count.index]
  type     = "SCHEDULED"

  actions {
    job_name = aws_glue_job.dbo-carga-update-ft5l[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start job that updates table ft5l - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

}