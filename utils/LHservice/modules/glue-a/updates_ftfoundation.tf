# This job is used to update tables
resource "aws_glue_job" "dbo-carga-update-ftfoundation" {
  count        = var.env == "dev" ? 0 : length(var.table_ftfoundation)
  name         = "dlr-job-pic-ftfoundation-${var.table_ftfoundation[count.index]}-full"
  role_arn     = var.glue_role
  connections  = [aws_glue_connection.ftfoundation_connection.name]
  glue_version = "2.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/glue_a_full_v1.py"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "job to update table ftfoundation - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  default_arguments = {
    "--PATH_S3"           = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftfoundation/${var.table_ftfoundation[count.index]}/"
    "--DATABASE"          = "FTFoundation"
    "--TABLE_NAME"        = var.table_name_ftfoundation_update[count.index]
    "--REFERENCE_COL"     = var.table_col_ftfoundation_update[count.index]
    "--ID_COL"            = var.reference_col_ftfoundation[count.index]
    "--TRANSIENT_PATH"    = "s3://dlr-${terraform.workspace}-bucket-transient/pic/ftfoundation/${var.table_ftfoundation[count.index]}/"
    "--LOG_PATH"          = "s3://dlr-${terraform.workspace}-bucket-log/jobs/"
    "--IS_FULL_UPDATE"    = var.full_update_ftfoundation[count.index]
    "--JDBC_USERNAME"     = var.jdbc_user
    "--JDBC_PASSWORD"     = var.jdbc_pass
    "--STRING_CONNECTION" = var.string_connection[var.env]
    "--LOG_PATH"          = "s3://dlr-${terraform.workspace}-bucket-log/jobs/glue/type=ingestion/origin=pic/"
    "--PROCESS_TYPE"      = "UPDATE"
  }
}

# This trigger calls job updates
resource "aws_glue_trigger" "job-trigger-dbo-ftfoundation-update" {
  count    = var.env == "dev" ? 0 : length(var.table_ftfoundation)
  name     = "dlr-trigger-job-pic-ftfoundation-${var.table_ftfoundation[count.index]}-updates"
  schedule = var.schedule_ftfoundation_updates[count.index]
  type     = "SCHEDULED"

  actions {
    job_name = aws_glue_job.dbo-carga-update-ftfoundation[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start job that updates table ftfoundation - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

}
