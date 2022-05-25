# This job is used to update tables
resource "aws_glue_job" "dbo-carga-update-ftcontacts" {
  count        = var.env == "dev" ? 0 : length(var.table_name_ftcontacts_update)
  name         = "dlr-job-pic-ftcontacts-${var.table_name_ftcontacts_update[count.index]}-${var.type_update_ftcontacts[count.index]}"
  role_arn     = var.glue_role
  connections  = [aws_glue_connection.ftcontacts_connection.name]
  glue_version = "2.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/${var.scripts_updates_ftcontacts[count.index]}"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "job to update table ftcontacts - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  default_arguments = {
    "--PATH_S3"                    = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcontacts/${var.table_name_ftcontacts_update[count.index]}/"
    "--DATABASE"                   = "FTContacts"
    "--TABLE_NAME"                 = var.table_ftcontacts_update[count.index]
    "--REFERENCE_COL"              = "${var.table_col_ftcontacts_update[count.index]}"
    "--ID_COL"                     = "${var.reference_col_ftcontacts_updates[count.index]}"
    "--TRANSIENT_PATH"             = "s3://dlr-${terraform.workspace}-bucket-transient/pic/ftcontacts/${var.table_name_ftcontacts_update[count.index]}/"
    "--LOG_PATH"                   = "s3://dlr-${terraform.workspace}-bucket-log/jobs/"
    "--IS_FULL_UPDATE"             = var.full_update_ftcontacts[count.index]
    "--JDBC_USERNAME"              = var.jdbc_user
    "--JDBC_PASSWORD"              = var.jdbc_pass
    "--STRING_CONNECTION"          = var.string_connection[var.env]
    "--LOG_PATH"                   = "s3://dlr-${terraform.workspace}-bucket-log/jobs/glue/type=ingestion/origin=pic/"
    "--PROCESS_TYPE"               = "${var.process_type_ftcontacts[count.index]}"
    "--write-shuffle-files-to-s3"  = true
    "--write-shuffle-spills-to-s3" = true
    "--conf"                       = "spark.shuffle.glue.s3ShuffleBucket=s3://dlr-${terraform.workspace}-bucket-log/jobs/glue/shuffle/"
  }
}

# This trigger calls job updates
resource "aws_glue_trigger" "job-trigger-dbo-ftcontacts-update" {
  count    = var.env == "dev" ? 0 : length(var.table_name_ftcontacts_update)
  name     = "dlr-trigger-job-pic-ftcontacts-${var.table_name_ftcontacts_update[count.index]}-updates"
  schedule = var.schedule_ftcontacts_updates[count.index]
  type     = "SCHEDULED"

  actions {
    job_name = aws_glue_job.dbo-carga-update-ftcontacts[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start job that updates table ftcontacts - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

}
