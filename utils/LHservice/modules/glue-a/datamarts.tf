# This job is used to extract the initial dump with the right schema
resource "aws_glue_job" "dbo-carga-inicial-datamart" {
  count        = length(var.table_datamart)
  name         = "dlr-job-pic-datamart-${var.table_datamart[count.index]}"
  role_arn     = var.glue_role
  connections  = [aws_glue_connection.datamart_connection.name]
  glue_version = "2.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/${var.scripts_ingestion_datamart[count.index]}"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "job for initial dump ingestion from datamarts - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  default_arguments = {
    "--S3_SOURCE"         = "s3://dlr-${terraform.workspace}-bucket-transfer/Dumps/${var.source_s3_datamart[count.index]}"
    "--S3_TARGET"         = "s3://dlr-${terraform.workspace}-bucket-refinedzone/pic/datamart/${var.table_datamart[count.index]}/"
    "--DATABASE"          = "Datamart"
    "--TABLE"             = var.tables_datamart_names[count.index]
    "--PARTITION_COL"     = var.partition_col_datamart[count.index]
    "--JDBC_USERNAME"     = var.jdbc_user
    "--JDBC_PASSWORD"     = var.jdbc_pass
    "--STRING_CONNECTION" = var.string_connection[var.env]
  }
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_trigger" "job-trigger-dbo-datamart" {
  count    = length(var.table_datamart)
  name     = "dlr-trigger-job-pic-${var.table_datamart[count.index]}"
  type     = "SCHEDULED"
  schedule = var.schedule_datamart[count.index]

  actions {
    job_name = aws_glue_job.dbo-carga-inicial-datamart[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger for initial dump ingestion from datamarts - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

}