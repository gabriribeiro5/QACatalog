# This WorkFlow is used to extract the initial dump with the right schema
resource "aws_glue_workflow" "workflow-carga-inicial-ftservices" {
  count = length(var.table_ftservices)
  name  = "dlr-workflow-ingesta-pic-ftservices-${var.table_ftservices[count.index]}"

  tags = merge(
    var.dlr_tags,
    {
      Description : "workflow to load tables from ftservices - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_job" "dbo-carga-inicial-ftservices" {
  count        = length(var.table_ftservices)
  name         = "dlr-job-pic-ftservices-${var.table_ftservices[count.index]}"
  role_arn     = var.glue_role
  connections  = [aws_glue_connection.ftservices_connection.name]
  glue_version = "2.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/${var.ftservices_script_location[count.index]}"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "job to load tables from ftservices - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  default_arguments = {
    "--JDBC_USERNAME"     = var.jdbc_user
    "--JDBC_PASSWORD"     = var.jdbc_pass
    "--STRING_CONNECTION" = var.string_connection[var.env]
    "--DATABASE"          = "FTServices"
    "--TABLE"             = var.table_name_ftservices_ingestion[count.index]
    "--S3_SOURCE"         = "s3://dlr-${terraform.workspace}-bucket-transfer/Dumps/${var.source_s3_ftservices[count.index]}"
    "--S3_TARGET"         = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftservices/${var.table_ftservices[count.index]}/"
    "--PARTITION_COL"     = var.partition_col_ftservices[count.index]
    "--QUERY"             = var.query_ftservices[count.index]
  }
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_trigger" "job-trigger-dbo-ftservices" {
  count         = length(var.table_ftservices)
  name          = "dlr-trigger-job-pic-ftservices-${var.table_ftservices[count.index]}"
  type          = "SCHEDULED"
  schedule      = var.schedule_ftservices[count.index]
  workflow_name = aws_glue_workflow.workflow-carga-inicial-ftservices[count.index].name

  actions {
    job_name = aws_glue_job.dbo-carga-inicial-ftservices[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start first crawler to load tables from ftservices - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

}

# This Crawler extracts the DataLake catalog
resource "aws_glue_crawler" "dbo-s3-crawler-ftservices" {
  count         = length(var.table_ftservices)
  database_name = aws_glue_catalog_database.ftservices_raw_database.name
  name          = "dlr-crawler-rawzone-ftservices-${var.table_ftservices[count.index]}"
  role          = var.glue_role_id
  description   = "Get schema about s3 parquet database"
  s3_target {
    path = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftservices/${var.table_ftservices[count.index]}/"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "second crawler to load tables ftservices - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

}

resource "aws_glue_trigger" "crawler-trigger-dbo-ftservices-s3" {
  count         = length(var.table_ftservices)
  name          = "dlr-trigger-crawler-rawzone-ftservices-${var.table_ftservices[count.index]}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.workflow-carga-inicial-ftservices[count.index].name

  actions {
    crawler_name = aws_glue_crawler.dbo-s3-crawler-ftservices[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start second crawler to load tables from ftservices - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  predicate {
    conditions {
      job_name = aws_glue_job.dbo-carga-inicial-ftservices[count.index].name
      state    = "SUCCEEDED"
    }
  }
}
