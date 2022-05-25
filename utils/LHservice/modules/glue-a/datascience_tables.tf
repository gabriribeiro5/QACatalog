# This workflow is used to extract the initial dump with the right schema
resource "aws_glue_workflow" "workflow-carga-inicial-datascience" {
  count = length(var.table_datascience)
  name  = "dlr-workflow-ingesta-pic-datascience-${var.table_datascience[count.index]}"

  tags = merge(
    var.dlr_tags,
    {
      Description : "workflow to load tables from data science - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_crawler" "dbo-pic-crawler-datascience" {
  count         = length(var.table_datascience)
  database_name = aws_glue_catalog_database.datascience_pic_database.name
  name          = "dlr-crawler-pic-datascience-${var.table_datascience[count.index]}"
  role          = var.glue_role_id
  description   = "Get schema about datascience database"

  jdbc_target {
    connection_name = aws_glue_connection.datascience_connection.name
    path            = var.jdbc_target_datascience[count.index]
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "first crawler to load tables from data science - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_trigger" "crawler-trigger-dbo-datascience" {
  count         = length(var.table_datascience)
  name          = "dlr-trigger-crawler-pic-datascience-${var.table_datascience[count.index]}"
  type          = "SCHEDULED"
  schedule      = var.schedule_datascience[count.index]
  workflow_name = aws_glue_workflow.workflow-carga-inicial-datascience[count.index].name

  actions {
    crawler_name = aws_glue_crawler.dbo-pic-crawler-datascience[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start first crawler to load tables from data science - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_job" "dbo-carga-inicial-datascience" {
  count        = length(var.table_datascience)
  name         = "dlr-job-pic-datascience-${var.table_datascience[count.index]}"
  role_arn     = var.glue_role
  connections  = [aws_glue_connection.datascience_connection.name]
  glue_version = "2.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/table_ingestion_v3.py"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "job to load tables from data science - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  default_arguments = {
    "--S3_SOURCE"        = "s3://dlr-${terraform.workspace}-bucket-transfer/Dumps/${var.source_s3_datascience[count.index]}"
    "--S3_TARGET"        = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/datascience/${var.table_datascience[count.index]}/"
    "--CRAWLER_DATABASE" = "dlr-database-pic-datascience"
    "--CRAWLER_TABLE"    = var.table_crawler_datascience[count.index]
  }
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_trigger" "job-trigger-dbo-datascience" {
  count         = length(var.table_datascience)
  name          = "dlr-trigger-job-pic-datascience-${var.table_datascience[count.index]}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.workflow-carga-inicial-datascience[count.index].name

  actions {
    job_name = aws_glue_job.dbo-carga-inicial-datascience[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start job to load tables from data science - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  predicate {
    conditions {
      crawler_name = aws_glue_crawler.dbo-pic-crawler-datascience[count.index].name
      crawl_state  = "SUCCEEDED"
    }
  }
}

# This Crawler extracts the DataLake catalog
resource "aws_glue_crawler" "dbo-s3-crawler-datascience" {
  count         = length(var.table_datascience)
  database_name = aws_glue_catalog_database.datascience_raw_database.name
  name          = "dlr-crawler-rawzone-datascience-${var.table_datascience[count.index]}"
  role          = var.glue_role_id
  description   = "Get schema about s3 parquet database"
  s3_target {
    path = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/datascience/${var.table_datascience[count.index]}/"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "second crawler to load tables from data science - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

resource "aws_glue_trigger" "crawler-trigger-dbo-datascience-s3" {
  count         = length(var.table_datascience)
  name          = "dlr-trigger-crawler-rawzone-datascience-${var.table_datascience[count.index]}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.workflow-carga-inicial-datascience[count.index].name

  actions {
    crawler_name = aws_glue_crawler.dbo-s3-crawler-datascience[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start second crawler to load tables from data science - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
  predicate {
    conditions {
      job_name = aws_glue_job.dbo-carga-inicial-datascience[count.index].name
      state    = "SUCCEEDED"
    }
  }
}