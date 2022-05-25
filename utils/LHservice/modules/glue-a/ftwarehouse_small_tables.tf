# This WorkFlow is used to extract the initial dump with the right schema
resource "aws_glue_workflow" "workflow-carga-inicial-small-ftwarehouse" {
  count = length(var.small_table_ftwarehouse)
  name  = "dlr-workflow-ingesta-pic-ftwarehouse-${var.small_table_ftwarehouse[count.index]}"

  tags = merge(
    var.dlr_tags,
    {
      Description : "workflow to load tables from ftcrm - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_crawler" "dbo-pic-crawler-small-ftwarehouse" {
  database_name = aws_glue_catalog_database.ftwarehouse_pic_database.name
  count         = length(var.small_table_ftwarehouse)
  name          = "dlr-crawler-pic-ftwarehouse-${var.small_table_ftwarehouse[count.index]}"
  role          = var.glue_role_id
  description   = "Get schema about ftwarehouse database"

  jdbc_target {
    connection_name = aws_glue_connection.ftwarehouse_connection.name
    path            = var.jdbc_target_small_ftwarehouse[count.index]
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "first crawler to load tables from ftcrm - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_trigger" "crawler-trigger-dbo-small-ftwarehouse" {
  count         = length(var.small_table_ftwarehouse)
  name          = "dlr-trigger-crawler-pic-ftwarehouse-${var.small_table_ftwarehouse[count.index]}"
  type          = "SCHEDULED"
  schedule      = var.schedule_small_ftwarehouse[count.index]
  workflow_name = aws_glue_workflow.workflow-carga-inicial-small-ftwarehouse[count.index].name

  actions {
    crawler_name = aws_glue_crawler.dbo-pic-crawler-small-ftwarehouse[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start first crawler to load tables from ftcrm - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_job" "dbo-carga-inicial-small-ftwarehouse" {
  count        = length(var.small_table_ftwarehouse)
  name         = "dlr-job-pic-ftwarehouse-${var.small_table_ftwarehouse[count.index]}"
  role_arn     = var.glue_role
  connections  = [aws_glue_connection.ftwarehouse_connection.name]
  glue_version = "2.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/small_table_ingestion_v3.py"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "job to load tables from ftcrm - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  default_arguments = {
    "--S3_TARGET"        = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftwarehouse/${var.small_table_ftwarehouse[count.index]}/"
    "--CRAWLER_DATABASE" = "dlr-database-pic-ftwarehouse"
    "--CRAWLER_TABLE"    = var.small_table_crawler_ftwarehouse[count.index]
  }
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_trigger" "job-trigger-dbo-small-ftwarehouse" {
  count         = length(var.small_table_ftwarehouse)
  name          = "dlr-trigger-job-pic-ftwarehouse-${var.small_table_ftwarehouse[count.index]}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.workflow-carga-inicial-small-ftwarehouse[count.index].name

  actions {
    job_name = aws_glue_job.dbo-carga-inicial-small-ftwarehouse[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start job to load tables from ftcrm - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  predicate {
    conditions {
      crawler_name = aws_glue_crawler.dbo-pic-crawler-small-ftwarehouse[count.index].name
      crawl_state  = "SUCCEEDED"
    }
  }
}

# This Crawler extracts the DataLake catalog
resource "aws_glue_crawler" "dbo-s3-crawler-small-ftwarehouse" {
  count         = length(var.small_table_ftwarehouse)
  database_name = aws_glue_catalog_database.ftwarehouse_raw_database.name
  name          = "dlr-crawler-rawzone-pic-ftwarehouse-${var.small_table_ftwarehouse[count.index]}"
  role          = var.glue_role_id
  description   = "Get schema about s3 parquet database"
  s3_target {
    path = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftwarehouse/${var.small_table_ftwarehouse[count.index]}/"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "second crawler to load tables from ftcrm - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

resource "aws_glue_trigger" "crawler-trigger-dbo-small-ftwarehouse-s3" {
  count         = length(var.small_table_ftwarehouse)
  name          = "dlr-trigger-crawler-rawzone-pic-ftwarehouse-${var.small_table_ftwarehouse[count.index]}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.workflow-carga-inicial-small-ftwarehouse[count.index].name

  actions {
    crawler_name = aws_glue_crawler.dbo-s3-crawler-small-ftwarehouse[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start second crawler to load tables ftcrm - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  predicate {
    conditions {
      job_name = aws_glue_job.dbo-carga-inicial-small-ftwarehouse[count.index].name
      state    = "SUCCEEDED"
    }
  }
}
