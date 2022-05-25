# This WorkFlow is used to extract the initial dump with the right schema
resource "aws_glue_workflow" "workflow-carga-inicial-small-ft5l" {
  count = length(var.small_table_ft5l)
  name  = "dlr-workflow-ingesta-pic-ft5l-${var.small_table_ft5l[count.index]}"

  tags = merge(
    var.dlr_tags,
    {
      Description : "workflow to load tables from ft5l - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_crawler" "dbo-pic-crawler-small-ft5l" {
  database_name = aws_glue_catalog_database.ft5l_pic_database.name
  count         = length(var.small_table_ft5l)
  name          = "dlr-crawler-pic-ft5l-${var.small_table_ft5l[count.index]}"
  role          = var.glue_role_id
  description   = "Get schema about ft5l database"

  jdbc_target {
    connection_name = aws_glue_connection.ft5l_connection.name
    path            = var.jdbc_target_small_ft5l[count.index]
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "first crawler to load tables from ft5l - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_trigger" "crawler-trigger-dbo-small-ft5l" {
  count         = length(var.small_table_ft5l)
  name          = "dlr-trigger-crawler-pic-ft5l-${var.small_table_ft5l[count.index]}"
  type          = "SCHEDULED"
  schedule      = var.schedule_small_ft5l[count.index]
  workflow_name = aws_glue_workflow.workflow-carga-inicial-small-ft5l[count.index].name

  actions {
    crawler_name = aws_glue_crawler.dbo-pic-crawler-small-ft5l[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start first crawler to load tables from ft5l - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_job" "dbo-carga-inicial-small-ft5l" {
  count        = length(var.small_table_ft5l)
  name         = "dlr-job-pic-ft5l-${var.small_table_ft5l[count.index]}"
  role_arn     = var.glue_role
  connections  = [aws_glue_connection.ft5l_connection.name]
  glue_version = "2.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/small_table_ingestion_v3.py"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "job to load tables from ft5l - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  default_arguments = {
    "--S3_TARGET"        = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ft5l/${var.small_table_ft5l[count.index]}/"
    "--CRAWLER_DATABASE" = "dlr-database-pic-ft5l"
    "--CRAWLER_TABLE"    = var.small_table_crawler_ft5l[count.index]
  }
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_trigger" "job-trigger-dbo-small-ft5l" {
  count         = length(var.small_table_ft5l)
  name          = "dlr-trigger-job-pic-ft5l-${var.small_table_ft5l[count.index]}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.workflow-carga-inicial-small-ft5l[count.index].name

  actions {
    job_name = aws_glue_job.dbo-carga-inicial-small-ft5l[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start job to load tables from ft5l - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  predicate {
    conditions {
      crawler_name = aws_glue_crawler.dbo-pic-crawler-small-ft5l[count.index].name
      crawl_state  = "SUCCEEDED"
    }
  }
}

# This Crawler extracts the DataLake catalog
resource "aws_glue_crawler" "dbo-s3-crawler-small-ft5l" {
  count         = length(var.small_table_ft5l)
  database_name = aws_glue_catalog_database.ft5l_raw_database.name
  name          = "dlr-crawler-rawzone-ft5l-${var.small_table_ft5l[count.index]}"
  role          = var.glue_role_id
  description   = "Get schema about s3 parquet database"
  s3_target {
    path = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ft5l/${var.small_table_ft5l[count.index]}/"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "second crawler to load tables from ft5l - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

resource "aws_glue_trigger" "crawler-trigger-dbo-small-ft5l-s3" {
  count         = length(var.small_table_ft5l)
  name          = "dlr-trigger-crawler-rawzone-ft5l-${var.small_table_ft5l[count.index]}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.workflow-carga-inicial-small-ft5l[count.index].name

  actions {
    crawler_name = aws_glue_crawler.dbo-s3-crawler-small-ft5l[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start second crawler to load tables from ft5l - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
  predicate {
    conditions {
      job_name = aws_glue_job.dbo-carga-inicial-small-ft5l[count.index].name
      state    = "SUCCEEDED"
    }
  }
}
