# This WorkFlow is used to extract the initial dump with the right schema
resource "aws_glue_workflow" "workflow-carga-inicial-small-ftcontacts" {
  count = length(var.small_table_ftcontacts)
  name  = "dlr-workflow-ingesta-pic-ftcontacts-${var.small_table_ftcontacts[count.index]}"

  tags = merge(
    var.dlr_tags,
    {
      Description : "workflow to load tables from ftcontacts - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_crawler" "dbo-pic-crawler-small-ftcontacts" {
  database_name = aws_glue_catalog_database.ftcontacts_pic_database.name
  count         = length(var.small_table_ftcontacts)
  name          = "dlr-crawler-pic-ftcontacts-${var.small_table_ftcontacts[count.index]}"
  role          = var.glue_role_id
  description   = "Get schema about ftcontacts database"

  jdbc_target {
    connection_name = aws_glue_connection.ftcontacts_connection.name
    path            = var.jdbc_target_small_ftcontacts[count.index]
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "first crawler to load tables from ftcontacts - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_trigger" "crawler-trigger-dbo-small-ftcontacts" {
  count         = length(var.small_table_ftcontacts)
  name          = "dlr-trigger-crawler-pic-ftcontacts-${var.small_table_ftcontacts[count.index]}"
  type          = "SCHEDULED"
  schedule      = var.schedule_small_ftcontacts[count.index]
  workflow_name = aws_glue_workflow.workflow-carga-inicial-small-ftcontacts[count.index].name

  actions {
    crawler_name = aws_glue_crawler.dbo-pic-crawler-small-ftcontacts[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start first crawler to load tables from ftaccounting - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_job" "dbo-carga-inicial-small-ftcontacts" {
  count        = length(var.small_table_ftcontacts)
  name         = "dlr-job-pic-ftcontacts-${var.small_table_ftcontacts[count.index]}"
  role_arn     = var.glue_role
  connections  = [aws_glue_connection.ftcontacts_connection.name]
  glue_version = "2.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/small_table_ingestion_v3.py"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "job to load tables from ftcontacts - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  default_arguments = {
    "--S3_TARGET"        = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcontacts/${var.small_table_ftcontacts[count.index]}/"
    "--CRAWLER_DATABASE" = "dlr-database-pic-ftcontacts"
    "--CRAWLER_TABLE"    = var.small_table_crawler_ftcontacts[count.index]
  }
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_trigger" "job-trigger-dbo-small-ftcontacts" {
  count         = length(var.small_table_ftcontacts)
  name          = "dlr-trigger-job-pic-ftcontacts-${var.small_table_ftcontacts[count.index]}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.workflow-carga-inicial-small-ftcontacts[count.index].name

  actions {
    job_name = aws_glue_job.dbo-carga-inicial-small-ftcontacts[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start job to load tables from ftcontacts - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  predicate {
    conditions {
      crawler_name = aws_glue_crawler.dbo-pic-crawler-small-ftcontacts[count.index].name
      crawl_state  = "SUCCEEDED"
    }
  }
}

# This Crawler extracts the DataLake catalog
resource "aws_glue_crawler" "dbo-s3-crawler-small-ftcontacts" {
  count         = length(var.small_table_ftcontacts)
  database_name = aws_glue_catalog_database.ftcontacts_raw_database.name
  name          = "dlr-crawler-rawzone-pic-ftcontacts-${var.small_table_ftcontacts[count.index]}"
  role          = var.glue_role_id
  description   = "Get schema about s3 parquet database"
  s3_target {
    path = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcontacts/${var.small_table_ftcontacts[count.index]}/"
  }
  tags = merge(
    var.dlr_tags,
    {
      Description : "second crawler to load tables from ftcontacts - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

resource "aws_glue_trigger" "crawler-trigger-dbo-small-ftcontacts-s3" {
  count         = length(var.small_table_ftcontacts)
  name          = "dlr-trigger-crawler-rawzone-pic-ftcontacts-${var.small_table_ftcontacts[count.index]}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.workflow-carga-inicial-small-ftcontacts[count.index].name

  actions {
    crawler_name = aws_glue_crawler.dbo-s3-crawler-small-ftcontacts[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start second crawler to load tables from ftcontacts - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  predicate {
    conditions {
      job_name = aws_glue_job.dbo-carga-inicial-small-ftcontacts[count.index].name
      state    = "SUCCEEDED"
    }
  }
}
