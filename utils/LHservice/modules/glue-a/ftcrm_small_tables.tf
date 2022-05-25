# This WorkFlow is used to extract the initial dump with the right schema
resource "aws_glue_workflow" "workflow-carga-inicial-small-ftcrm" {
  count = length(var.small_table_ftcrm)
  name  = "dlr-workflow-ingesta-pic-ftcrm-${var.small_table_ftcrm[count.index]}"

  tags = merge(
    var.dlr_tags,
    {
      Description : "workflow to load tables from ftcrm - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_crawler" "dbo-pic-crawler-small-ftcrm" {
  database_name = aws_glue_catalog_database.ftcrm_pic_database.name
  count         = length(var.small_table_ftcrm)
  name          = "dlr-crawler-pic-ftcrm-${var.small_table_ftcrm[count.index]}"
  role          = var.glue_role_id
  description   = "Get schema about ftcrm database"

  jdbc_target {
    connection_name = aws_glue_connection.ftcrm_connection.name
    path            = var.jdbc_target_small_ftcrm[count.index]
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
resource "aws_glue_trigger" "crawler-trigger-dbo-small-ftcrm" {
  count         = length(var.small_table_ftcrm)
  name          = "dlr-trigger-crawler-pic-ftcrm-${var.small_table_ftcrm[count.index]}"
  type          = "SCHEDULED"
  schedule      = var.schedule_small_ftcrm[count.index]
  workflow_name = aws_glue_workflow.workflow-carga-inicial-small-ftcrm[count.index].name

  actions {
    crawler_name = aws_glue_crawler.dbo-pic-crawler-small-ftcrm[count.index].name
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
resource "aws_glue_job" "dbo-carga-inicial-small-ftcrm" {
  count        = length(var.small_table_ftcrm)
  name         = "dlr-job-pic-ftcrm-${var.small_table_ftcrm[count.index]}"
  role_arn     = var.glue_role
  connections  = [aws_glue_connection.ftcrm_connection.name]
  glue_version = "2.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/${var.ftcrm_script_location[count.index]}"
    ##apagar
    #"s3://dlr-${terraform.workspace}-bucket-scripts/small_table_ingestion_v2.py"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "job to load tables from ftcrm - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  default_arguments = {
    "--S3_TARGET"         = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/${var.small_table_ftcrm[count.index]}/"
    "--CRAWLER_DATABASE"  = "dlr-database-pic-ftcrm"
    "--CRAWLER_TABLE"     = var.small_table_crawler_ftcrm[count.index]
    "--JDBC_USERNAME"     = var.jdbc_user
    "--JDBC_PASSWORD"     = var.jdbc_pass
    "--STRING_CONNECTION" = var.string_connection[var.env]
    "--DATABASE"          = "FTCRM"
    "--TABLE"             = "dbo.CampaignOptions"
    "--QUERY"             = var.query_ftcrm[count.index]

  }
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_trigger" "job-trigger-dbo-small-ftcrm" {
  count         = length(var.small_table_ftcrm)
  name          = "dlr-trigger-job-pic-ftcrm-${var.small_table_ftcrm[count.index]}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.workflow-carga-inicial-small-ftcrm[count.index].name

  actions {
    job_name = aws_glue_job.dbo-carga-inicial-small-ftcrm[count.index].name
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
      crawler_name = aws_glue_crawler.dbo-pic-crawler-small-ftcrm[count.index].name
      crawl_state  = "SUCCEEDED"
    }
  }
}

# This Crawler extracts the DataLake catalog
resource "aws_glue_crawler" "dbo-s3-crawler-small-ftcrm" {
  count         = length(var.small_table_ftcrm)
  database_name = aws_glue_catalog_database.ftcrm_raw_database.name
  name          = "dlr-crawler-rawzone-pic-ftcrm-${var.small_table_ftcrm[count.index]}"
  role          = var.glue_role_id
  description   = "Get schema about s3 parquet database"
  s3_target {
    path = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/${var.small_table_ftcrm[count.index]}/"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "second crawler to load tables from ftcrm - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

resource "aws_glue_trigger" "crawler-trigger-dbo-small-ftcrm-s3" {
  count         = length(var.small_table_ftcrm)
  name          = "dlr-trigger-crawler-rawzone-pic-ftcrm-${var.small_table_ftcrm[count.index]}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.workflow-carga-inicial-small-ftcrm[count.index].name

  actions {
    crawler_name = aws_glue_crawler.dbo-s3-crawler-small-ftcrm[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start second crawler to load tables from ftcrm - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  predicate {
    conditions {
      job_name = aws_glue_job.dbo-carga-inicial-small-ftcrm[count.index].name
      state    = "SUCCEEDED"
    }
  }
}
