# This workflow is used to extract the initial dump with the right schema
resource "aws_glue_workflow" "workflow-carga-inicial-ftaccounting" {
  count = length(var.table_ftaccounting)
  name  = "dlr-workflow-ingesta-pic-ftaccounting-${var.table_ftaccounting[count.index]}"

  tags = merge(
    var.dlr_tags,
    {
      Description : "workflow to load tables from ftaccounting - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_crawler" "dbo-pic-crawler-ftaccounting" {
  count         = length(var.table_ftaccounting)
  database_name = aws_glue_catalog_database.ftaccounting_pic_database.name
  name          = "dlr-crawler-pic-ftaccounting-${var.table_ftaccounting[count.index]}"
  role          = var.glue_role_id
  description   = "Get schema about ftaccounting database"

  jdbc_target {
    connection_name = aws_glue_connection.ftaccounting_connection.name
    path            = var.jdbc_target_ftaccounting[count.index]
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "first crawler to load tables from ftaccounting - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_trigger" "crawler-trigger-dbo-ftaccounting" {
  count         = length(var.table_ftaccounting)
  name          = "dlr-trigger-crawler-pic-ftaccounting-${var.table_ftaccounting[count.index]}"
  type          = "SCHEDULED"
  schedule      = var.schedule_ftaccounting[count.index]
  workflow_name = aws_glue_workflow.workflow-carga-inicial-ftaccounting[count.index].name

  actions {
    crawler_name = aws_glue_crawler.dbo-pic-crawler-ftaccounting[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to to extract the initial dump with the right schema dbo-ftaccounting - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_job" "dbo-carga-inicial-ftaccounting" {
  count        = length(var.table_ftaccounting)
  name         = "dlr-job-pic-ftaccounting-${var.table_ftaccounting[count.index]}"
  role_arn     = var.glue_role
  connections  = [aws_glue_connection.ftaccounting_connection.name]
  glue_version = "2.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/table_ingestion_v3.py"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "job to load tables from ftaccounting - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  default_arguments = {
    "--S3_SOURCE"        = "s3://dlr-${terraform.workspace}-bucket-transfer/Dumps/${var.source_s3_ftaccounting[count.index]}"
    "--S3_TARGET"        = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftaccounting/${var.table_ftaccounting[count.index]}/"
    "--CRAWLER_DATABASE" = "dlr-database-pic-ftaccounting"
    "--CRAWLER_TABLE"    = var.table_crawler_ftaccounting[count.index]
  }
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_trigger" "job-trigger-dbo-ftaccounting" {
  count         = length(var.table_ftaccounting)
  name          = "dlr-trigger-job-pic-ftaccounting-${var.table_ftaccounting[count.index]}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.workflow-carga-inicial-ftaccounting[count.index].name

  actions {
    job_name = aws_glue_job.dbo-carga-inicial-ftaccounting[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "Extracts the initial dump with the right schema trigger-dbo-ftaccounting - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  predicate {
    conditions {
      crawler_name = aws_glue_crawler.dbo-pic-crawler-ftaccounting[count.index].name
      crawl_state  = "SUCCEEDED"
    }
  }
}

# This Crawler extracts the DataLake catalog
resource "aws_glue_crawler" "dbo-s3-crawler-ftaccounting" {
  count         = length(var.table_ftaccounting)
  database_name = aws_glue_catalog_database.ftaccounting_raw_database.name
  name          = "dlr-crawler-rawzone-ftaccounting-${var.table_ftaccounting[count.index]}"
  role          = var.glue_role_id
  description   = "Get schema about s3 parquet database"
  s3_target {
    path = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftaccounting/${var.table_ftaccounting[count.index]}/"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "second crawler to load tables from ft5l - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

resource "aws_glue_trigger" "crawler-trigger-dbo-ftaccounting-s3" {
  count         = length(var.table_ftaccounting)
  name          = "dlr-trigger-crawler-rawzone-ftaccounting-${var.table_ftaccounting[count.index]}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.workflow-carga-inicial-ftaccounting[count.index].name

  actions {
    crawler_name = aws_glue_crawler.dbo-s3-crawler-ftaccounting[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start second crawler to load tables from ftaccounting - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  predicate {
    conditions {
      job_name = aws_glue_job.dbo-carga-inicial-ftaccounting[count.index].name
      state    = "SUCCEEDED"
    }
  }
}

# Incremental Load of ftaccounting

resource "aws_glue_job" "ftaccounting-carga-incremental" {
  count        = var.env == "dev" ? 0 : length(var.table_ftaccounting_insert)
  name         = "dlr-job-pic-ftaccounting-${var.table_ftaccounting_insert[count.index]}-incremental"
  role_arn     = var.glue_role
  glue_version = "2.0"
  connections  = [aws_glue_connection.ftaccounting_connection.name]

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/incremental_by_batch_v4.py"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "job for insert process into ftaccounting tables  - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  default_arguments = {
    "--PATH_S3"           = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftaccounting/${var.table_ftaccounting_insert[count.index]}/"
    "--DATABASE"          = "ftaccounting"
    "--TABLE_NAME"        = var.table_ftaccounting_insert[count.index]
    "--REFERENCE_COL"     = var.reference_col_ftaccounting[count.index]
    "--JDBC_USERNAME"     = var.jdbc_user
    "--JDBC_PASSWORD"     = var.jdbc_pass
    "--STRING_CONNECTION" = var.string_connection[var.env]
    "--TYPE_REF_COL"      = "int"
    "--BATCH_SIZE"        = "10000"
    "--LOG_PATH"          = "s3://dlr-${terraform.workspace}-bucket-log/jobs/glue/type=ingestion/origin=pic/"
    "--PROCESS_TYPE"      = "INSERT"
  }
}

resource "aws_glue_trigger" "job-trigger-pic-ftaccounting-incremental" {
  count = var.env == "dev" ? 0 : length(var.table_ftaccounting_insert)
  name  = "dlr-trigger-job-pic-ftaccounting-${var.table_ftaccounting_insert[count.index]}-incremental"

  schedule = var.schedule_ftaccounting_incremental[count.index]
  type     = "SCHEDULED"

  actions {
    job_name = aws_glue_job.ftaccounting-carga-incremental[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger for insert jobs ftaccounting - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}