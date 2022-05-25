# This WorkFlow is used to extract the initial dump with the right schema
resource "aws_glue_workflow" "workflow-carga-inicial-ftcrm" {
  count = length(var.table_ftcrm)
  name  = "dlr-workflow-ingesta-pic-ftcrm-${var.table_ftcrm[count.index]}"

  tags = merge(
    var.dlr_tags,
    {
      Description : "workflow to load tables from ftcrm - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_crawler" "dbo-pic-crawler-ftcrm" {
  database_name = aws_glue_catalog_database.ftcrm_pic_database.name
  count         = length(var.table_ftcrm)
  name          = "dlr-crawler-pic-ftcrm-${var.table_ftcrm[count.index]}"
  role          = var.glue_role_id
  description   = "Get schema about ftcrm database"

  jdbc_target {
    connection_name = aws_glue_connection.ftcrm_connection.name
    path            = var.jdbc_target_ftcrm[count.index]
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
resource "aws_glue_trigger" "crawler-trigger-dbo-ftcrm" {
  count         = length(var.table_ftcrm)
  name          = "dlr-trigger-crawler-pic-ftcrm-${var.table_ftcrm[count.index]}"
  type          = "SCHEDULED"
  schedule      = var.schedule_ftcrm[count.index]
  workflow_name = aws_glue_workflow.workflow-carga-inicial-ftcrm[count.index].name

  actions {
    crawler_name = aws_glue_crawler.dbo-pic-crawler-ftcrm[count.index].name
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
resource "aws_glue_job" "dbo-carga-inicial-ftcrm" {
  count        = length(var.table_ftcrm)
  name         = "dlr-job-pic-ftcrm-${var.table_ftcrm[count.index]}"
  role_arn     = var.glue_role
  connections  = [aws_glue_connection.ftcrm_connection.name]
  glue_version = "2.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/table_ingestion_v3.py"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "job to load tables from ftcrm - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  worker_type       = "G.1X"
  number_of_workers = "10"

  default_arguments = {
    "--S3_SOURCE"        = "s3://dlr-${terraform.workspace}-bucket-transfer/Dumps/${var.source_s3_ftcrm[count.index]}"
    "--S3_TARGET"        = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/${var.table_ftcrm[count.index]}/"
    "--CRAWLER_DATABASE" = "dlr-database-pic-ftcrm"
    "--CRAWLER_TABLE"    = var.table_crawler_ftcrm[count.index]
  }
}

# This job is used to extract the initial dump with the right schema
resource "aws_glue_trigger" "job-trigger-dbo-ftcrm" {
  count         = length(var.table_ftcrm)
  name          = "dlr-trigger-job-pic-ftcrm-${var.table_ftcrm[count.index]}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.workflow-carga-inicial-ftcrm[count.index].name

  actions {
    job_name = aws_glue_job.dbo-carga-inicial-ftcrm[count.index].name
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
      crawler_name = aws_glue_crawler.dbo-pic-crawler-ftcrm[count.index].name
      crawl_state  = "SUCCEEDED"
    }
  }
}

# This Crawler extracts the DataLake catalog
resource "aws_glue_crawler" "dbo-s3-crawler-ftcrm" {
  count         = length(var.table_ftcrm)
  database_name = aws_glue_catalog_database.ftcrm_raw_database.name
  name          = "dlr-crawler-rawzone-ftcrm-${var.table_ftcrm[count.index]}"
  role          = var.glue_role_id
  description   = "Get schema about s3 parquet database"
  s3_target {
    path = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/${var.table_ftcrm[count.index]}/"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "second crawler to load tables from ftcrm - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

resource "aws_glue_trigger" "crawler-trigger-dbo-ftcrm-s3" {
  count         = length(var.table_ftcrm)
  name          = "dlr-trigger-crawler-rawzone-ftcrm-${var.table_ftcrm[count.index]}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.workflow-carga-inicial-ftcrm[count.index].name

  actions {
    crawler_name = aws_glue_crawler.dbo-s3-crawler-ftcrm[count.index].name
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
      job_name = aws_glue_job.dbo-carga-inicial-ftcrm[count.index].name
      state    = "SUCCEEDED"
    }
  }
}

# Incremental Load of ftcrm

resource "aws_glue_job" "ftcrm-carga-incremental" {
  count        = var.env == "dev" ? 0 : length(var.table_ftcrm_insert)
  name         = "dlr-job-pic-ftcrm-${var.table_ftcrm_insert[count.index]}-incremental"
  role_arn     = var.glue_role
  glue_version = "2.0"
  connections  = [aws_glue_connection.ftcrm_connection.name]

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/incremental_by_batch_v4.py"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "job for insert process into ftcontacts ftcrm - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

  default_arguments = {
    "--PATH_S3"           = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/${var.table_ftcrm_insert[count.index]}/"
    "--DATABASE"          = "FTCRM"
    "--TABLE_NAME"        = var.table_name_ftcrm_insert[count.index]
    "--REFERENCE_COL"     = var.reference_col_ftcrm[count.index]
    "--JDBC_USERNAME"     = var.jdbc_user
    "--JDBC_PASSWORD"     = var.jdbc_pass
    "--STRING_CONNECTION" = var.string_connection[var.env]
    "--TYPE_REF_COL"      = "int"
    "--BATCH_SIZE"        = "10000"
    "--LOG_PATH"          = "s3://dlr-${terraform.workspace}-bucket-log/jobs/glue/type=ingestion/origin=pic/"
    "--PROCESS_TYPE"      = "INSERT"
  }
}

resource "aws_glue_trigger" "job-trigger-pic-ftcrm-incremental" {
  count = var.env == "dev" ? 0 : length(var.table_ftcrm_insert)
  name  = "dlr-trigger-job-pic-ftcrm-${var.table_ftcrm_insert[count.index]}-incremental"

  schedule = var.schedule_ftcrm_incremental[count.index]
  type     = "SCHEDULED"

  actions {
    job_name = aws_glue_job.ftcrm-carga-incremental[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger for insert jobs ftcrm - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}