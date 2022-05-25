# Workflow used to create Sandbox CRM
resource "aws_glue_workflow" "workflow-sandbox-ftcrm" {
  count = length(var.table_sandbox_ftcrm)
  name  = "dlr-workflow-sandbox-ftcrm-${var.table_sandbox_ftcrm[count.index]}"

  tags = merge(
    var.dlr_tags,
    {
      Description : "workflow to create Sandbox CRM - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

# This Crawler extracts the Sandbox CRM Catalog
resource "aws_glue_crawler" "sandbox-crawler-ftcrm" {
  count         = length(var.table_sandbox_ftcrm)
  database_name = aws_glue_catalog_database.crm_sandbox_database.name
  name          = "dlr-crawler-sandbox-ftcrm-${var.table_sandbox_ftcrm[count.index]}"
  role          = var.glue_role_id
  description   = "Get schema about s3 parquet database"
  s3_target {
    path = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/${var.table_sandbox_ftcrm[count.index]}/"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "Extracts the Sandbox CRM Catalog - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

resource "aws_glue_trigger" "crawler-trigger-sandbox-ftcrm" {
  count         = length(var.table_sandbox_ftcrm)
  name          = "dlr-trigger-crawler-sandbox-ftcrm-${var.table_sandbox_ftcrm[count.index]}"
  type          = "SCHEDULED"
  schedule      = var.schedule_sandbox
  workflow_name = aws_glue_workflow.workflow-sandbox-ftcrm[count.index].name

  actions {
    crawler_name = aws_glue_crawler.sandbox-crawler-ftcrm[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start crawler for sandbox ftcrm - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

}

# Workflow used to create Sandbox Datamart
resource "aws_glue_workflow" "workflow-sandbox-datamart" {
  count = length(var.table_sandbox_datamart)
  name  = "dlr-workflow-sandbox-datamart-${var.table_sandbox_datamart[count.index]}"

  tags = merge(
    var.dlr_tags,
    {
      Description : "workflow to create Sandbox Datamart - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

# This Crawler extracts the Sandbox Datamart Catalog
resource "aws_glue_crawler" "sandbox-crawler-datamart" {
  count         = length(var.table_sandbox_datamart)
  database_name = aws_glue_catalog_database.datamart_sandbox_database.name
  name          = "dlr-crawler-sandbox-datamart-${var.table_sandbox_datamart[count.index]}"
  role          = var.glue_role_id
  description   = "Get schema about s3 parquet database"
  s3_target {
    path = "s3://dlr-${terraform.workspace}-bucket-refinedzone/pic/datamart/${var.table_sandbox_datamart[count.index]}/"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "extracts the Sandbox Datamart Catalog - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

resource "aws_glue_trigger" "crawler-trigger-sandbox-datamart" {
  count         = length(var.table_sandbox_datamart)
  name          = "dlr-trigger-crawler-sandbox-datamart-${var.table_sandbox_datamart[count.index]}"
  type          = "SCHEDULED"
  schedule      = var.schedule_sandbox
  workflow_name = aws_glue_workflow.workflow-sandbox-datamart[count.index].name

  actions {
    crawler_name = aws_glue_crawler.sandbox-crawler-datamart[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start crawler for Sandbox Datamart - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

}

# Workflow used to create Sandbox CRM
resource "aws_glue_workflow" "workflow-sandbox-financial" {
  count = length(var.table_sandbox_financial)
  name  = "dlr-workflow-sandbox-financial-${var.table_sandbox_financial[count.index]}"

  tags = merge(
    var.dlr_tags,
    {
      Description : "workflow to create Sandbox CRM Financial - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

# This Crawler extracts the Sandbox Financial Catalog
resource "aws_glue_crawler" "sandbox-crawler-financial" {
  count         = length(var.table_sandbox_financial)
  database_name = aws_glue_catalog_database.financial_sandbox_database.name
  name          = "dlr-crawler-sandbox-financial-${var.table_sandbox_financial[count.index]}"
  role          = var.glue_role_id
  description   = "Get schema about s3 parquet database"
  s3_target {
    path = "s3://dlr-${terraform.workspace}-bucket-refinedzone/pic/datamart/${var.table_sandbox_financial[count.index]}/"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "extracts the Sandbox Financial Catalog - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )
}

resource "aws_glue_trigger" "crawler-trigger-sandbox-financial" {
  count         = length(var.table_sandbox_financial)
  name          = "dlr-trigger-crawler-sandbox-financial-${var.table_sandbox_financial[count.index]}"
  type          = "SCHEDULED"
  schedule      = var.schedule_sandbox
  workflow_name = aws_glue_workflow.workflow-sandbox-financial[count.index].name

  actions {
    crawler_name = aws_glue_crawler.sandbox-crawler-financial[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "trigger to start crawler for Sandbox Financial - glue-a module"
      Environment : var.env_tags[var.env]
    }
  )

}