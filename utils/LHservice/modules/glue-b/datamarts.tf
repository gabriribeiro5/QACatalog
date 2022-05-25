# Workflow Para datamarts
resource "aws_glue_workflow" "workflow-datamart" {
  count = length(var.datamart_name)
  name  = "dlr-workflow-etl-datamart-${var.datamart_name[count.index]}"

  tags = merge(
    var.dlr_tags,
    {
      Description: "workflow for Datamarts - glue-b module"
      Environment: var.env_tags[var.env]
    }
  )
}

# Datamarts
resource "aws_glue_job" "datamarts_job" {
  count                  = length(var.datamart_name)
  name                   = "dlr-job-refined-datamart-${var.datamart_name[count.index]}"
  role_arn               = var.glue_role
  glue_version           = "2.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/${var.datamart_scripts[count.index]}"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description: "job to run Datamart scripts - glue-b module"
      Environment: var.env_tags[var.env]
    }
  )
  default_arguments = {
    "--FTF_RESOURCES"                   = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftfoundation/resources/*"
    "--FTF_RESOURCECAPTIONS"            = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftfoundation/resourcecaptions/*"
    "--FT5L_LEGALASSIGNMENTS"           = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ft5l/legal-legalassignments/*"
    "--FT5L_LEGALOFFICES"               = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ft5l/legal-legaloffices/*"
    "--FT5L_SUITCOSTS"                  = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ft5l/legal-suitcosts/*"
    "--FT5L_SUITPARTIES"                = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ft5l/legal-suitparties/*"
    "--FT5L_SUITPARTYDEBTS"             = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ft5l/legal-suitpartydebts/*"
    "--FT5L_SUITS"                      = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ft5l/legal-suits/*"
    "--FTCONTACTS_CONTACTS"             = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcontacts/contacts/*"
    "--FTCONTACTS_ADDRESSES"            = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcontacts/addresses/*"
    "--FTCONTACTS_STATES"               = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcontacts/states/*"
    "--FTCONTACTS_IDENTITIES"           = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcontacts/identities/*"
    "--FTCONTACTS_PEOPLE"               = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcontacts/people/*"
    "--FTCRM_CANCELLATIONREASONS"       = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/cancellationreasons/*"
    "--FTCRM_CREDITRESTRICTIONS"        = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/creditrestrictions/*"
    "--FTCRM_ARRANGEMENTS"              = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/arrangements/*"
    "--FTCRM_ARRANGINSTALLMENTS"        = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/arrangementinstallments/*"
    "--FTCRM_AGENTS"                    = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/agents/*"
    "--FTCRM_BUSINESSUNITS"             = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/businessunits/*"
    "--FTCRM_BINDINGS"                  = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/bindings/*"
    "--FTCRM_DIMDATE"                   = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/datamart/aux-dimdate/*"
    "--FTCRM_ASSIGNMENTS"               = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/assignments/*"
    "--FTCRM_DEBTS"                     = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/debts/*"
    "--FTCRM_DEBTTRANSACTIONS"          = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/debttransactions/*"
    "--FTCRM_DEBTTRANSACTIONCODES"      = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/debttransactioncodes/*"
    "--FTCRM_DEBTCONTACTS"              = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/datamart/crm-debtcontacts/*"
    "--FTCRM_DEBTCUSTOMEXTENSIONS"      = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/debtcustomextensions/*"
    "--FTCRM_DEBTTRANSACTIONCOMISSIONS" = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/debttransactioncommissions/*"
    "--FTCRM_DIGITALDEBTASSIGNMENTS"    = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/digitaldebtassignments/*"
    "--FTCRM_INSTALLMENTTRANSACTIONS"   = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/installmenttransactions/*"
    "--FTCRM_PORTFOLIOS"                = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/portfolios/*"
    "--FTCRM_SUBPORTFOLIOS"             = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/subportfolios/*"
    "--FTCRM_PRODUCTS"                  = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/products/*"
    "--FTCRM_PRODUCTTYPE"               = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/producttypes/*"
    "--FTCRM_SCOREMODELS"               = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/scoremodels/*"
    "--FTCRM_SCORES"                    = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/scores/*"
    "--FTCRM_WORKFLOWTASKRESULTS"       = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/workflowtaskresults/"
    "--FTCRM_WORKFLOWTRACKINGS"         = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/workflowtrackings/"
    "--FTCRM_WORKFLOWSTATUSES"          = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/workflowstatuses/*"
    "--FTWAREHOUSE_AGENCIESGOALS"       = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftwarehouse/agencies_goals/"
    "--ARRANGEMENTS"                    = "s3://dlr-${terraform.workspace}-bucket-refinedzone/pic/datamart/arrangements/"
    "--ARRANGEMENTSINSTALLMENTS"        = "s3://dlr-${terraform.workspace}-bucket-refinedzone/pic/datamart/arrangementinstallments/"
    "--DEBTSTATUS"                      = "s3://dlr-${terraform.workspace}-bucket-refinedzone/pic/datamart/debtstatus/"
    "--DEBTCONTACTS"                    = "s3://dlr-${terraform.workspace}-bucket-refinedzone/pic/datamart/debtcontacts/"
    "--WORKFLOWTRACKINGS"               = "s3://dlr-${terraform.workspace}-bucket-refinedzone/pic/datamart/workflowtrackings/"
    "--PAYMENTS"                        = "s3://dlr-${terraform.workspace}-bucket-refinedzone/pic/datamart/payments/"
    "--AGENCYGOALS"                     = "s3://dlr-${terraform.workspace}-bucket-refinedzone/pic/datamart/agencygoals/"
    "--ASSIGNMENTS"                     = "s3://dlr-${terraform.workspace}-bucket-refinedzone/pic/datamart/assignments/"
    "--DUMP_PAYMENTS"                   = "s3://dlr-${terraform.workspace}-bucket-refinedzone/pic/datamart/payments/"
    "--DUMP_ARRANGEMENTS"               = "s3://dlr-${terraform.workspace}-bucket-refinedzone/pic/datamart/arrangements/"
    "--LOG_PATH"                        = "s3://dlr-${terraform.workspace}-bucket-log/jobs/glue/type=datamart/datamart=${var.datamart_name[count.index]}"
    "--PROCESS_TYPE"                    = "INSERT"
  }

}

# Trigger para o job
resource "aws_glue_trigger" "job_trigger_datamart" {
  count         = length(var.datamart_name)
  name          = "dlr-trigger-job-refined-datamart-${var.datamart_name[count.index]}"
  schedule      = var.schedule_datamart[count.index]
  type          = "SCHEDULED"
  workflow_name = aws_glue_workflow.workflow-datamart[count.index].name

  actions {
    job_name = aws_glue_job.datamarts_job[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description: "trigger to start job that runs Datamarts scripts - glue-a module"
      Environment: var.env_tags[var.env]
    }
  )
}

# This Crawler extracts the Datamart catalog
resource "aws_glue_crawler" "datamart-crawler" {
  count         = length(var.datamart_name)
  database_name = var.database_datamart_name
  name          = "dlr-crawler-refined-datamart-${var.datamart_name[count.index]}"
  role          = var.glue_role_id
  description   = "Put schema in database"
  s3_target {
    path = "s3://dlr-${terraform.workspace}-bucket-refinedzone/pic/datamart/${var.datamart_name[count.index]}/"
  }
  tags = merge(
    var.dlr_tags,
    {
      Description: "trigger to start datamart crawler - glue-a module"
      Environment: var.env_tags[var.env]
    }
  )
}

resource "aws_glue_trigger" "crawler-trigger-datamart" {
  count         = length(var.datamart_name)
  name          = "dlr-trigger-crawler-refined-datamart-${var.datamart_name[count.index]}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.workflow-datamart[count.index].name

  actions {
    crawler_name = aws_glue_crawler.datamart-crawler[count.index].name
  }

  predicate {
    conditions {
      job_name = aws_glue_job.datamarts_job[count.index].name
      state    = "SUCCEEDED"
    }
  }
}