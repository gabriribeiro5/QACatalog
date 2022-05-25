# This job is used to update datamarts
resource "aws_glue_job" "update_datamarts" {
  count                  = length(var.table_datamart_update)
  name                   = "dlr-job-refined-datamart-${var.table_datamart_update[count.index]}-updates"
  role_arn               = var.glue_role
  glue_version           = "2.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/${var.datamart_scripts_update[count.index]}.py"
  }

  tags = merge(
    var.dlr_tags,
    {
      Description: "job to update Datamarts - glue-b module"
      Environment: var.env_tags[var.env]
    }
  )

  default_arguments = {
    "--UPDATE_ARRANGEMENTS"            = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/arrangements/"
    "--UPDATE_INSTALLMENTS"            = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/arrangementinstallments/"
    "--UPDATE_DEBTS"                   = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/debts/"
    "--UPDATE_DEBT_TRANSACTIONS"       = "s3://dlr-${terraform.workspace}-bucket-rawzone/pic/ftcrm/debttransactions/"
    "--DATAMART_ARRANGEMENTS"          = "s3://dlr-${terraform.workspace}-bucket-refinedzone/pic/datamart/arrangements/"
    "--DATAMART_INSTALLMENTS"          = "s3://dlr-${terraform.workspace}-bucket-refinedzone/pic/datamart/arrangementinstallments/"
    "--DATAMART_DEBTCONTACTS"          = "s3://dlr-${terraform.workspace}-bucket-refinedzone/pic/datamart/debtcontacts/"
    "--DATAMART_PAYMENTS"              = "s3://dlr-${terraform.workspace}-bucket-refinedzone/pic/datamart/payments/"
    "--LOG_PATH"                       = "s3://dlr-${terraform.workspace}-bucket-log/jobs/glue/type=datamart/datamart=${var.table_datamart_update[count.index]}"
    "--PROCESS_TYPE"                   = "UPDATE"
  }
}

# This trigger calls job updates
resource "aws_glue_trigger" "job-trigger-datamart-update" {
  count    = length(var.table_datamart_update)
  name     = "dlr-trigger-job-refined-datamart-${var.table_datamart_update[count.index]}-updates"
  schedule = var.schedule_datamart_updates[count.index]
  type     = "SCHEDULED"

  actions {
    job_name = aws_glue_job.update_datamarts[count.index].name
  }

  tags = merge(
    var.dlr_tags,
    {
      Description: "trigger to start job that updates Datamarts - glue-b module"
      Environment: var.env_tags[var.env]
    }
  )

}
