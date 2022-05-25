# Job para aplicar as alterações dos arquivos da Coligada EmDia na area silver
resource "aws_glue_job" "job_apply_changes_coligadas" {
  count        = length(var.coligadas)
  name         = "dlr-job-apply-change-${var.coligadas[count.index]}"
  role_arn     = var.glue_role
  glue_version = "3.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/${var.scripts_coligadas[count.index]}"
  }

  tags = {
    Component = "Coligadas"
  }

  default_arguments = {
    "--JSON_FILE"          = "tmp/schema/${var.json_files[count.index]}"
    "--S3_SOURCE"          = "s3://dlr-${terraform.workspace}-bucket-bronze/coligadas/emdia/${var.coligadas[count.index]}/"
    "--S3_TARGET"          = "s3://dlr-${terraform.workspace}-bucket-silver/coligadas/emdia/${var.coligadas[count.index]}/"
    "--S3_JSON"            = "dlr-${terraform.workspace}-bucket-transient"
  }
}

# Job para aplicar as alterações dos arquivos do Parceiro Liderança na area silver
resource "aws_glue_job" "job_apply_changes_parceiros" {
  count        = length(var.arquivos_parceiros)
  name         = "dlr-job-apply-change-lideranca-${var.arquivos_parceiros[count.index]}"
  role_arn     = var.glue_role
  glue_version = "3.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/${var.scripts_parceiros[count.index]}"
  }

  tags = {
    Component = "Parceiros - Liderança"
  }

  default_arguments = {
    "--JSON_FILE"          = "tmp/schema/${var.json_files[count.index]}"
    "--S3_SOURCE"          = "s3://dlr-${terraform.workspace}-bucket-bronze/partners/lideranca/${var.arquivos_parceiros[count.index]}/"
    "--S3_TARGET"          = "s3://dlr-${terraform.workspace}-bucket-silver/partners/lideranca/${var.arquivos_parceiros[count.index]}/"
    "--S3_JSON"            = "dlr-${terraform.workspace}-bucket-transient"
  }
}