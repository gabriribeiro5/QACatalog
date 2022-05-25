# Criacao da instancia do Jupyter Notebook no SageMaker
resource "aws_sagemaker_notebook_instance" "jupyter-notebook" {
  name          = "dlr-sagemaker-notebook"
  role_arn      = var.role_arn
  instance_type = "ml.t2.medium"
  subnet_id     = var.subnet_ids_sagemaker[var.env]
  security_groups = [var.security_group[var.env]]

  tags = merge(
    var.instance_tags_sagemaker,
    var.dlr_tags,
    {
      Description: "Notebook instance - Sagemaker module"
      Environment: var.env_tags[var.env]
    }
  )
}