data "aws_secretsmanager_secret" "secrets_jdbc" {
  arn = var.arn_secret_jdbc[var.env]
}

data "aws_secretsmanager_secret_version" "current_jdbc" {
  secret_id = data.aws_secretsmanager_secret.secrets_jdbc.id
}

data "aws_secretsmanager_secret" "secrets_redshift" {
  arn = var.arn_secret_redshift[var.env]
}

data "aws_secretsmanager_secret_version" "current_redshift" {
  secret_id = data.aws_secretsmanager_secret.secrets_redshift.id
}

#data "aws_secretsmanager_secret" "secrets_lideranca" {
#  arn = var.arn_secret_lideranca[var.env]
#}

#data "aws_secretsmanager_secret_version" "current_lideranca" {
#  secret_id = data.aws_secretsmanager_secret.secrets_lideranca.id
#}