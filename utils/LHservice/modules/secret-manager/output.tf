output "dlr_jdbc_pass" {
  value = jsondecode(data.aws_secretsmanager_secret_version.current_jdbc.secret_string)["password"]
}

output "dlr_redshift_pass" {
  value = jsondecode(data.aws_secretsmanager_secret_version.current_redshift.secret_string)["password"]
}

output "dlr_redshift_user" {
  value = jsondecode(data.aws_secretsmanager_secret_version.current_redshift.secret_string)["username"]
}

#output "dlr_transfer_ssh_key_lideranca" {
#  value = jsondecode(data.aws_secretsmanager_secret_version.current_lideranca.secret_string)["ssh_key_lideranca"]
#}
