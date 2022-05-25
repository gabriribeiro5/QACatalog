variable "arn_secret_jdbc" {
  type = map(string)

  default = {
    "dev"  = "arn:aws:secretsmanager:us-east-2:642344871054:secret:dlr-dev-jdbc-PaNbtO"
    "prod" = "arn:aws:secretsmanager:us-east-1:276648988236:secret:dlr-prod-jdbc-xTKlng"
  }
}

variable "arn_secret_redshift" {
  type = map(string)

  default = {
    "dev"  = "arn:aws:secretsmanager:us-east-2:642344871054:secret:dlr-dev-redshift-FbPUDO"
    "prod" = "arn:aws:secretsmanager:us-east-1:276648988236:secret:dlr-prod-redshift-cluster-LEjG2q"
  }
}

variable "arn_secret_sandbox" {
  type = map(string)

  default = {
    "dev" = "arn:aws:secretsmanager:us-east-2:642344871054:secret:dlr-dev-sandbox-3GekSP"
    "prod" = "" 
  }
  
}

#variable "arn_secret_lideranca" {
#  type = map(string)
#  default = {
#    "dev"  = "arn:aws:secretsmanager:us-east-2:642344871054:secret:ssh_key-lideranca-GPzhI9"
#    "prod" = ""
#  }
#}

variable "env" {

}