variable "dlr_tags" {
  type        = map(string)
  description = "General tags for Dataleke Return resources"

  default = {
    Department  = "TI"
    Project     = "DATALAKE"
    Responsible = "TI Return + Lake NTT"
  }
}

variable "env_tags" {
  type = map(string)
  default = {
    "dev" = "DEV - Ohio"
    "prod" = "PROD - N. Virginia"
  }
}
variable "lambda_apollo_discador" {
  
}

variable "lambda_apollo_sms" {
  
}

variable "env" {
  
}

variable "role_arn_step_functions" {
  
}

variable "glue_validator_dialer_arn" {
  type = map(string)

  default = {
    "dev" = "arn:aws:glue:us-east-2:642344871054:job/dlr-job-validator-dialer"
    "prod" = "arn:aws:glue:us-east-1:276648988236:job/dlr-job-validator-dialer"

  }
}

variable "glue_validator_message_arn" {
  type = map(string)

  default = {
    "dev" = "arn:aws:glue:us-east-2:642344871054:job/dlr-job-validator-message"
    "prod" = "arn:aws:glue:us-east-1:276648988236:job/dlr-job-validator-message"

  }
}