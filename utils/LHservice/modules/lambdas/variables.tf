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
    "dev"  = "DEV - Ohio"
    "prod" = "PROD - N. Virginia"
  }
}
variable "env" {

}

variable "role_arn" {

}

variable "role_arn_step_functions" {

}

variable "iam_role" {

}

variable "redshift_user" {

}

variable "redshift_password" {

}

variable "bucket_transfer_id" {

}

variable "bucket_transfer_arn" {

}


variable "bucket_stage_id" {

}

variable "bucket_stage_arn" {

}
variable "bucket_silver_id" {

}

variable "bucket_silver_arn" {

}

variable "bucket_bronze_id" {

}

variable "bucket_bronze_arn" {

}


variable "schema" {
  type    = string
  default = "partnership"

}

variable "table_discador" {
  type    = string
  default = "discador"
}

variable "table_mensagem" {
  type    = string
  default = "mensagem"
}

variable "redshift_db" {
  default = "dlrdb"

}

variable "redshift_port" {
  type = string

  default = "5439"

}
variable "redshift_host" {
  type = map(string)
  default = {
    "dev"  = "172.31.0.224"
    "prod" = "10.50.6.57"
  }

}

variable "subnet_ids_redshift" {
  type = map(string)

  default = {
    "dev"  = "subnet-0bd22bb08377d8174"
    "prod" = "subnet-44eb7b0f"
  }

}

variable "security_group" {
  type = map(string)

  default = {
    "dev"  = "sg-0f8130f78db20b509"
    "prod" = "sg-7efb1d11"
  }
}

variable "groups_sandbox" {
  default = [
    "pricing",
    "financial",
    "data_science",
    "murabei",
    "portfolio_managers",
    "operational_managers",
    "commercial",
    "information_technology"
  ]
}

variable "pass_sandbox" {
  type    = string
  default = "DLR2021return"
}

variable "state_machine_discador_arn" {

}

variable "state_machine_sms_arn" {

}

variable "copy_file_s3_arn" {

}


variable "state_machine_validator_dialer_arn" {

}

variable "state_machine_validator_message_arn" {

}

variable "state_machine_validator_acordo_arn" {

}

variable "state_machine_apply_changes_acordo_arn" {

}

variable "state_machine_apply_changes_discador_arn" {
  
}