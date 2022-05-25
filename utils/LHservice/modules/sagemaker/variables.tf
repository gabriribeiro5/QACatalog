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

variable "instance_tags_sagemaker" {
  type = map(string)

  description = ""

  default = {
    Name      = "SageMakerNotebook"
    ManagedBy = "Terraform"
    UpdateAt  = "2021-08-26T11h59"
  }

}

variable "env" {
  type = string

}
variable "subnet_ids_sagemaker" {
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

variable "role_arn" {
}