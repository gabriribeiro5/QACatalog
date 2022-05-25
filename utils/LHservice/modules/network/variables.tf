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

variable "redshift_subnet_tags" {
  type = map(string)

  description = "Tags para grupo de subnets do Redshift"

  default = {
    Name      = "RedshiftSubnetIds"
    ManagedBy = "Terraform"
    UpdateAt  = "2021-09-14T00h48"
  }
  
}

variable "env" {
  type = string
}

variable "subnet_ids_redshift" {
  type = map(string)

  default = {
    "dev" = "subnet-0bd22bb08377d8174"
    "prod" = "subnet-44eb7b0f" 
  }
  
}