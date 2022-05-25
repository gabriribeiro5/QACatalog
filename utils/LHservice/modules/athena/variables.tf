variable "env" {
  type = string
}

variable "bucket_logs" {

}

variable "instance_tags_raw" {
  type = map(string)

  description = ""

  default = {
    Name      = "BucketRawZone"
    ManagedBy = "Terraform"
    UpdateAt  = "2021-08-25T15h28"
  }

}

variable "dlr_tags" {
  description = "General tags for Dataleke Return resources"
  type        = map(string)

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
