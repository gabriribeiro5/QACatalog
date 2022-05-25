variable "env" {
  description = "env: dev or prod"
}

variable "region" {
  type = map(string)

  default = {
    "dev"  = "us-east-2"
    "prod" = "us-east-1"
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