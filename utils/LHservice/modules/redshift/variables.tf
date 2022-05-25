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
variable "node_type" {
  default = "dc2.large"
}

variable "env" {
  type = string
}

variable "redshift_cluster_type" {
  type = map(string)

  default = {
    "dev"  = "single-node"
    "prod" = "multi-node"
  }

}

variable "redshift_nodes" {
  type = map(string)

  default = {
    "dev"  = "1"
    "prod" = "3"
  }
}

variable "redshift_role" {

}
variable "redshift_pass" {
  type = string
}

variable "redshift_user" {
  type = string

  default = "return"
}

variable "redshift_db" {
  type = string

  default = "dlrdb"

}

variable "availability_zone" {
  type = map(string)

  default = {
    "dev"  = "us-east-2a"
    "prod" = "us-east-1d"
  }
}

variable "redshift_subnet" {
  type = string

  default = "redshift-subnet"

}