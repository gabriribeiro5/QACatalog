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

variable "iam_policy_glue" {
  default = [
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    "arn:aws:iam::aws:policy/AmazonEC2FullAccess"
  ]
}

variable "iam_policy_redshift" {
  default = [
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/service-role/AmazonDMSRedshiftS3Role",
    "arn:aws:iam::aws:policy/AmazonRedshiftQueryEditor",
    "arn:aws:iam::aws:policy/AmazonRedshiftFullAccess",
    "arn:aws:iam::aws:policy/AmazonRedshiftDataFullAccess",
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  ]

}

variable "iam_policy_transfer" {
  default = [
    "arn:aws:iam::aws:policy/AmazonS3FullAccess"
  ]

}

variable "iam_policy_sagemaker" {
  default = [
    "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess",
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/AmazonRedshiftQueryEditor",
    "arn:aws:iam::aws:policy/AmazonRedshiftFullAccess",
    "arn:aws:iam::aws:policy/AmazonRedshiftDataFullAccess"
  ]

}

variable "iam_policy_glue_redshift" {
  default = [
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    "arn:aws:iam::aws:policy/AmazonEC2FullAccess",
    "arn:aws:iam::aws:policy/AmazonRedshiftQueryEditor",
    "arn:aws:iam::aws:policy/AmazonRedshiftFullAccess",
    "arn:aws:iam::aws:policy/AmazonRedshiftDataFullAccess"
  ]
}

variable "iam_policy_lambda_redshift" {
  default = [
    "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
  ]

}

variable "iam_policy_lambda_stepfunctions" {
  default = [
    "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess"
  ]

}

variable "iam_policy_redshift_sandbox" {
  type    = string
  default = "arn:aws:iam::aws:policy/AmazonRedshiftQueryEditor"

}

variable "iam_policy_redshift_event_bridge" {
  type    = string
  default = "arn:aws:iam::aws:policy/AmazonEventBridgeFullAccess"

}

variable "iam_policy_step_functions_lambda" {
  default = [
    "arn:aws:iam::aws:policy/service-role/AWSLambdaRole",
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  ]
}

variable "iam_groups" {
  default = [
    "pricing",
    "financial",
    "data-science",
    "murabei",
    "portfolio-managers",
    "operational-managers",
    "commercial",
    "information-technology",
    "ntt-data"
  ]
}

variable "sandbox_groups" {
  default = [
    "pricing",
    "financial",
    "data_science",
    "murabei",
    "portfolio_managers",
    "operational_managers",
    "commercial",
    "information_technology",
    "ntt-data"
  ]
}

variable "env" {
  type = string
}
variable "user_glue_a" {
  type = map(string)

  default = {
    "dev"  = "rkurosaki"
    "prod" = "TerraformUserDLR"
  }

}

variable "user_glue_b" {
  type = map(string)

  default = {
    "dev"  = "mrodrigues"
    "prod" = ""
  }

}

variable "user_glue_c" {
  type = map(string)

  default = {
    "dev"  = ""
    "prod" = ""
  }

}


variable "user_glue_d" {
  type = map(string)

  default = {
    "dev"  = "tviana"
    "prod" = ""
  }

}
variable "user_redshift_a" {
  type = map(string)

  default = {
    "dev"  = "rkurosaki"
    "prod" = "RedshiftUserDLR"
  }

}


variable "user_redshift_b" {
  type = map(string)

  default = {
    "dev"  = "mrodrigues"
    "prod" = ""
  }

}

variable "dlr_vpc_dev" {
  type = map(string)

  default = {
    "dev"  = "vpc-0c538d8fc05c35d21"
    "prod" = "vpc-aff230c4"
  }

}

variable "subnet" {
  type = map(string)

  default = {
    "dev"  = "subnet-0bd22bb08377d8174"
    "prod" = "subnet-3fb47954"
  }
}