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
variable "instance_tags_tfstate" {
  type = map(string)

  description = ""

  default = {
    Name      = "BucketTFState"
    ManagedBy = "Terraform"
    UpdateAt  = "2021-10-18T21h05"
  }

}

variable "instance_tags_refined" {
  type = map(string)

  description = ""

  default = {
    Name      = "BucketRefinedZone"
    ManagedBy = "Terraform"
    UpdateAt  = "2021-10-18T21h05"
  }

}

variable "instance_tags_raw" {
  type = map(string)

  description = ""

  default = {
    Name      = "BucketRawZone"
    ManagedBy = "Terraform"
    UpdateAt  = "2021-10-18T21h05"
  }

}

variable "instance_tags_scripts" {
  type = map(string)

  description = ""

  default = {
    Name      = "BucketScript"
    ManagedBy = "Terraform"
    UpdateAt  = "2021-10-18T21h05"
  }

}

variable "instance_tags_sandbox" {
  type = map(string)

  description = ""

  default = {
    Name      = "BucketSandbox"
    ManagedBy = "Terraform"
    UpdateAt  = "2021-10-18T21h05"
  }

}

variable "instance_tags_transfer" {
  type = map(string)

  description = ""

  default = {
    Name      = "BucketTransfer"
    ManagedBy = "Terraform"
    UpdateAt  = "2021-10-18T21h05"
  }

}


variable "instance_tags_transient" {
  type = map(string)

  description = ""

  default = {
    Name      = "BucketTransient"
    ManagedBy = "Terraform"
    UpdateAt  = "2021-10-19T13h35"
  }
}
variable "env" {
  type = string
}

variable "service_point" {
  type = map(string)

  default = {
    "dev"  = "com.amazonaws.us-east-2a.s3"
    "prod" = "com.amazonaws.us-east-1d.s3"
  }
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

variable "murabei_folders" {
  default = [
    "debt_info",
    "debt_time_categorical"
  ]
}

variable "bucket_objects" {
  default = [
    "table_ingestion_v3.py",
    "dlr-pic-carga-incremental-insert.py",
    "incremental_by_batch_v4.py",
    "datamart-arrangements-v13.py",
    "datamart-arrangements-installments-v11.py",
    "datamart-workflowtrackings-v5.py",
    "glue_a_updates_v10.py",
    "datamart-payments-v15.py",
    "datamart-debtcontacts-v12.py",
    "small_table_ingestion_v3.py",
    "datamart-arrangements-update-v6.py",
    "datamart-arrangements-installments-update-v6.py",
    "datamart-debtcontacts-update-v5.py",
    "datamart-payments-update-v6.py",
    "table_ingestion_with_variant_v3.py",
    "lambda_function_sandbox_definitions_v3.zip",
    "table_ingestion_with_partition_v6.py",
    "debt_info_v1.py",
    "deb_status_v1.py",
    "small_table_ingestion_with_variant_v2.py",
    "glue_a_updates_with_variant_v3.py",
    "datamart-agency-goals-v2.py",
    "table_ingestion_payments_v4.py",
    "datamart-assignment-debts-analytical-v1.py",
    "datamart-process-debtstatus-v1-py",
    "glue_a_full_v1.py",
    "main_validator.py",
    "validator_acordo.py",
    "hash-validator-v2.py",
    "apply_change_coligadas_acordo.py",
    "validador_telefone"
  ]

}

variable "bucket_sources" {
  default = [
    "../../scripts/glue-a/table_ingestion_v3.py",
    "../../scripts/glue-a/dlr-pic-carga-incremental-insert.py",
    "../../scripts/glue-a/incremental_by_batch_v4.py",
    "../../scripts/glue-b/datamart-arrangements-v13.py",
    "../../scripts/glue-b/datamart-arrangements-installments-v11.py",
    "../../scripts/glue-b/datamart-workflowtrackings-v5.py",
    "../../scripts/glue-a/glue_a_updates_v10.py",
    "../../scripts/glue-b/datamart-payments-v15.py",
    "../../scripts/glue-b/datamart-debtcontacts-v12.py",
    "../../scripts/glue-a/small_table_ingestion_v3.py",
    "../../scripts/glue-b/datamart-arrangements-update-v6.py",
    "../../scripts/glue-b/datamart-arrangements-installments-update-v5.py",
    "../../scripts/glue-b/datamart-debtcontacts-update-v5.py",
    "../../scripts/glue-b/datamart-payments-update-v6.py",
    "../../scripts/glue-a/table_ingestion_with_variant_v3.py",
    "../../scripts/lambda/lambda_function_sandbox_definitions_v3.zip",
    "../../scripts/glue-a/table_ingestion_with_partition_v6.py",
    "../../scripts/murabei/update_database/debt_info_v1.py",
    "../../scripts/murabei/update_database/debt_status_v1.py",
    "../../scripts/glue-a/small_table_ingestion_with_variant_v2.py",
    "../../scripts/glue-a/glue_a_updates_with_variant_v3.py",
    "../../scripts/glue-b/datamart-agency-goals-v2.py",
    "../../scripts/glue-a/table_ingestion_payments_v4.py",
    "../../scripts/glue-b/datamart-assignment-debts-analytical-v1.py",
    "../../scripts/glue-b/datamart-process-debtstatus-v1.py",
    "../../scripts/glue-a/glue_a_full_v1.py",
    "../../scripts/validador-parceiros/dialer/main_validator.py",
    "../../scripts/validador-coligadas/acordo/validator_acordo.py",
    "../../scripts/glue-b/hash-validator-v2.py",
    "../../scripts/glue-a/apply_change_coligadas_acordo.py",
    "../../scripts/validador-coligadas/telefone/validador_telefone.py",
  ]

}

variable "partnership_folders" {
  default = [
    "discador",
    "email",
    "sms"
  ]
}

variable "python_files_keys" {
  default = [
    "modules_validate.zip",
    "modules_coligadas_acordo.zip",
    "modules_coligadas_telefone.zip"
  ]
}

variable "python_files_sources" {
  default = [
    "../../scripts/validador-parceiros/dialer/modules_validate.zip",
    "../../scripts/validador-coligadas/acordo/modules_coligadas_acordo.zip",
    "../../scripts/validador-coligadas/telefone/modules_coligadas_telefone.zip"
  ]
}


variable "json_files_keys" {
  default = [
    "discador.json",
    "sms.json",
    "acordo.json",
    "telefone.json",
    "email.json",
    "pessoa.json"
  ]
}
variable "json_files_sources" {
  default = [
    "../../scripts/validator/discador.json",
    "../../scripts/validator/sms.json",
    "../../scripts/validador-coligadas/acordo/acordo.json",
    "../../scripts/validador-coligadas/telefone/telefone.json",
    "../../scripts/validador-coligadas/email/email.json",
    "../../scripts/validador-coligadas/pessoa/pessoa.json"
  ]
}

variable "coligadas_folders" {
  default = [
    "emdia"
  ]
}