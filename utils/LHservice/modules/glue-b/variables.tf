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

variable "env" {
  type = string
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

variable "glue_sql_security_group" {
  type = map(string)

  default = {
    "dev"  = "sg-06bdf97a065fd4773"
    "prod" = "sg-0c31d763"
  }

}

variable "availability_zone" {
  type = map(string)

  default = {
    "dev"  = "us-east-2a"
    "prod" = "us-east-1d"
  }
}


variable "glue_security_group" {
}

variable "glue_role" {
}

variable "glue_role_redshift" {
}

variable "user_redshift" {

}

variable "password_redshift" {

}


variable "glue_role_id" {

}

variable "glue_security_group_id" {

}

variable "kms_key" {

}
variable "glue_redshift_group" {
  type = map(string)

  default = {
    "dev"  = "sg-0f8130f78db20b509"
    "prod" = "sg-7efb1d11"
  }
}

variable "url_redshift" {
  type = map(string)

  default = {
    "dev"  = "jdbc:redshift://172.31.0.17:5439/dlrdb"
    "prod" = "jdbc:redshift://10.50.6.20:5439/dlrdb"
  }

}

# Variables to Datamart Creation

variable "datamart_name" {
  default = [
    "arrangements",
    "arrangementinstallments",
    "workflowtrackings",
    "payments",
    "debtcontacts",
    "agencygoals",
    "debtstatus",
    "assignments"
  ]

}

variable "datamart_scripts" {
  default = [
    "datamart-arrangements-v13.py",
    "datamart-arrangements-installments-v11.py",
    "datamart-workflowtrackings-v5.py",
    "datamart-payments-v15.py",
    "datamart-debtcontacts-v12.py",
    "datamart-agency-goals-v2.py",
    "datamart-process-debtstatus-v1.py",
    "datamart-assignment-debts-analytical-v1.py"
  ]

}

variable "schedule_datamart" {
  default = [
    "cron(30 4 * * ?)",
    "cron(30 6 * * ?)",
    "cron(30 4 * * ?)",
    "cron(30 2 * * ?)",
    "cron(30 6 * * ?)",
    "cron(30 7 * * ?)",
    "cron(30 8 * * ?)",
    "cron(40 11 * * ?)"
  ]

}

variable "table_datamart_update" {
  default = [
    "arrangements",
    "arrangementinstallments",
    "debtcontacts",
    "payments"
  ]

}

variable "datamart_scripts_update" {
  default = [
    "datamart-arrangements-update-v6",
    "datamart-arrangements-installments-update-v5",
    "datamart-debtcontacts-update-v5",
    "datamart-payments-update-v6"
  ]
}

variable "schedule_datamart_updates" {
  default = [
    "cron(30 5 * * ?)",
    "cron(30 8 * * ?)",
    "cron(30 9 * * ?)",
    "cron(20 01 * * ?)"
  ]
}

variable "database_datamart_name" {
  
}

variable "table_datamart_hash" {
  default = [
    "arrangements",
    "arrangementinstallments",
    "debtcontacts",
    "payments",
    "workflowtrackings"
  ]

}

variable "hash_datamart_table"{
  default = [
    "Financial.Arrangements",
    "Financial.Arrangementinstallments",
    "CRM.Debtcontacts",
    "Financial.Payments",
    "CRM.Workflowtrackings"

  ]
}

variable "hash_datamart_id" {
  default = [
    "arrangementid",
    "installmentid",
    "debtid",
    "debttransactionid",
    "wktrackingid"
  ]
}

variable "schedule_datamart_hash" {
  default = [
    "cron(30 6 1 * ?)",
    "cron(30 7 1 * ?)",
    "cron(30 8 1 * ?)",
    "cron(30 9 1 * ?)",
    "cron(30 10 1 * ?)"
  ]
}

# Variables for SQL database connection
variable "jdbc_user" {
  type    = string
  default = "dlr_user"
}

variable "jdbc_pass" {

}

variable "string_connection" {
  type = map(string)

  default = {
    "dev"  = "jdbc:sqlserver://172.31.0.122:1433"
    "prod" = "jdbc:sqlserver://10.50.2.71:1433"
  }

}

variable "creation_date_col"{
  default = [
    "date",
    "date",
    "debtcreationdate",
    "date",
    "date"
  ]
}

variable "datamart_connection"{
  
}