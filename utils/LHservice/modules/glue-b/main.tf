terraform {

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "3.55.0"
    }
  }
}

# == REDSHIFT ==
# Connection 

resource "aws_glue_connection" "redshift_connection" {
  name = "dlr-connection-redshift"

  connection_properties = {
    JDBC_CONNECTION_URL = var.url_redshift[var.env]
    USERNAME            = var.user_redshift
    PASSWORD            = var.password_redshift
  }

  physical_connection_requirements {
    availability_zone      = var.availability_zone[var.env]
    security_group_id_list = [var.glue_redshift_group[var.env]]
    subnet_id              = var.subnet[var.env]
  }

}
