terraform {

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "3.55.0"
    }
  }
}

# == FTCRM ==
# Connection 

resource "aws_glue_connection" "ftcrm_connection" {
  name = "dlr-connection-pic-ftcrm"

  connection_type = "JDBC"

  physical_connection_requirements {
    availability_zone      = var.availability_zone[var.env]
    security_group_id_list = ["${var.glue_sql_security_group[var.env]}", var.glue_security_group_id]
    subnet_id              = var.subnet[var.env]
  }


  connection_properties = {
    JDBC_CONNECTION_URL = "${var.string_connection[var.env]}/ftcrm"
    USERNAME            = var.jdbc_user
    PASSWORD            = var.jdbc_pass
  }

}

# database 

resource "aws_glue_catalog_database" "ftcrm_pic_database" {
  name = "dlr-database-pic-ftcrm"
}

resource "aws_glue_catalog_database" "ftcrm_raw_database" {
  name = "dlr-database-raw-ftcrm"
}


# == FTFoundation ==
# Connection

resource "aws_glue_connection" "ftfoundation_connection" {
  name = "dlr-connection-pic-ftfoundation"

  connection_type = "JDBC"

  physical_connection_requirements {
    availability_zone      = var.availability_zone[var.env]
    security_group_id_list = ["${var.glue_sql_security_group[var.env]}", var.glue_security_group_id]
    subnet_id              = var.subnet[var.env]
  }


  connection_properties = {
    JDBC_CONNECTION_URL = "${var.string_connection[var.env]}/ftfoundation"
    USERNAME            = var.jdbc_user
    PASSWORD            = var.jdbc_pass
  }
}

# database
resource "aws_glue_catalog_database" "ftfoundation_pic_database" {
  name = "dlr-database-pic-ftfoundation"
}

resource "aws_glue_catalog_database" "ftfoundation_raw_database" {
  name = "dlr-database-raw-ftfoundation"
}

# == Datamart ==
# Connection
resource "aws_glue_connection" "datamart_connection" {
  name = "dlr-connection-pic-datamart"

  connection_type = "JDBC"

  physical_connection_requirements {
    availability_zone      = var.availability_zone[var.env]
    security_group_id_list = ["${var.glue_sql_security_group[var.env]}", var.glue_security_group_id]
    subnet_id              = var.subnet[var.env]
  }


  connection_properties = {
    JDBC_CONNECTION_URL = "${var.string_connection[var.env]}/datamart"
    USERNAME            = var.jdbc_user
    PASSWORD            = var.jdbc_pass
  }
}

# database
resource "aws_glue_catalog_database" "datamart_pic_database" {
  name = "dlr-database-pic-datamart"
}

resource "aws_glue_catalog_database" "datamart_refined_database" {
  name = "dlr-database-refined-datamart"
}

# == FTContacts ==
# Connection
resource "aws_glue_connection" "ftcontacts_connection" {
  name = "dlr-connection-pic-ftcontacts"

  connection_type = "JDBC"

  physical_connection_requirements {
    availability_zone      = var.availability_zone[var.env]
    security_group_id_list = ["${var.glue_sql_security_group[var.env]}", var.glue_security_group_id]
    subnet_id              = var.subnet[var.env]
  }


  connection_properties = {
    JDBC_CONNECTION_URL = "${var.string_connection[var.env]}/ftcontacts"
    USERNAME            = var.jdbc_user
    PASSWORD            = var.jdbc_pass
  }

}

# database
resource "aws_glue_catalog_database" "ftcontacts_pic_database" {
  name = "dlr-database-pic-ftcontacts"
}

resource "aws_glue_catalog_database" "ftcontacts_raw_database" {
  name = "dlr-database-raw-ftcontacts"
}

# == FT5L ==
# Connection
resource "aws_glue_connection" "ft5l_connection" {
  name = "dlr-connection-pic-ft5l"

  connection_type = "JDBC"

  physical_connection_requirements {
    availability_zone      = var.availability_zone[var.env]
    security_group_id_list = ["${var.glue_sql_security_group[var.env]}", var.glue_security_group_id]
    subnet_id              = var.subnet[var.env]
  }


  connection_properties = {
    JDBC_CONNECTION_URL = "${var.string_connection[var.env]}/ft5l"
    USERNAME            = var.jdbc_user
    PASSWORD            = var.jdbc_pass
  }

}

# database
resource "aws_glue_catalog_database" "ft5l_pic_database" {
  name = "dlr-database-pic-ft5l"
}

resource "aws_glue_catalog_database" "ft5l_raw_database" {
  name = "dlr-database-raw-ft5l"
}

# == FTServices ==
# Connection
resource "aws_glue_connection" "ftservices_connection" {
  name = "dlr-connection-pic-ftservices"

  connection_type = "JDBC"

  physical_connection_requirements {
    availability_zone      = var.availability_zone[var.env]
    security_group_id_list = ["${var.glue_sql_security_group[var.env]}", var.glue_security_group_id]
    subnet_id              = var.subnet[var.env]
  }

  connection_properties = {
    JDBC_CONNECTION_URL = "${var.string_connection[var.env]}/ftservices"
    USERNAME            = var.jdbc_user
    PASSWORD            = var.jdbc_pass
  }

}

# database
resource "aws_glue_catalog_database" "ftservices_pic_database" {
  name = "dlr-database-pic-ftservices"
}

resource "aws_glue_catalog_database" "ftservices_raw_database" {
  name = "dlr-database-raw-ftservices"
}

# == FTAccounting ==
# Connection
resource "aws_glue_connection" "ftaccounting_connection" {
  name = "dlr-connection-pic-ftaccounting"

  connection_type = "JDBC"

  physical_connection_requirements {
    availability_zone      = var.availability_zone[var.env]
    security_group_id_list = ["${var.glue_sql_security_group[var.env]}", var.glue_security_group_id]
    subnet_id              = var.subnet[var.env]
  }

  connection_properties = {
    JDBC_CONNECTION_URL = "${var.string_connection[var.env]}/ftaccounting"
    USERNAME            = var.jdbc_user
    PASSWORD            = var.jdbc_pass
  }

}

# database
resource "aws_glue_catalog_database" "ftaccounting_pic_database" {
  name = "dlr-database-pic-ftaccounting"
}

resource "aws_glue_catalog_database" "ftaccounting_raw_database" {
  name = "dlr-database-raw-ftaccounting"
}

# == FTWarehouse ==
# Connection
resource "aws_glue_connection" "ftwarehouse_connection" {
  name = "dlr-connection-pic-ftwarehouse"

  connection_type = "JDBC"

  physical_connection_requirements {
    availability_zone      = var.availability_zone[var.env]
    security_group_id_list = ["${var.glue_sql_security_group[var.env]}", var.glue_security_group_id]
    subnet_id              = var.subnet[var.env]
  }

  connection_properties = {
    JDBC_CONNECTION_URL = "${var.string_connection[var.env]}/ftwarehouse"
    USERNAME            = var.jdbc_user
    PASSWORD            = var.jdbc_pass
  }

}

# database
resource "aws_glue_catalog_database" "ftwarehouse_pic_database" {
  name = "dlr-database-pic-ftwarehouse"
}

resource "aws_glue_catalog_database" "ftwarehouse_raw_database" {
  name = "dlr-database-raw-ftwarehouse"
}

# == SandBox ==
# database
resource "aws_glue_catalog_database" "crm_sandbox_database" {
  name = "dlr-database-sandbox-crm"
}

resource "aws_glue_catalog_database" "datamart_sandbox_database" {
  name = "dlr-database-sandbox-datamart"
}

resource "aws_glue_catalog_database" "financial_sandbox_database" {
  name = "dlr-database-sandbox-financial"
}

# == DataScience ==
# Connection
resource "aws_glue_connection" "datascience_connection" {
  name = "dlr-connection-pic-datascience"

  connection_type = "JDBC"

  physical_connection_requirements {
    availability_zone      = var.availability_zone[var.env]
    security_group_id_list = ["${var.glue_sql_security_group[var.env]}", var.glue_security_group_id]
    subnet_id              = var.subnet[var.env]
  }

  connection_properties = {
    JDBC_CONNECTION_URL = "${var.string_connection[var.env]}/datascience"
    USERNAME            = var.jdbc_user
    PASSWORD            = var.jdbc_pass
  }

}

# database
resource "aws_glue_catalog_database" "datascience_pic_database" {
  name = "dlr-database-pic-datascience"
}

resource "aws_glue_catalog_database" "datascience_raw_database" {
  name = "dlr-database-raw-datascience"
}