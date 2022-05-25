# declaration of variables from the outputs of other modules

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

variable "env" {
  type = string
}

variable "jdbc_pass" {

}

variable "kms_key" {

}

variable "glue_security_group" {

}

variable "glue_role" {

}

variable "glue_role_id" {

}

variable "glue_security_group_id" {

}

# Variables for SQL database connection
variable "jdbc_user" {
  type    = string
  default = "dlr_user"
}

variable "string_connection" {
  type = map(string)

  default = {
    "dev"  = "jdbc:sqlserver://172.31.0.122:1433"
    "prod" = "jdbc:sqlserver://10.50.2.71:1433"
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

##############################################
### Variables for tables FTRCM connection  ###
##############################################
variable "table_ftcrm" {
  default = [
    "agents",
    "arrangementinstallments",
    "arrangements",
    "bindings",
    "businessunits",
    "cancellationreasons",
    "creditrestrictions",
    "debtcustomextensions",
    "debttransactioncodes",
    "debttransactioncommissions",
    "debttransactions",
    "debts",
    "installmenttransactions",
    "portfolios",
    "products",
    "producttypes",
    "scores",
    "scoremodels",
    "workflowtaskresults",
    "workflowtrackings",
    "assignments",
    "customers",
    "digitaldebtassignments",
    "crmauditlookups",
    "crmauditsearches",
    "consumercredits",
    "crmschedules",
    "debtclaims",
    "vintagedebts"
  ]
}

variable "jdbc_target_ftcrm" {
  default = [
    "FTCRM/dbo/Agents",
    "FTCRM/dbo/ArrangementInstallments",
    "FTCRM/dbo/Arrangements",
    "FTCRM/dbo/Bindings",
    "FTCRM/dbo/BusinessUnits",
    "FTCRM/dbo/CancellationReasons",
    "FTCRM/dbo/CreditRestrictions",
    "FTCRM/dbo/DebtCustomExtensions",
    "FTCRM/dbo/DebtTransactionCodes",
    "FTCRM/dbo/DebtTransactionCommissions",
    "FTCRM/dbo/DebtTransactions",
    "FTCRM/dbo/Debts",
    "FTCRM/dbo/InstallmentTransactions",
    "FTCRM/dbo/Portfolios",
    "FTCRM/dbo/Products",
    "FTCRM/dbo/ProductTypes",
    "FTCRM/dbo/Scores",
    "FTCRM/dbo/ScoreModels",
    "FTCRM/dbo/WorkflowTaskResults",
    "FTCRM/dbo/WorkflowTrackings",
    "FTCRM/dbo/Assignments",
    "FTCRM/dbo/Customers",
    "FTCRM/dbo/DigitalDebtAssignments",
    "FTCRM/dbo/CRMAuditLookups",
    "FTCRM/dbo/CRMAuditSearches",
    "FTCRM/dbo/ConsumerCredits",
    "FTCRM/dbo/CRMSchedules",
    "FTCRM/dbo/DebtClaims",
    "FTCRM/dbo/VintageDebts"
  ]
}

variable "schedule_ftcrm" {
  default = [
    "cron(45 21 07 12 ? *)",
    "cron(0 30 00 13 01 ? 2022)",
    "cron(45 21 07 12 ? *)",
    "cron(45 21 07 12 ? *)",
    "cron(48 21 07 12 ? *)",
    "cron(48 21 07 12 ? *)",
    "cron(48 21 07 12 ? *)",
    "cron(48 21 07 12 ? *)",
    "cron(50 21 07 12 ? *)",
    "cron(50 21 07 12 ? *)",
    "cron(50 21 07 12 ? *)",
    "cron(50 21 07 12 ? *)",
    "cron(52 21 07 12 ? *)",
    "cron(52 21 07 12 ? *)",
    "cron(55 21 07 12 ? *)",
    "cron(55 21 07 12 ? *)",
    "cron(58 21 07 12 ? *)",
    "cron(58 21 07 12 ? *)",
    "cron(58 21 07 12 ? *)",
    "cron(58 21 07 12 ? *)",
    "cron(58 21 07 12 ? *)",
    "cron(58 21 07 12 ? *)",
    "cron(58 21 07 12 ? *)",
    "cron(30 22 07 12 ? *)",
    "cron(30 22 07 12 ? *)",
    "cron(45 21 29 03 ? *)",
    "cron(30 21 29 03 ? *)",
    "cron(40 21 29 03 ? *)",
    "cron(50 21 29 03 ? *)"
  ]
}

variable "source_s3_ftcrm" {
  default = [
    "FTCRM.dbo.Agents.txt",
    "FTCRM.dbo.ArrangementInstallments.txt",
    "FTCRM.dbo.Arrangements.txt",
    "FTCRM.dbo.Bindings.txt",
    "FTCRM.dbo.BusinessUnits.txt",
    "FTCRM.dbo.CancellationReasons.txt",
    "FTCRM.dbo.CreditRestrictions.txt",
    "FTCRM.dbo.DebtCustomExtensions.txt",
    "FTCRM.dbo.DebtTransactionCodes.txt",
    "FTCRM.dbo.DebtTransactionCommissions.txt",
    "FTCRM.dbo.DebtTransactions.txt",
    "FTCRM.dbo.Debts.txt",
    "FTCRM.dbo.InstallmentTransactions.txt",
    "FTCRM.dbo.Portfolios.txt",
    "FTCRM.dbo.Products.txt",
    "FTCRM.dbo.ProductTypes.txt",
    "FTCRM.dbo.Scores.txt",
    "FTCRM.dbo.ScoreModels.txt",
    "FTCRM.dbo.WorkflowTaskResults.txt",
    "FTCRM.dbo.WorkflowTrackings.txt",
    "FTCRM.dbo.Assignments.txt",
    "FTCRM.dbo.Customers.txt",
    "FTCRM.dbo.DigitalDebtAssignments.txt",
    "ftcrm.dbo.CRMAuditLookups.txt",
    "ftcrm.dbo.CRMAuditSearches.txt",
    "FTCRM.dbo.ConsumerCredits.txt",
    "FTCRM.dbo.CRMSchedules.txt",
    "FTCRM.dbo.DebtClaims.txt",
    "FTCRM.dbo.VintageDebts.txt"
  ]
}

variable "table_crawler_ftcrm" {
  default = [
    "ftcrm_dbo_agents",
    "ftcrm_dbo_arrangementinstallments",
    "ftcrm_dbo_arrangements",
    "ftcrm_dbo_bindings",
    "ftcrm_dbo_businessunits",
    "ftcrm_dbo_cancellationreasons",
    "ftcrm_dbo_creditrestrictions",
    "ftcrm_dbo_debtcustomextensions",
    "ftcrm_dbo_debttransactioncodes",
    "ftcrm_dbo_debttransactioncommissions",
    "ftcrm_dbo_debttransactions",
    "ftcrm_dbo_debts",
    "ftcrm_dbo_installmenttransactions",
    "ftcrm_dbo_portfolios",
    "ftcrm_dbo_products",
    "ftcrm_dbo_ProductTypes",
    "ftcrm_dbo_scores",
    "ftcrm_dbo_scoremodels",
    "ftcrm_dbo_workflowtaskresults",
    "ftcrm_dbo_workflowtrackings",
    "ftcrm_dbo_assignments",
    "ftcrm_dbo_customers",
    "ftcrm_dbo_digitaldebtassignments",
    "ftcrm_dbo_crmauditlookups",
    "ftcrm_dbo_crmauditsearches",
    "ftcrm_dbo_consumercredits",
    "ftcrm_dbo_crmschedules",
    "ftcrm_dbo_debtclaims",
    "ftcrm_dbo_vintagedebts"
  ]
}

# Variables for FTCRM Insert
variable "table_ftcrm_insert" {
  default = [
    "arrangementinstallments",
    "arrangements",
    "bindings",
    "debtcustomextensions",
    "debttransactions",
    "debts",
    "workflowtrackings",
    "crmauditlookups",
    "crmauditsearches",
    "digitaldebtassignments",
    "debtscores",
    "customers",
    "creditrestrictions",
    "consumercredits",
    "crmschedules",
    "debtclaims",
    "vintagedebts"
  ]

}
variable "reference_col_ftcrm" {
  default = [
    "installmentid",
    "arrangementid",
    "bindingid",
    "debtid",
    "debttransactionid",
    "debtid",
    "wktrackingid",
    "crmlookupid",
    "crmsearchid",
    "digitaldebtassignmentid",
    "debtscoreid",
    "customerid",
    "restrictionid",
    "debtid",
    "crmscheduleid",
    "debtclaimid",
    "debtid",
    "campaigndefinitionid",
    "filterid",
    "crmscheduleid",
    "storeid"
  ]

}

variable "table_crawler_ftcrm_insert" {
  default = [
    "ftcrm_dbo_arrangementinstallments",
    "ftcrm_dbo_arrangements",
    "ftcrm_dbo_bindings",
    "ftcrm_dbo_debtcustomextensions",
    "ftcrm_dbo_debttransactions",
    "ftcrm_dbo_debts",
    "ftcrm_dbo_workflowtrackings",
    "ftcrm_dbo_crmauditlookups",
    "ftcrm_dbo_crmauditsearches",
    "ftcrm_dbo_digitaldebtassignments",
    "ftcrm_dbo_debtscores",
    "ftcrm_dbo_customers",
    "ftcrm_dbo_creditrestrictions"
  ]
}

variable "table_name_ftcrm_insert" {
  default = [
    "dbo.ArrangementInstallments",
    "dbo.Arrangements",
    "dbo.Bindings",
    "dbo.DebtCustomExtensions",
    "dbo.DebtTransactions",
    "dbo.Debts",
    "dbo.WorkflowTrackings",
    "dbo.CRMAuditLookups",
    "dbo.CRMAuditSearches",
    "dbo.DigitalDebtAssignments",
    "dbo.DebtScores",
    "dbo.Customers",
    "dbo.CreditRestrictions",
    "dbo.ConsumerCredits",
    "dbo.CRMSchedules",
    "dbo.DebtClaims",
    "dbo.VintageDebts"
  ]

}
variable "schedule_ftcrm_incremental" {
  default = [
    "cron(0 0 6 * * ?)",
    "cron(0 0 6 * * ?)",
    "cron(0 0 4 * * ?)",
    "cron(0 0 4 * * ?)",
    "cron(0 0 4 * * ?)",
    "cron(0 0 4 * * ?)",
    "cron(0 0 6 * * ?)",
    "cron(0 0 4 * * ?)",
    "cron(0 0 4 * * ?)",
    "cron(0 0 5 * * ?)",
    "cron(0 0 5 * * ?)",
    "cron(0 0 5 * * ?)",
    "cron(0 0 4 * * ?)",
    "cron(0 0 6 * * ?)",
    "cron(0 0 5 * * ?)",
    "cron(0 0 4 * * ?)",
    "cron(0 0 6 * * ?)"
  ]

}

# Variables for FTCRM Updates
variable "tables_updates_ftcrm" {
  default = [
    "agents",
    "arrangementinstallments",
    "arrangements",
    "bindings",
    "businessunits",
    "cancellationreasons",
    "creditrestrictions",
    "debtcustomextensions",
    "debttransactioncodes",
    "debttransactioncommissions",
    "debttransactions",
    "debts",
    "installmenttransactions",
    "portfolios",
    "products",
    "producttypes",
    "scores",
    "scoremodels",
    "workflowtaskresults",
    "campaigns",
    "campaignoptions",
    "campaignservices",
    "negotiations",
    "negotiationcriteria",
    "crmactionings",
    "crmauditlookups",
    "crmauditsearches",
    "portfoliogroups",
    "digitaldebtassignments",
    "debtscores",
    "customers",
    "commissions",
    "workflowstatuses",
    "consumercredits",
    "crmschedules",
    "debtclaims",
    "vintagedebts",
    "campaigndefinitions",
    "filters",
    "positivenegotiationytdbindings",
    "stores"
  ]

}

variable "table_name_ftcrm_update" {
  default = [
    "dbo.Agents",
    "dbo.ArrangementInstallments",
    "dbo.Arrangements",
    "dbo.Bindings",
    "dbo.BusinessUnits",
    "dbo.CancellationReasons",
    "dbo.CreditRestrictions",
    "dbo.DebtCustomExtensions",
    "dbo.DebtTransactionCodes",
    "dbo.DebtTransactionCommissions",
    "dbo.DebtTransactions",
    "dbo.Debts",
    "dbo.InstallmentTransactions",
    "dbo.Portfolios",
    "dbo.Products",
    "dbo.ProductTypes",
    "dbo.Scores",
    "dbo.ScoreModels",
    "dbo.WorkflowTaskResults",
    "dbo.Campaigns",
    "dbo.CampaignOptions",
    "dbo.CampaignServices",
    "dbo.Negotiations",
    "dbo.NegotiationCriteria",
    "dbo.CRMActionings",
    "dbo.CRMAuditLookups",
    "dbo.CRMAuditSearches",
    "dbo.PortfolioGroups",
    "dbo.DigitalDebtAssignments",
    "dbo.DebtScores",
    "dbo.Customers",
    "dbo.Commissions",
    "dbo.WorkflowStatuses",
    "dbo.ConsumerCredits",
    "dbo.CRMSchedules",
    "dbo.DebtClaims",
    "dbo.VintageDebts",
    "dbo.CampaignDefinitions",
    "dbo.Filters",
    "dbo.PositiveNegotiationYTDBindings",
    "dbo.Stores"
  ]
}

variable "reference_col_ftcrm_update" {
  default = [
    "agentsid",                    #1
    "installmentid",               #2
    "arrangementid",               #3
    "bindingid",                   #4
    "businessunitid",              #5
    "cancellationreasonid",        #6
    "restrictionid",               #7
    "debtid",                      #8
    "transactioncode",             #9
    "debttransactioncommissionid", #10
    "debttransactionid",           #11
    "debtid",                      #12
    "installmenttransactionid",    #13 
    "portfolioid",                 #14 
    "productid",                   #15 
    "producttypeid",               #16
    "scoreid",                     #17
    "scoremodelid",                #18
    "wktaskresultid",              #19
    "campaignid",                  #20
    "campaignoptionid",            #21
    "campaignserviceid",           #22
    "negotiationid",               #23
    "negotiationcriteriaid",       #24
    "crmactioningid",              #25 
    "crmlookupid",                 #26
    "crmsearchid",                 #27
    "portfoliogroupid",            #28
    "digitaldebtassignmentid",     #29
    "debtscoreid",                 #30
    "customerid",                  #31
    "commissionid",                #32
    "wkentityid",                  #33
    "debtid",                      #34
    "crmscheduleid",               #35
    "debtclaimid",                 #36
    "debtid",                      #37
    "campaigndefinitionid",        #38
    "filterid",                    #39
    "bindingid",                   #40
    "storeid"                      #41
  ]

}

variable "table_col_ftcrm_update" {
  default = [
    "N/A",
    "lastmodificationdate",
    "lastmodificationdate",
    "lastmodificationdate",
    "N/A",
    "N/A",
    "lastmodificationdate",
    "lastmodificationdate",
    "N/A",
    "N/A",
    "lastmodificationdate",
    "lastmodificationdate",
    "N/A",
    "N/A",
    "N/A",
    "N/A",
    "N/A",
    "N/A",
    "N/A",
    "N/A",
    "N/A",
    "N/A",
    "N/A",
    "N/A",
    "N/A",
    "N/A",
    "N/A",
    "N/A",
    "N/A",
    "lastmodificationdate",
    "lastmodificationdate",
    "N/A",
    "N/A",
    "N/A",
    "lastmodificationdate",
    "lastmodificationdate",
    "N/A",
    "lastmodificationdate",
    "lastmodificationdate",
    "N/A",
    "N/A"
  ]

}

variable "full_update_ftcrm" {
  default = [
    "true",
    "false",
    "false",
    "false",
    "true",
    "true",
    "false",
    "false",
    "true",
    "true",
    "false",
    "false",
    "true",
    "true",
    "true",
    "true",
    "true",
    "true",
    "true",
    "true",
    "true",
    "true",
    "true",
    "true",
    "true",
    "true",
    "true",
    "true",
    "true",
    "false",
    "false",
    "true",
    "true",
    "true",
    "false",
    "false",
    "true",
    "false",
    "false",
    "true",
    "true"
  ]

}

variable "scripts_updates" {
  type = map(string)
  default = {
    "update" = "glue_a_updates_v10.py",
    "full"   = "glue_a_full_v1.py"
  }

}

variable "schedule_ftcrm_updates" {
  default = [
    "cron(0 45 5 * * ?)", #1
    "cron(0 5 * * ?)",    #2
    "cron(0 5 * * ?)",    #3
    "cron(0 5 * * ?)",    #4
    "cron(0 3 * * ?)",    #5
    "cron(0 50 4 * * ?)", #6
    "cron(0 30 4 * * ?)", #7
    "cron(30 4 * * ?)",   #8
    "cron(0 45 3 * * ?)", #9
    "cron(20 3 * * ?)",   #10
    "cron(30 4 * * ?)",   #11
    "cron(30 4 * * ?)",   #12
    "cron(0 45 5 * * ?)", #13
    "cron(0 3 * * ?)",    #14
    "cron(0 3 * * ?)",    #15
    "cron(0 3 * * ?)",    #16
    "cron(0 0 15 * ? *)", #17
    "cron(0 50 4 * * ?)", #18
    "cron(0 3 * * ?)",    #19
    "cron(0 3 * * ?)",    #20
    "cron(0 30 6 * * ?)", #21
    "cron(0 45 5 * * ?)", #22
    "cron(0 3 * * ?)",    #23
    "cron(0 45 3 * * ?)", #24
    "cron(0 3 * * ?)",    #25   
    "cron(0 0 3 1 * ?)",  #26
    "cron(0 0 3 1 * ?)",  #27
    "cron(0 0 3 * * ?)",  #28
    "cron(0 0 3 5 * ?)",  #29
    "cron(0 30 5 * * ?)", #30
    "cron(0 30 5 * * ?)", #31
    "cron(0 0 3 * * ?)",  #32
    "cron(0 45 3 * * ?)", #33
    "cron(0 0 3 * * ?)",  #34
    "cron(0 0 3 * * ?)",  #35
    "cron(0 0 3 * * ?)",  #36
    "cron(0 0 5 * * ?)",  #37
    "cron(0 0 3 * * ?)",  #38
    "cron(0 0 4 * * ?)",  #39
    "cron(0 0 3 * * ?)",  #40
    "cron(0 0 5 * * ?)"   #41
  ]
}

####################################
variable "update_query_ftcrm" {
  default = [
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "update_campaignoptions_query",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a"
  ]
}

variable "update_ftcrm_script_location" {
  default = [
    "glue_a_full_v1.py",
    "glue_a_updates_v10.py",
    "glue_a_updates_v10.py",
    "glue_a_updates_v10.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_updates_v10.py",
    "glue_a_updates_v10.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_updates_v10.py",
    "glue_a_updates_v10.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_updates_with_variant_v3.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_updates_v10.py",
    "glue_a_updates_v10.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py"
  ]
}

variable "type_update_ftcrm" {
  default = [
    "full",
    "updates",
    "updates",
    "updates",
    "full",
    "full",
    "updates",
    "updates",
    "full",
    "full",
    "updates",
    "updates",
    "full",
    "full",
    "full",
    "full",
    "full",
    "full",
    "full",
    "full",
    "full",
    "full",
    "full",
    "full",
    "full",
    "full",
    "full",
    "full",
    "full",
    "updates",
    "updates",
    "full",
    "full",
    "full",
    "update",
    "update",
    "full",
    "update",
    "update",
    "full",
    "full"

  ]

}

variable "process_type_ftcrm" {
  default = [
    "FULL",
    "UPDATE",
    "UPDATE",
    "UPDATE",
    "FULL",
    "FULL",
    "UPDATE",
    "UPDATE",
    "FULL",
    "FULL",
    "UPDATE",
    "UPDATE",
    "FULL",
    "FULL",
    "FULL",
    "FULL",
    "FULL",
    "FULL",
    "FULL",
    "FULL",
    "FULL",
    "FULL",
    "FULL",
    "FULL",
    "FULL",
    "FULL",
    "FULL",
    "FULL",
    "FULL",
    "UPDATE",
    "UPDATE",
    "FULL",
    "FULL",
    "FULL",
    "UPDATE",
    "UPDATE",
    "FULL",
    "UPDATE",
    "UPDATE",
    "FULL",
    "FULL"
  ]

}


#####################################

####################################################
### Variables for small tables FTRCM connection  ###
####################################################
variable "small_table_ftcrm" {
  default = [
    "commissions",
    "portfoliogroups",
    "debtscores",
    "businessunittypes",
    "campaigns",
    "campaignoptions",
    "campaignservices",
    "negotiations",
    "negotiationcriteria",
    "crmactionings",
    "workflowstatuses",
    "campaigndefinitions",
    "filters",
    "positivenegotiationytdbindings",
    "stores",
    "subportfolios"
  ]
}

variable "small_table_crawler_ftcrm" {
  default = [
    "ftcrm_dbo_commissions",
    "ftcrm_dbo_portfoliogroups",
    "ftcrm_dbo_debtscores",
    "ftcrm_dbo_businessunittypes",
    "ftcrm_dbo_campaigns",
    "ftcrm_dbo_campaignoptions",
    "ftcrm_dbo_campaignservices",
    "ftcrm_dbo_negotiations",
    "ftcrm_dbo_negotiationcriteria",
    "ftcrm_dbo_crmactionings",
    "ftcrm_dbo_workflowstatuses",
    "ftcrm_dbo_campaigndefinitions",
    "ftcrm_dbo_filters",
    "ftcrm_dbo_positivenegotiationytdbindings",
    "ftcrm_dbo_stores",
    "ftcrm_dbo_subportfolios"
  ]
}

variable "schedule_small_ftcrm" {
  default = [
    "cron(0 30 05 10 12 ? *)",
    "cron(0 30 05 10 12 ? *)",
    "cron(0 30 05 10 12 ? *)",
    "cron(0 30 05 10 12 ? *)",
    "cron(0 30 05 10 12 ? *)",
    "cron(0 30 05 10 12 ? *)",
    "cron(0 30 05 10 12 ? *)",
    "cron(0 30 05 10 12 ? *)",
    "cron(0 30 05 10 12 ? *)",
    "cron(0 30 05 10 12 ? *)",
    "cron(45 17 10 12 ? 2021)",
    "cron(0 30 05 30 03 ? *)",
    "cron(0 30 05 30 03 ? *)",
    "cron(0 30 05 30 03 ? *)",
    "cron(0 30 05 30 03 ? *)",
    "cron(0 30 05 03 05 ? *)"
  ]
}

variable "jdbc_target_small_ftcrm" {
  default = [
    "FTCRM/dbo/Commissions",
    "FTCRM/dbo/PortfolioGroups",
    "FTCRM/dbo/DebtScores",
    "FTCRM/dbo/BusinessUnitTypes",
    "FTCRM/dbo/Campaigns",
    "FTCRM/dbo/CampaignOptions",
    "FTCRM/dbo/CampaignServices",
    "FTCRM/dbo/Negotiations",
    "FTCRM/dbo/NegotiationCriteria",
    "FTCRM/dbo/CRMActionings",
    "FTCRM/dbo/WorkflowStatuses",
    "FTCRM/dbo/CampaignDefinitions",
    "FTCRM/dbo/Filters",
    "FTCRM/dbo/PositiveNegotiationYTDBindings",
    "FTCRM/dbo/Stores",
    "FTCRM/dbo/SubPortfolios"
  ]

}

variable "query_ftcrm" {
  default = [
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "campaignoptions_query",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a",
    "n/a"
  ]
}

variable "ftcrm_script_location" {
  default = [
    "small_table_ingestion_v3.py",
    "small_table_ingestion_v3.py",
    "small_table_ingestion_v3.py",
    "small_table_ingestion_v3.py",
    "small_table_ingestion_v3.py",
    "small_table_ingestion_with_variant_v2.py",
    "small_table_ingestion_v3.py",
    "small_table_ingestion_v3.py",
    "small_table_ingestion_v3.py",
    "small_table_ingestion_v3.py",
    "small_table_ingestion_v3.py",
    "small_table_ingestion_v3.py",
    "small_table_ingestion_v3.py",
    "small_table_ingestion_v3.py",
    "small_table_ingestion_v3.py",
    "small_table_ingestion_v3.py"
  ]
}

####################################################
### Variables for tables FTFoundation connection ###
####################################################
variable "table_ftfoundation" {
  default = [
    "resourcecaptions",
    "resources"
  ]

}

variable "jdbc_target_ftfoundation" {
  default = [
    "FTFoundation/dbo/ResourceCaptions",
    "FTFoundation/dbo/Resources"
  ]

}

variable "schedule_ftfoundation" {
  default = [
    "cron(35 22 07 12 ? *)",
    "cron(35 22 07 12 ? *)"
  ]

}

variable "source_s3_ftfoundation" {
  default = [
    "FTFoundation.dbo.ResourceCaptions.txt",
    "FTFoundation.dbo.Resources.txt"
  ]
}

variable "table_crawler_ftfoundation" {
  default = [
    "ftfoundation_dbo_resourcecaptions",
    "ftfoundation_dbo_resources"
  ]
}

# Variables for FTFoundation Updates
variable "reference_col_ftfoundation" {
  default = [
    "resourcecaptionid",
    "resourceid"
  ]

}

variable "table_name_ftfoundation_update" {
  default = [
    "dbo.ResourceCaptions",
    "dbo.Resources"
  ]

}

variable "table_col_ftfoundation_update" {
  default = [
    "N/A",
    "N/A"

  ]

}

variable "full_update_ftfoundation" {
  default = [
    "true",
    "true"
  ]
}

variable "schedule_ftfoundation_updates" {
  default = [
    "cron(0 3 * * ?)",
    "cron(0 3 * * ?)"
  ]

}

##################################################
### Variables for tables FTContacts connection ###
##################################################
variable "table_ftcontacts" {
  default = [
    "addresses",
    "contacts",
    "identities",
    "states",
    "contactaddresses",
    "contactemails",
    "contactphones",
    "phones",
    "people"
  ]
}

variable "jdbc_target_ftcontacts" {
  default = [
    "FTContacts/dbo/Addresses",
    "FTContacts/dbo/Contacts",
    "FTContacts/dbo/Identities",
    "FTContacts/dbo/States",
    "FTContacts/dbo/ContactAddresses",
    "FTContacts/dbo/ContactEmails",
    "FTContacts/dbo/ContactPhones",
    "FTContacts/dbo/Phones",
    "FTContacts/dbo/People"
  ]

}

variable "schedule_ftcontacts" {
  default = [
    "cron(10 21 07 12 ? *)",
    "cron(11 21 07 12 ? *)",
    "cron(12 21 07 12 ? *)",
    "cron(13 21 07 12 ? *)",
    "cron(14 21 07 12 ? *)",
    "cron(34 21 07 12 ? *)",
    "cron(15 22 07 12 ? *)",
    "cron(15 22 07 12 ? *)",
    "cron(30 22 07 12 ? *)"
  ]

}

variable "source_s3_ftcontacts" {
  default = [
    "FTContacts.dbo.Addresses.txt",
    "FTContacts.dbo.Contacts.txt",
    "FTContacts.dbo.Identities.txt",
    "FTContacts.dbo.States.txt",
    "FTContacts.dbo.ContactAddresses.txt",
    "FTContacts.dbo.ContactEmails.txt",
    "FTContacts.dbo.ContactPhones.txt",
    "FTContacts.dbo.Phones.txt",
    "FTContacts.dbo.People.txt"
  ]
}

variable "table_crawler_ftcontacts" {
  default = [
    "ftcontacts_dbo_addresses",
    "ftcontacts_dbo_contacts",
    "ftcontacts_dbo_identities",
    "ftcontacts_dbo_states",
    "ftcontacts_dbo_contactaddresses",
    "ftcontacts_dbo_contactemails",
    "ftcontacts_dbo_contactphones",
    "ftcontacts_dbo_phones",
    "ftcontacts_dbo_people"
  ]
}

# Variables for load FTContacts Insert

variable "table_ftcontacts_insert" {
  default = [
    "addresses",
    "contacts",
    "identities",
    "contactaddresses",
    "contactemails",
    "contactphones",
    "phones"
  ]
}

variable "table_crawler_ftcontacts_insert" {
  default = [
    "ftcontacts_dbo_addresses",
    "ftcontacts_dbo_contacts",
    "ftcontacts_dbo_identities",
    "ftcontacts_dbo_contactaddresses",
    "ftcontacts_dbo_contactemails",
    "ftcontacts_dbo_contactphones",
    "ftcontacts_dbo_phones"
  ]
}


variable "table_name_ftcontacts_insert" {
  default = [
    "dbo.Addresses",
    "dbo.Contacts",
    "dbo.Identities",
    "dbo.ContactAddresses",
    "dbo.ContactEmails",
    "dbo.ContactPhones",
    "dbo.Phones"
  ]

}

variable "reference_col_ftcontacts" {
  default = [
    "addressid",
    "contactid",
    "identityid",
    "contactid",
    "contactemailid",
    "phoneid",
    "phoneid"
  ]

}

variable "schedule_ftcontacts_incremental" {
  default = [
    "cron(0 0 5 * * ?)",
    "cron(0 0 5 * * ?)",
    "cron(0 0 4 * * ?)",
    "cron(0 0 5 * * ?)",
    "cron(0 0 5 * * ?)",
    "cron(0 0 5 * * ?)",
    "cron(0 0 5 * * ?)"
  ]

}

# Variables for tables FTContacts Updates
variable "full_update_ftcontacts" {
  default = [
    "false",
    "false",
    "true",
    "false",
    "false",
    "false",
    "false",
    "true",
    "true",
    "true"
  ]
}

variable "scripts_updates_ftcontacts" {
  default = [
    "glue_a_updates_v10.py",
    "glue_a_updates_v10.py",
    "glue_a_full_v1.py",
    "glue_a_updates_v10.py",
    "glue_a_updates_v10.py",
    "glue_a_updates_v10.py",
    "glue_a_updates_v10.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py",
    "glue_a_full_v1.py"
  ]
}

variable "type_update_ftcontacts" {
  default = [
    "updates",
    "updates",
    "full",
    "updates",
    "updates",
    "updates",
    "updates",
    "full",
    "full",
    "full"
  ]
}

variable "process_type_ftcontacts" {
  default = [
    "UPDATE",
    "UPDATE",
    "FULL",
    "UPDATE",
    "UPDATE",
    "UPDATE",
    "UPDATE",
    "FULL",
    "FULL",
    "FULL"
  ]
}

variable "table_ftcontacts_update" {
  default = [
    "dbo.Addresses",
    "dbo.Contacts",
    "dbo.Identities",
    "dbo.ContactAddresses",
    "dbo.ContactEmails",
    "dbo.ContactPhones",
    "dbo.Phones",
    "dbo.Regions",
    "dbo.InfoProviders",
    "dbo.States"
  ]

}

variable "table_name_ftcontacts_update" {
  default = [
    "addresses",
    "contacts",
    "identities",
    "contactaddresses",
    "contactemails",
    "contactphones",
    "phones",
    "regions",
    "infoproviders",
    "states"
  ]

}


variable "table_col_ftcontacts_update" {
  default = [
    "lastmodificationdate",
    "lastmodificationdate",
    "N/A",
    "lastmodificationdate",
    "lastmodificationdate",
    "lastmodificationdate",
    "lastmodificationdate",
    "N/A",
    "N/A",
    "N/A"
  ]
}

variable "reference_col_ftcontacts_updates" {
  default = [
    "addressid",
    "contactid",
    "identityid",
    "contactid",
    "contactemailid",
    "phoneid",
    "phoneid",
    "regionid",
    "infoproviderid",
    "stateid"
  ]

}

variable "schedule_ftcontacts_updates" {
  default = [
    "cron(0 30 5 * * ?)",
    "cron(0 30 5 * * ?)",
    "cron(0 0 3 1 * ?)",
    "cron(0 30 5 * * ?)",
    "cron(0 30 5 * * ?)",
    "cron(0 30 5 * * ?)",
    "cron(0 30 5 * * ?)",
    "cron(0 0 3 * * ?)",
    "cron(0 0 3 * * ?)",
    "cron(0 0 3 * * ?)"
  ]

}

########################################################
### Variables for Small tables FTContacts connection ###
########################################################

variable "small_table_ftcontacts" {
  default = [
    "regions",
    "infoproviders"
  ]
}

variable "small_table_crawler_ftcontacts" {
  default = [
    "ftcontacts_dbo_regions",
    "ftcontacts_dbo_infoproviders"
  ]
}

variable "schedule_small_ftcontacts" {
  default = [
    "cron(0 30 05 10 12 ? *)",
    "cron(0 30 05 10 12 ? *)"
  ]
}

variable "jdbc_target_small_ftcontacts" {
  default = [
    "FTContacts/dbo/Regions",
    "FTContacts/dbo/InfoProviders"
  ]
}

############################################
### Variables for tables FT5L connection ###
############################################
variable "table_ft5l" {
  default = [
    "legal-legalassignments",
    "legal-legaloffices",
    "legal-suitcosts",
    "legal-suitparties",
    "legal-suitpartydebts",
    "legal-suits"
  ]

}

variable "jdbc_target_ft5l" {
  default = [
    "FT5L/Legal/LegalAssignments",
    "FT5L/Legal/LegalOffices",
    "FT5L/Legal/SuitCosts",
    "FT5L/Legal/SuitParties",
    "FT5L/Legal/SuitPartyDebts",
    "FT5L/Legal/Suits"
  ]

}

variable "schedule_ft5l" {
  default = [
    "cron(25 21 07 12 ? *)",
    "cron(26 21 07 12 ? *)",
    "cron(27 21 07 12 ? *)",
    "cron(28 21 07 12 ? *)",
    "cron(29 21 07 12 ? *)",
    "cron(30 21 07 12 ? *)"
  ]

}

variable "source_s3_ft5l" {
  default = [
    "ft5l.legal.LegalAssignments.txt",
    "FT5L.Legal.LegalOffices.txt",
    "ft5l.legal.SuitCosts.txt",
    "ft5l.legal.SuitParties.txt",
    "ft5l.legal.SuitPartyDebts.txt",
    "ft5l.legal.suits.txt"
  ]
}

variable "table_crawler_ft5l" {
  default = [
    "ft5l_legal_legalassignments",
    "ft5l_legal_legaloffices",
    "ft5l_legal_suitcosts",
    "ft5l_legal_suitparties",
    "ft5l_legal_suitpartydebts",
    "ft5l_legal_suits"
  ]
}

# Variables for load FT5L Insert
variable "table_ft5l_insert" {
  default = [
    "legal-legalassignments",
    "legal-suitcosts",
    "legal-suitparties",
    "legal-suitpartydebts",
    "legal-suits",
    "legal-suittrackings"
  ]

}

variable "table_crawler_ft5l_insert" {
  default = [
    "ft5l_legal_legalassignments",
    "ft5l_legal_suitcosts",
    "ft5l_legal_suitparties",
    "ft5l_legal_suitpartydebts",
    "ft5l_legal_suits",
    "legal-suittrackings",
  ]
}

variable "table_name_ft5l_insert" {
  default = [
    "Legal.LegalAssignments",
    "Legal.SuitCosts",
    "Legal.SuitParties",
    "Legal.SuitPartyDebts",
    "Legal.Suits",
    "Legal.Suittrackings"
  ]
}

variable "reference_col_ft5l" {
  default = [
    "assignmentid",
    "suitcostid",
    "suitpartyid",
    "suitpartydebtid",
    "suitid",
    "suittrackingid"
  ]

}

variable "schedule_ft5l_incremental" {
  default = [
    "cron(0 0 4 * * ?)",
    "cron(0 0 4 * * ?)",
    "cron(0 0 4 * * ?)",
    "cron(0 0 4 * * ?)",
    "cron(0 0 4 * * ?)",
    "cron(0 0 4 * * ?)"
  ]

}

# Variables for FT5L Update
variable "table_name_ft5l_update" {
  default = [
    "legal-legalassignments",
    "legal-legaloffices",
    "legal-suitcosts",
    "legal-suitparties",
    "legal-suitpartydebts",
    "legal-suitrestrictions",
    "legal-suits",
    "legal-suittrackings",
    "legal-trackingtypes"
  ]
}

variable "table_col_ft5l_update" {
  default = [
    "lastmodificationdate",
    "N/A",
    "lastmodificationdate",
    "lastmodificationdate",
    "lastmodificationdate",
    "N/A",
    "lastmodificationdate",
    "lastmodificationdate",
    "N/A"
  ]

}
variable "table_ft5l_update" {
  default = [
    "Legal.LegalAssignments",
    "Legal.LegalOffices",
    "Legal.SuitCosts",
    "Legal.SuitParties",
    "Legal.SuitPartyDebts",
    "Legal.Suitrestrictions",
    "Legal.Suits",
    "Legal.Suittrackings",
    "Legal.Trackingtypes"
  ]

}

variable "reference_col_ft5l_updates" {
  default = [
    "assignmentid",
    "legalofficeid",
    "suitcostid",
    "suitpartyid",
    "suitpartydebtid",
    "suitrestrictionid",
    "suitid",
    "suittrackingid",
    "trackingtypeid"
  ]

}
variable "full_update_ft5l" {
  default = [
    "false",
    "true",
    "false",
    "false",
    "false",
    "true",
    "false",
    "false",
    "true"
  ]

}

variable "scripts_updates_ft5l" {
  default = [
    "glue_a_updates_v10.py",
    "glue_a_full_v1.py",
    "glue_a_updates_v10.py",
    "glue_a_updates_v10.py",
    "glue_a_updates_v10.py",
    "glue_a_full_v1.py",
    "glue_a_updates_v10.py",
    "glue_a_updates_v10.py",
    "glue_a_full_v1.py"
  ]

}

variable "type_update_ft5l" {
  default = [
    "updates",
    "full",
    "updates",
    "updates",
    "updates",
    "full",
    "updates",
    "updates",
    "full"
  ]

}

variable "process_type_ft5l" {
  default = [
    "UPDATE",
    "FULL",
    "UPDATE",
    "UPDATE",
    "UPDATE",
    "FULL",
    "UPDATE",
    "UPDATE",
    "FULL"
  ]

}

variable "schedule_ft5l_updates" {
  default = [
    "cron(0 30 4 * * ?)",
    "cron(0 0 3 * * ?	)",
    "cron(0 30 4 * * ?)",
    "cron(0 30 4 * * ?)",
    "cron(0 30 4 * * ?)",
    "cron(0 45 4 * * ?)",
    "cron(0 30 4 * * ?)",
    "cron(0 0 5 * * ?	)",
    "cron(0 0 3 * * ?	)"
  ]
}

##################################################
### Variables for Small tables FT5L connection ###
##################################################

variable "small_table_ft5l" {
  default = [
    "legal-suitrestrictions",
    "legal-suittrackings",
    "legal-trackingtypes"
  ]
}

variable "small_table_crawler_ft5l" {
  default = [
    "ft5l_legal_suitrestrictions",
    "ft5l_legal_suittrackings",
    "ft5l_legal_trackingtypes"
  ]
}

variable "schedule_small_ft5l" {
  default = [
    "cron(0 30 05 10 12 ? *)",
    "cron(0 30 05 10 12 ? *)",
    "cron(0 30 05 10 12 ? *)"
  ]
}

variable "jdbc_target_small_ft5l" {
  default = [
    "FT5L/Legal/SuitRestrictions",
    "FT5L/Legal/SuitTrackings",
    "FT5L/Legal/TrackingTypes"
  ]
}


# Variables for tables Datamart connection
variable "table_datamart" {
  default = [
    "dimdate",
    "debtcontacts",
    "arrangements",
    "payments"
  ]

}

variable "scripts_ingestion_datamart" {
  default = [
    "table_ingestion_with_partition_v6.py",
    "table_ingestion_with_partition_v6.py",
    "table_ingestion_with_partition_v6.py",
    "table_ingestion_payments_v4.py"
  ]

}

variable "partition_col_datamart" {
  default = [
    "n/a",
    "n/a",
    "date",
    "date"
  ]
}

variable "tables_datamart_names" {
  default = [
    "Aux.DimDate",
    "CRM.DebtContacts",
    "Financial.Arrangements",
    "Financial.Payments"
  ]

}
variable "jdbc_target_datamart" {
  default = [
    "Datamart/Aux/dimDate",
    "Datamart/CRM/DebtContacts",
    "Datamart/Financial/Arrangements",
    "Datamart/Financial/Payments"
  ]

}

variable "schedule_datamart" {
  default = [
    "cron(45 05 30 10 ? *)",
    "cron(45 21 11 11 ? *)",
    "cron(45 21 11 11 ? *)",
    "cron(45 21 11 11 ? *)"
  ]

}

variable "source_s3_datamart" {
  default = [
    "Datamart.Aux.dimDate.txt",
    "Datamart.CRM.DebtContacts.txt",
    "Datamart.Financial.Arrangements.txt",
    "Datamart.Financial.Payments.txt"
  ]
}

variable "table_crawler_datamart" {
  default = [
    "datamart_aux_dimdate",
    "datamart_crm_debtcontacts",
    "datamart_financial_arrangements",
    "datamart_financial_payments"
  ]
}

variable "reference_col_datamart" {
  default = [
    "datekey",
    "debtid",
    "arrangementid",
    "paymentmethodid"
  ]

}

variable "schedule_datamart_incremental" {
  default = [
    "cron(45 23 30 10 ? *)",
    "cron(45 23 30 10 ? *)",
    "cron(45 23 30 10 ? *)",
    "cron(45 23 30 10 ? *)"
  ]

}


##################################################
### Variables for tables FTServices connection ###
##################################################

# Variables for tables FTServices connection
variable "table_ftservices" {
  default = [
    "ticketoperations",
    "tickets"
  ]
}

variable "jdbc_target_ftservices" {
  default = [
    "FTServices/dbo/TicketOperations",
    "FTServices/dbo/Tickets"
  ]

}

variable "schedule_ftservices" {
  default = [
    "cron(00 21 07 12 ? *)",
    "cron(00 21 07 12 ? *)"
  ]

}

variable "source_s3_ftservices" {
  default = [
    "FTServices.dbo.TicketOperations_*",
    "FTServices.dbo.Tickets.txt"
  ]
}

variable "table_name_ftservices_ingestion" {
  default = [
    "dbo.TicketOperations",
    "dbo.Tickets"
  ]
}

variable "partition_col_ftservices" {
  default = [
    "operationdate",
    "ticketdate"
  ]
}

variable "query_ftservices" {
  default = [
    "ticketoperations_query",
    "n/a"
  ]
}

variable "ftservices_script_location" {
  default = [
    "table_ingestion_with_variant_v3.py",
    "table_ingestion_with_partition_v6.py"
  ]
}

########################################################
###   Variables for tables FTAccounting connection   ###
########################################################
variable "table_ftaccounting" {
  default = [
    "accounttransactions"
  ]
}

variable "jdbc_target_ftaccounting" {
  default = [
    "FTAccounting/dbo/AccountTransactions"
  ]

}

variable "schedule_ftaccounting" {
  default = [
    "cron(0 30 22 07 12 ? 2021)"
  ]

}

variable "source_s3_ftaccounting" {
  default = [
    "FTAccounting.dbo.AccountTransactions.txt"
  ]
}

variable "table_crawler_ftaccounting" {
  default = [
    "ftaccounting_dbo_accounttransactions"
  ]
}

# Variables for load FTAccounting Insert

variable "table_ftaccounting_insert" {
  default = [
    "accounttransactions"
  ]
}

variable "table_crawler_ftaccounting_insert" {
  default = [
    "ftaccounting_dbo_accounttransactions"
  ]
}


variable "table_name_ftaccounting_insert" {
  default = [
    "dbo.AccountTransactions"
  ]

}

variable "reference_col_ftaccounting" {
  default = [
    "accounttransactionid"
  ]

}

variable "schedule_ftaccounting_incremental" {
  default = [
    "cron(0 0 4 * * ?)"
  ]

}

# Variables for tables FTAccounting Update
variable "full_update_ftaccounting" {
  default = [
    "true",
    "true",
    "true",
    "true",
    "true"
  ]
}

variable "table_ftaccounting_update" {
  default = [
    "dbo.Banks",
    "dbo.BankAccounts",
    "dbo.BankAccountServices",
    "dbo.BankServiceBillets",
    "dbo.AccountTransactions"
  ]

}

variable "table_name_ftaccounting_update" {
  default = [
    "banks",
    "bankaccounts",
    "bankaccountservices",
    "bankservicebillets",
    "accounttransactions"
  ]

}


variable "table_col_ftaccounting_update" {
  default = [
    "N/A",
    "N/A",
    "N/A",
    "N/A",
    "N/A"
  ]
}

variable "reference_col_ftaccounting_updates" {
  default = [
    "bankid",
    "bankaccountid",
    "bankaccountid",
    "bankserviceid",
    "accounttransactionid"
  ]

}

variable "schedule_ftaccounting_updates" {
  default = [
    "cron(0 0 3 * * ?)",
    "cron(0 0 3 * * ?)",
    "cron(0 0 3 * * ?)",
    "cron(0 0 3 * * ?)",
    "cron(0 0 3 1 * ?)"
  ]

}


########################################################
## Variables for Small tables FTAccounting connection ##
########################################################

variable "small_table_ftaccounting" {
  default = [
    "banks",
    "bankaccounts",
    "bankaccountservices",
    "bankservicebillets"
  ]
}

variable "small_table_crawler_ftaccounting" {
  default = [
    "ftaccounting_dbo_banks",
    "ftaccounting_dbo_bankaccounts",
    "ftaccounting_dbo_bankaccountservices",
    "ftaccounting_dbo_bankservicebillets"
  ]
}

variable "schedule_small_ftaccounting" {
  default = [
    "cron(0 30 09 12 11 ? *)",
    "cron(0 30 09 12 11 ? *)",
    "cron(0 30 09 12 11 ? *)",
    "cron(0 30 09 12 11 ? *)",
    "cron(0 30 09 12 11 ? *)"
  ]
}

variable "jdbc_target_small_ftaccounting" {
  default = [
    "FTAccounting/dbo/Banks",
    "FTAccounting/dbo/BankAccounts",
    "FTAccounting/dbo/BankAccountServices",
    "FTAccounting/dbo/BankServiceBillets"
  ]
}

########################################################
## Variables for Small tables FTWarehouse connection  ##
########################################################
variable "small_table_ftwarehouse" {
  default = [
    "agencies_goals"
  ]
}

variable "small_table_crawler_ftwarehouse" {
  default = [
    "ftwarehouse_dbo_agencies_goals"
  ]
}

variable "schedule_small_ftwarehouse" {
  default = [
    "cron(15 17 10 12 ? 2021)"
  ]
}

variable "jdbc_target_small_ftwarehouse" {
  default = [
    "FTWarehouse/dbo/Agencies_Goals"
  ]
}

# Variables for tables FTWarehouse Update
variable "full_update_ftwarehouse" {
  default = [
    "true"
  ]
}

variable "table_ftwarehouse_update" {
  default = [
    "dbo.Agencies_Goals"
  ]

}

variable "table_name_ftwarehouse_update" {
  default = [
    "agencies_goals"
  ]

}


variable "table_col_ftwarehouse_update" {
  default = [
    "agency"
  ]
}

variable "reference_col_ftwarehouse_updates" {
  default = [
    "N/A"
  ]

}

variable "schedule_ftwarehouse_updates" {
  default = [
    "cron(0 0 3 * * ?)"
  ]

}

######## Sandbox Variables #########

variable "table_sandbox_ftcrm" {
  default = [
    "agents",
    "arrangementinstallments",
    "arrangements",
    "bindings",
    "businessunits",
    "cancellationreasons",
    "creditrestrictions",
    "debtcustomextensions",
    "debttransactioncodes",
    "debttransactioncommissions",
    "debttransactions",
    "debts",
    "installmenttransactions",
    "portfolios",
    "products",
    "producttypes",
    "scores",
    "scoremodels",
    "workflowtaskresults",
    "workflowtrackings",
    "assignments",
    "customers",
    "digitaldebtassignments",
    "commissions",
    "portfoliogroups",
    "debtscores"
  ]
}

variable "table_sandbox_datamart" {
  default = [
    "arrangements",
    "arrangementinstallments",
    "workflowtrackings",
    "debtcontacts"
  ]

}

variable "schedule_sandbox" {
  type    = string
  default = "cron(45 06 10 12 ? *)"
}

variable "table_sandbox_financial" {
  default = [
    "payments"
  ]
}

########################################################
###   Variables for tables DataScience connection   ####
########################################################
variable "table_datascience" {
  default = [
    "assignmentsoptimizationrestrictive",
    "assignmentsoptimizationrestrictivesegments",
    "assignmentsoptimizationrestrictiveportfolios"
  ]
}

variable "jdbc_target_datascience" {
  default = [
    "DataScience/model/AssignmentsOptimizationRestrictive",
    "DataScience/model/AssignmentsOptimizationRestrictiveSegments",
    "DataScience/Model/AssignmentsOptimizationRestrictivePortfolios"
  ]

}

variable "schedule_datascience" {
  default = [
    "cron(0 30 00 13 01 ? 2022)",
    "cron(0 30 00 13 01 ? 2022)",
    "cron(0 30 00 13 01 ? 2022)"
  ]

}

variable "source_s3_datascience" {
  default = [
    "DataScience.Model.AssignmentsOptimizationRestrictive.txt",
    "AssignmentsOptimizationRestrictiveSegments.txt",
    "DataScience.Model.AssignmentsOptimizationRestrictivePortfolios.txt"
  ]
}

variable "table_crawler_datascience" {
  default = [
    "datascience_model_assignmentsoptimizationrestrictive",
    "datascience_model_assignmentsoptimizationrestrictivesegments",
    "datascience_model_assignmentsoptimizationrestrictiveportfolios"
  ]
}

########################################################
## Variables for Small tables Data Science connection ##
########################################################

variable "small_table_datascience" {
  default = [
    "assignmentsoptimizationrestrictiveblackList"
  ]
}

variable "small_table_crawler_datascience" {
  default = [
    "datascience_model_assignmentsoptimizationrestrictiveblackList"
  ]
}

variable "schedule_small_datascience" {
  default = [
    "cron(0 30 00 13 01 ? 2022)"
  ]
}

variable "jdbc_target_small_datascience" {
  default = [
    "DataScience/Model/AssignmentsOptimizationRestrictiveBlackList"
  ]
}

#### Coligadas ####
variable "coligadas" {
  default = [
    "acordo",
    "telefone",
    "email",
    "pessoa",
    "score"
  ]
  
}

variable "json_files" {
  default = [
    "acordo.json",
    "telefone.json",
    "email.json",
    "pessoa.json",
    "score.json"
  ]
}

variable "scripts_validador_coligadas" {
  default = [
    "validator_acordo.py",
    "validador_telefone.py",
    "validador_email.py",
    "validador_pessoa.py",
    "validador_score.py"
  ]
}

variable "modulos_coligadas" {
  default = [
    "modules_coligadas_acordo.zip",
    "modules_coligadas_telefone.zip",
    "modules_coligadas_email.zip",
    "modules_coligadas_pessoa.zip",
    "modules_coligadas_score.zip"
  ]
}

variable "scripts_coligadas" {
  default = [
    "apply_change_coligadas_acordo.py",
    "apply_change_coligadas_telefone.py",
    "apply_change_coligadas_email.py",
    "apply_change_coligadas_pessoa.py",
    "apply_change_coligadas_score.py"
  ]
}

####  Parceiros ####
variable "arquivos_parceiros" {
  default = [
   "dialer",
   "message",
   "email" 
  ]
  
}

variable "scripts_parceiros" {
  default = [
    "apply_change_parceiros_discador.py",
    "apply_change_parceiros_message.py",
    "apply_change_parceiros_email"
  ]
}