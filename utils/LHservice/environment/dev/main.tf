provider "aws" {
  region = var.region[var.env]
}

# Bucket where the terraform.tfstate file will be stored
#
terraform {

  # configuracao backend DEV
  backend "s3" {
    bucket = "dlr-dev-bucket-tfstate"
    key    = "dev/terraform.tfstate"
    region = "us-east-2"
  }


  # configuracao backend PROD
  #backend "s3" {
  #bucket = "dlr-prod-bucket-tfstate"
  #key    = "prod/terraform.tstate"
  #region = "us-east-1"
  #}

}

module "resource_s3_bucket" {
  source = "../../modules/buckets-s3"
  env    = var.env
}

module "resource_glue_a" {
  source                 = "../../modules/glue-a"
  env                    = var.env
  kms_key                = module.resource_s3_bucket.kms_key
  jdbc_pass              = module.resource_aws_secret_manager.dlr_jdbc_pass
  glue_role              = module.resource_iam_policies.glue_role
  glue_role_id           = module.resource_iam_policies.glue_role_id
  glue_security_group    = module.resource_iam_policies.security_group
  glue_security_group_id = module.resource_iam_policies.security_group_id
}

module "resource_glue_b" {
  source                 = "../../modules/glue-b"
  env                    = var.env
  kms_key                = module.resource_s3_bucket.kms_key
  glue_role              = module.resource_iam_policies.glue_role
  glue_role_id           = module.resource_iam_policies.glue_role_id
  glue_role_redshift     = module.resource_iam_policies.glue_role_redshift
  glue_security_group    = module.resource_iam_policies.security_group
  glue_security_group_id = module.resource_iam_policies.security_group_id
  user_redshift          = module.resource_aws_secret_manager.dlr_redshift_user
  password_redshift      = module.resource_aws_secret_manager.dlr_redshift_pass
  database_datamart_name = module.resource_glue_a.datamart_database
  jdbc_pass              = module.resource_aws_secret_manager.dlr_jdbc_pass
  datamart_connection    = module.resource_glue_a.datamart_connection
}

module "resource_redshift" {
  source        = "../../modules/redshift"
  env           = var.env
  redshift_role = module.resource_iam_policies.redshift_role
  redshift_pass = module.resource_aws_secret_manager.dlr_redshift_pass
}

module "resource_transfer_family" {
  source          = "../../modules/transfer-family"
  env             = var.env
  role_transfer   = module.resource_iam_policies.transfer_role
  bucket_transfer = module.resource_s3_bucket.bucket_transfer_id
}

module "resource_sagemaker" {
  source   = "../../modules/sagemaker"
  env      = var.env
  role_arn = module.resource_iam_policies.sagemaker_role
}

module "resource_athena_database" {
  source      = "../../modules/athena"
  env         = var.env
  bucket_logs = module.resource_s3_bucket.bucket_log
}

module "resource_iam_policies" {
  source = "../../modules/policies"
  env    = var.env
}

module "resource_aws_network" {
  source = "../../modules/network"
  env    = var.env
}

module "resource_aws_lambda" {
  source                                   = "../../modules/lambdas"
  env                                      = var.env
  role_arn                                 = module.resource_iam_policies.lambda_redshift_role
  iam_role                                 = module.resource_iam_policies.redshift_role
  redshift_user                            = module.resource_aws_secret_manager.dlr_redshift_user
  redshift_password                        = module.resource_aws_secret_manager.dlr_redshift_pass
  role_arn_step_functions                  = module.resource_iam_policies.lambda_stepfunctions_role
  state_machine_discador_arn               = module.resource_aws_step_functions.state_machine_discador_arn
  state_machine_sms_arn                    = module.resource_aws_step_functions.state_machine_sms_arn
  bucket_transfer_id                       = module.resource_s3_bucket.bucket_transfer_id
  bucket_stage_id                          = module.resource_s3_bucket.bucket_stage_id
  bucket_bronze_id                         = module.resource_s3_bucket.bucket_bronze_id
  bucket_silver_id                         = module.resource_s3_bucket.bucket_silver_id
  bucket_transfer_arn                      = module.resource_s3_bucket.bucket_transfer_arn
  bucket_stage_arn                         = module.resource_s3_bucket.bucket_stage_arn
  bucket_bronze_arn                        = module.resource_s3_bucket.bucket_bronze_arn
  bucket_silver_arn                        = module.resource_s3_bucket.bucket_silver_arn
  copy_file_s3_arn                         = module.resource_iam_policies.lambda_bucket_role
  state_machine_validator_dialer_arn       = module.resource_aws_step_functions.state_machine_validator_dialer_arn
  state_machine_validator_message_arn      = module.resource_aws_step_functions.state_machine_validator_message_arn
  state_machine_validator_acordo_arn       = module.resource_aws_step_functions.state_machine_validator_acordo_arn
  state_machine_apply_changes_acordo_arn   = module.resource_aws_step_functions.state_machine_apply_changes_acordo_arn
  state_machine_apply_changes_discador_arn = module.resource_aws_step_functions.state_machine_apply_changes_discador_arn

}

module "resource_aws_secret_manager" {
  source = "../../modules/secret-manager"
  env    = var.env
}

module "resource_aws_step_functions" {
  source                  = "../../modules/step-functions"
  env                     = var.env
  lambda_apollo_discador  = module.resource_aws_lambda.lambda_discador_arn
  lambda_apollo_sms       = module.resource_aws_lambda.lambda_sms_arn
  role_arn_step_functions = module.resource_iam_policies.stepfunctions_lambda_role
}
