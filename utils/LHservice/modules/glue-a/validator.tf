# Job do Glue para acionar o validador de arquivos de parceiros.

resource "aws_glue_job" "job_validator_dialer" {
  name         = "dlr-job-validator-dialer"
  role_arn     = var.glue_role
  glue_version = "3.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/main_validator.py"
  }

  tags = {
    Component = "Validator"
  }

  default_arguments = {
    "--BUCKET_KEY"          = "tmp/schema/discador.json"
    "--BUCKET_NAME"         = "s3://dlr-${terraform.workspace}-bucket-transient"
    "--DATA_PATH"           = "s3://dlr-${terraform.workspace}-bucket-stage/partners/lideranca/discador/"
    "--LOG_PATH"            = "s3://dlr-${terraform.workspace}-bucket-log/partners/lideranca/discador"
    "--DOC_TYPE"            = "identitytypeid"
    "--DOC_COL"             = "identitynumber"
    "--CAL_DIR"             = "directionid"
    "--DF_ID"               = "originaldialingid"
    "--CALL_STATUS"         = "dialingstatusid"
    "--STATUS_COL"          = "conversationstatusid"
    "--AGENT_COL"           = "agentid"
    "--CONVERSATION_STATUS" = "conversationstatusid"
    "--VOICE_START_COL"     = "voicestarttime"
    "--DDD_COL"             = "area"
    "--PHONE_NUMBERS"       = "number"
    "--DOC_CODE"            = "identifierid"
    "--URA_CAPTURE"         = "journeytyped"
    "--URA_PATH"            = "journey"
    "--URA_ROUTE"           = "journeyid"
    "--BUCKET_BRONZE"       = "s3://dlr-${terraform.workspace}-bucket-bronze/partners/lideranca/discador/"
    "--extra-py-files"      = "s3://dlr-${terraform.workspace}-bucket-scripts/modules/validator/modules_validate.zip"
  }
}



resource "aws_glue_job" "job_validator_message" {
  name         = "dlr-job-validator-message"
  role_arn     = var.glue_role
  glue_version = "3.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/main_validator.py"
  }

  tags = {
    Component = "Validator"
  }

  default_arguments = {
    "--BUCKET_KEY"          = "tmp/schema/sms.json"
    "--BUCKET_NAME"         = "s3://dlr-${terraform.workspace}-bucket-transient"
    "--DATA_PATH"           = "s3://dlr-${terraform.workspace}-bucket-stage/partners/lideranca/sms/"
    "--LOG_PATH"            = "s3://dlr-${terraform.workspace}-bucket-log/"
    "--DOC_TYPE"            = "identitytypeid"
    "--DOC_COL"             = "identitynumber"
    "--CAL_DIR"             = ""
    "--DF_ID"               = "originaltextmessageid"
    "--CALL_STATUS"         = ""
    "--STATUS_COL"          = "textmessagestatusid"
    "--AGENT_COL"           = ""
    "--CONVERSATION_COL"    = ""
    "--CONVERSATION_STATUS" = ""
    "--VOICE_START_COL"     = ""
    "--DDD_COL"             = "area"
    "--PHONE_NUMBERS"       = "number"
    "--DOC_CODE"            = "identifierid"
    "--URA_CAPTURE"         = ""
    "--URA_PATH"            = ""
    "--URA_ROUTE"           = ""
    "--extra-py-files"      = "s3://dlr-${terraform.workspace}-bucket-scripts/modules/validator/modules_validate.zip"
  }
}

##### Validador de Arquivos das coligadas

# Validador Arquivo Acordo

resource "aws_glue_job" "job_validator_acordo" {
  count        = length(var.coligadas)
  name         = "dlr-job-validador-coligadas-${var.coligadas[count.index]}"
  role_arn     = var.glue_role
  glue_version = "3.0"

  command {
    python_version  = 3
    script_location = "s3://dlr-${terraform.workspace}-bucket-scripts/${var.scripts_validador_coligadas[count.index]}"
  }

  tags = {
    Component = "Validator"
  }

  default_arguments = {
    "--BUCKET_KEY"          = "tmp/schema/${var.json_files[count.index]}"
    "--BUCKET_JSON"         = "dlr-${terraform.workspace}-bucket-transient"
    "--DATA_PATH"           = "s3://dlr-${terraform.workspace}-bucket-stage/coligadas/emdia/${var.coligadas[count.index]}/"
    "--LOG_PATH"            = "s3://dlr-${terraform.workspace}-bucket-log/coligadas/emdia/${var.coligadas[count.index]}/"
    "--BUCKET_BRONZE"       = "s3://dlr-${terraform.workspace}-bucket-bronze/coligadas/emdia/${var.coligadas[count.index]}/"
    "--extra-py-files"      = "s3://dlr-${terraform.workspace}-bucket-scripts/modules/validator/${var.modulos_coligadas[count.index]}"
  }
}