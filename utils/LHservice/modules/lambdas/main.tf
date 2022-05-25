# Criacao da Layer para uso na Lambda de conexao ao Redshift
resource "aws_lambda_layer_version" "lambda_layer" {
  filename   = "../../scripts/lambda/python.zip"
  layer_name = "dlr_lambda_layer_psycopg2"

  compatible_runtimes = ["python3.9"]
}

resource "aws_lambda_function" "lambda_external_table" {
  filename      = "../../scripts/lambda/lambda_function_external_table_v3.zip"
  function_name = "dlr_lambda_create_external_table_v3"
  layers        = [aws_lambda_layer_version.lambda_layer.arn]
  role          = var.role_arn
  handler       = "lambda_function_external_table_v3.lambda_handler"

  tags = merge(
    var.dlr_tags,
    {
      Description : "lambda function for external table - lambdas module"
      Environment : var.env_tags[var.env]
    }
  )

  runtime = "python3.9"

  vpc_config {
    subnet_ids         = [var.subnet_ids_redshift[var.env]]
    security_group_ids = [var.security_group[var.env]]
  }

  environment {
    variables = {
      iam_role                             = var.iam_role
      redshift_db                          = var.redshift_db
      redshift_user                        = var.redshift_user
      redshift_host                        = var.redshift_host[var.env]
      redshift_port                        = var.redshift_port
      redshift_password                    = var.redshift_password
      database_catalog                     = "dlr-database-refinedzone-datamart"
    }
  }

}

resource "aws_lambda_function" "lambda_sandbox_permissions" {
  filename      = "../../scripts/lambda/lambda_function_sandbox_definitions_v3.zip"
  function_name = "dlr_lambda_create_sandbox_v3"
  layers        = [aws_lambda_layer_version.lambda_layer.arn]
  role          = var.role_arn
  handler       = "lambda_function_sandbox_definitions_v3.lambda_handler"

  tags = merge(
    var.dlr_tags,
    {
      Description : "lambda function for sandbox permissions - lambdas module"
      Environment : var.env_tags[var.env]
    }
  )

  runtime = "python3.9"

  vpc_config {
    subnet_ids         = [var.subnet_ids_redshift[var.env]]
    security_group_ids = [var.security_group[var.env]]
  }

  environment {
    variables = {
      iam_role          = var.iam_role
      redshift_db       = var.redshift_db
      redshift_user     = var.redshift_user
      redshift_host     = var.redshift_host[var.env]
      redshift_port     = var.redshift_port
      redshift_password = var.redshift_password
      pass_sandbox      = var.pass_sandbox

    }
  }

}


######### Lambdas functions para ingestão dos arquivos do projeto Apollo #########

### Lambda que inicia a copia dos arquivos do discador  de parceiros ###
resource "aws_lambda_function" "lambda_copy_file_dialer" {
  filename      = "../../scripts/lambda/copy_file_v1.zip"
  function_name = "dlr-copy-file-dialer"
  role          = var.copy_file_s3_arn
  handler       = "copy_file.lambda_handler"

  runtime = "python3.9"

  timeout = 90
  environment {
    variables = {
      bucket_path = "dlr-${terraform.workspace}-bucket-stage"
    }
  }
}

resource "aws_lambda_permission" "allow_dialer" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_copy_file_dialer.arn
  principal     = "s3.amazonaws.com"
  source_arn    = var.bucket_transfer_arn
}

### Lambda que inicia a copia dos arquivos de sms dos parceiros ###
resource "aws_lambda_function" "lambda_copy_file_message" {
  filename      = "../../scripts/lambda/copy_file_v1.zip"
  function_name = "dlr-copy-file-message"
  role          = var.copy_file_s3_arn
  handler       = "copy_file.lambda_handler"

  runtime = "python3.9"

  timeout = 90
  environment {
    variables = {
      bucket_path = "dlr-${terraform.workspace}-bucket-stage"
    }
  }
}

resource "aws_lambda_permission" "allow_message" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_copy_file_message.arn
  principal     = "s3.amazonaws.com"
  source_arn    = var.bucket_transfer_arn
}

######### Lambdas functions para ingestão dos arquivos das Coligadas #########

### Lambda que inicia a copia dos arquivos de acordo ###
resource "aws_lambda_function" "lambda_copy_file_acordo" {
  filename      = "../../scripts/lambda/copy_file_v1.zip"
  function_name = "dlr-copy-file-coligadas-acordo"
  role          = var.copy_file_s3_arn
  handler       = "copy_file.lambda_handler"

  runtime = "python3.9"

  timeout = 90
  environment {
    variables = {
      bucket_path = "dlr-${terraform.workspace}-bucket-stage"
    }
  }
}

resource "aws_lambda_permission" "allow_acordo" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_copy_file_acordo.arn
  principal     = "s3.amazonaws.com"
  source_arn    = var.bucket_transfer_arn
}

### Lambda que inicia a copia dos arquivos de telefone ###
resource "aws_lambda_function" "lambda_copy_file_telefone" {
  filename      = "../../scripts/lambda/copy_file_v1.zip"
  function_name = "dlr-copy-file-coligadas-telefone"
  role          = var.copy_file_s3_arn
  handler       = "copy_file.lambda_handler"

  runtime = "python3.9"

  timeout = 90
  environment {
    variables = {
      bucket_path = "dlr-${terraform.workspace}-bucket-stage"
    }
  }
}

resource "aws_lambda_permission" "allow_telefone" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_copy_file_telefone.arn
  principal     = "s3.amazonaws.com"
  source_arn    = var.bucket_transfer_arn
}

### Lambda que inicia a copia dos arquivos de email ###
resource "aws_lambda_function" "lambda_copy_file_email" {
  filename      = "../../scripts/lambda/copy_file_v1.zip"
  function_name = "dlr-copy-file-coligadas-email"
  role          = var.copy_file_s3_arn
  handler       = "copy_file.lambda_handler"

  runtime = "python3.9"

  timeout = 90
  environment {
    variables = {
      bucket_path = "dlr-${terraform.workspace}-bucket-stage"
    }
  }
}

resource "aws_lambda_permission" "allow_email" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_copy_file_email.arn
  principal     = "s3.amazonaws.com"
  source_arn    = var.bucket_transfer_arn
}

### Lambda que inicia a copia dos arquivos de pessoa ###
resource "aws_lambda_function" "lambda_copy_file_pessoa" {
  filename      = "../../scripts/lambda/copy_file_v1.zip"
  function_name = "dlr-copy-file-coligadas-pessoa"
  role          = var.copy_file_s3_arn
  handler       = "copy_file.lambda_handler"

  runtime = "python3.9"

  timeout = 90
  environment {
    variables = {
      bucket_path = "dlr-${terraform.workspace}-bucket-stage"
    }
  }
}

resource "aws_lambda_permission" "allow_pessoa" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_copy_file_pessoa.arn
  principal     = "s3.amazonaws.com"
  source_arn    = var.bucket_transfer_arn
}

### Trigger para as lambdas de cópia do arquivo
resource "aws_s3_bucket_notification" "lambda-trigger-transfer" {
  bucket = var.bucket_transfer_id

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_copy_file_dialer.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "partners/lideranca/discador/"
    filter_suffix       = ".txt"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_copy_file_dialer.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "partners/millennium/discador/"
    filter_suffix       = ".txt"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_copy_file_message.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "partners/lideranca/sms/"
    filter_suffix       = ".txt"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_copy_file_message.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "partners/millennium/sms/"
    filter_suffix       = ".txt"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_copy_file_acordo.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "coligadas/emdia/acordo/"
    filter_suffix       = ".csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_copy_file_acordo.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "coligadas/lideranca/acordo/"
    filter_suffix       = ".csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_copy_file_telefone.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "coligadas/emdia/telefone/"
    filter_suffix       = ".csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_copy_file_telefone.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "coligadas/lideranca/telefone/"
    filter_suffix       = ".csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_copy_file_email.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "coligadas/emdia/email/"
    filter_suffix       = ".csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_copy_file_email.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "coligadas/lideranca/email/"
    filter_suffix       = ".csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_copy_file_pessoa.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "coligadas/emdia/pessoa/"
    filter_suffix       = ".csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_copy_file_pessoa.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "coligadas/lideranca/pessoa/"
    filter_suffix       = ".csv"
  }

  depends_on = [
    aws_lambda_permission.allow_dialer,
    aws_lambda_permission.allow_message,
    aws_lambda_permission.allow_acordo,
    aws_lambda_permission.allow_telefone,
    aws_lambda_permission.allow_email,
    aws_lambda_permission.allow_pessoa
  ]

}

# Função Lambda que inicia o Validador dos Arquivos de Parceiros Discador
resource "aws_lambda_function" "lambda_start_validator_dialer" {
  filename      = "../../scripts/lambda/lambda_start_validator_dialer.zip"
  function_name = "dlr-start-machine-validator-dialer"
  role          = var.role_arn_step_functions
  handler       = "lambda_start_validator_dialer.lambda_handler"

  runtime = "python3.9"
  environment {
    variables = {
      state_machine_arn = var.state_machine_validator_dialer_arn
    }
  }

}

resource "aws_lambda_permission" "allow_validator_dialer" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_start_validator_dialer.arn
  principal     = "s3.amazonaws.com"
  source_arn    = var.bucket_stage_arn
}

# Função Lambda que inicia o Validador dos Arquivos de Parceiros Message
resource "aws_lambda_function" "lambda_start_validator_message" {
  filename      = "../../scripts/lambda/lambda_start_validator_message.zip"
  function_name = "dlr-start-machine-validator-message"
  role          = var.role_arn_step_functions
  handler       = "lambda_start_validator_message.lambda_handler"

  runtime = "python3.9"
  environment {
    variables = {
      state_machine_arn = var.state_machine_validator_message_arn
    }
  }

}

resource "aws_lambda_permission" "allow_validator_message" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_start_validator_message.arn
  principal     = "s3.amazonaws.com"
  source_arn    = var.bucket_stage_arn
}

# Função lambda que inicia o validador dos arquivos de acordo das coligadas 
resource "aws_lambda_function" "lambda_start_validator_acordo" {
  filename      = "../../scripts/lambda/lambda_start_validator.zip"
  function_name = "dlr-start-machine-validator-acordo"
  role          = var.role_arn_step_functions
  handler       = "lambda_start_validator.lambda_handler"

  runtime = "python3.9"
  environment {
    variables = {
      state_machine_arn = var.state_machine_validator_acordo_arn
    }
  }

}

resource "aws_lambda_permission" "allow_validator_acordo" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_start_validator_acordo.arn
  principal     = "s3.amazonaws.com"
  source_arn    = var.bucket_stage_arn
}

# Função lambda que inicia o validador dos arquivos de telefone das coligadas 
resource "aws_lambda_function" "lambda_start_validator_telefone" {
  filename      = "../../scripts/lambda/lambda_start_validator.zip"
  function_name = "dlr-start-machine-validator-telefone"
  role          = var.role_arn_step_functions
  handler       = "lambda_start_validator.lambda_handler"

  runtime = "python3.9"
  environment {
    variables = {
      state_machine_arn = var.state_machine_validator_telefone_arn
    }
  }

}

resource "aws_lambda_permission" "allow_validator_telefone" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_start_validator_telefone.arn
  principal     = "s3.amazonaws.com"
  source_arn    = var.bucket_stage_arn
}

# Função lambda que inicia o validador dos arquivos de email das coligadas 
resource "aws_lambda_function" "lambda_start_validator_email" {
  filename      = "../../scripts/lambda/lambda_start_validator.zip"
  function_name = "dlr-start-machine-validator-email"
  role          = var.role_arn_step_functions
  handler       = "lambda_start_validator.lambda_handler"

  runtime = "python3.9"
  environment {
    variables = {
      state_machine_arn = var.state_machine_validator_email_arn
    }
  }

}

resource "aws_lambda_permission" "allow_validator_email" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_start_validator_email.arn
  principal     = "s3.amazonaws.com"
  source_arn    = var.bucket_stage_arn
}

# Função lambda que inicia o validador dos arquivos de pessoa das coligadas 
resource "aws_lambda_function" "lambda_start_validator_pessoa" {
  filename      = "../../scripts/lambda/lambda_start_validator.zip"
  function_name = "dlr-start-machine-validator-pessoa"
  role          = var.role_arn_step_functions
  handler       = "lambda_start_validator.lambda_handler"

  runtime = "python3.9"
  environment {
    variables = {
      state_machine_arn = var.state_machine_validator_pessoa_arn
    }
  }

}

resource "aws_lambda_permission" "allow_validator_pessoa" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_start_validator_pessoa.arn
  principal     = "s3.amazonaws.com"
  source_arn    = var.bucket_stage_arn
}


resource "aws_s3_bucket_notification" "lambda-trigger-stage" {
  bucket = var.bucket_stage_id

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_start_validator_dialer.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "partners/lideranca/discador/"
    filter_suffix       = ".txt"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_start_validator_dialer.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "partners/millennium/discador/"
    filter_suffix       = ".txt"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_start_validator_message.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "partners/lideranca/sms/"
    filter_suffix       = ".txt"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_start_validator_message.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "partners/millennium/sms/"
    filter_suffix       = ".txt"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_start_validator_acordo.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "coligadas/emdia/acordo/"
    filter_suffix       = ".csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_start_validator_telefone.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "coligadas/emdia/telefone/"
    filter_suffix       = ".csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_start_validator_email.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "coligadas/emdia/email/"
    filter_suffix       = ".csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_start_validator_pessoa.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "coligadas/emdia/pessoa/"
    filter_suffix       = ".csv"
  }

  depends_on = [
    aws_lambda_permission.allow_validator_dialer,
    aws_lambda_permission.allow_validator_message,
    aws_lambda_permission.allow_validator_acordo,
    aws_lambda_permission.allow_validator_telefone,
    aws_lambda_permission.allow_validator_email,
    aws_lambda_permission.allow_validator_pessoa
  ]

}

# Funções Lambdas que iniciam as transformações dos Arquivos dos Parceiros
resource "aws_lambda_function" "lambda_start_apply_changes_discador" {
  filename      = "../../scripts/lambda/lambda_start_apply_changes.zip"
  function_name = "dlr-start-machine-apply-changes-parceiros-discador"
  role          = var.role_arn_step_functions
  handler       = "lambda_start_apply_change_acordo.lambda_handler"

  runtime = "python3.9"
  environment {
    variables = {
      state_machine_arn = var.state_machine_apply_changes_discador_arn
    }
  }

}

resource "aws_lambda_permission" "allow_apply_changes_discador" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_start_apply_changes_discador.arn
  principal     = "s3.amazonaws.com"
  source_arn    = var.bucket_bronze_arn
}


# Funções Lambdas que iniciam as transformações dos Arquivos de Coligadas
resource "aws_lambda_function" "lambda_start_apply_changes_acordo" {
  filename      = "../../scripts/lambda/lambda_start_apply_changes.zip"
  function_name = "dlr-start-machine-apply-changes-coligadas-acordo"
  role          = var.role_arn_step_functions
  handler       = "lambda_start_apply_change_acordo.lambda_handler"

  runtime = "python3.9"
  environment {
    variables = {
      state_machine_arn = var.state_machine_apply_changes_acordo_arn
    }
  }

}

resource "aws_lambda_permission" "allow_apply_changes_acordo" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_start_apply_changes_acordo.arn
  principal     = "s3.amazonaws.com"
  source_arn    = var.bucket_bronze_arn
}

resource "aws_lambda_function" "lambda_start_apply_changes_telefone" {
  filename      = "../../scripts/lambda/lambda_start_apply_changes.zip"
  function_name = "dlr-start-machine-apply-changes-coligadas-telefone"
  role          = var.role_arn_step_functions
  handler       = "lambda_start_apply_change_telefone.lambda_handler"

  runtime = "python3.9"
  environment {
    variables = {
      state_machine_arn = var.state_machine_apply_changes_telefone_arn
    }
  }

}

resource "aws_lambda_permission" "allow_apply_changes_telefone" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_start_apply_changes_telefone.arn
  principal     = "s3.amazonaws.com"
  source_arn    = var.bucket_bronze_arn
}

resource "aws_lambda_function" "lambda_start_apply_changes_email" {
  filename      = "../../scripts/lambda/lambda_start_apply_changes.zip"
  function_name = "dlr-start-machine-apply-changes-coligadas-email"
  role          = var.role_arn_step_functions
  handler       = "lambda_start_apply_change_email.lambda_handler"

  runtime = "python3.9"
  environment {
    variables = {
      state_machine_arn = var.state_machine_apply_changes_email_arn
    }
  }

}

resource "aws_lambda_permission" "allow_apply_changes_email" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_start_apply_changes_email.arn
  principal     = "s3.amazonaws.com"
  source_arn    = var.bucket_bronze_arn
}

resource "aws_lambda_function" "lambda_start_apply_changes_pessoa" {
  filename      = "../../scripts/lambda/lambda_start_apply_changes.zip"
  function_name = "dlr-start-machine-apply-changes-coligadas-pessoa"
  role          = var.role_arn_step_functions
  handler       = "lambda_start_apply_change_pessoa.lambda_handler"

  runtime = "python3.9"
  environment {
    variables = {
      state_machine_arn = var.state_machine_apply_changes_pessoa_arn
    }
  }

}

resource "aws_lambda_permission" "allow_apply_changes_pessoa" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_start_apply_changes_pessoa.arn
  principal     = "s3.amazonaws.com"
  source_arn    = var.bucket_bronze_arn
}

resource "aws_s3_bucket_notification" "lambda-trigger-stage-coligadas" {
  bucket = var.bucket_bronze_id

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_start_apply_changes_acordo.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "coligadas/emdia/acordo/"
    filter_suffix       = ".csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_start_apply_changes_telefone.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "coligadas/emdia/telefone/"
    filter_suffix       = ".csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_start_apply_changes_email.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "coligadas/emdia/email/"
    filter_suffix       = ".csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_start_apply_changes_pessoa.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "coligadas/emdia/pessoa/"
    filter_suffix       = ".csv"
  }

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_start_apply_changes_discador.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "partners/lideranca/discador/"
    filter_suffix       = ".csv"
  }

  depends_on = [
    aws_lambda_permission.allow_apply_changes_acordo,
    aws_lambda_permission.allow_apply_changes_telefone,
    aws_lambda_permission.allow_apply_changes_email,
    aws_lambda_permission.allow_apply_changes_pessoa,
    aws_lambda_permission.allow_apply_changes_discador
  ]

}