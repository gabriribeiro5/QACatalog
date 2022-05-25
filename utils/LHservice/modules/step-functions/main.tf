resource "aws_sfn_state_machine" "sfn_state_machine_lideranca_discador" {
    name = "dlr-${terraform.workspace}-state-machine-lideranca-discador"

    tags = merge(
    var.dlr_tags,
    {
      Description: "sfn state machine lideranca discador - step-functions module"
      Environment: var.env_tags[var.env]
    }
    )

    role_arn = var.role_arn_step_functions
    definition = jsonencode(
        {
            "Comment": "Ingestao dos arquivos do discador como tabelas gerenciadas no Redshift",
            "StartAt": "startIngestaJob",
            "States": {
                    "startIngestaJob": {
                            "Type": "Task",
                            "Resource": var.lambda_apollo_discador,
                            "Next": "isIngestaJobComplete?"
                        },
                    "isIngestaJobComplete?": {
                            "Type": "Choice",
                            "Choices": [
                                {
                                    "Variable": "$.body",
                                    "StringEquals": "COMPLETED",
                                    "Next": "finishWorkflow"
                                },
                                {
                                    "Variable": "$.body",
                                    "StringEquals": "FAILED",
                                    "Next": "finishWorkflow"
                                }
                            ],
                            "Default": "waitIngestaJob"
                        },
                    "waitIngestaJob": {
                            "Type": "Wait",
                            "Seconds": 60,
                            "Next": "isIngestaJobComplete?"
                    },
                    "finishWorkflow": {
                        "Type": "Pass",
                        "End": true
                    }
             }

        }

    )
}

resource "aws_sfn_state_machine" "sfn_state_machine_lideranca_sms" {
    name = "dlr-${terraform.workspace}-state-machine-lideranca-sms"
    role_arn = var.role_arn_step_functions

    tags = merge(
    var.dlr_tags,
    {
      Description: "sfn state machine lideranca SMS - step-functions module"
      Environment: var.env_tags[var.env]
    }
    )
    definition = jsonencode(
        {
            "Comment": "Ingestao dos arquivos de SMS como tabelas gerenciadas no Redshift",
            "StartAt": "startIngestaJob",
            "States": {
                    "startIngestaJob": {
                            "Type": "Task",
                            "Resource": var.lambda_apollo_sms,
                            "Next": "isIngestaJobComplete?"
                        },
                    "isIngestaJobComplete?": {
                            "Type": "Choice",
                            "Choices": [
                                {
                                    "Variable": "$.body",
                                    "StringEquals": "COMPLETED",
                                    "Next": "finishWorkflow"
                                },
                                {
                                    "Variable": "$.body",
                                    "StringEquals": "FAILED",
                                    "Next": "finishWorkflow"
                                }
                            ],
                            "Default": "waitIngestaJob"
                        },
                    "waitIngestaJob": {
                            "Type": "Wait",
                            "Seconds": 60,
                            "Next": "isIngestaJobComplete?"
                    },
                    "finishWorkflow": {
                        "Type": "Pass",
                        "End": true
                    }
             }

        }

    )
}

###### Step Functions do projeto Apollo ######

resource "aws_sfn_state_machine" "sfn_state_machine_validator_dialer" {
    name = "dlr-${terraform.workspace}-state-machine-validator-dialer"
    role_arn = var.role_arn_step_functions
    definition = jsonencode(
        {
            "Comment": "Aciona o processo de validação dos arquivos de parceiros",
            "StartAt": "Glue StartJobRun",
            "States": {
                    "Glue StartJobRun": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                            "Parameters": {
                                "JobName": "dlr-job-validator-dialer"    
                                "Arguments": {
                                            "--FILE_NAME.$": "$.originalFile.s3FileName"
                                }    
                            }
                            "Next": "finishWorkflow"
                        },
                    "finishWorkflow": {
                        "Type": "Pass",
                        "End": true
                    }
             }

        }

    )
}

resource "aws_sfn_state_machine" "sfn_state_machine_validator_message" {
    name = "dlr-${terraform.workspace}-state-machine-validator-message"
    role_arn = var.role_arn_step_functions
    definition = jsonencode(
        {
            "Comment": "Aciona o processo de validação dos arquivos de parceiros",
            "StartAt": "Glue StartJobRun",
            "States": {
                    "Glue StartJobRun": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                            "Parameters": {
                                "JobName": "dlr-job-validator-message"    
                                "Arguments": {
                                            "--FILE_NAME.$": "$.originalFile.s3FileName"
                                }    
                            }
                            "Next": "finishWorkflow"
                        },
                    "finishWorkflow": {
                        "Type": "Pass",
                        "End": true
                    }
             }

        }

    )
}

#  Step Functions para apply changes dos arquivos de parceiros

resource "aws_sfn_state_machine" "sfn_state_machine_apply_changes_parceiros_discador" {
    name = "dlr-${terraform.workspace}-state-machine-apply-changes-parceiros-discador"
    role_arn = var.role_arn_step_functions
    definition = jsonencode(
        {
            "Comment": "Aciona o processo de transformação dos arquivos de parceiros - dialer",
            "StartAt": "Glue StartJobRun",
            "States": {
                    "Glue StartJobRun": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                            "Parameters": {
                                "JobName": "dlr-job-apply-change-lideranca-discador",
                                "Arguments": {
                                            "--FILE_NAME.$": "$.originalFile.s3FileName"
                                }       
                            }
                            "Next": "finishWorkflow"
                        },
                    "finishWorkflow": {
                        "Type": "Pass",
                        "End": true
                    }
             }

        }

    )
}

#Step Functions do projeto validador-coligadas 'Acordo'

resource "aws_sfn_state_machine" "sfn_state_machine_validator_coligadas_acordo" {
    name = "dlr-${terraform.workspace}-state-machine-validator-coligadas-acordo"
    role_arn = var.role_arn_step_functions
    definition = jsonencode(
        {
            "Comment": "Aciona o processo de validação dos arquivos de coligadas - acordo",
            "StartAt": "Glue StartJobRun",
            "States": {
                    "Glue StartJobRun": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                            "Parameters": {
                                "JobName": "dlr-job-validador-coligadas-acordo",
                                "Arguments": {
                                            "--FILE_NAME.$": "$.originalFile.s3FileName"
                                }    
                            }
                            "Next": "finishWorkflow"
                        },
                    "finishWorkflow": {
                        "Type": "Pass",
                        "End": true
                    }
             }

        }

    )
}

#  Step Functions para apply changes dos arquivos de coligadas

resource "aws_sfn_state_machine" "sfn_state_machine_apply_changes_coligadas_acordo" {
    name = "dlr-${terraform.workspace}-state-machine-apply-changes-coligadas-acordo"
    role_arn = var.role_arn_step_functions
    definition = jsonencode(
        {
            "Comment": "Aciona o processo de transformação dos arquivos de coligadas - acordo",
            "StartAt": "Glue StartJobRun",
            "States": {
                    "Glue StartJobRun": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                            "Parameters": {
                                "JobName": "dlr-job-apply-change-acordo",
                                "Arguments": {
                                            "--FILE_NAME.$": "$.originalFile.s3FileName"
                                }       
                            }
                            "Next": "finishWorkflow"
                        },
                    "finishWorkflow": {
                        "Type": "Pass",
                        "End": true
                    }
             }

        }

    )
}


resource "aws_sfn_state_machine" "sfn_state_machine_apply_changes_coligadas_telefone" {
    name = "dlr-${terraform.workspace}-state-machine-apply-changes-coligadas-telefone"
    role_arn = var.role_arn_step_functions
    definition = jsonencode(
        {
            "Comment": "Aciona o processo de transformação dos arquivos de coligadas - telefone",
            "StartAt": "Glue StartJobRun",
            "States": {
                    "Glue StartJobRun": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                            "Parameters": {
                                "JobName": "dlr-job-apply-change-telefone",
                                "Arguments": {
                                            "--FILE_NAME.$": "$.originalFile.s3FileName"
                                }       
                            }
                            "Next": "finishWorkflow"
                        },
                    "finishWorkflow": {
                        "Type": "Pass",
                        "End": true
                    }
             }

        }

    )
}


resource "aws_sfn_state_machine" "sfn_state_machine_apply_changes_coligadas_pessoa" {
    name = "dlr-${terraform.workspace}-state-machine-apply-changes-coligadas-pessoa"
    role_arn = var.role_arn_step_functions
    definition = jsonencode(
        {
            "Comment": "Aciona o processo de transformação dos arquivos de coligadas - pessoa",
            "StartAt": "Glue StartJobRun",
            "States": {
                    "Glue StartJobRun": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                            "Parameters": {
                                "JobName": "dlr-job-apply-change-pessoa",
                                "Arguments": {
                                            "--FILE_NAME.$": "$.originalFile.s3FileName"
                                }       
                            }
                            "Next": "finishWorkflow"
                        },
                    "finishWorkflow": {
                        "Type": "Pass",
                        "End": true
                    }
             }

        }

    )
}


resource "aws_sfn_state_machine" "sfn_state_machine_apply_changes_coligadas_email" {
    name = "dlr-${terraform.workspace}-state-machine-apply-changes-coligadas-email"
    role_arn = var.role_arn_step_functions
    definition = jsonencode(
        {
            "Comment": "Aciona o processo de transformação dos arquivos de coligadas - email",
            "StartAt": "Glue StartJobRun",
            "States": {
                    "Glue StartJobRun": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                            "Parameters": {
                                "JobName": "dlr-job-apply-change-email",
                                "Arguments": {
                                            "--FILE_NAME.$": "$.originalFile.s3FileName"
                                }       
                            }
                            "Next": "finishWorkflow"
                        },
                    "finishWorkflow": {
                        "Type": "Pass",
                        "End": true
                    }
             }

        }

    )
}


resource "aws_sfn_state_machine" "sfn_state_machine_apply_changes_coligadas_score" {
    name = "dlr-${terraform.workspace}-state-machine-apply-changes-coligadas-score"
    role_arn = var.role_arn_step_functions
    definition = jsonencode(
        {
            "Comment": "Aciona o processo de transformação dos arquivos de coligadas - score",
            "StartAt": "Glue StartJobRun",
            "States": {
                    "Glue StartJobRun": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::glue:startJobRun.sync",
                            "Parameters": {
                                "JobName": "dlr-job-apply-change-score",
                                "Arguments": {
                                            "--FILE_NAME.$": "$.originalFile.s3FileName"
                                }       
                            }
                            "Next": "finishWorkflow"
                        },
                    "finishWorkflow": {
                        "Type": "Pass",
                        "End": true
                    }
             }

        }

    )
}