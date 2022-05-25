# Criação dos grupos de usuarios do Sandbox

resource "aws_iam_group" "grupos_sandbox" {
  count = length(var.iam_groups)
  name  = "dlr-${terraform.workspace}-${var.iam_groups[count.index]}"
}


# Criacao das roles para os componentes do DataLake Return Capital

resource "aws_iam_role" "role_glue" {
  name = "dlr-${terraform.workspace}-role-glue"

  tags = merge(
    var.dlr_tags,
    {
      Description: "IAM role for glue - polices module"
      Environment: var.env_tags[var.env]
    }
  )
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "glue.amazonaws.com"
        }
      },
    ]
  })

}

resource "aws_iam_role" "role_redshift" {
  name = "dlr-${terraform.workspace}-role-redshift"

  tags = merge(
    var.dlr_tags,
    {
      Description: "IAM role for redshift - polices module"
      Environment: var.env_tags[var.env]
    }
  )
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      }
    ]
  })
}


resource "aws_iam_role" "role_transfer" {
  name = "dlr-${terraform.workspace}-role-transfer"

  tags = merge(
    var.dlr_tags,
    {
      Description: "IAM role for transfer - polices module"
      Environment: var.env_tags[var.env]
    }
  )
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "transfer.amazonaws.com"
        }
      },
    ]
  })

}

resource "aws_iam_role" "role_sagemaker" {
  name = "dlr-${terraform.workspace}-role-sagemaker"

  tags = merge(
    var.dlr_tags,
    {
      Description: "IAM role for sagemaker - polices module"
      Environment: var.env_tags[var.env]
    }
  )
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "sagemaker.amazonaws.com"
        }
      },
    ]
  })

}

resource "aws_iam_role" "role_lambda_redshift" {
  name = "dlr-${terraform.workspace}-role-lambda-redshift"

  tags = merge(
    var.dlr_tags,
    {
      Description: "IAM role for lambda redshift - polices module"
      Environment: var.env_tags[var.env]
    }
  )
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      },
    ]
  })

}


resource "aws_iam_role" "role_lambda_stepfunctions" {
  name = "dlr-${terraform.workspace}-role-lambda-stepfunctions"

  tags = merge(
    var.dlr_tags,
    {
      Description: "IAM role for lambda stepfunctions - polices module"
      Environment: var.env_tags[var.env]
    }
  )
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      },
    ]
  })

}

resource "aws_iam_role" "role_stepfunctions_lambda" {
  name = "dlr-${terraform.workspace}-role-stepfunctions-lambda"

  tags = merge(
    var.dlr_tags,
    {
      Description: "IAM role for stepfunctions lambda - polices module"
      Environment: var.env_tags[var.env]
    }
  )
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "states.amazonaws.com"
        }
      },
    ]
  })

}

resource "aws_iam_role" "role_glue_redshift" {
  name = "dlr-${terraform.workspace}-role-glue-redshift"

  tags = merge(
    var.dlr_tags,
    {
      Description: "IAM role for glue redshift - polices module"
      Environment: var.env_tags[var.env]
    }
  )
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "glue.amazonaws.com"
        }
      },
    ]
  })

}

# Criacao das politicas de acesso para uso nas roles de componentes e nos usuarios
resource "aws_iam_policy" "policy_glue" {
  name        = "dlr-${terraform.workspace}-glue-policy"
  description = "Politica de acesso aos Buckets S3 e EC2 pelo Glue"

  tags = merge(
    var.dlr_tags,
    {
      Description: "Creates polices to access S3 and EC2  - polices module"
      Environment: var.env_tags[var.env]
    }
  )
  policy = jsonencode(
    {
      "Version" : "2012-10-17",
      "Statement" : [
        {
          "Effect" : "Allow",
          "Action" : [
            "iam:GetRole",
            "iam:PassRole"
          ],
          "Resource" : [aws_iam_role.role_glue.arn]
        }
      ]
    }
  )
}


resource "aws_iam_policy" "policy_redshift" {
  name        = "dlr-${terraform.workspace}-redshift-policy"
  description = "Politica de acesso ao S3 pelo Redshift"

  tags = merge(
    var.dlr_tags,
    {
      Description: "Police to access S3 via Redshift  - polices module"
      Environment: var.env_tags[var.env]
    }
  )
  policy = jsonencode(
    {
      "Version" : "2012-10-17",
      "Statement" : [
        {
          "Effect" : "Allow",
          "Action" : [
            "iam:GetRole",
            "iam:PassRole"
          ],
          "Resource" : [aws_iam_role.role_redshift.arn]
        }
      ]
    }
  )
}


resource "aws_iam_policy" "policy_transfer" {
  name        = "dlr-${terraform.workspace}-transfer-policy"
  description = "Politica de acesso do S3 pelo Transfer Server"

  tags = merge(
    var.dlr_tags,
    {
      Description: "Police to access S3 via Transfer Server  - polices module"
      Environment: var.env_tags[var.env]
    }
  )
  policy = jsonencode(
    {
      "Version" : "2012-10-17",
      "Statement" : [
        {
          "Effect" : "Allow",
          "Action" : [
            "iam:GetRole",
            "iam:PassRole"
          ],
          "Resource" : [aws_iam_role.role_transfer.arn]
        }
      ]
    }
  )
}

resource "aws_iam_policy" "policy_sagemaker" {
  name        = "dlr-${terraform.workspace}-sagemaker-policy"
  description = "Politica de acesso dos componentes do DataLake pelo SageMaker"

  tags = merge(
    var.dlr_tags,
    {
      Description: "Access police for DLR components via SageMaker  - polices module"
      Environment: var.env_tags[var.env]
    }
  )
  policy = jsonencode(
    {
      "Version" : "2012-10-17",
      "Statement" : [
        {
          "Effect" : "Allow",
          "Action" : [
            "iam:GetRole",
            "iam:PassRole"
          ],
          "Resource" : [aws_iam_role.role_sagemaker.arn]
        }
      ]
    }
  )
}

resource "aws_iam_policy" "policy_glue_redshift" {
  name        = "dlr-${terraform.workspace}-glue-redshift-policy"
  description = "Politica de acesso ao Redshift pelo Glue"

  tags = merge(
    var.dlr_tags,
    {
      Description: "Police to access Redshift via Glue  - polices module"
      Environment: var.env_tags[var.env]
    }
  )
  policy = jsonencode(
    {
      "Version" : "2012-10-17",
      "Statement" : [
        {
          "Effect" : "Allow",
          "Action" : [
            "iam:GetRole",
            "iam:PassRole"
          ],
          "Resource" : [aws_iam_role.role_glue_redshift.arn]
        }
      ]
    }
  )
}

resource "aws_iam_policy" "policies_bucket_sandbox" {
  count       = length(var.iam_groups)
  name        = "dlr-${terraform.workspace}-${var.iam_groups[count.index]}-sandbox-policy"
  description = "politicas de acesso aos diretorios do bucket sandbox"

  tags = merge(
    var.dlr_tags,
    {
      Description: "Police to access S3 sandbox - polices module"
      Environment: var.env_tags[var.env]
    }
  )
  policy = jsonencode(
    {
      "Version" : "2012-10-17",
      "Statement" : [
        {
          "Sid" : "",
          "Action" : [
            "s3:ListAllMyBuckets"
          ],
          "Effect" : "Allow",
          "Resource" : [
            "arn:aws:s3:::*"
          ]
        },
        {
          "Sid" : "",
          "Action" : [
            "s3:ListBucket"
          ],
          "Effect" : "Allow",
          "Resource" : [
            "arn:aws:s3:::*"
          ]
        },
        {
          "Sid" : "",
          "Action" : [
            "s3:ListBucket"
          ],
          "Effect" : "Allow",
          "Resource" : [
            "arn:aws:s3:::dlr-${terraform.workspace}-bucket-sandbox"
          ],
          "Condition" : {
            "StringLike" : {
              "s3:prefix" : [
                "${var.sandbox_groups[count.index]}/*"
              ]
            }
          }
        },
        {
          "Sid" : "",
          "Action" : [
            "s3:ListBucket"
          ],
          "Effect" : "Deny",
          "Resource" : [
            "arn:aws:s3:::dlr-${terraform.workspace}-bucket-sandbox"
          ],
          "Condition" : {
            "StringNotLike" : {
              "s3:prefix" : [
                "${var.sandbox_groups[count.index]}/*",
                ""
              ]
            },
            "Null" : {
              "s3:prefix" : false
            }
          }
        },
        {
          "Sid" : "",
          "Action" : [
            "s3:GetObject",
            "s3:PutObject"
          ],
          "Effect" : "Allow",
          "Resource" : [
            "arn:aws:s3:::dlr-${terraform.workspace}-bucket-sandbox/${var.sandbox_groups[count.index]}/*"
          ]
        }
      ]
    }
  )

}


# Anexo das politicas e roles aos usuarios do sistema
resource "aws_iam_role_policy_attachment" "glue_role_policy_attach" {
  role       = aws_iam_role.role_glue.name
  count      = length(var.iam_policy_glue)
  policy_arn = var.iam_policy_glue[count.index]
}

resource "aws_iam_role_policy_attachment" "redshift_role_policy_attach" {
  role       = aws_iam_role.role_redshift.name
  count      = length(var.iam_policy_redshift)
  policy_arn = var.iam_policy_redshift[count.index]
}

resource "aws_iam_role_policy_attachment" "transfer_role_policy_attach" {
  role       = aws_iam_role.role_transfer.name
  count      = length(var.iam_policy_transfer)
  policy_arn = var.iam_policy_transfer[count.index]
}

resource "aws_iam_role_policy_attachment" "sagemaker_role_policy_attach" {
  role       = aws_iam_role.role_sagemaker.name
  count      = length(var.iam_policy_sagemaker)
  policy_arn = var.iam_policy_sagemaker[count.index]
}

resource "aws_iam_role_policy_attachment" "glue_role_redshift_policy_attach" {
  role       = aws_iam_role.role_glue_redshift.name
  count      = length(var.iam_policy_glue_redshift)
  policy_arn = var.iam_policy_glue_redshift[count.index]
}

resource "aws_iam_role_policy_attachment" "lambda_role_step_functions_policy_attach" {
  role       = aws_iam_role.role_lambda_stepfunctions.name
  count      = length(var.iam_policy_lambda_stepfunctions)
  policy_arn = var.iam_policy_lambda_stepfunctions[count.index]
}

resource "aws_iam_role_policy_attachment" "step_functions_lambda_policy_attach" {
  role       = aws_iam_role.role_stepfunctions_lambda.name
  count      = length(var.iam_policy_step_functions_lambda)
  policy_arn = var.iam_policy_step_functions_lambda[count.index]
}
resource "aws_iam_role_policy_attachment" "lambda_role_redshift_policy_attach" {
  role       = aws_iam_role.role_lambda_redshift.name
  count      = length(var.iam_policy_lambda_redshift)
  policy_arn = var.iam_policy_lambda_redshift[count.index]
}
resource "aws_iam_policy_attachment" "users_glue_policy_attach" {
  name       = "dlr-${terraform.workspace}-glue-users-policy"
  users      = ["${var.user_glue_a[var.env]}", "${var.user_glue_b[var.env]}", "${var.user_glue_d[var.env]}"]
  roles      = [aws_iam_role.role_glue.name]
  policy_arn = aws_iam_policy.policy_glue.arn
}

resource "aws_iam_policy_attachment" "users_redshift_policy_attach" {
  name       = "dlr-${terraform.workspace}-redshift-users-policy"
  users      = ["${var.user_redshift_a[var.env]}", "${var.user_redshift_b[var.env]}"]
  roles      = [aws_iam_role.role_redshift.name]
  policy_arn = aws_iam_policy.policy_redshift.arn
}

resource "aws_iam_group_policy_attachment" "sandbox_groups_policy_attach_redshift" {
  count      = length(var.iam_groups)
  group      = aws_iam_group.grupos_sandbox[count.index].name
  policy_arn = var.iam_policy_redshift_sandbox
}
resource "aws_iam_group_policy_attachment" "sandbox_groups_policy_attach_buckets" {
  count      = length(var.iam_groups)
  group      = aws_iam_group.grupos_sandbox[count.index].name
  policy_arn = aws_iam_policy.policies_bucket_sandbox[count.index].arn
}

# Criacao de security group
resource "aws_security_group" "glue-security-group" {
  name        = "dlr-${terraform.workspace}-glue-security-group"
  description = "Security group para uso nos Glues A e B"
  vpc_id      = var.dlr_vpc_dev[var.env]

  tags = merge(
    var.dlr_tags,
    {
      Description: "Glue security group - polices module"
      Environment: var.env_tags[var.env]
    }
  )
  
  ingress = [
    {
      description      = "Inbound Rules"
      protocol         = "-1"
      self             = true
      from_port        = 0
      to_port          = 0
      cidr_blocks      = null
      ipv6_cidr_blocks = null
      security_groups  = null
      prefix_list_ids  = null

    }
  ]

  egress = [
    {
      description      = "Outbound Rules"
      from_port        = 0
      to_port          = 0
      protocol         = "-1"
      self             = true
      cidr_blocks      = ["0.0.0.0/0"]
      ipv6_cidr_blocks = null
      security_groups  = null
      prefix_list_ids  = null
    }
  ]
}


# Policy para uso na função lambda que copia objetos da transfer para a bronze
resource "aws_iam_policy" "policy_copy_s3_objects" {
  name        = "dlr-${terraform.workspace}-lambda-policy"
  description = "Politica de cópia dos objetos S3 pelo Lambda"

  policy = jsonencode(
    {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:RestoreObject",
                "s3:DeleteObject"
            ],
            "Resource": "arn:aws:s3:::*/*"
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::*"
        }
      ]
    }
  )
}


resource "aws_iam_role" "role_lambda_s3" {
  name = "dlr-${terraform.workspace}-lambda-bucket3-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      },
    ]
  })

}

resource "aws_iam_role_policy_attachment" "lambda_role_policy_attach" {
  role       = aws_iam_role.role_lambda_s3.name
  policy_arn = aws_iam_policy.policy_copy_s3_objects.arn
}

