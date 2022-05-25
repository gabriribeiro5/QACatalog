# Criacao dos buckets S3
terraform {

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "3.55.0"
    }
  }
}
# KMS Key configuration
resource "aws_kms_key" "bucket_new_key" {
  description         = "This key is used to encrypt bucket objects"
  enable_key_rotation = true

  tags = merge(
    var.dlr_tags,
    {
      Description : "kms keys for new buckets - buckets-s3 module"
      Environment : var.env_tags[var.env]
    }
  )
}

# Bucket para gravação de Log
resource "aws_s3_bucket" "bucket_log" {
  bucket = "dlr-${terraform.workspace}-bucket-log"
  acl    = "log-delivery-write"
  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "aws:kms"
      }
    }
  }

  tags = merge(
    var.dlr_tags,
    {
      Description : "logging bucket - buckets-s3 module"
      Environment : var.env_tags[var.env]
    }
  )
}


# Buckets para os dados orinais e refinados
resource "aws_s3_bucket" "bucket_refined" {
  bucket = "dlr-${terraform.workspace}-bucket-refinedzone"
  acl    = "private"

  tags = merge(
    var.instance_tags_refined,
    var.dlr_tags,
    {
      Description : "bucket for original and refined data - buckets-s3 module"
      Environment : var.env_tags[var.env]
    }
  )
  versioning {
    enabled = true
  }

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "aws:kms"
      }
    }
  }
  logging {
    target_bucket = aws_s3_bucket.bucket_log.id
    target_prefix = "log/refined/"
  }

}

resource "aws_s3_bucket" "bucket_raw" {
  count  = var.env == "prod" ? 1 : 0
  bucket = "dlr-${terraform.workspace}-bucket-rawzone"
  acl    = "private"

  tags = merge(
    var.instance_tags_raw,
    var.dlr_tags,
    {
      Description : "bucket for raw data - buckets-s3 module"
      Environment : var.env_tags[var.env]
    }
  )
  versioning {
    enabled = true
  }

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "aws:kms"
      }
    }
  }
  logging {
    target_bucket = aws_s3_bucket.bucket_log.id
    target_prefix = "log/rawzone/"
  }

}

# Bucket para armazenar os scripts em Python/Pyspark
resource "aws_s3_bucket" "bucket_scripts" {
  bucket = "dlr-${terraform.workspace}-bucket-scripts"
  acl    = "private"

  tags = merge(
    var.instance_tags_scripts,
    var.dlr_tags,
    {
      Description : "bucket for scripts - buckets-s3 module"
      Environment : var.env_tags[var.env]
    }
  )
  versioning {
    enabled = true
  }

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "aws:kms"
      }
    }
  }
  logging {
    target_bucket = aws_s3_bucket.bucket_log.id
    target_prefix = "log/scripts/"
  }

}

# Bucket para a área Bronze do DataLake
resource "aws_s3_bucket" "bucket_bronze" {
  bucket = "dlr-${terraform.workspace}-bucket-bronze"
  acl    = "private"

  tags = merge(
    var.instance_tags_scripts,
    var.dlr_tags,
    {
      Description: "bucket for bronze area - buckets-s3 module"
      Environment: var.env_tags[var.env]
    }
  )
  versioning {
    enabled = true
  }

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "aws:kms"
      }
    }
  }
  logging {
    target_bucket = aws_s3_bucket.bucket_log.id
    target_prefix = "log/bronze/"
  }

}


# Bucket para a área Silver do DataLake
resource "aws_s3_bucket" "bucket_silver" {
  bucket = "dlr-${terraform.workspace}-bucket-silver"
  acl    = "private"

  tags = merge(
    var.instance_tags_scripts,
    var.dlr_tags,
    {
      Description : "bucket for silver area - buckets-s3 module"
      Environment : var.env_tags[var.env]
    }
  )
  versioning {
    enabled = true
  }

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "aws:kms"
      }
    }
  }
  logging {
    target_bucket = aws_s3_bucket.bucket_log.id
    target_prefix = "log/silver/"
  }

}

# Bucket para a área de Sandbox
resource "aws_s3_bucket" "bucket_sandbox" {
  bucket = "dlr-${terraform.workspace}-bucket-sandbox"
  acl    = "private"

  tags = merge(
    var.instance_tags_sandbox,
    var.dlr_tags,
    {
      Description : "bucket for sandbox zone - buckets-s3 module"
      Environment : var.env_tags[var.env]
    }
  )

  versioning {
    enabled = true
  }

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "aws:kms"
      }
    }
  }
  logging {
    target_bucket = aws_s3_bucket.bucket_log.id
    target_prefix = "log/sandbox/"
  }
}

# Bucket para a área de Stage
resource "aws_s3_bucket" "bucket_stage" {
  bucket = "dlr-${terraform.workspace}-bucket-stage"
  acl    = "private"

  tags = merge(
    var.dlr_tags,
    {
      Description : "bucket for stage zone - buckets-s3 module"
      Environment : var.env_tags[var.env]
    }
  )

  versioning {
    enabled = true
  }

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "aws:kms"
      }
    }
  }
  logging {
    target_bucket = aws_s3_bucket.bucket_log.id
    target_prefix = "log/stage/"
  }
}


# Bucket para ser usado no servidor de transferencia
resource "aws_s3_bucket" "bucket_transfer" {
  bucket = "dlr-${terraform.workspace}-bucket-transfer"
  acl    = "private"

  tags = merge(
    var.instance_tags_transfer,
    var.dlr_tags,
    {
      Description : "bucket for transfer server - buckets-s3 module"
      Environment : var.env_tags[var.env]
    }
  )
  versioning {
    enabled = true
  }

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "aws:kms"
      }
    }
  }
  logging {
    target_bucket = aws_s3_bucket.bucket_log.id
    target_prefix = "log/transfer/"
  }

}

# Bucket para ser usado como transient
resource "aws_s3_bucket" "bucket_transient" {
  bucket = "dlr-${terraform.workspace}-bucket-transient"
  acl    = "private"

  tags = merge(
    var.instance_tags_transient,
    var.dlr_tags,
    {
      Description : "bucket for transient zone - buckets-s3 module"
      Environment : var.env_tags[var.env]
    }
  )
  versioning {
    enabled = true
  }

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "aws:kms"
      }
    }
  }
  logging {
    target_bucket = aws_s3_bucket.bucket_log.id
    target_prefix = "log/transient/"
  }

}

# Carregamento de objetos para buckets S3
resource "aws_s3_bucket_object" "objetos_nos_buckets" {
  count  = length(var.bucket_objects)
  bucket = aws_s3_bucket.bucket_scripts.id
  acl    = "private"
  key    = var.bucket_objects[count.index]
  source = var.bucket_sources[count.index]

  tags = merge(
    var.dlr_tags,
    {
      Description : "load bucket objects - buckets-s3 module"
      Environment : var.env_tags[var.env]
    }
  )
}

# Criacao de diretorios do Sandbox
resource "aws_s3_bucket_object" "sandbox_folders" {
  count  = length(var.sandbox_groups)
  bucket = aws_s3_bucket.bucket_sandbox.id
  acl    = "private"
  key    = "${var.sandbox_groups[count.index]}/"
  source = "/dev/null"

  tags = merge(
    var.dlr_tags,
    {
      Description : "make directories for sandbox - buckets-s3 module"
      Environment : var.env_tags[var.env]
    }
  )
}

# Criacao de diretorios do processo ETL Murabei
resource "aws_s3_bucket_object" "murabei_folders" {
  bucket = aws_s3_bucket.bucket_refined.id
  acl    = "private"
  key    = "models/"
  source = "/dev/null"

  tags = merge(
    var.dlr_tags,
    {
      Description : "make directories for sandbox - buckets-s3 module"
      Environment : var.env_tags[var.env]
    }
  )
}
resource "aws_s3_bucket_object" "etl_folders" {
  count  = length(var.murabei_folders)
  bucket = aws_s3_bucket.bucket_refined.id
  acl    = "private"
  key    = "models/${var.murabei_folders[count.index]}/"
  source = "/dev/null"

  tags = merge(
    var.dlr_tags,
    {
      Description : "make directories for ETL - buckets-s3 module"
      Environment : var.env_tags[var.env]
    }
  )
}

# Criacao de diretorios de partnership
resource "aws_s3_bucket_object" "lideranca_folders" {
  bucket = aws_s3_bucket.bucket_transfer.id
  acl    = "private"
  key    = "partners/lideranca/"
  source = "/dev/null"

  tags = merge(
    var.dlr_tags,
    {
      Description : "make directories for lideranca partnership - buckets-s3 module"
      Environment : var.env_tags[var.env]
    }
  )
}
resource "aws_s3_bucket_object" "millennium_folders" {
  bucket = aws_s3_bucket.bucket_transfer.id
  acl    = "private"
  key    = "partners/millennium/"
  source = "/dev/null"

  tags = merge(
    var.dlr_tags,
    {
      Description : "make directories for millennium - buckets-s3 module"
      Environment : var.env_tags[var.env]
    }
  )
}
resource "aws_s3_bucket_object" "lideranca_sub_folders" {
  count  = length(var.partnership_folders)
  bucket = aws_s3_bucket.bucket_transfer.id
  acl    = "private"
  key    = "partners/lideranca/${var.partnership_folders[count.index]}/"
  source = "/dev/null"

  tags = merge(
    var.dlr_tags,
    {
      Description : "make sub-directories for lideranca - buckets-s3 module"
      Environment : var.env_tags[var.env]
    }
  )
}
resource "aws_s3_bucket_object" "millennium_sub_folders" {
  count  = length(var.partnership_folders)
  bucket = aws_s3_bucket.bucket_transfer.id
  acl    = "private"
  key    = "partners/millennium/${var.partnership_folders[count.index]}/"
  source = "/dev/null"

  tags = merge(
    var.dlr_tags,
    {
      Description : "make sub-directories for millennium - buckets-s3 module"
      Environment : var.env_tags[var.env]
    }
  )
}

resource "aws_s3_bucket_object" "shuffle_folder" {
  bucket = aws_s3_bucket.bucket_log.id
  acl    = "private"
  key    = "jobs/glue/shuffle/"
  source = "/dev/null"

  tags = merge(
    var.dlr_tags,
    {
      Description : "make directories for Glue shuffle - buckets-s3 module"
      Environment : var.env_tags[var.env]
    }
  )
}

resource "aws_s3_bucket_object" "coligadas_sub_folders" {
  count  = length(var.coligadas_folders)
  bucket = aws_s3_bucket.bucket_transfer.id
  acl    = "private"
  key    = "coligadas/${var.coligadas_folders[count.index]}/acordo/"
  source = "/dev/null"

  tags = merge(
    var.dlr_tags,
    {
      Description: "make sub-directories for coligadas acordo - buckets-s3 module"
      Environment: var.env_tags[var.env]
    }
  )
}


# Criacao de diretorio dos modulos Python
resource "aws_s3_bucket_object" "modules_folders" {
  bucket = aws_s3_bucket.bucket_scripts.id
  acl    = "private"
  key    = "modules/validator/"
  source = "/dev/null"
}

resource "aws_s3_bucket_object" "validator_objects" {
  count  = length(var.python_files_sources)
  bucket = aws_s3_bucket.bucket_scripts.id
  acl    = "private"
  key    = "modules/validator/${var.python_files_keys[count.index]}"
  source = var.python_files_sources[count.index]
}

# Criacao de diretorio dos arquivos json para arquivos de parceiros
resource "aws_s3_bucket_object" "json_folders" {
  bucket = aws_s3_bucket.bucket_transient.id
  acl    = "private"
  key    = "tmp/schema/"
  source = "/dev/null"
}

resource "aws_s3_bucket_object" "json_objects" {
  count  = length(var.json_files_sources)
  bucket = aws_s3_bucket.bucket_transient.id
  acl    = "private"
  key    = "tmp/schema/${var.json_files_keys[count.index]}"
  source = var.json_files_sources[count.index]
}
