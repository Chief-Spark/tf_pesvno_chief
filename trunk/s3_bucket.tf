# Bucket S3 principal para Input PESVNO
resource "aws_s3_bucket" "pesvno_bucket" {
  bucket = "${var.stage}-${local.account_id}-${var.bucket_name}-bucket"

  tags = merge(var.required_tags, {
    Name        = "${var.stage}-${local.account_id}-${var.bucket_name}-bucket"
    Description = "Input bucket for PESVNO process with S3-SQS notification"
  })

  lifecycle {
    prevent_destroy = true
  }
}

# Notificación S3 -> SQS para archivos YAML en cualquier subcarpeta
resource "aws_s3_bucket_notification" "pesvno_yaml_notification" {
  bucket = aws_s3_bucket.pesvno_bucket.id

  queue {
    queue_arn     = aws_sqs_queue.pesvno_queue.arn
    events        = ["s3:ObjectCreated:Put", "s3:ObjectCreated:Copy", "s3:ObjectCreated:CompleteMultipartUpload"]
    filter_prefix = "" # Permite subcarpetas dinámicas
    filter_suffix = ".yaml"
  }

  depends_on = [
    aws_sqs_queue.pesvno_queue,
    aws_sqs_queue_policy.pesvno_queue_policy,
    aws_s3_bucket_policy.pesvno_bucket_policy
  ]
}

# Configuración de versionado del bucket
resource "aws_s3_bucket_versioning" "pesvno_bucket_versioning" {
  bucket = aws_s3_bucket.pesvno_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Configuración de encriptación SSE-KMS con clave personalizada
resource "aws_s3_bucket_server_side_encryption_configuration" "pesvno_bucket_encryption" {
  bucket = aws_s3_bucket.pesvno_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.pesvno_key.arn
    }
    bucket_key_enabled = true
  }
  
  depends_on = [aws_kms_key.pesvno_key]
}

# Configuración para bloquear acceso público
resource "aws_s3_bucket_public_access_block" "pesvno_bucket_pab" {
  bucket = aws_s3_bucket.pesvno_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Política del bucket
resource "aws_s3_bucket_policy" "pesvno_bucket_policy" {
  bucket = aws_s3_bucket.pesvno_bucket.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowEnvironmentRoleAccess"
        Effect = "Allow"
        Principal = {
          AWS = [
           local.batch_role_arn,
            "arn:aws:iam::${var.id_cargue_de_bases}:role/BURoleFor-${var.env}-${var.id_cargue_de_bases}-carguedebases-GlueDataInPutRole",
            local.environment_role_arn
          ]
        }
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "${aws_s3_bucket.pesvno_bucket.arn}",
          "${aws_s3_bucket.pesvno_bucket.arn}/*"
        ]
      },
      {
        Sid    = "DenyInsecureConnections"
        Effect = "Deny"
        Principal = "*"
        Action = "s3:*"
        Resource = [
           "${aws_s3_bucket.pesvno_bucket.arn}",
          "${aws_s3_bucket.pesvno_bucket.arn}/*"
        ]
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      },
      {
        Sid    = "AllowLambdaAccess"
        Effect = "Allow"
        Principal = {
          AWS = local.batch_role_arn
        }
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "${aws_s3_bucket.pesvno_bucket.arn}",
          "${aws_s3_bucket.pesvno_bucket.arn}/*"
        ]
      },
      {
        Sid    = "AllowJenkinsDeploymentAccess"
        Effect = "Allow"
        Principal = {
          AWS = [
            "arn:aws:iam::${local.account_id}:role/BURoleForJenkinsGlobalSpLatam${var.env}",
            "arn:aws:sts::${local.account_id}:assumed-role/BURoleForJenkinsGlobalSpLatam${var.env}/terraform"
          ]
        }
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "${aws_s3_bucket.pesvno_bucket.arn}",
          "${aws_s3_bucket.pesvno_bucket.arn}/*"
        ]
      },
      {
        Sid    = "RequireSSEKMS"
        Effect = "Deny"
        Principal = "*"
        Action = "s3:PutObject"
        Resource = "${aws_s3_bucket.pesvno_bucket.arn}/*"
        Condition = {
          StringNotEquals = {
            "s3:x-amz-server-side-encryption" = "aws:kms"
            "aws:PrincipalArn" = [
              local.environment_role_arn,
              local.batch_role_arn,
              "arn:aws:iam::${local.account_id}:role/BURoleForJenkinsGlobalSpLatam${var.env}",
              "arn:aws:sts::${local.account_id}:assumed-role/BURoleForJenkinsGlobalSpLatam${var.env}/terraform",
              "arn:aws:iam::${var.id_cargue_de_bases}:role/BURoleFor-${var.env}-${var.id_cargue_de_bases}-carguedebases-GlueDataInPutRole"
            ]
          }
        }
      },
      {
        Sid    = "DenyDeleteGlueScripts"
        Effect = "Deny"
        Principal = "*"
        Action = [
          "s3:DeleteObject",
          "s3:DeleteObjectVersion"
        ]
        Resource = "${aws_s3_bucket.pesvno_bucket.arn}/${join("", [join("_", [var.stage, "sla_co_batch/glue/*"])])}"
      }
    ]
  })
}

# Política de ciclo de vida - Standard 7 días, Delete > 7 días
resource "aws_s3_bucket_lifecycle_configuration" "pesvno_bucket_lifecycle" {
  depends_on = [aws_s3_bucket_versioning.pesvno_bucket_versioning]
  bucket     = aws_s3_bucket.pesvno_bucket.id

  # Regla CRÍTICA: Proteger carpeta sla_co_batch (incluye scripts Glue) - NUNCA expiran
  rule {
    id     = "protect_sla_co_batch_permanent"
    status = "Enabled"

    filter {
      prefix = join("", [join("_", [var.stage, "sla_co_batch/"])])
    }

    # Sin expiration = PERMANENTE (no se borran archivos)
    # Transición a Intelligent Tiering después de 30 días para optimizar costos
    transition {
      days          = 30
      storage_class = "INTELLIGENT_TIERING"
    }
  }

  # Regla para limpiar archivos temporales de input (NO afecta sla_co_batch)
  rule {
    id     = "lifecycle_input_files_cleanup"
    status = "Enabled"

    filter {
      prefix = "input/"
    }

    expiration {
      days = 7
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  # Regla para manejo de versiones y multipart - SIN expiración general
  rule {
    id     = "PurgeVersionsAndMultipart"
    status = "Enabled"

    filter {
      prefix = "input/"
    }

    transition {
      days          = 1
      storage_class = "INTELLIGENT_TIERING"
    }

    noncurrent_version_transition {
      noncurrent_days = 1
      storage_class   = "INTELLIGENT_TIERING"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  # Regla para limpiar multipart uploads incompletos en todo el bucket
  rule {
    id     = "CleanupIncompleteMultipart"
    status = "Enabled"

    filter {
      prefix = ""
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

