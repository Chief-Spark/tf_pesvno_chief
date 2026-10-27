# KMS Key para encriptación del bucket S3 y SQS
resource "aws_kms_key" "pesvno_key" {
  description             = "KMS key for PESVNO S3 bucket and SQS encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Sid    = "Enable IAM User Permissions"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      },
      {
        Sid    = "Allow Environment Role"
        Effect = "Allow"
        Principal = {
          AWS = local.environment_role_arn
        }
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:CreateGrant",
          "kms:DescribeKey"
        ]
        Resource = "*"
      },
      {
        Sid    = "Allow S3 Service"
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = "*"
      },
      {
        Sid    = "Allow SQS Service"
        Effect = "Allow"
        Principal = {
          Service = "sqs.amazonaws.com"
        }
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = "*"
      },
      {
        Sid    = "AllowDevelopersReadAccess"
        Effect = "Allow"
        Principal = {
          AWS = compact([
            "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.role_default}",
            local.batch_role_arn
          ])
        }
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "kms:ViaService" = "s3.${var.region}.amazonaws.com"
          }
        }
      }
    ],
    var.enable_batch_role_kms ? [{
      Sid    = "Allow Batch Role"
      Effect = "Allow"
      Principal = {
        AWS = local.batch_role_arn
      }
      Action = [
        "kms:Decrypt",
        "kms:GenerateDataKey",
        "kms:CreateGrant",
        "kms:DescribeKey"
      ]
      Resource = "*"
    }] : [],
    [{
      Sid    = "AllowAdminRoleDecrypt"
      Effect = "Allow"
      Principal = {
        AWS = [
          "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/BUAdministratorAccessRole"
        ]
      }
      Action = [
        "kms:Decrypt",
        "kms:DescribeKey"
      ]
      Resource = "*"
    }])
  })

  tags = merge(var.required_tags, {
    Name = "${var.stage}-${local.account_id}-${local.default_name_resources}-kms-key"
  })
}


resource "aws_kms_alias" "pesvno_key_alias" {
  name          = "alias/${var.stage}-${local.account_id}-${local.default_name_resources}-key"
  target_key_id = aws_kms_key.pesvno_key.key_id
}
