# Dead Letter Queue
resource "aws_sqs_queue" "pesvno_dlq" {
  name = "${var.stage}-${local.account_id}-${local.default_name_resources}-dlq"

  kms_master_key_id                 = aws_kms_key.pesvno_key.arn
  kms_data_key_reuse_period_seconds = 300
  
  depends_on = [aws_kms_key.pesvno_key]

  # Política para requerir transporte seguro
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Deny"
        Principal = "*"
        Action = "sqs:*"
        Resource = "*"
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      }
    ]
  })

  tags = merge(var.required_tags, {
    Name = "${var.stage}-${local.account_id}-${local.default_name_resources}-dlq"
    Type = "DeadLetterQueue"
  })
}

# Main SQS Queue
resource "aws_sqs_queue" "pesvno_queue" {
  name = "${var.stage}-${local.account_id}-${local.default_name_resources}-queue"

  # Configuración de Dead Letter Queue
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.pesvno_dlq.arn
    maxReceiveCount     = 6
  })

  # Encriptación KMS
  kms_master_key_id                 = aws_kms_key.pesvno_key.arn
  kms_data_key_reuse_period_seconds = 300

  # Configuración de timeouts
  visibility_timeout_seconds = 300
  message_retention_seconds  = 1209600 # 14 días

  # Política movida a recurso separado aws_sqs_queue_policy para evitar self-reference

  tags = merge(var.required_tags, {
    Name = "${var.stage}-${local.account_id}-${local.default_name_resources}-queue"
    Type = "MainQueue"
  })
  
  depends_on = [
    aws_kms_key.pesvno_key,
    aws_sqs_queue.pesvno_dlq,
    aws_s3_bucket.pesvno_bucket
  ]
}

# Política asociada al queue principal (evita self-referential block dentro del mismo recurso)
resource "aws_sqs_queue_policy" "pesvno_queue_policy" {
  queue_url = aws_sqs_queue.pesvno_queue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DenyInsecureTransport"
        Effect = "Deny"
        Principal = "*"
        Action = "sqs:*"
        Resource = aws_sqs_queue.pesvno_queue.arn
        Condition = {
          Bool = { "aws:SecureTransport" = "false" }
        }
      },
      {
        Sid    = "AllowS3PublishDynamicBatchSource"
        Effect = "Allow"
        Principal = { Service = "s3.amazonaws.com" }
        Action   = "sqs:SendMessage"
        Resource = aws_sqs_queue.pesvno_queue.arn
        Condition = {
          StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
          ArnEquals    = { "aws:SourceArn" = "arn:aws:s3:::${local.lambda_prefix}-${local.account_id}-fwdynbtch-dynamic-batch-source-repository"}
        }
      },
      {
        Sid    = "AllowS3PublishPesvnoInput"
        Effect = "Allow"
        Principal = { Service = "s3.amazonaws.com" }
        Action   = "sqs:SendMessage"
        Resource = aws_sqs_queue.pesvno_queue.arn
        Condition = {
          StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
          ArnEquals    = { "aws:SourceArn" = aws_s3_bucket.pesvno_bucket.arn }
        }
      },
      {
        Sid    = "AllowLambdaRead"
        Effect = "Allow"
        Principal = { AWS = local.batch_role_arn }
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:ChangeMessageVisibility"
        ]
        Resource = aws_sqs_queue.pesvno_queue.arn
      }
    ]
  })

  depends_on = [aws_sqs_queue.pesvno_queue]
}