# Política inline para dar permisos específicos de PESVNO al rol batch existente
# El rol es administrado por Dynamic Batch y usa local.lambda_prefix (stg, dev, qa, prd)
resource "aws_iam_role_policy" "batch_role_pesvno_permissions" {
  name = "pesvno-permissions"
  role = "BURoleFor-${local.pesvno_prefix}-${local.account_id}-procs-espec-batch"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "PesvnoS3Permissions"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetObjectAcl"
        ]
        Resource = [
          aws_s3_bucket.pesvno_bucket.arn,
          "${aws_s3_bucket.pesvno_bucket.arn}/*",
          "arn:aws:s3:::${local.result_repository_bucket}",
          "arn:aws:s3:::${local.result_repository_bucket}/*",
          "arn:aws:s3:::${local.output_redshift}",
          "arn:aws:s3:::${local.output_redshift}/*",
          "arn:aws:s3:::${local.lambda_prefix}-${local.account_id}-fwdynbtch-error-files",
          "arn:aws:s3:::${local.lambda_prefix}-${local.account_id}-fwdynbtch-error-files/*",
          "arn:aws:s3:::${local.output_onpremises}",
          "arn:aws:s3:::${local.output_onpremises}/*",
          "arn:aws:s3:::${local.output_onpremises}/logs/*",
          "arn:aws:s3:::${local.output_onpremises}/temp/*",
          "arn:aws:s3:::${local.output_onpremises}/spark-events/*"
        ]
      },
      {
        Sid    = "PesvnoSQSPermissions"
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:ChangeMessageVisibility",
          "sqs:GetQueueUrl"
        ]
        Resource = [
          aws_sqs_queue.pesvno_queue.arn,
          aws_sqs_queue.pesvno_dlq.arn
        ]
      },
      {
        Sid    = "PesvnoKMSPermissions"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:GenerateDataKey",
          "kms:Encrypt"
        ]
        Resource = [
          aws_kms_key.pesvno_key.arn,
          local.kms_dynamic_batch_key, # Llave KMS del bucket dynamic batch source (lectura y escritura .val/.dup/.inc)
          local.kms_carguedebases_key, # Llave KMS del bucket carguedebases-output (administrada por equipo CargaDeBases)
          local.kms_destino_default_key # Llave KMS por defecto para buckets DESTINO dinámicos (business table)
        ]
      },
      {
        Sid    = "PesvnoCloudWatchLogsPermissions"
        Effect = "Allow"
        Action = [
          "logs:*",
        ]
        Resource = [
          "*"
        ]
      },
      {
        Sid    = "PesvnoGlueServicePermissions"
        Effect = "Allow"
        Action = [
          "glue:GetJob",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:StartJobRun",
          "glue:BatchStopJobRun",
          "glue:UpdateJob"
        ]
        Resource = [
          "arn:aws:glue:${var.region}:${local.account_id}:job/${aws_glue_job.glue_pesvno.name}"
        ]
      },
      {
        Sid    = "PesvnoGlueDataCatalogPermissions"
        Effect = "Allow"
        Action = [
          "glue:GetConnection",
          "glue:GetConnections",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:BatchCreatePartition",
          "glue:BatchDeletePartition",
          "glue:BatchUpdatePartition"
        ]
        Resource = [
          "arn:aws:glue:${var.region}:${local.account_id}:catalog",
          "arn:aws:glue:${var.region}:${local.account_id}:database/*",
          "arn:aws:glue:${var.region}:${local.account_id}:table/*",
          "arn:aws:glue:${var.region}:${local.account_id}:connection/*"
        ]
      },
      {
        Sid    = "PesvnoStepFunctionsPermissions"
        Effect = "Allow"
        Action = [
          "states:StartExecution",
          "states:DescribeExecution",
          "states:StopExecution",
          "states:ListExecutions",
          "states:DescribeStateMachine"
        ]
        Resource = [
          aws_sfn_state_machine.sf_pesvno_process.arn,
          "${aws_sfn_state_machine.sf_pesvno_process.arn}:*"
        ]
      },
      {
        Sid    = "PesvnoEventBridgePermissions"
        Effect = "Allow"
        Action = [
          "events:PutEvents",
          "events:DescribeRule",
          "events:ListTargetsByRule"
        ]
        Resource = [
          "arn:aws:events:${var.region}:${local.account_id}:event-bus/default",
          "arn:aws:events:${var.region}:${local.account_id}:rule/*"
        ]
      },
      {
        Sid    = "PesvnoLambdaInvokePermissions"
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = [
          "arn:aws:lambda:${var.region}:${local.dynamic_batch_account_id}:function:${local.lambda_prefix}-${local.dynamic_batch_account_id}-fwdynbtch-send-email-log-lambda",
          "arn:aws:lambda:${var.region}:${local.dynamic_batch_account_id}:function:${local.lambda_prefix}-${local.dynamic_batch_account_id}-fwdynbtch-error-log-file-generation-lambda",
          aws_lambda_function.download_glue_logs.arn
        ]
      },
      {
        Sid    = "PesvnoDynamoDBPermissions"
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Query",
          "dynamodb:Scan"
        ]
        Resource = [
          "arn:aws:dynamodb:${var.region}:${local.account_id}:table/eec-aws-us-eits-procesosbatch-${var.env}-fwdynbtch_logs",
          "arn:aws:dynamodb:${var.region}:${local.account_id}:table/eec-aws-us-eits-procesosbatch-${var.env}-fwdynbtch_logs/index/*",     
          "arn:aws:dynamodb:${var.region}:${local.account_id}:table/eec-aws-us-eits-procesosbatch-${var.env}-business",
          "arn:aws:dynamodb:${var.region}:${local.account_id}:table/eec-aws-us-eits-procesosbatch-${var.env}-object-tracking"

        ]
      },
      {
        Sid    = "GlueVPCNetworking"
        Effect = "Allow"
        Action = [
          "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeVpcs",
          "ec2:DescribeVpcEndpoints",
          "ec2:DescribeRouteTables",
          "ec2:DescribeNetworkInterfaces",
          "ec2:CreateNetworkInterface",
          "ec2:DeleteNetworkInterface",
          "ec2:CreateTags"
        ]
        Resource = ["*"]
      },
      {
        Sid    = "PesvnoSecretsManagerPermissions"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = [
          "arn:aws:secretsmanager:${var.region}:${local.account_id}:secret:*"
        ]
      },
      {
        Sid    = "PesvnoRedshiftCredentialsPermissions"
        Effect = "Allow"
        Action = [
          "redshift:GetClusterCredentials",
          "redshift:GetClusterCredentialsWithIAM",
          "redshift:DescribeClusters"
        ]
        Resource = [
          "arn:aws:redshift:${var.region}:${local.account_id}:dbname:${var.redshift_cluster_id}/${var.redshift_database}",
          "arn:aws:redshift:${var.region}:${local.account_id}:dbuser:${var.redshift_cluster_id}/${var.redshift_user}",
          "arn:aws:redshift:${var.region}:${local.account_id}:cluster:${var.redshift_cluster_id}"
        ]
      },
      {
        Sid    = "PesvnoCargueDeBasesOutputS3Permissions"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:PutObjectTagging"
        ]
        Resource = [
          "arn:aws:s3:::${local.output_onpremises}/*"
        ]
      },
      {
        Sid    = "PesvnoDynamicBusinessDestinoS3Permissions"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:GetObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.stage}-${local.account_id}-*",
          "arn:aws:s3:::${var.stage}-${local.account_id}-*/*"
        ]
      }
    ]
  })
  
  depends_on = [
    aws_s3_bucket.pesvno_bucket,
    aws_sqs_queue.pesvno_queue,
    aws_sqs_queue.pesvno_dlq,
    aws_kms_key.pesvno_key,
    aws_sfn_state_machine.sf_pesvno_process,
    aws_lambda_function.download_glue_logs
  ]
}