# Lambda Function para procesar mensajes SQS de archivos .prn
resource "aws_lambda_function" "pesvno_processor" {
  function_name    = "${var.stage}-${local.account_id}-${local.default_name_resources}-processor"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  role            = local.batch_role_arn
  handler         = "lambda_function.lambda_handler"
  runtime         = "python3.9"
  timeout         = 300  # 5 minutos
  memory_size     = 512
  reserved_concurrent_executions = 30

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.lambda_processor_splunk.name
  }

  # Configuración de red si es necesario
  # vpc_config {
  #   subnet_ids         = var.subnet_ids
  #   security_group_ids = [aws_security_group.lambda_sg.id]
  # }

  # Variables de entorno
  environment {
    variables = {
      S3_BUCKET_NAME         = aws_s3_bucket.pesvno_bucket.bucket
      SOURCE_REPOSITORY_BUCKET = local.output_redshift
      KMS_KEY_ID            = aws_kms_key.pesvno_key.key_id
      ENVIRONMENT           = var.stage
      STEP_FUNCTION_ARN     = aws_sfn_state_machine.sf_pesvno_process.arn
      DB_EDF_RELATIONALIZE  = "${var.stage}-${local.account_id}-batch-ascend-link-relat"
      PRODUCT_RULES_S3_URI  = "s3://${aws_s3_object.product_rules_csv_object.bucket}/${aws_s3_object.product_rules_csv_object.key}"
    }
  }

  # Encriptación de variables de entorno
  kms_key_arn = aws_kms_key.pesvno_key.arn

  depends_on = [
    data.archive_file.lambda_zip,
    aws_s3_object.product_rules_csv_object
  ]

  tags = merge(var.required_tags, {
    Name = "${var.stage}-${local.account_id}-${local.default_name_resources}-processor"
    Type = "LambdaFunction"
  })
}

# Event Source Mapping: SQS -> Lambda
resource "aws_lambda_event_source_mapping" "sqs_trigger" {
  event_source_arn = aws_sqs_queue.pesvno_queue.arn
  function_name    = aws_lambda_function.pesvno_processor.arn
  batch_size       = 1  # Procesar un mensaje a la vez

  scaling_config {
    maximum_concurrency = 30
  }
  
  # Configuración de Dead Letter Queue
  function_response_types = ["ReportBatchItemFailures"]
  
  depends_on = [
    aws_lambda_function.pesvno_processor,
    aws_sqs_queue.pesvno_queue
  ]
}

# Permisos para que SQS pueda invocar la Lambda
resource "aws_lambda_permission" "allow_sqs" {
  statement_id  = "AllowSQSInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.pesvno_processor.function_name
  principal     = "sqs.amazonaws.com"
  source_arn    = aws_sqs_queue.pesvno_queue.arn
}

# Crear el ZIP de la función Lambda siguiendo el estándar del proyecto
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "src/lambda/process-prn-files"
  output_path = "files/process-prn-files.zip"
}



