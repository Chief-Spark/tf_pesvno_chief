# Lambda Function para descargar logs de CloudWatch y subirlos a S3
# Esta Lambda es invocada por el Step Function cuando falla un job de Glue
# NOTA: Usa el rol IAM compartido BURoleFor-{stage}-{account_id}-procs-espec-batch
# Los permisos de CloudWatch Logs se añaden en role_permissions.tf

# Crear el ZIP de la función Lambda
data "archive_file" "lambda_download_logs_zip" {
  type        = "zip"
  source_dir  = "src/lambda/download-glue-logs"
  output_path = "files/download-glue-logs.zip"
}

# Lambda Function usando rol compartido
resource "aws_lambda_function" "download_glue_logs" {
  function_name    = "${var.stage}-${local.account_id}-${local.default_name_resources}-download-logs"
  filename         = data.archive_file.lambda_download_logs_zip.output_path
  source_code_hash = data.archive_file.lambda_download_logs_zip.output_base64sha256
  role             = local.batch_role_arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.9"
  timeout          = 300  # 5 minutos
  memory_size      = 256

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.lambda_download_logs_splunk.name
  }

  environment {
    variables = {
      ENVIRONMENT = var.stage
    }
  }

  depends_on = [
    data.archive_file.lambda_download_logs_zip
  ]

  tags = merge(var.required_tags, {
    Name = "${var.stage}-${local.account_id}-${local.default_name_resources}-download-logs"
    Type = "LambdaFunction"
  })
}

# Permiso para que Step Functions pueda invocar la Lambda
resource "aws_lambda_permission" "allow_stepfunctions" {
  statement_id  = "AllowStepFunctionsInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.download_glue_logs.function_name
  principal     = "states.amazonaws.com"
  source_arn    = aws_sfn_state_machine.sf_pesvno_process.arn
}
