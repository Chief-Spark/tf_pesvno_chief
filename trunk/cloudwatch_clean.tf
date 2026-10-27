# EventBridge Rule para capturar SOLO eventos de ÉXITO del Step Function
resource "aws_cloudwatch_event_rule" "sf_pesvno_completion" {
  name        = "${var.stage}-${local.account_id}-sf-pesvno-completion-rule"
  description = "Captura eventos de ÉXITO del Step Function PESVNO"
  
  event_pattern = jsonencode({
    source      = ["aws.states"]
    detail-type = ["Step Functions Execution Status Change"]
    detail = {
      status           = ["SUCCEEDED"]
      stateMachineArn  = [aws_sfn_state_machine.sf_pesvno_process.arn]
    }
  })
}

# Target para enviar al Lambda
resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.sf_pesvno_completion.name
  target_id = "SendEmailLambdaTarget"
  arn       = "arn:aws:lambda:${data.aws_region.current.name}:${local.dynamic_batch_account_id}:function:${local.lambda_prefix}-${local.dynamic_batch_account_id}-fwdynbtch-send-email-log-lambda"
  
  # Transformar el evento para que coincida con los campos requeridos por la Lambda
  input_transformer {
    input_paths = {
      execution_name = "$.detail.name"
      status         = "$.detail.status"
    }
    
    input_template = <<TEMPLATE
{
  "subject": "Proceso PESVNO Finalizado: <status>",
  "body": "La ejecución <execution_name> del proceso PESVNO ha finalizado con estado: <status>.",
  "batch_name": "PESVNO",
  "template_name": "batch_success"
}
TEMPLATE
  }
}

# Permiso para que EventBridge invoque el Lambda
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = "${local.lambda_prefix}-${local.dynamic_batch_account_id}-fwdynbtch-send-email-log-lambda"
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.sf_pesvno_completion.arn
}



# ========================================================================
# EventBridge Rule para eventos CUSTOM enviados desde el Step Function
# ========================================================================
# Esta regla captura los eventos enviados MANUALMENTE desde el estado 
# EnviarEventoErrorEventBridge del Step Function PESVNO
resource "aws_cloudwatch_event_rule" "sf_pesvno_custom_error" {
  name        = "${var.stage}-${local.account_id}-sf-pesvno-custom-error-rule"
  description = "Captura eventos CUSTOM de error enviados desde Step Function PESVNO"
  
  event_pattern = jsonencode({
    source      = ["mi.aplicacion.pesvno"]
    detail-type = ["ProcesoPESVNOCoordinado"]
    detail = {
      status = ["FAILED"]
    }
  })
}


# Target para enviar correo de ERROR al Lambda usando evento CUSTOM
resource "aws_cloudwatch_event_target" "lambda_custom_error_target" {
  rule      = aws_cloudwatch_event_rule.sf_pesvno_custom_error.name
  target_id = "SendCustomErrorEmailTarget"
  arn       = "arn:aws:lambda:${data.aws_region.current.name}:${local.dynamic_batch_account_id}:function:${local.lambda_prefix}-${local.dynamic_batch_account_id}-fwdynbtch-send-email-log-lambda"
  
  # Transformar el evento CUSTOM de error para que coincida con los campos requeridos por la Lambda
  input_transformer {
    input_paths = {
      ejecucion_id    = "$.detail.ejecucion_id"
      job_name        = "$.detail.jobName"
      error_message   = "$.detail.errorMessage"
      error_type      = "$.detail.error_type"
      batch_name      = "$.detail.batch_name"
      log_bucket      = "$.detail.s3_log_path.bucket"
      log_key         = "$.detail.s3_log_path.key"
      started_on      = "$.detail.startedOn"
      completed_on    = "$.detail.completedOn"
    }
    
    input_template = <<TEMPLATE
{
  "subject": "ERROR en Proceso PESVNO - <batch_name>",
  "body": "ERROR en ejecución <ejecucion_id> del job <job_name>. Error: <error_message>. El log de error se encuentra adjunto.",
  "batch_name": "<batch_name>",
  "template_name": "batch_error",
  "key_file": "<log_key>",
  "file_name": "pesvno_error_<ejecucion_id>.log",
  "error_details": "Job: <job_name> | Error: <error_type> - <error_message> | Inicio: <started_on> | Fin: <completed_on>"
}
TEMPLATE
  }
}

# Permiso para que EventBridge invoque el Lambda desde la regla CUSTOM
resource "aws_lambda_permission" "allow_eventbridge_custom_error" {
  statement_id  = "AllowExecutionFromEventBridgeCustomError"
  action        = "lambda:InvokeFunction"
  function_name = "${local.lambda_prefix}-${local.dynamic_batch_account_id}-fwdynbtch-send-email-log-lambda"
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.sf_pesvno_custom_error.arn
}


# ========================================================================
# EventBridge Rule para eventos NATIVOS de AWS Step Functions (DESHABILITADA)
# ========================================================================
# NOTA: Esta regla fue DESHABILITADA porque genera notificaciones duplicadas.
# Ahora solo se usa la regla CUSTOM (sf_pesvno_custom_error) que envía logs adjuntos.
resource "aws_cloudwatch_event_rule" "sf_pesvno_error" {
  name        = "${var.stage}-${local.account_id}-sf-pesvno-error-rule"
  description = "Captura eventos nativos de ERROR del Step Function PESVNO (DESHABILITADA)"
  state       = "DISABLED"  # DESHABILITADA para evitar notificaciones duplicadas
  
  event_pattern = jsonencode({
    source      = ["aws.states"]
    detail-type = ["Step Functions Execution Status Change"]
    detail = {
      status           = ["FAILED", "TIMED_OUT", "ABORTED"]
      stateMachineArn  = [aws_sfn_state_machine.sf_pesvno_process.arn]
    }
  })
}

# Target para enviar correo de ERROR al Lambda usando evento nativo (SIN ADJUNTO)
resource "aws_cloudwatch_event_target" "lambda_error_target" {
  rule      = aws_cloudwatch_event_rule.sf_pesvno_error.name
  target_id = "SendErrorEmailTarget"
  arn       = "arn:aws:lambda:${data.aws_region.current.name}:${local.dynamic_batch_account_id}:function:${local.lambda_prefix}-${local.dynamic_batch_account_id}-fwdynbtch-send-email-log-lambda"
  
  # Transformar el evento de error nativo (sin adjunto porque no tiene s3_log_path)
  input_transformer {
    input_paths = {
      execution_name = "$.detail.name"
      status         = "$.detail.status"
    }
    
    input_template = <<TEMPLATE
{
  "subject": "ERROR en Proceso PESVNO: <status>",
  "body": "La ejecución <execution_name> del proceso PESVNO ha fallado con estado <status>. Este es un evento nativo sin log adjunto.",
  "batch_name": "PESVNO",
  "template_name": "batch_error",
  "error_details": "Ejecución: <execution_name> | Estado: <status>"
}
TEMPLATE
  }
}

# Permiso para que EventBridge invoque el Lambda de errores nativos
resource "aws_lambda_permission" "allow_eventbridge_error" {
  statement_id  = "AllowExecutionFromEventBridgeError"
  action        = "lambda:InvokeFunction"
  function_name = "${local.lambda_prefix}-${local.dynamic_batch_account_id}-fwdynbtch-send-email-log-lambda"
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.sf_pesvno_error.arn
}

# Data sources necesarios
data "aws_region" "current" {}