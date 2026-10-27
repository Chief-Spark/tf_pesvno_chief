######################################################################
# EventBridge Rule para detectar fallos en Glue Job PESVNO
######################################################################
# Esta regla captura eventos cuando el Glue job de PESVNO falla,
# se agota el tiempo o encuentra un error, y envía una notificación
# al SQS de email del Dynamic Batch (mismo patrón que Step Function de upload).
######################################################################


resource "aws_cloudwatch_event_rule" "glue_pesvno_failure" {
  name        = "${var.stage}-${local.account_id}-glue-pesvno-failure-rule"
  description = "Captura eventos de fallo en Glue Job PESVNO para el ambiente ${var.stage}"
  state       = "ENABLED"
  
  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail = {
      jobName = [aws_glue_job.glue_pesvno.name]
      state   = ["FAILED", "ERROR", "TIMEOUT"]
    }
  })


  tags = merge(var.required_tags, {
    Name = "${var.stage}-${local.account_id}-glue-pesvno-failure-rule"
    Type = "EventBridgeRule"
  })
}

# Permiso para que EventBridge pueda invocar la Lambda
resource "aws_lambda_permission" "allow_eventbridge_invoke_send_email" {
  statement_id  = "AllowExecutionFromEventBridgeSendEmailPESVNO"
  action        = "lambda:InvokeFunction"
  function_name = local.send_email_log_lambda_arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.glue_pesvno_failure.arn
}

# Target para invocar la Lambda de email cuando falla el Glue job
resource "aws_cloudwatch_event_target" "glue_pesvno_failure_notification" {
  rule      = aws_cloudwatch_event_rule.glue_pesvno_failure.name
  target_id = "SendGlueFailureLambdaTarget"
  arn       = local.send_email_log_lambda_arn
  
  # Transformar el evento de Glue para que coincida con los campos requeridos por la Lambda de email
  input_transformer {
    input_paths = {
      job_name       = "$.detail.jobName"
      job_run_id     = "$.detail.jobRunId"
      state          = "$.detail.state"
      message        = "$.detail.message"
      started_on     = "$.detail.startedOn"
      completed_on   = "$.detail.completedOn"
    }
    
    input_template = <<TEMPLATE
{
  "subject": "ERROR en Glue Job PESVNO - <state>",
  "body": "El Glue Job <job_name> ha fallado con estado <state>.ID de ejecucion: <job_run_id>Mensaje: <message>Por favor revise los logs en CloudWatch para mas detalles.",
  "batch_name": "PESVNO-GLUE",
  "template_name": "batch_error",
  "error_details": "Job: <job_name> | Estado: <state> | Run ID: <job_run_id> | Inicio: <started_on> | Fin: <completed_on>",
  "process_type": "batch"
}
TEMPLATE
  }
}
