resource "aws_sfn_state_machine" "sf_pesvno_process" {
  name     = join("-", [var.stage, "${local.account_id}", "sf", "pesvno"])
  role_arn = join("", ["arn:aws:iam::", local.account_id, ":role/", join("-", ["BURoleFor", var.env, "${local.account_id}", "procs-espec-batch"])])

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_pesvno_splunk.arn}:*"
    level                  = "ALL"
    include_execution_data = true
  }

  definition = jsonencode({
  Comment = "Step Function para proceso PESVNO con configuración YAML dinámica"
  StartAt = "ValidarParametrosEntrada"
  States = {
    ValidarParametrosEntrada = {
      Type    = "Pass"
      Comment = "Valida que los parámetros mínimos estén presentes"
      Parameters = {
        "config_yaml.$"   = "$.config_yaml"
        "glue_arguments.$" = "$.glue_arguments"
        "metadata.$"       = "$.metadata"
      }
      Next = "VerificarConfiguracionValida"
    },
    VerificarConfiguracionValida = {
      Type    = "Choice"
      Comment = "Verifica si la configuración YAML es válida"
      Choices = [{
        Variable  = "$.config_yaml.version"
        IsPresent = true
        Next      = "PrepararParametrosGlue"
      }]
      Default = "ErrorConfiguracionInvalida"
    },
    PrepararParametrosGlue = {
      Type    = "Pass"
      Comment = "Prepara parámetros legacy para el Glue actual"
      Parameters = {
        JobName   = aws_glue_job.glue_pesvno.name
        "Arguments.$" = "$.glue_arguments",
        "Arguments.--s3_temp_dir" = "s3://${local.lambda_prefix}-${local.account_id}-fwdynbtch-error-files/logs/"
      }
      ResultPath = "$.glue_job_params"
      Next       = "EjecutarGluePESVNO"
    },
    EjecutarGluePESVNO = {
      Type            = "Task"
      Comment         = "Ejecuta el job de Glue PESVNO con los parámetros configurados"
      Resource        = "arn:aws:states:::glue:startJobRun.sync"
      Parameters = {
        "JobName.$"   = "$.glue_job_params.JobName"
        "Arguments.$" = "$.glue_job_params.Arguments"
        Timeout       = 2880
      }
      TimeoutSeconds = 10800
      ResultPath     = "$.resultado_glue"
      End            = true
      Catch = [{
        ErrorEquals = ["States.ALL"]
        ResultPath  = "$.error_info"
        Next        = "ExtraerDatosError"
      }]
    },
    ExtraerDatosError = {
      "Type": "Pass",
      "Parameters": {
        "cause_parsed.$": "States.StringToJson($.error_info.Cause)",
        "error_type.$": "$.error_info.Error"
      },
      "ResultPath": "$.error_parsing",
      "Next": "DescargarLogsAS3"
    },
    DescargarLogsAS3 = {
      "Type": "Task",
      "Comment": "Invoca Lambda para descargar logs de CloudWatch y subirlos a S3",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "${aws_lambda_function.download_glue_logs.arn}",
        "Payload": {
          "job_run_id.$": "$.error_parsing.cause_parsed.Id",
          "job_name.$": "$.error_parsing.cause_parsed.JobName",
          "log_group_name.$": "$.error_parsing.cause_parsed.LogGroupName",
          "stage": "${var.stage}",
          "account_id": "${local.account_id}",
          "error_bucket": "${local.lambda_prefix}-${local.account_id}-fwdynbtch-error-files"
        }
      },
      "ResultSelector": {
        "bucket.$": "$.Payload.bucket",
        "key.$": "$.Payload.key",
        "size_bytes.$": "$.Payload.size_bytes",
        "timestamp.$": "$.Payload.timestamp"
      },
      "ResultPath": "$.s3_log_result",
      "Next": "PrepararDatosError",
      "Catch": [{
        "ErrorEquals": ["States.ALL"],
        "ResultPath": "$.lambda_error",
        "Next": "PrepararDatosError"
      }]
    },
    PrepararDatosError = {
      "Type": "Pass",
      "Parameters": {
        "jobRunId.$": "$.error_parsing.cause_parsed.Id",
        "jobName.$": "$.error_parsing.cause_parsed.JobName",
        "errorMessage.$": "$.error_parsing.cause_parsed.ErrorMessage",
        "logGroupName.$": "$.error_parsing.cause_parsed.LogGroupName",
        "startedOn.$": "$.error_parsing.cause_parsed.StartedOn",
        "completedOn.$": "$.error_parsing.cause_parsed.CompletedOn",
        "error_type.$": "$.error_parsing.error_type",
        "batch_name.$": "$.config_yaml.prn_file_name",
        "ejecucion_id.$": "$$.Execution.Name",
        "s3_log_path.$": "States.JsonMerge(States.StringToJson('{\"bucket\":\"\",\"key\":\"\"}'), $.s3_log_result, false)"
      },
      "ResultPath": "$.error_report",
      "Next": "EnviarEventoErrorEventBridge"
    },
    EnviarEventoErrorEventBridge = {
      "Type": "Task",
      "Resource": "arn:aws:states:::events:putEvents",
      "Parameters": {
        "Entries": [
          {
            "Detail": {
              "status": "FAILED",
              "proceso": "PESVNO",
              "ejecucion_id.$": "$.error_report.ejecucion_id",
              "jobRunId.$": "$.error_report.jobRunId",
              "jobName.$": "$.error_report.jobName",
              "errorMessage.$": "$.error_report.errorMessage",
              "error_type.$": "$.error_report.error_type",
              "batch_name.$": "$.error_report.batch_name",
              "logGroupName.$": "$.error_report.logGroupName",
              "startedOn.$": "$.error_report.startedOn",
              "completedOn.$": "$.error_report.completedOn",
              "s3_log_path.$": "$.error_report.s3_log_path",
              "config_yaml.$": "$.config_yaml",
              "metadata.$": "$.metadata"
            },
            "DetailType": "ProcesoPESVNOCoordinado",
            "Source": "mi.aplicacion.pesvno",
            "EventBusName": "default"
          }
        ]
      },
      "Next": "NotificacionDeFalloEnviada"
    },
    NotificacionDeFalloEnviada = {
      "Type": "Fail",
      "Comment": "Marca la ejecución como fallida después de enviar la notificación.",
      "Error": "JobFailed",
      "Cause": "States.Format('El job de Glue falló. Log disponible en s3://{}/{}', $.error_report.s3_log_path.bucket, $.error_report.s3_log_path.key)"
    },
    ErrorConfiguracionInvalida = {
      Type  = "Fail"
      Error = "ConfigurationError"
      Cause = "La configuración YAML proporcionada es inválida o incompleta"
    }
  }
})


  depends_on = [
    aws_glue_job.glue_pesvno
  ]
}

 