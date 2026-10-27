# ============================================================================
# CloudWatch Logs → Kinesis (Splunk/Cribl) - Subscription Filters
# ============================================================================
# Envía los logs de TODOS los recursos de tf_pesvno al Kinesis Data Stream
# que ya existe en tf_prcss_batch (misma cuenta AWS).
#
# KINESIS EXISTENTE (creado por tf_prcss_batch via módulo LOGS_TO_SPLUNK):
#   Stream: arn:aws:kinesis:us-east-1:{account_id}:stream/splatam-kinesis-prcs-espec-batch-splunk
#   Rol CWL: arn:aws:iam::{account_id}:role/BURoleFor-{stage}-{account_id}-prcs-batch-CWLSubscriptionFilter
#
# IMPORTANTE: El repo tf_prcss_batch DEBE estar desplegado PRIMERO para que
# el Kinesis y el rol CWL existan antes de crear estos subscription filters.
# ============================================================================

# --- Variable de control ---

variable "splunk_logs_enabled" {
  type        = bool
  description = "Cuando es true, crea subscription filters para enviar logs de todos los recursos al Kinesis de tf_prcss_batch."
  default     = false
}

# --- Data source para validar que el Kinesis existe (fail-fast en plan) ---

data "aws_kinesis_stream" "splunk" {
  count = var.splunk_logs_enabled ? 1 : 0
  name  = "splatam-kinesis-prcs-espec-batch-splunk"
}

# --- Locals con ARNs del Kinesis existente en tf_prcss_batch ---

locals {
  # ARN del Kinesis Data Stream (validado via data source)
  splunk_kinesis_stream_arn = var.splunk_logs_enabled ? data.aws_kinesis_stream.splunk[0].arn : ""

  # ARN del rol CWL Subscription Filter creado por tf_prcss_batch
  # El módulo LOGS_TO_SPLUNK crea el rol con: BURoleFor-{env}-{account_id}-{name_role[2]}
  # En tf_prcss_batch, name_role[2] = "prcs-batch-CWLSubscriptionFilter"
  # Se usa var.env_prcss_batch para que coincida con el stage real de tf_prcss_batch
  # (ej: demo → tf_pesvno stage=uat, pero tf_prcss_batch stage=stg)
  splunk_cwl_role_arn = "arn:aws:iam::${local.account_id}:role/BURoleFor-${var.env_prcss_batch}-${local.account_id}-prcs-batch-CWLSubscriptionFilter"
}

# ============================================================================
# GLUE JOB: pesvno
# Log group: pesvno-cloud-pesvno/output (con --custom-logGroup-prefix)
# ============================================================================

resource "aws_cloudwatch_log_group" "glue_pesvno_splunk" {
  name              = "${local.default_name_resources}-pesvno/output"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_subscription_filter" "glue_pesvno_to_splunk" {
  count           = var.splunk_logs_enabled ? 1 : 0
  name            = "splunk-${local.default_name_resources}-glue-pesvno"
  role_arn        = local.splunk_cwl_role_arn
  log_group_name  = aws_cloudwatch_log_group.glue_pesvno_splunk.name
  filter_pattern  = ""
  destination_arn = local.splunk_kinesis_stream_arn
}

# ============================================================================
# LAMBDA: pesvno_processor
# Log group: pesvno-cloud-lambda-processor
# ============================================================================

resource "aws_cloudwatch_log_group" "lambda_processor_splunk" {
  name              = "${local.default_name_resources}-lambda-processor"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_subscription_filter" "lambda_processor_to_splunk" {
  count           = var.splunk_logs_enabled ? 1 : 0
  name            = "splunk-${local.default_name_resources}-lambda-processor"
  role_arn        = local.splunk_cwl_role_arn
  log_group_name  = aws_cloudwatch_log_group.lambda_processor_splunk.name
  filter_pattern  = ""
  destination_arn = local.splunk_kinesis_stream_arn
}

# ============================================================================
# LAMBDA: download_glue_logs
# Log group: pesvno-cloud-lambda-download-logs
# ============================================================================

resource "aws_cloudwatch_log_group" "lambda_download_logs_splunk" {
  name              = "${local.default_name_resources}-lambda-download-logs"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_subscription_filter" "lambda_download_logs_to_splunk" {
  count           = var.splunk_logs_enabled ? 1 : 0
  name            = "splunk-${local.default_name_resources}-lambda-download-logs"
  role_arn        = local.splunk_cwl_role_arn
  log_group_name  = aws_cloudwatch_log_group.lambda_download_logs_splunk.name
  filter_pattern  = ""
  destination_arn = local.splunk_kinesis_stream_arn
}

# ============================================================================
# STEP FUNCTION: sf_pesvno_process
# Log group: pesvno-cloud-sfn-pesvno
# ============================================================================

resource "aws_cloudwatch_log_group" "sfn_pesvno_splunk" {
  name              = "${local.default_name_resources}-sfn-pesvno"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_subscription_filter" "sfn_pesvno_to_splunk" {
  count           = var.splunk_logs_enabled ? 1 : 0
  name            = "splunk-${local.default_name_resources}-sfn-pesvno"
  role_arn        = local.splunk_cwl_role_arn
  log_group_name  = aws_cloudwatch_log_group.sfn_pesvno_splunk.name
  filter_pattern  = ""
  destination_arn = local.splunk_kinesis_stream_arn
}
