# =====================================
# PESVNO Infrastructure Outputs
# =====================================

# S3 Bucket Information
output "bucket_name" {
  description = "Nombre del bucket S3 creado"
  value       = aws_s3_bucket.pesvno_bucket.bucket
}

output "bucket_arn" {
  description = "ARN del bucket S3 creado"
  value       = aws_s3_bucket.pesvno_bucket.arn
}

output "bucket_domain_name" {
  description = "Domain name del bucket S3"
  value       = aws_s3_bucket.pesvno_bucket.bucket_domain_name
}

output "bucket_region" {
  description = "Región del bucket S3"  
  value       = aws_s3_bucket.pesvno_bucket.region
}

# Deployment Summary
output "deployment_summary" {
  description = "Resumen del despliegue PESVNO"
  value = {
    environment     = var.stage
    account_id     = local.account_id
    region         = var.region
    bucket_name    = aws_s3_bucket.pesvno_bucket.bucket
    lambda_name    = aws_lambda_function.pesvno_processor.function_name
    lambda_download_logs = aws_lambda_function.download_glue_logs.function_name
    step_function  = aws_sfn_state_machine.sf_pesvno_process.name
    glue_job      = aws_glue_job.glue_pesvno.name
    kms_key       = aws_kms_key.pesvno_key.arn
    # Buckets dinámicos (resueltos automáticamente)
    input_bucket   = local.input_bucket
    output_redshift = local.output_redshift
    output_onpremises = local.output_onpremises
    deployed_at   = timestamp()
  }
}

# Outputs de KMS
output "kms_key_id" {
  description = "ID de la clave KMS para encriptación"
  value       = aws_kms_key.pesvno_key.key_id
}

output "kms_key_arn" {
  description = "ARN de la clave KMS para encriptación"
  value       = aws_kms_key.pesvno_key.arn
}

output "kms_alias_name" {
  description = "Nombre del alias de la clave KMS"
  value       = aws_kms_alias.pesvno_key_alias.name
}

# Outputs de SQS
output "sqs_queue_url" {
  description = "URL de la cola SQS principal"
  value       = aws_sqs_queue.pesvno_queue.url
}

output "sqs_queue_arn" {
  description = "ARN de la cola SQS principal"
  value       = aws_sqs_queue.pesvno_queue.arn
}

output "sqs_dlq_url" {
  description = "URL de la Dead Letter Queue"
  value       = aws_sqs_queue.pesvno_dlq.url
}

output "sqs_dlq_arn" {
  description = "ARN de la Dead Letter Queue"
  value       = aws_sqs_queue.pesvno_dlq.arn
}

# Output del lifecycle configuration
output "lifecycle_configuration_bucket" {
  description = "Bucket asociado a la configuración de ciclo de vida"
  value       = aws_s3_bucket_lifecycle_configuration.pesvno_bucket_lifecycle.bucket
}

# DEBUG: Outputs para verificar ARNs de roles
output "debug_environment_role_arn" {
  description = "DEBUG: ARN del rol de ambiente que se usa en KMS policy"
  value       = local.environment_role_arn
}

output "debug_batch_role_arn" {
  description = "DEBUG: ARN del rol de batch que se usa en KMS policy"
  value       = local.batch_role_arn
}

output "debug_account_id" {
  description = "DEBUG: Account ID detectado"
  value       = local.account_id
}