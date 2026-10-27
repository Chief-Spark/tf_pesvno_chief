variable "region" {
  type        = string
  description = "region aws"
}

variable "assume_role_arn" {
  type        = string
  description = "Assume Role ARN"
  default     = ""
}

variable "assume_role_external_id" {
  type        = string
  description = "Assume Role external id"
  default     = ""
}

variable "required_tags" {
  description = "A map of tags to add to all resources"
  type        = map(string)
}

variable "env" {
  type        = string
  description = "environment"
}

variable "stage" {
  description = "stage"
  type        = string
  validation {
    condition     = var.stage == "sbx" || var.stage == "dev" || var.stage == "qa" || var.stage == "stg" ||  var.stage == "demo" ||  var.stage == "uat" || var.stage == "pro"
    error_message = "The value for stage var is invalid may be sandbox,dev,qa or pro"
  }
}

variable "bucket_name" {
  description = "Nombre del bucket S3 (sin prefijos de ambiente)"
  type        = string
  default     = "input-pesvno"
}

variable "role_default" {
  type        = string
  description = "el rol que se usa para acceder a aws por ambiente"
}

variable "default_settings_glue" {
  description = "Default configuration settings for AWS Glue jobs"
  type = object({
    glue_version      = string
    number_of_workers = string
    timeout          = string
    worker_type      = string
  })
}


variable "id_cargue_de_bases" {
  description = "Nombre del bucket S3 para el cargue de bases"
  type        = string
}

variable "enable_batch_role_kms" {
  description = "Habilitar permisos del batch role en la KMS key (solo si existe el rol)"
  type        = bool
  default     = true
}

variable "env_fw_dynbtch" {
  description = "Nombre del stage/entorno que usa el repositorio tf_fw_dynbtch (puede diferir del stage local, ej: prd vs pro)"
  type        = string
}

variable "env_prcss_batch" {
  description = "Nombre del stage/entorno que usa el repositorio tf_prcss_batch (puede diferir del stage local, ej: stg vs uat en demo). Se usa para formar los ARN de Kinesis/Rol CWL de Splunk."
  type        = string
}

# Variables input_bucket, output_redshift y output_onpremises movidas a variables_local.tf
# para construcción dinámica con interpolaciones

// Variable eliminada: ahora se usa local.output_redshift para el bucket dinámico


# =====================================================
# Variables de conexión a Redshift (fuente oficial edf_views)
# =====================================================

variable "redshift_host" {
  type        = string
  description = "Host del cluster Redshift (ej: eec-aws-us-eits-procesosbatch-dev-redshift.cee2enck00mg.us-east-1.redshift.amazonaws.com)"
}

variable "redshift_cluster_id" {
  type        = string
  description = "Identificador del cluster Redshift (ej: eec-aws-us-eits-procesosbatch-dev-redshift)"
}

variable "redshift_database" {
  type        = string
  description = "Nombre de la base de datos en Redshift"
  default     = "dba_batch"
}

variable "redshift_user" {
  type        = string
  description = "Usuario de base de datos Redshift para consultas (user_sp)"
  default     = "user_sp"
}

variable "redshift_secret_arn" {
  type        = string
  description = "ARN del secret en Secrets Manager con las credenciales (username/password) del usuario de Redshift"
}

variable "redshift_glue_connection_name" {
  type        = string
  description = "Nombre de la Glue Connection de tipo NETWORK para acceso VPC a Redshift"
  default     = "VPC_Network_Connection_Redshift"
}

# =====================================================
# Networking — Glue VPC Connection a Redshift
# =====================================================

variable "glue_connection_subnet_id" {
  type        = string
  description = "Subnet ID donde el Glue Job crea la ENI para acceder a Redshift (debe estar en la misma VPC del cluster)"
}

variable "glue_connection_security_group_id" {
  type        = string
  description = "Security Group del Glue Job. El SG del cluster Redshift debe permitir inbound TCP 5440 desde este SG."
}

variable "glue_connection_availability_zone" {
  type        = string
  description = "Availability Zone de la subnet de la Glue Connection"
}