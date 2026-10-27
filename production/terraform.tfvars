# VARIABLES AMBIENTE PRODUCTION - PESVNO
stage          = "pro"
region         = "us-east-1"
env            = "pro"

# Tags requeridos para ambiente PRODUCTION
required_tags = {
    Environment = "pro"
    AppID       = "26099"
    CostString  = "1850.CO.135.601023"
    Project     = "PESVNO"
    Purpose     = "Input bucket with S3-SQS notification for PESVNO process"
}

# Role de acceso para ambiente PRODUCTION
role_default = "BURoleForDTOProd"

# Variables input_bucket, output_redshift y output_onpremises ahora se construyen dinámicamente en variables_local.tf

default_settings_glue = {
  glue_version = "5.0"
  number_of_workers = "3"
  timeout = "120"
  worker_type = "G.4X"
}

# Configuración de infraestructura para PRODUCTION
# Ambiente de producción con configuraciones optimizadas y de alta disponibilidad

#cuentas cargue de bases: 
id_cargue_de_bases = "031471595101"

# Nombre del entorno en tf_fw_dynbtch para construcción de recursos
env_fw_dynbtch = "prd"

# Control de roles opcionales en KMS policy
enable_batch_role_kms = true  # Evita principal inválido en KMS si el rol externo no existe

# Splunk/Cribl: subscription filters hacia Kinesis de tf_prcss_batch (ver trunk/splunk_logs.tf)
splunk_logs_enabled = true

# Stage que usa tf_prcss_batch en este ambiente (debe coincidir con su stage para formar ARN de Kinesis/Rol CWL)
env_prcss_batch = "pro"

# =====================================================
# Redshift - Fuente oficial (edf_views)
# =====================================================
redshift_host                 = "eec-aws-us-eits-procesosbatch-pro-redshift.crebjnbqba6u.us-east-1.redshift.amazonaws.com"
redshift_cluster_id           = "eec-aws-us-eits-procesosbatch-pro-redshift"
redshift_database             = "dba_batch"
redshift_user                 = "user_sp"
# TODO: reemplazar con el ARN del secret de credenciales Redshift para PRODUCCIÓN
redshift_secret_arn           = "arn:aws:secretsmanager:us-east-1:324037294240:secret:eec-aws-us-eits-procesosbatch-pro-redshift-user-redshift-pesvno-pro20260912024523140300000001-gbS52a"
redshift_glue_connection_name = "VPC_Network_Connection_Redshift"

# Networking — Glue VPC Connection a Redshift
glue_connection_subnet_id              = "subnet-06c64324b5ea59e38"
glue_connection_security_group_id      = "sg-0be85434fa1858488"
glue_connection_availability_zone      = "us-east-1c"