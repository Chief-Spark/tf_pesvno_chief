# VARIABLES AMBIENTE DEVELOP - PESVNO
stage          = "dev"
region         = "us-east-1"
env            = "dev"


# Tags requeridos para ambiente DEVELOP
required_tags = {
    Environment = "dev"
    AppID       = "26099"
    CostString  = "1850.CO.135.601023"
    Project     = "PESVNO"
    Purpose     = "Input bucket with S3-SQS notification for PESVNO process"
}

# Role de acceso para ambiente DEVELOP
role_default = "BURoleForDTODev-NonProd"

# Variables input_bucket, output_redshift y output_onpremises ahora se construyen dinámicamente en variables_local.tf

default_settings_glue = {
  glue_version = "5.0"
  number_of_workers = "3"
  timeout = "120"
  worker_type = "G.4X"
}

# Configuración de infraestructura para DEVELOP
# Ambiente de desarrollo con configuraciones optimizadas para testing

#cuentas cargue de bases: 
id_cargue_de_bases = "470008751117"

# Control de roles opcionales en KMS policy
enable_batch_role_kms = true  # El batch role existe en dev

# Nombre del entorno en tf_fw_dynbtch para construcción de recursos
env_fw_dynbtch = "dev"

# Splunk/Cribl: subscription filters hacia Kinesis de tf_prcss_batch (ver trunk/splunk_logs.tf)
splunk_logs_enabled = true

# Stage que usa tf_prcss_batch en este ambiente (debe coincidir con su stage para formar ARN de Kinesis/Rol CWL)
env_prcss_batch = "dev"

# =====================================================
# Redshift - Fuente oficial (edf_views)
# =====================================================
redshift_host                 = "eec-aws-us-eits-procesosbatch-dev-redshift.cee2enck00mg.us-east-1.redshift.amazonaws.com"
redshift_cluster_id           = "eec-aws-us-eits-procesosbatch-dev-redshift"
redshift_database             = "dba_batch"
redshift_user                 = "user_sp"
redshift_secret_arn           = "arn:aws:secretsmanager:us-east-1:651706752126:secret:eec-aws-us-eits-procesosbatch-dev-redshift-user-redshift-pesvno-dev20260904225435328200000001-zFUKBZ"
redshift_glue_connection_name = "VPC_Network_Connection_Redshift"

# Networking — Glue VPC Connection a Redshift
glue_connection_subnet_id              = "subnet-03be60e58c21d9aa7"
glue_connection_security_group_id      = "sg-0c24fcac12c64f488"
glue_connection_availability_zone      = "us-east-1a"