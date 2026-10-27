# VARIABLES AMBIENTE QA - PESVNO
stage          = "qa"
region         = "us-east-1"
env            = "qa"

# Tags requeridos para ambiente QA
required_tags = {
    Environment = "qa"
    AppID       = "26099"
    CostString  = "1850.CO.135.601023"
    Project     = "PESVNO"
    Purpose     = "Input bucket with S3-SQS notification for PESVNO process"
}

# Role de acceso para ambiente QA
role_default = "BURoleForDTOQa-NonProd"

# Variables input_bucket, output_redshift y output_onpremises ahora se construyen dinámicamente en variables_local.tf

default_settings_glue = {
  glue_version = "5.0"
  number_of_workers = "3"
  timeout = "120"
  worker_type = "G.4X"
}


# Configuración de infraestructura para QA
# Se pueden ajustar según las necesidades del ambiente de testing

#cuentas cargue de bases: 
id_cargue_de_bases = "738272403214"

# Control de roles opcionales en KMS policy
enable_batch_role_kms = true  # El batch role no existe en QA

# Nombre del entorno en tf_fw_dynbtch para construcción de recursos
env_fw_dynbtch = "qa"

# Splunk/Cribl: subscription filters hacia Kinesis de tf_prcss_batch (ver trunk/splunk_logs.tf)
splunk_logs_enabled = true

# Stage que usa tf_prcss_batch en este ambiente (debe coincidir con su stage para formar ARN de Kinesis/Rol CWL)
env_prcss_batch = "qa"

# =====================================================
# Redshift - Fuente oficial (edf_views)
# =====================================================
redshift_host                 = "eec-aws-us-eits-procesosbatch-qa-redshift.cwefyckvccgy.us-east-1.redshift.amazonaws.com"
redshift_cluster_id           = "eec-aws-us-eits-procesosbatch-qa-redshift"
redshift_database             = "dba_batch"
redshift_user                 = "user_sp"
# TODO: reemplazar con el ARN del secret de credenciales Redshift para QA
redshift_secret_arn           = "arn:aws:secretsmanager:us-east-1:124355679563:secret:eec-aws-us-eits-procesosbatch-qa-redshift-user-redshift-pesvno-qa20260910210251437300000001-FbJve0"
redshift_glue_connection_name = "VPC_Network_Connection_Redshift"

# Networking — Glue VPC Connection a Redshift
glue_connection_subnet_id              = "subnet-06323c31c721da00e"
glue_connection_security_group_id      = "sg-0efec19647bd12515"
glue_connection_availability_zone      = "us-east-1c"