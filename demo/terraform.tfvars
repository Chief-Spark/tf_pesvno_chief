# VARIABLES AMBIENTE DEMO - PESVNO
stage          = "uat"
region         = "us-east-1"
env            = "stg"
# Tags requeridos para ambiente DEMO (UAT)
required_tags = {
    Environment = "uat"
    AppID       = "26099"
    CostString  = "1850.CO.135.601023"
    Project     = "PESVNO"
    Purpose     = "Input bucket with S3-SQS notification for PESVNO process"
}

# Role de acceso para ambiente DEMO
role_default = "BURoleForDTOUat-NonProd"

# Variables input_bucket, output_redshift y output_onpremises ahora se construyen dinámicamente en variables_local.tf

default_settings_glue = {
  glue_version = "5.0"
  number_of_workers = "3"
  timeout = "120"
  worker_type = "G.4X"
}
# Configuración de infraestructura para DEMO
# Ambiente para demostraciones y pruebas de usuario

#cuentas cargue de bases: 
id_cargue_de_bases = "160125787052"

# Nombre del entorno en tf_fw_dynbtch para construcción de recursos
env_fw_dynbtch = "stg" # demo

# Splunk/Cribl: subscription filters hacia Kinesis de tf_prcss_batch (ver trunk/splunk_logs.tf)
splunk_logs_enabled = true

# Stage que usa tf_prcss_batch en este ambiente (debe coincidir con su stage para formar ARN de Kinesis/Rol CWL)
# IMPORTANTE: En demo, tf_prcss_batch usa stage="stg" (no "uat" como tf_pesvno)
env_prcss_batch = "stg"

# =====================================================
# Redshift - Fuente oficial (edf_views)
# =====================================================
redshift_host                 = "eec-aws-us-eits-procesosbatch-stg-redshift.cisjjoyk3j5j.us-east-1.redshift.amazonaws.com"
redshift_cluster_id           = "eec-aws-us-eits-procesosbatch-stg-redshift"
redshift_database             = "dba_batch"
redshift_user                 = "user_sp"
# TODO: reemplazar con el ARN del secret de credenciales Redshift para UAT/STG
redshift_secret_arn           = "arn:aws:secretsmanager:us-east-1:311141534073:secret:eec-aws-us-eits-procesosbatch-stg-redshift-user-redshift-pesvno-stg20260910222236086000000001-57UUzi"
redshift_glue_connection_name = "VPC_Network_Connection_Redshift"

# Networking — Glue VPC Connection a Redshift
glue_connection_subnet_id              = "subnet-0ae926d3264fe842b"
glue_connection_security_group_id      = "sg-0adf1b40586fbeb23"
glue_connection_availability_zone      = "us-east-1a"