# informacion de la cuenta
data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  default_name_resources = "pesvno-cloud"
  
  # Prefijo para lambdas externas (send-email, error-log-file-generation) administradas por Dynamic Batch
  # Dynamic Batch usa: stg (demo), dev, qa, prd (NO "pro")
  # Mapeo de env a nomenclatura de Dynamic Batch
  # IMPORTANTE: Definido primero para usarse en otras variables locales
  lambda_prefix_map = {
    "stg" = "stg"
    "dev" = "dev"
    "qa"  = "qa"
    "pro" = "prd"  # Producción usa "prd" en Dynamic Batch, no "pro"
  }
  lambda_prefix = local.lambda_prefix_map[var.env]
  

pesvno_prefix_map = {
    "stg" = "stg"
    "dev" = "dev"
    "qa"  = "qa"
    "pro" = "pro"  # Producción usa "pro" en  Batch, no "prd"
  }
pesvno_prefix = local.pesvno_prefix_map[var.env]

  # Roles para politicas S3
  # El rol batch usa lambda_prefix (stg, dev, qa, prd) porque es administrado por Dynamic Batch
  # NO usa var.stage porque en demo: stage=uat pero el rol real es BURoleFor-stg-...-procs-espec-batch
  batch_role_arn = "arn:aws:iam::${local.account_id}:role/BURoleFor-${local.pesvno_prefix}-${local.account_id}-procs-espec-batch"
  environment_role_arn = "arn:aws:iam::${local.account_id}:role/${var.role_default}"


  # Construccion dinamica de buckets
  # NOTA: 
  # - Buckets de Dynamic Batch usan lambda_prefix (prd en prod)
  # - Bucket de Cargue de Bases usa var.env (pro en prod) - sistema diferente
  input_bucket = "${var.stage}-${local.account_id}-${var.bucket_name}-bucket"
  output_redshift = "${local.lambda_prefix}-${local.account_id}-fwdynbtch-dynamic-batch-source-repository"
  output_onpremises = "${var.env}-${var.id_cargue_de_bases}-carguedebases-procs-espec-batch"  # Cargue de Bases usa 'pro', no 'prd'
  result_repository_bucket = "${local.lambda_prefix}-${local.account_id}-fwdynbtch-dynamic-batch-result-repository"

  # Bucket destino de access logs (result-repository en tf_fw_dynbtch, lifecycle 7d)
  s3_access_logs_bucket_name = local.result_repository_bucket

  # Mapa de cuentas AWS de Dynamic Batch por ambiente
  # Las lambdas externas están en cuentas de Dynamic Batch
  dynamic_batch_account_map = {
    "651706752126" = "651706752126"   # dev
    "124355679563" = "124355679563"  # qa
    "311141534073" = "311141534073"  # demo
    "324037294240" = "324037294240"  # prd: Lambda en cuenta de Dynamic Batch
     
  }
  dynamic_batch_account_id = lookup(local.dynamic_batch_account_map, local.account_id, local.account_id)

  # Mapa de llaves KMS del bucket carguedebases-output por cuenta
  # Estas llaves son administradas por el equipo de CargaDeBases
  kms_carguedebases_map = {
    "470008751117" = "arn:aws:kms:${var.region}:470008751117:key/96737a6c-5ba6-4981-9118-b5e4af704ba3"  # dev
    "738272403214" = "arn:aws:kms:${var.region}:738272403214:key/eb696d7c-202c-407b-9a50-3b8a140b3110"  # qa
    "160125787052" = "arn:aws:kms:${var.region}:160125787052:key/a6399eb5-92d6-4a3a-a9e5-efc526ee1b4e"  # demo
    "031471595101" = "arn:aws:kms:${var.region}:031471595101:key/7fb8e318-0eac-4c8e-a26f-12fedd8f532e"  # prd
  }

  # Seleccion dinamica de la llave KMS segun el ambiente
  kms_carguedebases_key = local.kms_carguedebases_map[var.id_cargue_de_bases]

  # Llave KMS del bucket source-repository de Dynamic Batch (tf_fw_dynbtch) por cuenta
  kms_dynamic_batch_map = {
    "651706752126" = "arn:aws:kms:${var.region}:651706752126:key/dee5ccd5-960d-4373-aa9a-cef3d03b99ad" # dev
    "124355679563" = "arn:aws:kms:${var.region}:124355679563:key/0722e19e-d697-456e-b663-6c1bfd835f0a" # qa
    "311141534073" = "arn:aws:kms:${var.region}:311141534073:key/eccc2a3a-6c2f-4dd6-8852-338418b0a95e" # demo
    "324037294240" = "arn:aws:kms:${var.region}:324037294240:key/ec83a59a-c1ce-4e7e-bb13-febb281e8a3b" # prd
  }

  kms_dynamic_batch_key = lookup(local.kms_dynamic_batch_map, local.account_id, local.kms_dynamic_batch_map["651706752126"])

  # KMS por defecto para buckets destino dinámicos (campo DESTINO en business table)
  kms_destino_default_key = local.kms_dynamic_batch_key

  # Nombre de la cola SQS de send_email_log formado dinámicamente (mismo patrón que tf_prcss_batch)
  sqs_send_email_log_queue_name = "us-procesosbatch-${var.env_fw_dynbtch}-send_email_log-sqs-queue"

  # ARN de la cola SQS de send_email_log construido dinámicamente
  sqs_send_email_log_arn = "arn:aws:sqs:${var.region}:${local.account_id}:${local.sqs_send_email_log_queue_name}"
  
  # ARN de la Lambda de send_email_log (mismo patrón de ambiente y cuenta)
  send_email_log_lambda_arn = "arn:aws:lambda:${var.region}:${local.account_id}:function:${var.env_fw_dynbtch}-${local.account_id}-fwdynbtch-send-email-log-lambda"
}
