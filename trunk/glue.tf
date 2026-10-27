resource "aws_cloudwatch_log_group" "logsGlues" {
  name              = "${local.default_name_resources}-glue"
  retention_in_days = 14
}

resource "aws_s3_object" "glue_pesvno_object" {
  bucket = join("-", [var.stage, "${local.account_id}","input", "pesvno", "bucket"])
  key    = join("", [join("_", [var.stage, "sla_co_batch/"]),"glue/pesvno.py"])
  source = "src/glue/pesvno.py"
  etag   = filemd5("src/glue/pesvno.py")
  
  depends_on = [aws_s3_bucket.pesvno_bucket]
}

resource "aws_s3_object" "product_rules_csv_object" {
  bucket = join("-", [var.stage, "${local.account_id}","input", "pesvno", "bucket"])
  key    = join("", [join("_", [var.stage, "sla_co_batch/"]), "product_file_rules.csv"])
  source = "src/lambda/process-prn-files/product_file_rules.csv"
  etag   = filemd5("src/lambda/process-prn-files/product_file_rules.csv")

  depends_on = [aws_s3_bucket.pesvno_bucket]
}

resource "aws_glue_job" "glue_pesvno" {
  name              = join("-", [var.stage, "${local.account_id}","${local.default_name_resources}","glue", "pesvno" ])
  role_arn          = local.batch_role_arn
  glue_version      = var.default_settings_glue.glue_version
  worker_type       = var.default_settings_glue.worker_type
  number_of_workers = var.default_settings_glue.number_of_workers
  timeout = var.default_settings_glue.timeout

  # Asocia la Glue Connection de red VPC para que el job cree la ENI dentro de la
  # VPC del cluster Redshift. Sin esto, el job corre en la red pública gestionada
  # por Glue y no puede alcanzar el Redshift privado (error: "The connection attempt failed").
  connections = [aws_glue_connection.pesvno_vpc_redshift.name]

  execution_property {
    max_concurrent_runs = 30
  }

  command {
    script_location = "s3://${aws_s3_object.glue_pesvno_object.bucket}/${aws_s3_object.glue_pesvno_object.key}"
    python_version  = "3"
  }


  default_arguments = {
    "--continuous-log-logGroup"          = "prcs-espec-batch-glue"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = "true"
    "--custom-logGroup-prefix"           = aws_cloudwatch_log_group.glue_pesvno_splunk.name
    
    # Configuración para enviar logs a S3
    "--enable-job-insights"              = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--TempDir"                          = "s3://${local.lambda_prefix}-${local.account_id}-fwdynbtch-error-files/logs/"
    "--enable-s3-parquet-optimized-committer" = "true"
    "--enable-rename-algorithm-v2"       = "true"
   
    # Configuración de infraestructura (NO se sobrescribe desde Lambda)
    "--imputbucket" = "${local.input_bucket}"
    "--additional_bucket" = "${local.lambda_prefix}-${local.account_id}-fwdynbtch-dynamic-batch-result-repository" 
    "--OUTPUTBUCKET_REDSHIFT" = "${local.lambda_prefix}-${local.account_id}-fwdynbtch-dynamic-batch-source-repository"
    "--OUTPUTBUCKET_ONPREMISES" = "${local.output_onpremises}"
    "--db_edf_relationalize" = "${var.env}-${local.account_id}-batch-ascend-link-relat"
    "--redshift_jdbc_url"    = "jdbc:redshift://${var.redshift_host}:5440/${var.redshift_database}"
    "--redshift_cluster_id"  = var.redshift_cluster_id
    "--redshift_database"    = var.redshift_database
    "--redshift_user"        = var.redshift_user
    "--redshift_schema"      = "edf_views"
    "--redshift_secret_arn"  = var.redshift_secret_arn
    "--kms_carguedebases_key"  = "${local.kms_carguedebases_key}"
    "--kms_dynamic_batch_key"  = "${local.kms_dynamic_batch_key}"
    "--kms_destino_default_key" = "${local.kms_destino_default_key}"
    "--business_table"         = "eec-aws-us-eits-procesosbatch-${var.env}-business"
    "--object_tracking_table"  = "eec-aws-us-eits-procesosbatch-${var.env}-object-tracking"
    "--nombre_producto"        = "PESVNO"
    "--redshift_jdbc_url"    = "jdbc:redshift://${var.redshift_host}:5440/${var.redshift_database}"
    "--redshift_cluster_id"  = var.redshift_cluster_id
    "--redshift_database"    = var.redshift_database
    "--redshift_user"        = var.redshift_user
    "--redshift_schema"      = "edf_views"
    "--redshift_secret_arn"  = var.redshift_secret_arn
    # CONFIGURACIÓN SPARK UI
    "--enable-spark-ui" = "true"
    "--spark-event-logs-path" = "s3://${local.input_bucket}/spark-logs/"

    # Driver JDBC de Redshift
    # "--extra-jars" = "s3://${local.input_bucket}/${var.stage}_sla_co_batch/jars/redshift-jdbc42-2.1.0.12.jar"

    # =====================================================
    # CONFIGURACIONES SPARK CRÍTICAS (No se modifican en runtime)
    # =====================================================
    
    # ⚠️ CRÍTICO: Driver MaxResultSize - Evita error al recolectar resultados grandes
    # Error sin esto: "Total size of serialized results bigger than maxResultSize"
    # Requerido cuando small input + large catalog join = large output
    "--conf spark.driver.maxResultSize" = "4g"
    
    # ===================== OPTIMIZACIONES DE ESCRITURA S3 - CRÍTICAS =====================
    
    # S3A Fast Upload - CRÍTICO para archivos grandes
    "--conf spark.hadoop.fs.s3a.fast.upload" = "true"
    "--conf spark.hadoop.fs.s3a.fast.upload.buffer" = "disk"
    "--conf spark.hadoop.fs.s3a.fast.upload.active.blocks" = "12"
    
    # Multipart upload optimizado para mejor rendimiento
    "--conf spark.hadoop.fs.s3a.multipart.size" = "67108864"       # 64MB
    "--conf spark.hadoop.fs.s3a.multipart.threshold" = "134217728" # 128MB
    
    # Conexiones S3 optimizadas para escritura paralela
    "--conf spark.hadoop.fs.s3a.connection.maximum" = "200"
    "--conf spark.hadoop.fs.s3a.threads.max" = "20"
    "--conf spark.hadoop.fs.s3a.connection.timeout" = "200000"
    
    # Buffers de red optimizados para S3
    "--conf spark.hadoop.fs.s3a.socket.recv.buffer" = "65536"
    "--conf spark.hadoop.fs.s3a.socket.send.buffer" = "65536"
    
    # Block size optimizado para escritura
    "--conf spark.hadoop.fs.s3a.block.size" = "134217728"  # 128MB
    
    # Retry policy para robustez en escritura
    "--conf spark.hadoop.fs.s3a.retry.limit" = "10"
    "--conf spark.hadoop.fs.s3a.retry.interval" = "1000"
    
    # Magic committer para mejor rendimiento en S3
    "--conf spark.hadoop.fs.s3a.committer.name" = "magic"
    "--conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version" = "2"
    "--conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored" = "true"
    
    # Buffer optimizations para escritura
    "--conf spark.hadoop.io.file.buffer.size" = "131072"  # 128KB buffer
    "--conf spark.hadoop.dfs.stream-buffer-size" = "131072"
    
    # Optimizaciones específicas para escritura de archivos .txt
    "--conf spark.sql.sources.commitProtocolClass" = "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol"
    
    # =====================================================
    # NOTA: Las configuraciones dinámicas de Spark (AQE, broadcast, partitions, etc.)
    # se gestionan en el código Python (pesvno.py) según el tamaño del archivo.
    # Esto permite optimizaciones específicas por categoría: PEQUEÑO, MEDIANO, GRANDE, MUY_GRANDE
    # =====================================================
    
    # Los siguientes argumentos se pasan dinámicamente desde Lambda/Step Function:
    # --nombreval, --nombreinc, --params_step_function
  }

  
  depends_on = [
    aws_s3_object.glue_pesvno_object,
    aws_cloudwatch_log_group.logsGlues,
    aws_glue_connection.pesvno_vpc_redshift
  ]
}