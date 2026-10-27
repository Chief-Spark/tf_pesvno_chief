import json
import boto3
import logging
import time
import os
import csv
from io import StringIO
from datetime import datetime
from urllib.parse import unquote_plus
from botocore.exceptions import ClientError
from botocore.config import Config

# CONFIGURACIÓN BASE
logger = logging.getLogger()
logger.setLevel(logging.INFO)

_BOTO_CONFIG = Config(
    connect_timeout=3,
    read_timeout=8,
    retries={"max_attempts": 3, "mode": "standard"}
)

s3_client = boto3.client("s3", config=_BOTO_CONFIG)
stepfunctions = boto3.client("stepfunctions", config=_BOTO_CONFIG)

RULES_FILE_NAME = os.environ.get("PRODUCT_RULES_FILE", "product_file_rules.csv")
RULES_S3_URI = os.environ.get("PRODUCT_RULES_S3_URI", "")
SOURCE_REPOSITORY_BUCKET = os.environ.get("SOURCE_REPOSITORY_BUCKET", "").strip()
SQS_MAX_RECEIVE_BEFORE_STALE = int(os.environ.get("SQS_MAX_RECEIVE_BEFORE_STALE", "6"))


class StaleS3EventError(Exception):
    """Evento S3 cuyo objeto ya no existe en ningun bucket candidato (mensaje SQS obsoleto)."""


def normalize_extension(extension):
    value = (extension or "").strip()
    if not value:
        return ""
    if value.startswith("."):
        value = value[1:]
    return value.upper()


def _parse_rules_csv(csv_content):
    rules = {}

    reader = csv.DictReader(StringIO(csv_content))
    for row in reader:
        folder = (row.get("folder") or "").strip().lower()
        if not folder:
            continue

        raw_extensions = row.get("required_extensions", "")
        required_extensions = {
            normalize_extension(ext)
            for ext in raw_extensions.split("|")
            if normalize_extension(ext)
        }
        if not required_extensions:
            logger.warning(f"Regla ignorada para carpeta {folder}: sin required_extensions")
            continue

        expected_count_raw = (row.get("expected_count") or "").strip()
        expected_count = int(expected_count_raw) if expected_count_raw.isdigit() else len(required_extensions)

        rules[folder] = {
            "expected_count": expected_count,
            "required_extensions": required_extensions
        }

    return rules


def _parse_s3_uri(s3_uri):
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"URI S3 invalida: {s3_uri}")
    path = s3_uri.replace("s3://", "", 1)
    parts = path.split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"URI S3 invalida: {s3_uri}")
    return parts[0], parts[1]


def load_required_files_rules():
    rules = {}

    # Prioridad 1: cargar reglas desde S3 para poder actualizar sin redeploy.
    if RULES_S3_URI:
        try:
            bucket, key = _parse_s3_uri(RULES_S3_URI)
            response = s3_client.get_object(Bucket=bucket, Key=key)
            csv_content = response["Body"].read().decode("utf-8")
            rules = _parse_rules_csv(csv_content)
            logger.info(f"Reglas de validacion cargadas desde S3: {len(rules)} carpeta(s) [{RULES_S3_URI}]")
            return rules
        except Exception as exc:
            logger.error(f"No fue posible cargar reglas desde S3 ({RULES_S3_URI}): {str(exc)}")

    # Prioridad 2: fallback a archivo local empacado en el zip de la Lambda.
    rules_path = os.path.join(os.path.dirname(__file__), RULES_FILE_NAME)

    if not os.path.exists(rules_path):
        logger.warning(f"Archivo de reglas no encontrado: {rules_path}. Se omitira validacion avanzada por carpeta.")
        return rules

    try:
        with open(rules_path, mode="r", encoding="utf-8", newline="") as csv_file:
            rules = _parse_rules_csv(csv_file.read())
        logger.info(f"Reglas de validacion cargadas desde archivo local: {len(rules)} carpeta(s)")
    except Exception as exc:
        logger.error(f"No fue posible cargar reglas desde {rules_path}: {str(exc)}")

    return rules

def lambda_handler(event, context):
    start_ts = time.time()
    logger.info(f"[T0 +{0:.3f}s] Evento recibido: {json.dumps(event)}")
    batch_item_failures = []
    required_files_rules = load_required_files_rules()

    try:
        for record in event.get("Records", []):
            try:
                sqs_message = json.loads(record["body"])

                # Ignorar test event S3
                if sqs_message.get("Event") == "s3:TestEvent":
                    logger.info("Ignorando s3:TestEvent (mensaje de prueba de configuración S3).")
                    continue

                if "Records" not in sqs_message:
                    logger.warning("Mensaje sin 'Records' (no formato estándar S3). Consumido y descartado.")
                    continue

                for s3_record in sqs_message["Records"]:
                    receive_count = int(record.get("attributes", {}).get("ApproximateReceiveCount", "1"))
                    process_s3_event(
                        s3_record, record["messageId"], start_ts, required_files_rules, receive_count
                    )

            except StaleS3EventError as e:
                logger.warning(f"Mensaje SQS obsoleto descartado ({record['messageId']}): {str(e)}")
            except Exception as e:
                logger.error(f"Error procesando record {record['messageId']}: {str(e)}")
                batch_item_failures.append({"itemIdentifier": record["messageId"]})

        elapsed = time.time() - start_ts
        logger.info(f"[Tfinal +{elapsed:.3f}s] Lambda completada (batchItemFailures={len(batch_item_failures)})")
        return {"statusCode": 200, "batchItemFailures": batch_item_failures}

    except Exception as e:
        logger.error(f"Error general en lambda_handler: {str(e)}")
        raise e

def process_s3_event(s3_record, message_id, start_ts, required_files_rules, receive_count=1):
    bucket_name = s3_record["s3"]["bucket"]["name"]
    object_key = unquote_plus(s3_record["s3"]["object"]["key"])
    event_name = s3_record.get("eventName", "(sin eventName)")

    t_rel = time.time() - start_ts
    logger.info(f"[T+{t_rel:.3f}s] Procesando archivo: s3://{bucket_name}/{object_key}")
    logger.info(f"[T+{t_rel:.3f}s] Evento: {event_name}")

    # Validación: EXACTAMENTE una carpeta top-level + nombre.yaml
    parts = object_key.split('/')
    if len(parts) != 2:
        logger.warning(f"Archivo ignorado (profundidad distinta de 1 carpeta): {object_key}")
        return
    folder, filename = parts
    if not filename.lower().endswith('.yaml'):
        logger.warning(f"Archivo ignorado (no .yaml): {object_key}")
        return

    # pesvno: solo procesar eventos del source-repository (destino de Kiro Transfer)
    if folder.lower() == "pesvno" and SOURCE_REPOSITORY_BUCKET and bucket_name != SOURCE_REPOSITORY_BUCKET:
        logger.info(json.dumps({
            "event": "PESVNO_EVENT_IGNORED",
            "event_bucket": bucket_name,
            "expected_bucket": SOURCE_REPOSITORY_BUCKET,
            "object_key": object_key,
            "mensaje": (
                "Evento ignorado: pesvno debe procesarse desde "
                f"s3://{SOURCE_REPOSITORY_BUCKET}/pesvno/ tras Kiro Transfer"
            )
        }))
        return

    try:
        process_yaml_config(
            bucket_name, object_key, start_ts, required_files_rules, receive_count=receive_count
        )
        logger.info(f"Archivo procesado OK: {object_key}")
    except StaleS3EventError:
        raise
    except Exception as e:
        logger.error(f"Fallo procesando {object_key}: {str(e)}")
        raise e

def process_yaml_config(bucket_name, yaml_key, start_ts, required_files_rules, receive_count=1):
    logger.info(f"[T+{time.time() - start_ts:.3f}s] Iniciando procesamiento YAML: {yaml_key}")
    try:
        yaml_name = os.path.basename(yaml_key)
        base_name = os.path.splitext(yaml_name)[0]
        folder = os.path.dirname(yaml_key)
        search_buckets = get_batch_search_buckets(bucket_name, folder)

        t0 = time.time()
        config_data, yaml_bucket = get_yaml_config_multi(search_buckets, yaml_key)
        logger.info(f"[T+{time.time() - start_ts:.3f}s][dt={time.time()-t0:.3f}s] YAML parseado")
        if not config_data:
            logger.error(json.dumps({
                "event": "YAML_NOT_FOUND",
                "event_bucket": bucket_name,
                "buckets_searched": search_buckets,
                "yaml_key": yaml_key,
                "receive_count": receive_count,
                "mensaje": "YAML no encontrado en source-repository (destino Kiro Transfer)",
                "sugerencia": (
                    f"Verifica que Kiro Transfer haya copiado yaml+prn a "
                    f"s3://{SOURCE_REPOSITORY_BUCKET or search_buckets[0]}/pesvno/"
                )
            }))
            if receive_count >= SQS_MAX_RECEIVE_BEFORE_STALE:
                raise StaleS3EventError(
                    f"YAML obsoleto tras {receive_count} intentos: s3://{bucket_name}/{yaml_key}"
                )
            raise FileNotFoundError(
                f"YAML no encontrado en buckets candidatos: {search_buckets}"
            )

        if yaml_bucket != bucket_name:
            logger.info(json.dumps({
                "event": "YAML_FOUND_CROSS_BUCKET",
                "event_bucket": bucket_name,
                "yaml_bucket": yaml_bucket,
                "yaml_key": yaml_key
            }))

        prn_key_guess = f"{folder}/{base_name}.prn"
        logger.info(f"PRN esperado (default .prn): {prn_key_guess}")
        logger.info(f"Buckets candidatos para PRN: {search_buckets}")

        t1 = time.time()
        prn_bucket, prn_key, prn_head = resolve_prn_key_multi(
            search_buckets, folder, base_name, yaml_bucket
        )
        logger.info(f"[T+{time.time() - start_ts:.3f}s][dt={time.time()-t1:.3f}s] HEAD PRN")
        if not prn_key:
            logger.error(json.dumps({
                "event": "PRN_NOT_FOUND",
                "yaml_bucket": yaml_bucket,
                "event_bucket": bucket_name,
                "buckets_searched": search_buckets,
                "prn_key": prn_key_guess,
                "yaml_key": yaml_key,
                "receive_count": receive_count,
                "mensaje": "Lote incompleto: YAML presente pero PRN aun no llega a source-repository",
                "sugerencia": (
                    "Kiro Transfer debe copiar yaml Y prn a "
                    f"s3://{SOURCE_REPOSITORY_BUCKET or search_buckets[0]}/pesvno/ "
                    "antes de que venza el reintento SQS"
                )
            }))
            if receive_count >= SQS_MAX_RECEIVE_BEFORE_STALE:
                raise StaleS3EventError(
                    f"Lote incompleto/obsoleto tras {receive_count} intentos: {yaml_key}"
                )
            raise FileNotFoundError(
                f"PRN no encontrado en buckets candidatos: {search_buckets}"
            )

        t1b = time.time()
        validate_required_batch_files(
            yaml_bucket, folder, base_name, yaml_key, required_files_rules, prn_bucket=prn_bucket
        )
        logger.info(f"[T+{time.time() - start_ts:.3f}s][dt={time.time()-t1b:.3f}s] Validacion de archivos requeridos")

        t2 = time.time()
        prn_metadata = get_prn_metadata(prn_bucket, prn_key, prn_head)
        logger.info(f"[T+{time.time() - start_ts:.3f}s][dt={time.time()-t2:.3f}s] Metadata PRN")

        t3 = time.time()
        output_config = process_output_configuration(config_data, base_name, folder)
        logger.info(f"[T+{time.time() - start_ts:.3f}s][dt={time.time()-t3:.3f}s] Output config")

        t4 = time.time()
        glue_parameters = prepare_glue_parameters(
            config_data, prn_bucket, prn_key, output_config, folder
        )
        logger.info(f"[T+{time.time() - start_ts:.3f}s][dt={time.time()-t4:.3f}s] Glue params")

        t5 = time.time()
        start_step_function(
            yaml_bucket, yaml_key, prn_key, config_data, glue_parameters, prn_metadata, prn_bucket
        )
        logger.info(f"[T+{time.time() - start_ts:.3f}s][dt={time.time()-t5:.3f}s] Step Function lanzada")

        logger.info(f"Procesamiento completado YAML: {yaml_key}")
    except Exception as e:
        logger.error(f"Error en process_yaml_config: {str(e)}")
        raise e

def simple_yaml_parse(yaml_content):
    """
    Parser YAML simple para configuraciones básicas (sin dependencias externas)
    """
    config = {}
    current_section = None
    
    lines = yaml_content.strip().split('\n')
    
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
            
        # Sección principal (sin indentación)
        if ':' in line and not line.startswith(' ') and not line.startswith('\t'):
            key, value = line.split(':', 1)
            key = key.strip()
            value = value.strip()
            
            # Remover comentarios del valor (todo después de #)
            if '#' in value:
                value = value.split('#')[0].strip()
            
            if value:
                # Valor directo
                if value.isdigit():
                    config[key] = int(value)
                elif value.lower() in ['true', 'false']:
                    config[key] = value.lower() == 'true'
                else:
                    config[key] = value.strip("'\"")
            else:
                # Nueva sección
                config[key] = {}
                current_section = key
        
        # Elementos de sección (con indentación)
        elif current_section and ':' in line and (line.startswith('  ') or line.startswith('\t')):
            key, value = line.split(':', 1)
            key = key.strip()
            value = value.strip()
            
            # Remover comentarios del valor (todo después de #)
            if '#' in value:
                value = value.split('#')[0].strip()
            
            if value.isdigit():
                config[current_section][key] = int(value)
            elif value.lower() in ['true', 'false']:
                config[current_section][key] = value.lower() == 'true'
            else:
                config[current_section][key] = value.strip("'\"")
    
    return config

def get_yaml_config(bucket_name, yaml_key):
    config_data, _ = get_yaml_config_multi([bucket_name], yaml_key)
    if not config_data:
        raise FileNotFoundError(f"YAML no encontrado: s3://{bucket_name}/{yaml_key}")
    return config_data

def _read_yaml_object(bucket_name, yaml_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=yaml_key)
    return response["Body"].read().decode("utf-8")

def get_yaml_config_multi(buckets, yaml_key):
    """Busca y parsea el YAML en multiples buckets. Retorna (config, bucket_encontrado)."""
    searched = []
    stem = yaml_key.rsplit(".", 1)[0] if "." in yaml_key else yaml_key
    seen_keys = []
    for candidate in [yaml_key, f"{stem}.yaml", f"{stem}.YAML", f"{stem}.yml"]:
        if candidate not in seen_keys:
            seen_keys.append(candidate)

    for bucket in buckets:
        if not bucket or bucket in searched:
            continue
        searched.append(bucket)
        logger.info(f"Buscando YAML en bucket: {bucket}")
        for candidate in seen_keys:
            try:
                yaml_content = _read_yaml_object(bucket, candidate)
                config_data = simple_yaml_parse(yaml_content)
                logger.info(f"Configuracion YAML cargada desde s3://{bucket}/{candidate}")
                logger.info(f"   - Version: {config_data.get('version', 'N/A')}")
                logger.info(f"   - Validar: {config_data.get('opciones', {}).get('validar', 'N/A')}")
                logger.info(f"   - Particulas: {config_data.get('opciones', {}).get('particulas', 'N/A')}")
                logger.info(f"   - Derechos: {config_data.get('opciones', {}).get('derechos', 'N/A')}")
                logger.info(f"   - Modo salida: {config_data.get('salidas', {}).get('modo', 'N/A')}")
                logger.info(f"   - Destino: {config_data.get('destino', 'N/A')}")
                return config_data, bucket
            except ClientError as error:
                if _is_s3_not_found_error(error):
                    continue
                logger.error(
                    f"Error S3 leyendo YAML s3://{bucket}/{candidate}: "
                    f"code={error.response.get('Error', {}).get('Code')} "
                    f"message={error.response.get('Error', {}).get('Message')}"
                )
                raise

    return None, None

def _is_s3_not_found_error(error):
    if not isinstance(error, ClientError):
        return False
    code = str(error.response.get("Error", {}).get("Code", ""))
    return code in ("404", "NoSuchKey", "NotFound")

def _find_prn_in_bucket(bucket_name, folder, base_name):
    """Busca PRN en un bucket sin emitir error final si no existe."""
    candidates = [
        f"{folder}/{base_name}.prn",
        f"{folder}/{base_name}.PRN",
        f"{folder}/{base_name}.Prn",
    ]

    for candidate in candidates:
        try:
            head = s3_client.head_object(Bucket=bucket_name, Key=candidate)
            if candidate != candidates[0]:
                logger.info(f"PRN encontrado con variante de extension: {candidate}")
            return candidate, head
        except ClientError as error:
            if _is_s3_not_found_error(error):
                continue
            logger.error(
                f"Error S3 verificando PRN {candidate}: "
                f"code={error.response.get('Error', {}).get('Code')} "
                f"message={error.response.get('Error', {}).get('Message')}"
            )
            raise

    prefix = f"{folder}/{base_name}."
    paginator = s3_client.get_paginator("list_objects_v2")
    listed_keys = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            listed_keys.append(key)
            filename = os.path.basename(key)
            stem, dot, ext = filename.rpartition(".")
            if dot == "." and stem == base_name and ext.lower() == "prn":
                head = s3_client.head_object(Bucket=bucket_name, Key=key)
                logger.info(f"PRN encontrado via list_objects_v2: s3://{bucket_name}/{key}")
                return key, head

    if listed_keys:
        logger.info(
            f"Objetos bajo s3://{bucket_name}/{prefix} (sin .prn): {listed_keys}"
        )
    return None, None

def get_batch_search_buckets(event_bucket, folder):
    """
    Buckets donde buscar YAML/PRN del lote.

    pesvno: SOLO source-repository (destino final de Kiro Transfer).
    Otros folders: bucket del evento + input PESVNO como fallback.
    """
    if folder.lower() == "pesvno":
        if SOURCE_REPOSITORY_BUCKET:
            return [SOURCE_REPOSITORY_BUCKET]
        logger.warning("SOURCE_REPOSITORY_BUCKET no configurado; usando bucket del evento S3")
        return [event_bucket] if event_bucket else []

    buckets = []
    input_bucket = os.environ.get("S3_BUCKET_NAME", "").strip()
    for bucket in [event_bucket, input_bucket]:
        if bucket and bucket not in buckets:
            buckets.append(bucket)
    return buckets

def get_prn_search_buckets(yaml_bucket, folder):
    """Alias de compatibilidad."""
    return get_batch_search_buckets(yaml_bucket, folder)

def resolve_prn_key(bucket_name, folder, base_name):
    """Compatibilidad: busca PRN en un solo bucket."""
    prn_key, prn_head = _find_prn_in_bucket(bucket_name, folder, base_name)
    if not prn_key:
        logger.error(json.dumps({
            "event": "PRN_LOOKUP_FAILED",
            "bucket": bucket_name,
            "folder": folder,
            "base_name": base_name,
            "mensaje": "No se encontro PRN en el bucket indicado"
        }))
    return prn_key, prn_head

def resolve_prn_key_multi(buckets, folder, base_name, yaml_bucket):
    """Busca PRN en los buckets del lote (para pesvno: solo source-repository)."""
    searched = []
    for bucket in buckets:
        if not bucket or bucket in searched:
            continue
        searched.append(bucket)
        logger.info(f"Buscando PRN en bucket: {bucket}")
        prn_key, prn_head = _find_prn_in_bucket(bucket, folder, base_name)
        if prn_key:
            logger.info(json.dumps({
                "event": "PRN_FOUND",
                "bucket": bucket,
                "prn_key": prn_key,
                "yaml_bucket": yaml_bucket
            }))
            return bucket, prn_key, prn_head

    logger.error(json.dumps({
        "event": "PRN_LOOKUP_FAILED",
        "yaml_bucket": yaml_bucket,
        "buckets_searched": searched,
        "folder": folder,
        "base_name": base_name,
        "prefix_searched": f"{folder}/{base_name}.",
        "mensaje": "No se encontro PRN en ningun bucket candidato"
    }))
    return None, None, None

def check_prn_file_exists(bucket_name, prn_key):
    try:
        head = s3_client.head_object(Bucket=bucket_name, Key=prn_key)
        logger.info(f"Archivo PRN encontrado: {prn_key}")
        return True, head
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            logger.error(f"Archivo PRN no encontrado: {prn_key}")
            return False, None
        logger.error(
            f"Error verificando archivo PRN: code={code} "
            f"message={error.response.get('Error', {}).get('Message')}"
        )
        return False, None
    except Exception as e:
        logger.error(f"Error verificando archivo PRN: {str(e)}")
        return False, None

def get_prn_metadata(bucket_name, prn_key, head_response=None):
    """
    Obtiene metadata mínima (tamaño y última modificación) sin leer el contenido.
    Se elimina completamente el conteo de líneas para evitar consumo innecesario.
    """
    try:
        if head_response is None:
            head_response = s3_client.head_object(Bucket=bucket_name, Key=prn_key)
        file_size = head_response["ContentLength"]
        last_modified = head_response["LastModified"]
        metadata = {"file_size": file_size, "lines_count": -1, "last_modified": last_modified}
        logger.info(f"Metadata archivo PRN: {prn_key}")
        logger.info(f"   Tamaño: {file_size} bytes")
        logger.info(f"   Lineas: NO_CALCULADO (funcion deshabilitada)")
        logger.info(f"   Ultima modificacion: {last_modified}")
        return metadata
    except Exception as e:
        logger.error(f"Error obteniendo metadata PRN: {str(e)}")
        raise e
#proceso de validacion de archivos requeridos por carpeta, basado en reglas definidas en un archivo CSV externo (product_file_rules.csv)


def list_batch_file_extensions(bucket_name, folder, base_name):
    prefix = f"{folder}/{base_name}."
    paginator = s3_client.get_paginator("list_objects_v2")
    found = {}

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = os.path.basename(key)
            stem, dot, ext = filename.rpartition(".")
            if dot != ".":
                continue
            if stem != base_name:
                continue

            ext_norm = normalize_extension(ext)
            if not ext_norm:
                continue
            found.setdefault(ext_norm, []).append(key)

    return found


def validate_required_batch_files(
    bucket_name, folder, base_name, yaml_key, required_files_rules, prn_bucket=None
):
    prn_bucket = prn_bucket or bucket_name
    rule = required_files_rules.get(folder.lower())
    if not rule:
        logger.info(f"Sin regla de validacion para carpeta {folder}. Se continua con validaciones base.")
        return

    found_by_extension = list_batch_file_extensions(bucket_name, folder, base_name)
    if prn_bucket != bucket_name:
        found_prn_bucket = list_batch_file_extensions(prn_bucket, folder, base_name)
        for ext, keys in found_prn_bucket.items():
            found_by_extension.setdefault(ext, []).extend(keys)

    found_extensions = set(found_by_extension.keys())
    required_extensions = rule["required_extensions"]

    missing_extensions = sorted(required_extensions - found_extensions)
    if missing_extensions:
        raise FileNotFoundError(
            "Faltan archivos requeridos para ejecutar el lote. "
            f"yaml={yaml_key}, faltantes={missing_extensions}, encontrados={sorted(found_extensions)}"
        )

    matched_count = len(required_extensions.intersection(found_extensions))
    expected_count = rule["expected_count"]
    if matched_count != expected_count:
        raise ValueError(
            "Cantidad de archivos requeridos no coincide con la regla. "
            f"yaml={yaml_key}, esperados={expected_count}, encontrados={matched_count}"
        )

    logger.info(
        f"Validacion por carpeta OK ({folder}): {matched_count}/{expected_count} extensiones requeridas presentes"
    )

######
def process_output_configuration(config_data, base_name, folder):
    try:
        salidas = config_data.get("salidas", {})
        modo = salidas.get("modo", "derive")
        
        output_config = {"modo": modo}
        
        if modo == "derive":
            derive_config = salidas.get("derive", {})
            output_config["nombres"] = {
                "validos": f"{folder}/{base_name}{derive_config.get('extension_val', '.val')}",
                "inconsistencias": f"{folder}/{base_name}{derive_config.get('extension_inc', '.inc')}",
                "duplicados": f"{folder}/{base_name}{derive_config.get('extension_dup', '.dup')}"
            }
            
        elif modo == "explicit":
            explicit_config = salidas.get("explicit", {})
            output_config["nombres"] = {
                "validos": explicit_config.get("nombre_val", f"{folder}/{base_name}_validos.txt"),
                "inconsistencias": explicit_config.get("nombre_inc", f"{folder}/{base_name}_inconsistencias.txt"),
                "duplicados": explicit_config.get("nombre_dup", f"{folder}/{base_name}_duplicados.txt")
            }
            
        else:
            logger.warning(f"Modo de salida no reconocido: {modo}, usando 'derive' por defecto")
            output_config["modo"] = "derive"
            output_config["nombres"] = {
                "validos": f"{folder}/{base_name}.val",
                "inconsistencias": f"{folder}/{base_name}.inc",
                "duplicados": f"{folder}/{base_name}.dup"
            }
        
        logger.info(f"Configuracion de salidas ({modo}):")
        logger.info(f"   Validos: {output_config['nombres']['validos']}")
        logger.info(f"   Inconsistencias: {output_config['nombres']['inconsistencias']}")
        logger.info(f"   Duplicados: {output_config['nombres']['duplicados']}")
        
        return output_config
        
    except Exception as e:
        logger.error(f"Error procesando configuracion de salidas: {str(e)}")
        raise e

def prepare_glue_parameters(config_data, bucket_name, prn_key, output_config, folder):
    """Prepara parámetros legacy para Glue, seleccionando destino dinámicamente:
    - Si la carpeta de entrada (folder) es 'pesvno' => destino 'onpremises'
    - En otro caso => destino 'redshift'
    """
    try:
        opciones = config_data.get("opciones", {})
        base_name = os.path.splitext(os.path.basename(prn_key))[0]

        # Selección automática de destino según carpeta top-level
        destino_selected = "onpremises" if folder.lower() == "pesvno" else "redshift"

        params_step_function = {
            "archivoprn": prn_key,
            "derechos": str(opciones.get("derechos", 1)),
            "destino": destino_selected,  # destino dinámico
            "ejecucion": "automatico",
            "nombre_archivos": base_name,
            "nombre_proceso": f"pesvno_{os.environ.get('ENVIRONMENT', 'dev')}",
            "particulas": str(opciones.get("particulas", 0)),
            "validar": str(opciones.get("validar", 1))
        }

        glue_params = {
            "nombreval": output_config["nombres"]["validos"],
            "nombreinc": output_config["nombres"]["inconsistencias"],
            "nombredup": output_config["nombres"]["duplicados"],
            "params_step_function": json.dumps(params_step_function),
            "sourcebucket": bucket_name  # Nuevo: bucket real origen del evento
        }

        logger.info("Parametros Glue:")
        logger.info(f"   destino seleccionado: {destino_selected}")
        logger.info(f"   nombreval: {glue_params['nombreval']}")
        logger.info(f"   nombreinc: {glue_params['nombreinc']}")
        logger.info(f"   nombredup: {glue_params['nombredup']}")
        return glue_params
    except Exception as e:
        logger.error(f"Error preparando parametros Glue: {str(e)}")
        raise e

def generate_lote_id(base_name):
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    return f"{base_name}-{timestamp}"

def start_step_function(
    yaml_bucket, yaml_key, prn_key, config_data, glue_parameters, prn_metadata, prn_bucket=None
):
    try:
        prn_bucket = prn_bucket or yaml_bucket
        base_name = os.path.splitext(os.path.basename(yaml_key))[0]
        lote_id = generate_lote_id(base_name)

        step_function_input = {
            "config_yaml": {
                "version": config_data.get("version", "1.0"),
                "project": "PESVNO",
                "environment": os.environ.get("ENVIRONMENT", "dev"),
                "yaml_file": yaml_key,
                "prn_file": prn_key,
                "prn_file_name": os.path.basename(prn_key)
            },
            "prn_file_name": os.path.basename(prn_key),
            "glue_arguments": {
                "--nombreval": glue_parameters["nombreval"],
                "--nombreinc": glue_parameters["nombreinc"],
                "--nombredup": glue_parameters["nombredup"],
                "--params_step_function": glue_parameters["params_step_function"],
                "--sourcebucket": glue_parameters["sourcebucket"]
            },
            "metadata": {
                "bucket": yaml_bucket,
                "prn_bucket": prn_bucket,
                "file_size": prn_metadata["file_size"],
                "lines_count": prn_metadata["lines_count"],
                "lote_id": lote_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        }

        # Glue debe leer el PRN desde el bucket donde realmente existe el archivo.
        step_function_input["glue_arguments"]["--imputbucket"] = prn_bucket
        logger.info(f"Override --imputbucket -> {prn_bucket} (yaml en {yaml_bucket})")
        logger.info(f"Set --sourcebucket -> {glue_parameters['sourcebucket']}")

        prn_file_name = os.path.basename(prn_key)
        logger.info(f"DEBUG prn_key: {prn_key} | prn_file_name: {prn_file_name}")

        step_function_arn = os.environ.get("STEP_FUNCTION_ARN")
        if not step_function_arn:
            raise ValueError("STEP_FUNCTION_ARN no configurado")

        base_execution_name = f"pesvno-execution-{lote_id}"
        execution_name = base_execution_name
        logger.info(f"Iniciando StepFunction: {execution_name} (size={prn_metadata['file_size']})")

        try:
            response = stepfunctions.start_execution(
                stateMachineArn=step_function_arn,
                name=execution_name,
                input=json.dumps(step_function_input, default=str)
            )
        except ClientError as ce:
            if ce.response.get('Error', {}).get('Code') == 'ExecutionAlreadyExists':
                suffix = datetime.utcnow().strftime('%H%M%S%f')
                execution_name = f"{base_execution_name}-{suffix}"
                logger.warning(f"ExecutionAlreadyExists, reintento con {execution_name}")
                response = stepfunctions.start_execution(
                    stateMachineArn=step_function_arn,
                    name=execution_name,
                    input=json.dumps(step_function_input, default=str)
                )
            else:
                raise

        logger.info("Step Function iniciada:")
        logger.info(f"   Execution ARN: {response['executionArn']}")
        logger.info(f"   YAML: {yaml_key}")
        logger.info(f"   PRN: {prn_key}")
        logger.info(f"   Lote ID: {lote_id}")
        logger.info(f"   Execution Name: {execution_name}")
        return response
    except Exception as e:
        logger.error(f"Error iniciando Step Function: {str(e)}")
        raise e
