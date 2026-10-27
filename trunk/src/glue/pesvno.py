import json
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, isnull, explode, lit,coalesce, array, udf, split, row_number, when,struct,count,regexp_replace,trim,regexp_extract,from_unixtime, date_format,lpad, rpad, expr, substring, length, concat,upper, first, broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
from pyspark.sql.window import Window
import re
import pyspark.sql.functions as F
import unicodedata
import math
import logging
import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from io import BytesIO
import time
from datetime import datetime
import traceback


 
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
secretsmanager = boto3.client('secretsmanager', region_name='us-east-1')


def obtener_credenciales_redshift(secret_arn):
    """
    Recupera las credenciales de conexión a Redshift desde AWS Secrets Manager.

    El secret administrado para el usuario de Redshift contiene, como mínimo, las
    claves 'username' y 'password'. Opcionalmente puede incluir 'host', 'port',
    'dbname' y 'engine'.

    Args:
        secret_arn: ARN (o nombre) del secret en Secrets Manager.

    Returns:
        dict: Contenido del secret parseado (username, password, y opcionales).

    Raises:
        ValueError: Si el secret está vacío, no existe o le faltan claves obligatorias.
    """
    if not secret_arn or not secret_arn.strip():
        raise ValueError("❌ CRÍTICO: 'redshift_secret_arn' vacío. No se puede autenticar contra Redshift.")

    print(f"🔐 Recuperando credenciales de Redshift desde Secrets Manager...")
    try:
        response = secretsmanager.get_secret_value(SecretId=secret_arn)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ResourceNotFoundException':
            raise ValueError(f"❌ CRÍTICO: Secret no encontrado: {secret_arn}")
        if error_code == 'AccessDeniedException':
            raise ValueError(
                f"❌ CRÍTICO: Sin permisos para leer el secret: {secret_arn}\n"
                f"🔧 SOLUCIÓN: El rol de Glue necesita secretsmanager:GetSecretValue sobre este ARN."
            )
        raise ValueError(f"❌ Error accediendo al secret de Redshift: {e}")

    secret_string = response.get('SecretString')
    if not secret_string:
        raise ValueError(f"❌ CRÍTICO: El secret {secret_arn} no contiene SecretString.")

    try:
        secret = json.loads(secret_string)
    except json.JSONDecodeError as e:
        raise ValueError(f"❌ CRÍTICO: El secret {secret_arn} no es un JSON válido: {e}")

    if 'username' not in secret or 'password' not in secret:
        raise ValueError(
            f"❌ CRÍTICO: El secret {secret_arn} debe contener las claves 'username' y 'password'. "
            f"Claves encontradas: {list(secret.keys())}"
        )

    print(f"✅ Credenciales de Redshift recuperadas (usuario: {secret['username']})")
    return secret

# Globals inicializados tras parsear argumentos del job
tabla_business = None
tabla_tracking = None
nombre_producto = 'PESVNO'
destino_buckets_business = set()
kms_destino_default_key = None
output_onpremises_bucket = None
USE_BUSINESS_DESTINO = False
reglas_out = []
MAX_TRACKING_RETRIES = 5



# ========== SISTEMA DE CONFIGURACIÓN YAML ULTRA-ROBUSTO ==========
class ConfiguracionYAML:
    """
    Sistema robusto para leer, validar y procesar archivos YAML de configuración PESVNO.
    Implementa todas las reglas de negocio y validaciones cruzadas.
    Parser YAML simple nativo - sin dependencias externas, compatible con AWS Glue.
    """
    
    def __init__(self):
        self.config = None
        self.archivo_prn = None
        self.formato_detectado = None
        
    def parsear_yaml_simple(self, yaml_content):
        """
        Parser YAML simple para la estructura específica de PESVNO.
        No requiere dependencias externas, maneja solo la estructura necesaria.
        """
        try:
            lines = yaml_content.strip().split('\n')
            config = {}
            current_section = None
            current_subsection = None
            
            for line in lines:
                # Guardar indentación antes de strip
                original_line = line
                line = line.strip()
                
                # Ignorar comentarios y líneas vacías
                if not line or line.startswith('#'):
                    continue
                
                # Eliminar comentarios en línea (después del valor)
                # Solo si el # no está dentro de comillas
                if '#' in line:
                    # Buscar # que no esté dentro de comillas
                    in_quotes = False
                    quote_char = None
                    for i, char in enumerate(line):
                        if char in ['"', "'"]:
                            if not in_quotes:
                                in_quotes = True
                                quote_char = char
                            elif char == quote_char:
                                in_quotes = False
                                quote_char = None
                        elif char == '#' and not in_quotes:
                            # Encontramos un comentario fuera de comillas
                            line = line[:i].strip()
                            break
                
                # Detectar secciones principales
                if ':' in line and not original_line.startswith(' '):
                    if line.endswith(':'):
                        # Sección sin valor (ej: "opciones:")
                        current_section = line.replace(':', '').strip()
                        config[current_section] = {}
                        current_subsection = None
                    else:
                        # Valor directo (ej: "version: 1.0")
                        key, value = line.split(':', 1)
                        config[key.strip()] = self._parse_value(value.strip())
                        current_section = None
                        current_subsection = None
                
                # Detectar subsecciones (con indentación)
                elif original_line.startswith('  ') and current_section:
                    if ':' in line:
                        if line.endswith(':'):
                            # Subsección (ej: "  derive:")
                            current_subsection = line.replace(':', '').strip()
                            config[current_section][current_subsection] = {}
                        else:
                            # Valor en sección (ej: "  validar: 1")
                            key, value = line.split(':', 1)
                            if current_subsection:
                                config[current_section][current_subsection][key.strip()] = self._parse_value(value.strip())
                            else:
                                config[current_section][key.strip()] = self._parse_value(value.strip())
                
                # Detectar valores en subsección (doble indentación)
                elif original_line.startswith('    ') and current_section and current_subsection:
                    if ':' in line:
                        key, value = line.split(':', 1)
                        config[current_section][current_subsection][key.strip()] = self._parse_value(value.strip())
            
            return config
            
        except Exception as e:
            raise ValueError(f"Error parseando YAML simple: {e}")
    
    def _parse_value(self, value):
        """Convierte valores YAML a tipos Python apropiados"""
        value = value.strip().strip('"').strip("'")
        
        # Números enteros
        if value.isdigit():
            return int(value)
        
        # Números decimales
        try:
            if '.' in value:
                return float(value)
        except:
            pass
        
        # Strings
        return value

    def leer_yaml_desde_s3(self, ruta_prn):
        """
        Lee el archivo YAML correspondiente al PRN desde S3.
        
        Args:
            ruta_prn: Ruta del archivo PRN (ej: s3://bucket/folder/archivo.prn)
            
        Returns:
            dict: Configuración parseada del YAML
            
        Raises:
            ValueError: Si no encuentra el YAML o hay errores de formato
        """
        try:
            # Generar ruta del YAML basada en el PRN
            ruta_yaml = ruta_prn.replace('.prn', '.yaml').replace('.PRN', '.yaml')
            
            print(f"🔍 DEBUG - Ruta PRN original: {ruta_prn}")
            print(f"🔍 DEBUG - Ruta YAML generada: {ruta_yaml}")
            
            # Parsear la ruta S3
            bucket_name = ruta_yaml.replace('s3://', '').split('/')[0]
            file_key = '/'.join(ruta_yaml.replace('s3://', '').split('/')[1:])
            
            print(f"🔍 DEBUG - Bucket extraído: '{bucket_name}'")
            print(f"🔍 DEBUG - Key extraído: '{file_key}'")
            print(f"🔍 Buscando archivo YAML: {ruta_yaml}")
            
            # Validar que tenemos bucket y key válidos
            if not bucket_name.strip():
                raise ValueError(f"❌ CRÍTICO: Bucket vacío en ruta: {ruta_yaml}")
            if not file_key.strip():
                raise ValueError(f"❌ CRÍTICO: Key vacío en ruta: {ruta_yaml}")
            
            # Leer archivo desde S3
            try:
                response = s3.get_object(Bucket=bucket_name, Key=file_key)
                yaml_content = response['Body'].read().decode('utf-8')
                print(f"✅ Archivo YAML encontrado ({len(yaml_content)} caracteres)")
                
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    raise ValueError(f"❌ CRÍTICO: Archivo YAML no encontrado: {ruta_yaml}")
                elif e.response['Error']['Code'] == 'AccessDenied':
                    raise ValueError(f"❌ CRÍTICO: Sin permisos para leer YAML: {ruta_yaml}\n" +
                                   f"🔧 SOLUCIÓN: El rol de Glue necesita permisos s3:GetObject para archivos .yaml\n" +
                                   f"📋 Rol actual debe tener acceso a: {bucket_name}/*.yaml")
                else:
                    raise ValueError(f"❌ Error accediendo YAML: {e}")
            
            # Parsear YAML con parser simple
            try:
                config = self.parsear_yaml_simple(yaml_content)
                print(f"✅ YAML parseado correctamente")
                print(f"🔍 Debug - Estructura detectada: {list(config.keys())}")
                return config
                
            except Exception as e:
                raise ValueError(f"❌ CRÍTICO: Error en formato YAML: {e}")
                
        except Exception as e:
            print(f"❌ Error leyendo YAML: {e}")
            raise
    
    def validar_configuracion(self, config, formato_archivo):
        """
        Valida todos los parámetros del YAML según las reglas de negocio.
        
        Args:
            config: Configuración parseada del YAML
            formato_archivo: Formato detectado del PRN (ej: "1-11-45")
            
        Returns:
            dict: Configuración validada y normalizada
            
        Raises:
            ValueError: Si hay errores de validación
        """
        print("🔍 Iniciando validación EXHAUSTIVA de configuración YAML...")
        
        # ===== VALIDACIÓN 1: Estructura básica =====
        if not isinstance(config, dict):
            raise ValueError("❌ CRÍTICO: YAML debe ser un objeto/diccionario")
        
        if 'version' not in config:
            raise ValueError("❌ CRÍTICO: Campo 'version' es obligatorio")
            
        # Convertir a string para comparar (puede venir como float 1.0 o string "1.0")
        version_valor = str(config.get('version'))
        if version_valor != "1.0":
            raise ValueError(f"❌ CRÍTICO: Version '{version_valor}' no soportada. Usar '1.0'")
        
        if 'opciones' not in config:
            raise ValueError("❌ CRÍTICO: Sección 'opciones' es obligatoria")
        
        opciones = config['opciones']
        
        # ===== VALIDACIÓN 2: Campo 'validar' =====
        if 'validar' not in opciones:
            raise ValueError("❌ CRÍTICO: Campo 'opciones.validar' es obligatorio")
        
        validar = opciones['validar']
        if not isinstance(validar, int) or validar not in [0, 1]:
            raise ValueError(f"❌ CRÍTICO: 'validar' debe ser 0 o 1, recibido: {validar}")
        
        print(f"✅ validar: {validar} ({'Ejecutar match' if validar == 1 else 'Sin match'})")
        
        # ===== VALIDACIÓN 3: Campo 'particulas' =====
        particulas = opciones.get('particulas', 0)  # Default 0 = usar lógica por tipoid
        
        if not isinstance(particulas, int):
            raise ValueError(f"❌ CRÍTICO: 'particulas' debe ser entero, recibido: {type(particulas)}")
        
        if particulas < 0 or particulas > 3:
            raise ValueError(f"❌ CRÍTICO: 'particulas' debe estar entre 0-3, recibido: {particulas}")
        
        # ===== VALIDACIÓN CRUZADA: particulas vs formato archivo =====
        if validar == 1 and particulas > 0:  # Solo validar si se ejecutará match
            if formato_archivo == "1-11":
                if particulas >= 1:
                    raise ValueError(f"❌ CRÍTICO: Formato '1-11' (sin nombres) no compatible con particulas={particulas}. Solo se permite particulas=0 (automático)")
                    
            elif formato_archivo == "1-11-15":
                if particulas > 1:
                    raise ValueError(f"❌ CRÍTICO: Formato '1-11-15' (solo apellido) no compatible con particulas={particulas}. Máximo particulas=1")
                    
            elif formato_archivo == "1-11-45":
                # Formato completo, acepta 1-3 partículas
                pass
            else:
                raise ValueError(f"❌ CRÍTICO: Formato de archivo desconocido: {formato_archivo}")
        
        print(f"✅ particulas: {particulas} (compatible con formato {formato_archivo})")
        
        # ===== VALIDACIÓN 4: Campo 'derechos' =====
        derechos = opciones.get('derechos', 1)  # Default 1 = aplicar validación
        
        if not isinstance(derechos, int) or derechos not in [0, 1]:
            raise ValueError(f"❌ CRÍTICO: 'derechos' debe ser 0 o 1, recibido: {derechos}")
        
        print(f"✅ derechos: {derechos} ({'Aplicar validación' if derechos == 1 else 'Sin validación'})")
        
        # ===== VALIDACIÓN 5: Sección 'salidas' =====
        if 'salidas' not in config:
            raise ValueError("❌ CRÍTICO: Sección 'salidas' es obligatoria")
        
        salidas = config['salidas']
        modo = salidas.get('modo', 'derive')
        
        if modo not in ['derive', 'explicit']:
            raise ValueError(f"❌ CRÍTICO: 'salidas.modo' debe ser 'derive' o 'explicit', recibido: '{modo}'")
        
        print(f"✅ modo salidas: {modo}")
        
        # Validar configuración específica del modo
        if modo == 'derive':
            if 'derive' not in salidas:
                raise ValueError("❌ CRÍTICO: Modo 'derive' requiere sección 'salidas.derive'")
            
            derive_config = salidas['derive']
            required_extensions = ['extension_val', 'extension_inc', 'extension_dup']
            
            for ext in required_extensions:
                if ext not in derive_config:
                    raise ValueError(f"❌ CRÍTICO: Campo 'salidas.derive.{ext}' es obligatorio")
                if not isinstance(derive_config[ext], str) or not derive_config[ext].startswith('.'):
                    raise ValueError(f"❌ CRÍTICO: 'salidas.derive.{ext}' debe ser extensión válida (ej: '.val')")
            
            print(f"✅ Extensiones: {derive_config['extension_val']}, {derive_config['extension_inc']}, {derive_config['extension_dup']}")
            
        elif modo == 'explicit':
            if 'explicit' not in salidas:
                raise ValueError("❌ CRÍTICO: Modo 'explicit' requiere sección 'salidas.explicit'")
            
            explicit_config = salidas['explicit']
            required_names = ['nombre_val', 'nombre_inc', 'nombre_dup']
            
            for name in required_names:
                if name not in explicit_config:
                    raise ValueError(f"❌ CRÍTICO: Campo 'salidas.explicit.{name}' es obligatorio")
                if not isinstance(explicit_config[name], str) or not explicit_config[name].strip():
                    raise ValueError(f"❌ CRÍTICO: 'salidas.explicit.{name}' debe ser nombre válido no vacío")
            
            print(f"✅ Nombres explícitos: {explicit_config['nombre_val']}, {explicit_config['nombre_inc']}, {explicit_config['nombre_dup']}")
        
        # ===== VALIDACIÓN 6: Campo 'destino' =====
        destino = config.get('destino', 'onpremises')
        if destino not in ['redshift', 'onpremises']:
            raise ValueError(f"❌ CRÍTICO: 'destino' debe ser 'redshift' o 'onpremises', recibido: '{destino}'")
        
        print(f"✅ destino: {destino}")
        
        print("🎉 TODAS las validaciones PASSED - Configuración VÁLIDA")
        return config
    
    def generar_nombres_salida(self, config, archivo_prn):
        """
        Genera los nombres de archivos de salida según la configuración.
        
        Args:
            config: Configuración validada del YAML
            archivo_prn: Nombre del archivo PRN (ej: "pruebas.prn")
            
        Returns:
            dict: Nombres de archivos de salida
        """
        salidas = config['salidas']
        modo = salidas['modo']
        
        # Extraer nombre base del PRN (sin extensión)
        nombre_base = archivo_prn.replace('.prn', '').replace('.PRN', '')
        
        if modo == 'derive':
            derive_config = salidas['derive']
            
            nombres = {
                'aceptados': f"{nombre_base}{derive_config['extension_val']}",
                'duplicados': f"{nombre_base}{derive_config['extension_dup']}",
                'inconsistencias': f"{nombre_base}{derive_config['extension_inc']}"
            }
            
        elif modo == 'explicit':
            explicit_config = salidas['explicit']
            
            nombres = {
                'aceptados': explicit_config['nombre_val'],
                'duplicados': explicit_config['nombre_dup'], 
                'inconsistencias': explicit_config['nombre_inc']
            }
        
        print(f"📁 Nombres de salida generados ({modo}):")
        for tipo, nombre in nombres.items():
            print(f"   • {tipo}: {nombre}")
        
        return nombres
    
    def procesar_configuracion_completa(self, ruta_prn, formato_archivo):
        """
        Función principal que lee, valida y procesa la configuración completa.
        
        Args:
            ruta_prn: Ruta completa del archivo PRN
            formato_archivo: Formato detectado del archivo
            
        Returns:
            tuple: (configuracion_validada, nombres_salida)
        """
        try:
            print("🚀 INICIANDO procesamiento de configuración YAML...")
            
            # 1. Leer YAML desde S3
            config = self.leer_yaml_desde_s3(ruta_prn)
            
            # 2. Validar configuración con reglas de negocio
            config_validada = self.validar_configuracion(config, formato_archivo)
            
            # 3. Generar nombres de salida
            archivo_prn = ruta_prn.split('/')[-1]  # Extraer solo el nombre del archivo
            nombres_salida = self.generar_nombres_salida(config_validada, archivo_prn)
            
            print("✅ Configuración YAML procesada EXITOSAMENTE")
            return config_validada, nombres_salida
            
        except Exception as e:
            print(f"❌ ERROR CRÍTICO en configuración YAML: {e}")
            print("🚨 DETENIENDO EJECUCIÓN - Corregir archivo YAML")
            raise

# Instancia global del configurador
configurador_yaml = ConfiguracionYAML()

# ========== BUSINESS TABLE + OBJECT TRACKING (on-premise OUT) ==========
def parse_destino_s3(destino_str):
    """Convierte 'bucket/prefix' en ruta s3://bucket/prefix/"""
    destino_str = destino_str.strip().strip('/')
    if not destino_str:
        raise ValueError("DESTINO vacío en regla de negocio")
    parts = destino_str.split('/', 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ''
    if prefix and not prefix.endswith('/'):
        prefix = f"{prefix}/"
    return f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}/"

def extraer_bucket_destino(destino_str):
    """Extrae el nombre del bucket desde un DESTINO 'bucket/prefix'."""
    destino_str = destino_str.strip().strip('/')
    if not destino_str:
        return None
    return destino_str.split('/', 1)[0]

def extraer_buckets_destino(reglas):
    """Obtiene el conjunto único de buckets referenciados en DESTINO de las reglas OUT."""
    buckets = set()
    for regla in reglas:
        bucket = extraer_bucket_destino(regla.get('DESTINO', ''))
        if bucket:
            buckets.add(bucket)
    return buckets

def cargar_reglas_out(tabla, producto):
    """Carga reglas TIPO=OUT para el producto desde la tabla business."""
    reglas = []
    scan_kwargs = {
        'FilterExpression': 'TIPO = :tipo AND NOMBRE_PRODUCTO = :prod',
        'ExpressionAttributeValues': {
            ':tipo': 'OUT',
            ':prod': producto
        }
    }
    while True:
        response = tabla.scan(**scan_kwargs)
        reglas.extend(response.get('Items', []))
        if 'LastEvaluatedKey' not in response:
            break
        scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
    return reglas

def resolver_regla_out(nombre_archivo, reglas):
    """Retorna la primera regla OUT cuyo NOMBRE_ARCHIVO (regex) coincide con el archivo."""
    for regla in reglas:
        pattern = regla.get('NOMBRE_ARCHIVO', '')
        if pattern and re.search(pattern, nombre_archivo):
            return regla
    return None

def parse_archivos_tracking(value):
    """Convierte ARCHIVOS_SALIDA/ENTRADA almacenado en DynamoDB a lista de strings."""
    if not value:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []

def registrar_archivo_salida(tabla, regla_out, paquete_id, nombre_archivo):
    """Completa un archivo OUT en el registro existente de object-tracking (update-only)."""
    producto = regla_out.get('PRODUCTO', '')
    if not producto:
        print(f"⚠️ Regla OUT sin PRODUCTO para tracking: {nombre_archivo}")
        return

    key = {'PRODUCTO': producto, 'NOMBRE_ARCHIVO': paquete_id}
    origen_salida = str(regla_out.get('ORIGEN', '') or '').strip()
    destino_salida = str(regla_out.get('DESTINO', '') or '').strip()

    for attempt in range(1, MAX_TRACKING_RETRIES + 1):
        try:
            response = tabla.get_item(Key=key, ConsistentRead=True)
            item = response.get('Item')
            if not item:
                print(
                    f"⚠️ object-tracking sin registro previo para {producto}/{paquete_id}. "
                    f"No se crea item nuevo para {nombre_archivo}."
                )
                return

            archivos_salida = parse_archivos_tracking(item.get('ARCHIVOS_SALIDA'))
            if nombre_archivo in archivos_salida:
                print(f"ℹ️ Archivo {nombre_archivo} ya registrado en object-tracking para {producto}/{paquete_id}")
                return

            expected_revision = int(item.get('TRACKING_REVISION', 0))
            new_archivos = archivos_salida + [nombre_archivo]
            update_parts = [
                'ARCHIVOS_SALIDA = :archivos',
                'ARCHIVOS_RECIBIDOS_SALIDA = :recibidos',
                'ESTADO_SALIDA = :estado',
                'TRACKING_REVISION = :newrev',
            ]
            expression_values = {
                ':archivos': json.dumps(new_archivos, ensure_ascii=True),
                ':recibidos': len(new_archivos),
                ':estado': 'COMPLETADO',
                ':expected_revision': expected_revision,
                ':newrev': expected_revision + 1,
                ':zero': 0,
            }
            if origen_salida:
                update_parts.append('ORIGEN_SALIDA = :origen_salida')
                expression_values[':origen_salida'] = origen_salida
            if destino_salida:
                update_parts.append('DESTINO_SALIDA = :destino_salida')
                expression_values[':destino_salida'] = destino_salida

            tabla.update_item(
                Key=key,
                UpdateExpression='SET ' + ', '.join(update_parts),
                ConditionExpression=(
                    'TRACKING_REVISION = :expected_revision OR '
                    '(attribute_not_exists(TRACKING_REVISION) AND :expected_revision = :zero)'
                ),
                ExpressionAttributeValues=expression_values,
            )

            print(f"✅ object-tracking actualizado: {producto}/{paquete_id} -> {nombre_archivo}")
            return
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                print(f"⚠️ Conflicto de concurrencia en tracking (intento {attempt}/{MAX_TRACKING_RETRIES}), reintentando...")
                time.sleep(0.2 * attempt)
                continue
            raise

    raise RuntimeError(f"No se pudo actualizar object-tracking tras {MAX_TRACKING_RETRIES} intentos")

def resolver_sse_kms_key(final_bucket):
    """Selecciona la llave KMS según el bucket destino final."""
    if final_bucket == output_redshift_bucket:
        return kms_dynamic_batch_key
    if output_onpremises_bucket and final_bucket == output_onpremises_bucket:
        return kms_carguedebases_key
    # Buckets destino dinámicos definidos en business table DESTINO (on-premise)
    if final_bucket in destino_buckets_business and kms_destino_default_key:
        return kms_destino_default_key
    return kms_destino_default_key or kms_carguedebases_key

# ========== FUNCIÓN ULTRA-OPTIMIZADA DE CONCATENACIÓN S3 CON COPIA FINAL ==========
def concatenar_particiones_s3(temp_path, final_path, tipo_archivo, nombre_archivo_final=None):
    """
    Concatena particiones de Spark en un archivo único usando operaciones S3 nativas.
    NUEVA LÓGICA: Concatena en bucket staging (con permisos) y luego copia a destino final.
    
    Args:
        temp_path: Ruta temporal con particiones (ej: s3://bucket/temp_parts/)
        final_path: Ruta final para archivo único (ej: s3://bucket/parts/)
        tipo_archivo: Tipo para logging (ej: "aceptados", "duplicados")
        nombre_archivo_final: Nombre específico del archivo final (desde YAML)
    """
    concat_start_time = time.time()
    print(f"🔧 Iniciando concatenación S3 para {tipo_archivo}...")
    
    try:
        # Parsear rutas S3
        temp_bucket = temp_path.replace('s3://', '').split('/')[0]
        temp_prefix = '/'.join(temp_path.replace('s3://', '').split('/')[1:])
        
        final_bucket = final_path.replace('s3://', '').split('/')[0] 
        final_prefix = '/'.join(final_path.replace('s3://', '').split('/')[1:])
        
        # ===== PASO 1: Listar particiones en bucket temporal (con permisos) =====
        print(f"🔍 PASO 1: Listando particiones en bucket staging (con permisos)...")
        print(f"   📍 Bucket: {temp_bucket}, Prefix: {temp_prefix}")
        
        response = s3.list_objects_v2(Bucket=temp_bucket, Prefix=temp_prefix)
        
        if 'Contents' not in response:
            print(f"⚠️ No se encontraron particiones en {temp_path}")
            return
            
        # Filtrar solo archivos part-*
        part_files = []
        total_size = 0
        for obj in response['Contents']:
            if '/part-' in obj['Key'] and not obj['Key'].endswith('_SUCCESS'):
                part_files.append(obj['Key'])
                total_size += obj['Size']
        
        if not part_files:
            print(f"⚠️ No se encontraron archivos part-* en {temp_path}")
            return
            
        part_files.sort()
        print(f"📊 Encontrados {len(part_files)} archivos part-* ({total_size:,} bytes total)")
        
        # Generar nombre del archivo final
        if nombre_archivo_final:
            final_filename = nombre_archivo_final
            print(f"📁 Usando nombre desde YAML: {final_filename}")
        else:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            final_filename = f"{tipo_archivo}_{timestamp}.txt"
            print(f"📁 Generando nombre con timestamp: {final_filename}")
        
        # ===== PASO 2: Concatenar en bucket STAGING (donde tenemos permisos) =====
        # Usar el mismo bucket temporal para staging
        staging_bucket = temp_bucket
        staging_key = f"{temp_prefix}staging/{final_filename}"
        
        print(f"🔧 PASO 2: Concatenando en bucket staging (con permisos)...")
        print(f"   📍 Staging: s3://{staging_bucket}/{staging_key}")
        
        if total_size < 500_000_000:  # < 500MB
            print("🚀 Usando concatenación directa (archivo mediano)")
            
            concatenated_content = bytearray()
            for i, part_key in enumerate(part_files):
                print(f"📖 Leyendo parte {i+1}/{len(part_files)}: {part_key.split('/')[-1]}")
                
                response = s3.get_object(Bucket=temp_bucket, Key=part_key)
                content = response['Body'].read()
                concatenated_content.extend(content)
            
            # Escribir en STAGING
            print(f"📝 Escribiendo archivo concatenado en staging...")
            s3.put_object(
                Bucket=staging_bucket,
                Key=staging_key,
                Body=bytes(concatenated_content),
                ContentType='text/plain'
            )
            
        else:
            # Multipart upload en staging
            print("🚀 Usando multipart upload (archivo grande)")
            
            mp_response = s3.create_multipart_upload(
                Bucket=staging_bucket,
                Key=staging_key,
                ContentType='text/plain'
            )
            upload_id = mp_response['UploadId']
            
            parts = []
            part_number = 1
            
            for i, part_key in enumerate(part_files):
                print(f"📤 Subiendo parte {i+1}/{len(part_files)} como multipart {part_number}")
                
                copy_source = {'Bucket': temp_bucket, 'Key': part_key}
                
                response = s3.upload_part_copy(
                    Bucket=staging_bucket,
                    Key=staging_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    CopySource=copy_source
                )
                
                parts.append({
                    'ETag': response['CopyPartResult']['ETag'],
                    'PartNumber': part_number
                })
                part_number += 1
            
            s3.complete_multipart_upload(
                Bucket=staging_bucket,
                Key=staging_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
        
        print(f"✅ Archivo concatenado en staging: s3://{staging_bucket}/{staging_key}")
        
        # ===== PASO 3: Copiar desde staging al bucket destino final =====
        if final_prefix and not final_prefix.endswith('/'):
            final_prefix = f"{final_prefix}/"
        final_key = f"{final_prefix}{final_filename}" if final_prefix else final_filename
        
        print(f"🔧 PASO 3: Copiando al bucket destino final...")
        print(f"   📍 Origen: s3://{staging_bucket}/{staging_key}")
        print(f"   📍 Destino: s3://{final_bucket}/{final_key}")

        sse_kms_key = resolver_sse_kms_key(final_bucket)
        print(f"🔐 KMS seleccionada para {final_bucket}: {sse_kms_key}")
        
        copy_source = {'Bucket': staging_bucket, 'Key': staging_key}
        
        try:
            if total_size <= 5_000_000_000:  # ≤5GB: copy_object directo
                s3.copy_object(
                    Bucket=final_bucket,
                    Key=final_key,
                    CopySource=copy_source,
                    ContentType='text/plain',
                    ServerSideEncryption='aws:kms',
                    SSEKMSKeyId=sse_kms_key
                )
                print(f"✅ Archivo copiado exitosamente al destino final con encriptación KMS")
            else:
                # >5GB: multipart copy con rangos de bytes (copy_object tiene limite de 5GB)
                print(f"🚀 Archivo >5GB ({total_size:,} bytes), usando multipart copy al destino final...")
                mp_final = s3.create_multipart_upload(
                    Bucket=final_bucket,
                    Key=final_key,
                    ContentType='text/plain',
                    ServerSideEncryption='aws:kms',
                    SSEKMSKeyId=sse_kms_key
                )
                final_upload_id = mp_final['UploadId']
                copied_parts = []
                pn = 1
                chunk_size = 512 * 1024 * 1024  # 512MB por parte
                try:
                    for start in range(0, total_size, chunk_size):
                        end = min(start + chunk_size - 1, total_size - 1)
                        print(f"📤 Copiando rango {pn}: bytes {start:,}-{end:,}")
                        resp = s3.upload_part_copy(
                            Bucket=final_bucket,
                            Key=final_key,
                            PartNumber=pn,
                            UploadId=final_upload_id,
                            CopySource=copy_source,
                            CopySourceRange=f"bytes={start}-{end}"
                        )
                        copied_parts.append({
                            'ETag': resp['CopyPartResult']['ETag'],
                            'PartNumber': pn
                        })
                        pn += 1
                    s3.complete_multipart_upload(
                        Bucket=final_bucket,
                        Key=final_key,
                        UploadId=final_upload_id,
                        MultipartUpload={'Parts': copied_parts}
                    )
                    print(f"✅ Multipart copy completado: {pn-1} partes copiadas con encriptación KMS")
                except Exception:
                    s3.abort_multipart_upload(
                        Bucket=final_bucket,
                        Key=final_key,
                        UploadId=final_upload_id
                    )
                    print(f"❌ Multipart copy abortado por error")
                    raise
            
            # Eliminar archivo de staging después de copia exitosa
            try:
                s3.delete_object(Bucket=staging_bucket, Key=staging_key)
                print(f"🗑️ Archivo staging eliminado: s3://{staging_bucket}/{staging_key}")
            except Exception as e_del:
                print(f"⚠️ No se pudo eliminar staging (no crítico): {e_del}")

            if USE_BUSINESS_DESTINO and nombre_archivo_final and tabla_tracking is not None:
                regla_tracking = resolver_regla_out(nombre_archivo_final, reglas_out)
                if regla_tracking:
                    registrar_archivo_salida(
                        tabla_tracking,
                        regla_tracking,
                        prn_basename,
                        nombre_archivo_final
                    )
                else:
                    print(f"⚠️ Sin regla OUT para tracking: {nombre_archivo_final}")
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'AccessDenied':
                print(f"❌ ERROR: Sin permisos para escribir en bucket destino: {final_bucket}")
                print(f"🔧 SOLUCIÓN: Agregar permisos s3:PutObject al rol de Glue para bucket: {final_bucket}")
                print(f"📋 El archivo quedó disponible en staging: s3://{staging_bucket}/{staging_key}")
                raise
            else:
                raise
        
        # Limpiar particiones temporales
        print("🗑️ Limpiando archivos temporales...")
        for part_key in part_files:
            s3.delete_object(Bucket=temp_bucket, Key=part_key)
        
        try:
            s3.delete_object(Bucket=temp_bucket, Key=temp_prefix.rstrip('/') + '/_SUCCESS')
        except:
            pass
        
        concat_end_time = time.time()
        concat_time = concat_end_time - concat_start_time
        
        print(f"✅ Proceso completo en {concat_time:.2f}s")
        print(f"📁 Archivo final: s3://{final_bucket}/{final_key}")
        print(f"📊 Tamaño: {total_size:,} bytes, Particiones: {len(part_files)} → 1 archivo")
        
    except Exception as e:
        print(f"❌ Error en concatenar_particiones_s3 para {tipo_archivo}: {e}")
        print("⚠️ Conservando archivos temporales para análisis")
        raise

# =============================================================
# Limpieza de staging en modo plano
# =============================================================
def limpiar_staging_flat_mode(staging_root, folder_input, prn_basename):
    """Elimina objetos residuales bajo las carpetas de partes temporales
    cuando FLAT_MODE está activo. staging_root ejemplo:
    s3://dev-XXX-input-pesvno-bucket/<folder>/<basename>/PESVNO
    Se borran:
      aceptados/formateado/temp_parts/*
      duplicados/formateado/temp_parts/*
      inconsistencias/formateado/temp_parts/*
    """
    try:
        if 'FLAT_MODE' not in globals() or not FLAT_MODE:
            return
        if not staging_root.startswith('s3://'):
            print(f"[FLAT_MODE][LIMPIEZA] staging_root inválido: {staging_root}")
            return
        bucket = staging_root.replace('s3://','').split('/',1)[0]
        prefix_base = staging_root.replace('s3://','').split('/',1)[1].rstrip('/')
        categorias = ['aceptados','duplicados','inconsistencias']
        for cat in categorias:
            temp_prefix = f"{prefix_base}/{cat}/formateado/temp_parts/"
            print(f"[FLAT_MODE][LIMPIEZA] Revisando {bucket}/{temp_prefix}")
            continuation = None
            eliminados = 0
            while True:
                kwargs = {'Bucket': bucket, 'Prefix': temp_prefix}
                if continuation:
                    kwargs['ContinuationToken'] = continuation
                resp = s3.list_objects_v2(**kwargs)
                for obj in resp.get('Contents', []):
                    key = obj['Key']
                    try:
                        s3.delete_object(Bucket=bucket, Key=key)
                        eliminados += 1
                    except Exception as e_del:
                        print(f"[FLAT_MODE][WARN] No se pudo eliminar {key}: {e_del}")
                if resp.get('IsTruncated'):
                    continuation = resp.get('NextContinuationToken')
                else:
                    break
            print(f"[FLAT_MODE][LIMPIEZA] {cat}: {eliminados} objetos eliminados")
        print("[FLAT_MODE][LIMPIEZA] Limpieza staging completada")
    except Exception as e:
        print(f"[FLAT_MODE][ERROR] Limpieza staging falló: {e}")

## Limpieza: funciones de concatenación/multipart eliminadas al usar escritura estándar directa.

# (Eliminada la lógica de escritura optimizada de archivo único. Ahora se escriben
# directamente los DataFrames repartitionados estándar.)

def format_field(value, length, align='left', fill_char=' '):
    """
    Formatea un campo a una longitud específica.
    :param value: Valor a formatear
    :param length: Longitud deseada
    :param align: 'left' para alinear a la izquierda, 'right' para la derecha
    :param fill_char: Carácter para rellenar
    :return: Cadena formateada
    """
    if value is None:
        value = ''
    value = str(value)
    if align == 'left':
        return value.ljust(length, fill_char)[:length]
    else:
        return value.rjust(length, fill_char)[:length]

# UDFs para cada campo
format_tip = udf(lambda x: format_field(x, 1), StringType())
format_ced = udf(lambda x: format_field(x, 11, 'right', '0'), StringType())
format_nom = udf(lambda x: format_field(x, 45), StringType())
format_fec_exp = udf(lambda x: format_field(x, 8), StringType())
format_mun_exp = udf(lambda x: format_field(x, 15), StringType())
format_dep_exp = udf(lambda x: format_field(x, 15), StringType())
format_vig = udf(lambda x: format_field(x, 2), StringType())
format_genero = udf(lambda x: format_field(x, 1), StringType())
format_edad = udf(lambda x: format_field(x, 3, 'right', '0'), StringType())
format_edad_rango = udf(lambda x: format_field(x, 5), StringType())
format_filler = udf(lambda x: format_field('', 51), StringType())

# UDFs adicionales para inconsistencias
format_nom1_inc = udf(lambda x: format_field(x, 45), StringType())
format_nom2_inc = udf(lambda x: format_field(x, 45), StringType())
format_coderr_inc = udf(lambda x: format_field(x, 2, 'right', '0'), StringType())
format_filler_inc = udf(lambda x: format_field('', 60), StringType())

# Función para crear una cadena de longitud fija para válidos y duplicados
def create_fixed_length_string_valid(row):
    return (
        format_tip(row['TIP']) +
        format_ced(row['CED']) +
        format_nom(row['NOM']) +
        format_fec_exp(row['FEC-EXP']) +
        format_mun_exp(row['MUN-EXP']) +
        format_dep_exp(row['DEP-EXP']) +
        format_vig(row['VIG']) +
        format_genero(row['GENERO']) +
        format_edad(row['EDAD']) +
        format_edad_rango(row['EDAD-RANGO']) +
        format_filler('')
    )

# Función para crear una cadena de longitud fija para inconsistencias
def create_fixed_length_string_inconsistencies(row):
    return (
        format_tip(row['TIP-INC']) +
        format_ced(row['CED-INC']) +
        format_nom1_inc(row['NOM1-INC']) +
        format_nom2_inc(row['NOM2-INC']) +
        format_fec_exp(row['FEC-EXP-INC']) +
        format_mun_exp(row['MUN-EXP-INC']) +
        format_dep_exp(row['DEP-EXP-INC']) +
        format_vig(row['VIG-INC']) +
        format_coderr_inc(row['CODERR-INC']) +
        format_filler_inc('')
    )

# Registrar las UDFs
create_fixed_length_string_valid_udf = udf(create_fixed_length_string_valid, StringType())
create_fixed_length_string_inconsistencies_udf = udf(create_fixed_length_string_inconsistencies, StringType())

# Funciones de formateo
def format_tipoid(col):
    return lpad(col, 1, ' ')

def format_numeroid(col):
    return lpad(col, 11, '0')

def format_nombre(col):
    return rpad(substring(col, 1, 45), 45, ' ')

def format_fecha_expedicion(col):
    return rpad(substring(col, 1, 8), 8, ' ')

def format_municipio_expedicion(col):
    return rpad(substring(col, 1, 15), 15, ' ')

def format_departamento_expedicion(col):
    return rpad(substring(col, 1, 15), 15, ' ')

def format_codigo_vigencia(col):
    return lpad(col, 2, '0')

def format_genero(col):
    return rpad(substring(col, 1, 1), 1, ' ')

def format_edad(col):
    return lpad(col, 3, '0')

def format_rango_edad(col):
    return rpad(substring(col, 1, 5), 5, ' ')

def format_filler(length):
    return lit(' ' * length)

# Función para formatear campos de inconsistencias
def format_codigo_error(col):
    return lpad(col, 2, '0')

# Función para combinar todos los campos en una sola cadena de longitud fija
def combine_fields_validos(row):
    return concat(
        format_tipoid(row["TIPO_ID"]),
        format_numeroid(row["NUMERO_ID"]),
        format_nombre(row["NOMBRE_COMPLETO"]),
        format_fecha_expedicion(row["FECHA_EXPEDICION_DOCUMENTO"]),
        format_municipio_expedicion(row["NOMBRE_MUNICIPIO_EXPEDICION"]),
        format_departamento_expedicion(row["NOMBRE_DEPARTAMENTO_EXPEDICION"]),
        format_codigo_vigencia(row["CODIGO_ESTADO_VIGENCIA_DOCUMENTO"]),
        format_genero(row["GENERO_SEXO"]),
        format_edad(row["EDAD_CALCULADA"]),
        format_edad_rango(row["RANGO_EDAD"]),
        format_filler(51)
    )

def combine_fields_inconsistencias(row):
    return concat(
        format_tipoid(row["TIP-INC"]),
        lit(";"),
        format_numeroid(row["CED-INC"]),
        lit(";"),
        format_nombre(row["NOM1-INC"]),
        lit(";"),
        format_nombre(row["NOM2-INC"]),
        lit(";"),
        format_fecha_expedicion(row["FEC-EXP-INC"]),
        lit(";"),
        format_municipio_expedicion(row["MUN-EXP-INC"]),
        lit(";"),
        format_departamento_expedicion(row["DEP-EXP-INC"]),
        lit(";"),
        format_codigo_vigencia(row["VIG-INC"]),
        lit(";"),
        format_codigo_error(row["CODERR-INC"]),
        lit(";")
    )

# Registrar la UDF
combine_fields_inconsistencias_udf = udf(combine_fields_inconsistencias, StringType())


######################################################################## Inicialización del job de Glue ######################################################################################

# ⏱️ TIEMPO DE INICIO DEL PROCESO
start_time = time.time()
start_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print(f"🚀 ⏱️ INICIO DEL PROCESO PESVNO: {start_timestamp}")
print(f"⏱️ Timestamp de inicio: {start_time}")

time_checkpoint_1 = time.time()
print(f"⏱️ CHECKPOINT 1 - Iniciando resolución de argumentos: {datetime.now().strftime('%H:%M:%S')}")

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'nombreval', 'nombreinc', 'nombredup', 'imputbucket', 'OUTPUTBUCKET_REDSHIFT', 'OUTPUTBUCKET_ONPREMISES', 'params_step_function', 'db_edf_relationalize', 'kms_carguedebases_key', 'kms_dynamic_batch_key', 'kms_destino_default_key', 'business_table', 'object_tracking_table', 'nombre_producto', 'redshift_jdbc_url', 'redshift_cluster_id', 'redshift_database', 'redshift_user', 'redshift_schema', 'redshift_secret_arn'])

time_checkpoint_2 = time.time()
elapsed_1_to_2 = time_checkpoint_2 - time_checkpoint_1
print(f"⏱️ CHECKPOINT 2 - Argumentos resueltos en {elapsed_1_to_2:.2f}s: {datetime.now().strftime('%H:%M:%S')}")

print(f"⏱️ CHECKPOINT 3 - Iniciando contexto Spark: {datetime.now().strftime('%H:%M:%S')}")
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ========== CONFIGURACIONES DE SPARK OPTIMIZADAS PARA PESVNO ==========
print("🔧 APLICANDO CONFIGURACIONES DE OPTIMIZACIÓN SPARK...")

try:
    # Configurar Adaptive Query Execution (AQE) - Estas SÍ se pueden modificar en runtime
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")  # 128MB
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "268435456")  # 256MB
    spark.conf.set("spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled", "true")

    # Configuraciones SQL que SÍ se pueden modificar
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
    spark.conf.set("spark.sql.files.openCostInBytes", "4194304")  # 4MB

    # Optimizaciones para joins y broadcast
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "52428800")  # 50MB
    spark.conf.set("spark.sql.broadcastTimeout", "600")
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
    
    # ⚠️ IMPORTANTE: spark.driver.maxResultSize es INMUTABLE y NO puede configurarse aquí
    # Para aumentar el límite (actual: 1GB por defecto), debe agregarse como parámetro del Job de Glue:
    # --conf spark.driver.maxResultSize=4g
    # Esto evitará errores: "Total size of serialized results bigger than maxResultSize"
    print("ℹ️ Para resultados grandes, asegurar que el Job tenga: --conf spark.driver.maxResultSize=4g")

    # Configuraciones para Cost-Based Optimizer (CBO)
    spark.conf.set("spark.sql.cbo.enabled", "true")
    spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")
    spark.conf.set("spark.sql.cbo.planStats.enabled", "true")
    spark.conf.set("spark.sql.cbo.starSchemaDetection", "true")

    # Configuraciones específicas para procesamiento de strings y UDFs
    spark.conf.set("spark.sql.codegen.wholeStage", "true")
    spark.conf.set("spark.sql.codegen.maxFields", "200")
    spark.conf.set("spark.sql.codegen.aggregate.map.twolevel.enabled", "true")

    # Configuraciones de cache y persistencia
    spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", "true")
    spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "10000")

    print("✅ CONFIGURACIONES SPARK APLICADAS EXITOSAMENTE")
    print("📊 CONFIGURACIONES ACTIVAS:")
    print(f"   🔄 AQE Habilitado: {spark.conf.get('spark.sql.adaptive.enabled')}")
    print(f"   � Particiones Shuffle: {spark.conf.get('spark.sql.shuffle.partitions')}")
    print(f"   🔗 Broadcast Threshold: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")
    print(f"   🧠 CBO Habilitado: {spark.conf.get('spark.sql.cbo.enabled')}")
    print("="*70)
    
except Exception as e:
    print(f"⚠️ Error aplicando algunas configuraciones: {e}")
    print("📌 Continuando con configuraciones por defecto...")

# NOTA: Las siguientes configuraciones deben establecerse en los parámetros del job de Glue:
# --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
# --conf spark.sql.execution.arrow.pyspark.enabled=true
# --conf spark.hadoop.fs.s3a.connection.maximum=200
# --conf spark.hadoop.fs.s3a.threads.max=20
# --conf spark.hadoop.fs.s3a.multipart.size=134217728

time_checkpoint_3 = time.time()
elapsed_2_to_3 = time_checkpoint_3 - time_checkpoint_2
print(f"⏱️ CHECKPOINT 4 - Contexto Spark inicializado en {elapsed_2_to_3:.2f}s: {datetime.now().strftime('%H:%M:%S')}")
print(f"⏱️ TIEMPO TOTAL INICIALIZACIÓN: {elapsed_1_to_2 + elapsed_2_to_3:.2f}s")


# Configurar el logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Crear un manejador que escriba en sys.stdout
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Extraer los valores de los argumentos
time_checkpoint_params_start = time.time()
print(f"⏱️ CHECKPOINT 5 - Iniciando procesamiento de parámetros: {datetime.now().strftime('%H:%M:%S')}")

nombre_val  = args['nombreval']
nombre_inc  = args['nombreinc']
imputbucket = args['imputbucket']
db_edf_relationalize = args['db_edf_relationalize']
kms_carguedebases_key = args['kms_carguedebases_key']
kms_dynamic_batch_key = args['kms_dynamic_batch_key']
kms_destino_default_key = args['kms_destino_default_key']
nombre_producto = args['nombre_producto']
output_onpremises_bucket = args['OUTPUTBUCKET_ONPREMISES'].replace('s3://', '').split('/')[0]
output_redshift_bucket = args['OUTPUTBUCKET_REDSHIFT'].replace('s3://', '').split('/')[0]

tabla_business = dynamodb.Table(args['business_table'])
tabla_tracking = dynamodb.Table(args['object_tracking_table'])

print(f"🔐 KMS Key CargaDeBases: {kms_carguedebases_key}")
print(f"🔐 KMS Key Dynamic Batch: {kms_dynamic_batch_key}")
print(f"🔐 KMS Key destino dinámico (business DESTINO): {kms_destino_default_key}")
print(f"📋 Business table: {args['business_table']}")
print(f"📋 Object tracking table: {args['object_tracking_table']}")
print(f"📋 Producto: {nombre_producto}")
print(f"📦 Bucket Redshift/Dynamic Batch: {output_redshift_bucket}")

# NOTA: Los buckets de Dynamic Batch usan prefijo 'prd-' en producción.
# NO se debe normalizar/cambiar los nombres de bucket recibidos de Terraform.
print(f"📦 Bucket de entrada (imputbucket): {imputbucket}")

variables = json.loads(args['params_step_function'])
validar     = float(variables['validar'])
particulas  = float(variables['particulas'])
archivoprn  = variables['archivoprn']
tag_derechos= variables['derechos']
ejecucion = variables['ejecucion']
nombre_proceso = variables['nombre_proceso']
nombre_archivos = variables['nombre_archivos']
destino = variables['destino']

time_checkpoint_params_end = time.time()
elapsed_params = time_checkpoint_params_end - time_checkpoint_params_start
print(f"⏱️ CHECKPOINT 6 - Parámetros procesados en {elapsed_params:.2f}s: {datetime.now().strftime('%H:%M:%S')}")
print(f"📋 Archivo a procesar: {archivoprn}")
print(f"📋 Validar: {validar}, Partículas: {particulas}, Derechos: {tag_derechos}")

time_checkpoint_bucket_start = time.time()
print(f"⏱️ CHECKPOINT 7 - Configurando bucket de salida: {datetime.now().strftime('%H:%M:%S')}")

if destino.lower() == "redshift":
    outputbucket_seleccionado_raw = args['OUTPUTBUCKET_REDSHIFT']
    print(f"🎯 DESTINO SELECCIONADO (raw): Redshift - Bucket: {outputbucket_seleccionado_raw}")
else:
    outputbucket_seleccionado_raw = args['OUTPUTBUCKET_ONPREMISES']
    print(f"🎯 DESTINO SELECCIONADO (raw): OnPremises - salida vía business table DESTINO (cargue-de-bases solo logs)")

# NOTA: No normalizar buckets - usar directamente los valores de Terraform
# Los buckets de Dynamic Batch usan prefijo 'prd-' en producción
outputbucket_seleccionado = outputbucket_seleccionado_raw.replace('s3://', '')
print(f"📦 Bucket de salida legacy (outputbucket_seleccionado): {outputbucket_seleccionado}")

USE_BUSINESS_DESTINO = destino.lower() == 'onpremises'
reglas_out = []

# CORRECCIÓN CRÍTICA: usar imputbucket como fallback en lugar de outputbucket_seleccionado
# para derivar correctamente el staging_bucket (debe tener permisos completos)
source_bucket = args.get('sourcebucket', imputbucket)
folder_input = archivoprn.split('/')[0] if '/' in archivoprn else archivoprn.replace('.prn','').replace('.PRN','')
prn_basename = os.path.basename(archivoprn).replace('.PRN', '').replace('.prn', '')

# Prefijo base real donde vive el YAML/PRN (primer segmento si hay carpeta, o '' si está en raíz)
yaml_base_prefix = archivoprn.split('/')[0] if '/' in archivoprn else ''

FLAT_MODE = folder_input.lower() == 'pesvno'
FINAL_FLAT_PREFIX = None

# Derivar staging bucket (SIEMPRE usar el bucket input para staging, para consistencia)
staging_bucket = imputbucket

if USE_BUSINESS_DESTINO:
    reglas_out = cargar_reglas_out(tabla_business, nombre_producto)
    destino_buckets_business = extraer_buckets_destino(reglas_out)
    outputbucket = f"s3://{staging_bucket}/{folder_input}/{prn_basename}/PESVNO"
    print(f"[ONPREMISES] Reglas OUT cargadas: {len(reglas_out)}")
    print(f"[ONPREMISES] Partes staging -> {outputbucket}")
elif FLAT_MODE:
    outputbucket = f"s3://{staging_bucket}/{folder_input}/{prn_basename}/PESVNO"
    FINAL_FLAT_PREFIX = f"s3://{outputbucket_seleccionado}"
    print(f"[FLAT_MODE] Activado para carpeta 'pesvno'. Partes -> {outputbucket}")
    print(f"[FLAT_MODE] Bucket staging (pesvno): {staging_bucket}")
    print(f"[FLAT_MODE] Archivos planos finales en RAÍZ: {FINAL_FLAT_PREFIX}/<archivo>.{{val|dup|inc}}")
else:
    outputbucket = f"s3://{staging_bucket}/{folder_input}/{prn_basename}/PESVNO"
    origin_bucket_prn = imputbucket
    if yaml_base_prefix:
        FINAL_FLAT_PREFIX = f"s3://{origin_bucket_prn}/{yaml_base_prefix}"
    else:
        FINAL_FLAT_PREFIX = f"s3://{origin_bucket_prn}"
    print(f"[HIERARCHICAL_MODE] Activado para carpeta '{folder_input}'. Partes -> {outputbucket}")
    print(f"[HIERARCHICAL_MODE] Bucket origen notificación (PRN/YAML): {origin_bucket_prn}")
    print(f"[HIERARCHICAL_MODE] Bucket staging (pesvno): {staging_bucket}")
    print(f"[HIERARCHICAL_MODE] Archivos finales en subcarpeta origen: {FINAL_FLAT_PREFIX}/<archivo>.{{val|dup|inc}}")
    if origin_bucket_prn != staging_bucket:
        print(f"[HIERARCHICAL_MODE] Movimiento final cross-bucket: {staging_bucket} -> {origin_bucket_prn}")

def obtener_final_path(nombre_archivo):
    """Resuelve la ruta S3 destino final para un archivo OUT."""
    if USE_BUSINESS_DESTINO:
        regla = resolver_regla_out(nombre_archivo, reglas_out)
        if not regla:
            raise ValueError(f"No se encontró regla OUT en business table para: {nombre_archivo}")
        destino_str = regla.get('DESTINO', '')
        if not destino_str:
            raise ValueError(f"Regla OUT sin DESTINO para archivo: {nombre_archivo}")
        print(f"[ONPREMISES] Regla ID={regla.get('ID')} -> DESTINO={destino_str} | archivo={nombre_archivo}")
        return parse_destino_s3(destino_str)
    return f"{FINAL_FLAT_PREFIX}/"

outputbucketOriginalName = outputbucket_seleccionado

# Construir la ruta completa del archivo
ruta_archivo = f"s3://{imputbucket}/{archivoprn}"

time_checkpoint_bucket_end = time.time()
elapsed_bucket = time_checkpoint_bucket_end - time_checkpoint_bucket_start
print(f"⏱️ CHECKPOINT 8 - Configuración de bucket completada en {elapsed_bucket:.2f}s: {datetime.now().strftime('%H:%M:%S')}")
print(f"📂 Ruta de salida (partes staging): {outputbucket}")
# Mostrar destino final según modo
ejemplo_nombre = prn_basename
if USE_BUSINESS_DESTINO:
    print(f"📂 Ruta final archivos planos: business DESTINO/<archivo>.{{val|dup|inc}}")
else:
    print(f"📂 Ruta final archivos planos: {FINAL_FLAT_PREFIX}/{ejemplo_nombre}.{{val|dup|inc}}")
print(f"📂 Ruta archivo PRN: {ruta_archivo}")

# Configuraciones adicionales dinámicas aplicadas después de la detección del tamaño
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.broadcastTimeout", "600")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
print("==============================================================")

"""Control de eliminación del archivo PRN de entrada.

Por requerimiento actual NO se debe eliminar el archivo original .prn al finalizar.
Si en algún momento se desea volver a habilitar la eliminación, establecer la variable
de entorno DELETE_INPUT_PRN=1 en la ejecución del job (o pasarla como parámetro en Glue).
"""
DELETE_INPUT_PRN = os.environ.get('DELETE_INPUT_PRN', '0') == '1'

# Función opcional para eliminar el PRN (deshabilitada por defecto)
def eliminar_objeto_s3(bucket_name=imputbucket, file_key=archivoprn):
    if not DELETE_INPUT_PRN:
        print(f"[INFO] Se omite eliminación del archivo de entrada {file_key} (DELETE_INPUT_PRN=0)")
        return
    try:
        print(f"Eliminando objeto {file_key}")
        s3.delete_object(Bucket=bucket_name, Key=file_key)
        print("Archivo de entrada eliminado correctamente.")
    except Exception as e:
        print(f"[WARN] No se pudo eliminar el archivo de entrada {file_key}: {e}")



# Validar si se recibieron los parámetros obligatorios
if 'archivoprn' not in variables or 'nombreval' not in args or 'nombreinc' not in args:
    raise ValueError("Faltan parámetros obligatorios: archivoprn, nombreval, nombreinc")

print(f"⏱️ CHECKPOINT 8.5 - Configuración estática aplicada: {datetime.now().strftime('%H:%M:%S')}")

# Parámetros de conexión a Redshift
redshift_jdbc_url = args.get('redshift_jdbc_url', '')
redshift_cluster_id = args.get('redshift_cluster_id', '')
redshift_database = args.get('redshift_database', 'dba_batch')
redshift_schema = args.get('redshift_schema', 'edf_views')
redshift_secret_arn = args.get('redshift_secret_arn', '')

# Credenciales (usuario/contraseña) recuperadas desde Secrets Manager.
redshift_credentials = obtener_credenciales_redshift(redshift_secret_arn)
redshift_user = redshift_credentials['username']
redshift_password = redshift_credentials['password']

print(f"📊 Redshift Config: cluster={redshift_cluster_id}, db={redshift_database}, user={redshift_user}, schema={redshift_schema}")

time_checkpoint_read_start = time.time()
print(f"⏱️ CHECKPOINT 9 - Iniciando lectura del archivo PRN: {datetime.now().strftime('%H:%M:%S')}")
print(f"Se inicia ejecución del archivo: {archivoprn}")

# ========== DETECCIÓN INTELIGENTE DEL TAMAÑO DEL ARCHIVO ==========
def detectar_tamano_archivo_optimizado(spark, ruta_archivo):
    """
    Detecta el tamaño del archivo de manera eficiente para configurar Spark dinámicamente.
    Retorna: (categoria, estimacion_registros, configuracion_spark)
    """
    print(f"🔍 Analizando tamaño del archivo: {ruta_archivo}")
    
    # Leer solo una muestra pequeña para estimar el tamaño total
    sample_df = spark.read.text(ruta_archivo).limit(1000)
    sample_count = sample_df.count()
    
    if sample_count < 1000:
        # Archivo muy pequeño, leer todo para conteo exacto
        df_completo = spark.read.text(ruta_archivo)
        total_registros = df_completo.count()
        df_completo.unpersist()  # Limpiar de memoria
    else:
        # Estimar basado en tamaño del archivo
        try:
            # Usar boto3 para obtener el tamaño del archivo S3
            bucket_name = ruta_archivo.replace('s3://', '').split('/')[0]
            file_key = '/'.join(ruta_archivo.replace('s3://', '').split('/')[1:])
            
            response = s3.head_object(Bucket=bucket_name, Key=file_key)
            file_size_bytes = response['ContentLength']
            
            # Estimar registros basado en el tamaño promedio de línea (asumiendo ~157 chars por línea)
            avg_line_size = 157
            total_registros = file_size_bytes // avg_line_size
            
            print(f"📊 Tamaño del archivo: {file_size_bytes:,} bytes")
            print(f"📊 Registros estimados: {total_registros:,}")
            
        except Exception as e:
            print(f"⚠️ Error estimando tamaño del archivo: {e}")
            # Fallback: usar sample para estimar
            primera_linea = sample_df.first()
            avg_line_size = len(primera_linea[0]) if primera_linea else 157
            total_registros = sample_count * 100  # Estimación conservadora
    
    # Categorizar y configurar
    if total_registros < 10000:  # Menos de 10K registros
        categoria = "PEQUEÑO"
        config = {
            "partitions": 2,
            "worker_type": "G.1X",
            "max_workers": 2,
            "broadcast_threshold": "200MB",
            "coalesce_partitions": "false"
        }
    elif total_registros < 1000000:  # Menos de 1M registros
        categoria = "MEDIANO"
        config = {
            "partitions": 8,
            "worker_type": "G.1X", 
            "max_workers": 4,
            "broadcast_threshold": "100MB",
            "coalesce_partitions": "true"
        }
    elif total_registros < 10000000:  # Menos de 10M registros
        categoria = "GRANDE"
        config = {
            "partitions": 64,
            "worker_type": "G.2X",
            "max_workers": 10,
            "broadcast_threshold": "50MB",
            "coalesce_partitions": "true"
        }
    else:  # 10M+ registros
        categoria = "MUY_GRANDE"
        config = {
            "partitions": 200,
            "worker_type": "G.2X",
            "max_workers": 20,
            "broadcast_threshold": "20MB",
            "coalesce_partitions": "true"
        }
    
    print(f"📈 CATEGORÍA DETECTADA: {categoria}")
    print(f"📈 Registros estimados: {total_registros:,}")
    print(f"📈 Configuración aplicada: {config}")
    
    return categoria, total_registros, config

# Detectar tamaño del archivo y configurar dinámicamente
CATEGORIA_ARCHIVO, ESTIMACION_REGISTROS, SPARK_CONFIG = detectar_tamano_archivo_optimizado(spark, ruta_archivo)

# ========== CONFIGURACIÓN DINÁMICA DE SPARK ==========
print(f"🔧 Aplicando configuración DINÁMICA para archivo {CATEGORIA_ARCHIVO}")

# Aplicar configuraciones dinámicas
SPARK_PARTITIONS_DINAMICAS = SPARK_CONFIG["partitions"]
spark.conf.set("spark.sql.shuffle.partitions", str(SPARK_PARTITIONS_DINAMICAS))
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", SPARK_CONFIG["broadcast_threshold"])
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", SPARK_CONFIG["coalesce_partitions"])

# Configuraciones específicas por tamaño
if CATEGORIA_ARCHIVO == "PEQUEÑO":
    spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.files.maxPartitionBytes", "256MB")
elif CATEGORIA_ARCHIVO == "MEDIANO":
    spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "2")
    spark.conf.set("spark.default.parallelism", "16")
    spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")
elif CATEGORIA_ARCHIVO == "GRANDE":
    spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "8")
    spark.conf.set("spark.default.parallelism", "64")
    spark.conf.set("spark.sql.files.maxPartitionBytes", "64MB")
else:  # MUY_GRANDE
    spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "16")
    spark.conf.set("spark.default.parallelism", "200")
    spark.conf.set("spark.sql.files.maxPartitionBytes", "32MB")

print(f"   🔢 Particiones dinámicas: {SPARK_PARTITIONS_DINAMICAS}")
print(f"   🔗 Broadcast threshold: {SPARK_CONFIG['broadcast_threshold']}")
print(f"   📦 Coalesce habilitado: {SPARK_CONFIG['coalesce_partitions']}")

# Leer el archivo PRN con configuración optimizada
print(f"📖 Leyendo archivo PRN con configuración optimizada...")
df_base_PESVNO = spark.read.text(ruta_archivo)

# Solo repartir si es necesario (archivos grandes)
if CATEGORIA_ARCHIVO in ["GRANDE", "MUY_GRANDE"]:
    df_base_PESVNO = df_base_PESVNO.repartition(SPARK_PARTITIONS_DINAMICAS)
    print(f"🔄 Archivo reparticionado a {SPARK_PARTITIONS_DINAMICAS} particiones")

time_checkpoint_read_end = time.time()
elapsed_read = time_checkpoint_read_end - time_checkpoint_read_start
print(f"⏱️ CHECKPOINT 10 - Archivo PRN leído y configurado en {elapsed_read:.2f}s: {datetime.now().strftime('%H:%M:%S')}")

# ========== SISTEMA DE CONTEO OPTIMIZADO ==========
_count_cache_global = {}
ENABLE_LOG_COUNTS = os.environ.get('ENABLE_LOG_COUNTS', '0') == '1'  # DESHABILITADO por defecto en producción (usar env var para habilitar)

def cached_count_global(name, dframe):
    """
    Sistema de conteo optimizado que evita acciones costosas excepto cuando es necesario.
    Usa estimaciones inteligentes basadas en el análisis inicial del archivo.
    """
    if not ENABLE_LOG_COUNTS:
        # Retornar estimaciones basadas en el análisis inicial
        if name == 'base_prn_leidos':
            return ESTIMACION_REGISTROS
        elif 'post_' in name or 'entrada' in name:
            return ESTIMACION_REGISTROS  # Estimación conservadora
        return 0  # Placeholder para otros casos
    
    if name not in _count_cache_global:
        _count_cache_global[name] = dframe.count()
    return _count_cache_global[name]

# Informar estado de conteos
if ENABLE_LOG_COUNTS:
    print("✅ CONTEOS HABILITADOS - Se mostrarán métricas detalladas de registros en cada etapa")
else:
    print("⚠️ CONTEOS DESHABILITADOS - Los conteos retornarán 0 (modo optimizado)")

print(f"🎯 REGISTROS ESTIMADOS EN EL ARCHIVO: {cached_count_global('base_prn_leidos', df_base_PESVNO):,}")

# Mostrar primeras líneas para debug (solo si es archivo pequeño)
if CATEGORIA_ARCHIVO == "PEQUEÑO":
    print("🔍 Primeras 3 líneas del archivo:")
    primeras_lineas = df_base_PESVNO.limit(3).collect()
    for i, row in enumerate(primeras_lineas, 1):
        print(f"   Línea {i}: '{row[0]}' (longitud: {len(row[0])})")
else:
    print("🔍 Archivo grande detectado - omitiendo vista previa para optimizar rendimiento")



# Determinar el formato basado en la longitud de la segunda línea del archivo PRN
time_checkpoint_format_start = time.time()
print(f"⏱️ CHECKPOINT 11 - Determinando formato del archivo: {datetime.now().strftime('%H:%M:%S')}")

second_line_length = df_base_PESVNO.limit(2).collect()[1][0].__len__()

if second_line_length == 12:  # 1-11
    formato = "1-11"
    schema = StructType([
        StructField("tipoid", StringType(), True),
        StructField("numeroid", StringType(), True),
        StructField("nombre", StringType(), True)
    ])
elif second_line_length == 27:  # 1-11-15
    formato = "1-11-15"
    schema = StructType([
        StructField("tipoid", StringType(), True),
        StructField("numeroid", StringType(), True),
        StructField("nombre", StringType(), True)
    ])
else:  # 1-11-45
    formato = "1-11-45"
    schema = StructType([
        StructField("tipoid", StringType(), True),
        StructField("numeroid", StringType(), True),
        StructField("nombre", StringType(), True)
    ])

time_checkpoint_format_end = time.time()
elapsed_format = time_checkpoint_format_end - time_checkpoint_format_start
print(f"⏱️ CHECKPOINT 12 - Formato determinado en {elapsed_format:.2f}s: {datetime.now().strftime('%H:%M:%S')}")
print(f"📋 Formato detectado: {formato} (longitud línea: {second_line_length})")

# ========== PROCESAMIENTO CONFIGURACIÓN YAML ULTRA-ROBUSTO ==========
time_checkpoint_yaml_start = time.time()
print(f"⏱️ CHECKPOINT 12.5 - Procesando configuración YAML: {datetime.now().strftime('%H:%M:%S')}")
print(f"🔍 DEBUG - Archivo PRN recibido: {archivoprn}")

try:
    # Procesar configuración YAML completa
    config_yaml, nombres_salida_yaml = configurador_yaml.procesar_configuracion_completa(
        ruta_prn=ruta_archivo,  # ✅ Usar ruta completa de S3 en lugar de solo el nombre del archivo
        formato_archivo=formato
    )
    
    # Extraer parámetros de configuración
    opciones = config_yaml['opciones']
    validar_yaml = opciones['validar']
    particulas_yaml = opciones.get('particulas', 0)
    derechos_yaml = opciones.get('derechos', 1)
    destino_yaml = config_yaml.get('destino', 'onpremises')
    
    # SOBRESCRIBIR variables del step function con las del YAML
    validar = float(validar_yaml)
    particulas = float(particulas_yaml)
    # derechos se usará según el YAML
    
    print(f"✅ Configuración YAML aplicada exitosamente:")
    print(f"   📋 validar: {validar} ({'Ejecutar match' if validar == 1 else 'Sin match'})")
    print(f"   📋 particulas: {particulas}")
    print(f"   📋 derechos: {derechos_yaml}")
    print(f"   📋 destino: {destino_yaml}")
    print(f"   📁 Nombres salida: {nombres_salida_yaml}")
    
except Exception as e:
    print(f"❌ ERROR CRÍTICO procesando YAML: {e}")
    print("🚨 DETENIENDO EJECUCIÓN - Corregir configuración YAML")
    
    # Mostrar información detallada para troubleshooting
    print(f"\n📋 INFORMACIÓN DE TROUBLESHOOTING:")
    print(f"   🔍 Archivo PRN procesado: {archivoprn}")
    print(f"   🔍 Archivo YAML buscado: {archivoprn.replace('.prn', '.yaml').replace('.PRN', '.yaml')}")
    print(f"   🔧 Verificar que el rol de Glue tenga permisos s3:GetObject")
    print(f"   📁 Verificar que ambos archivos (.prn y .yaml) estén en la misma ubicación S3")
    
    raise SystemExit(1)  # Terminar job con error

time_checkpoint_yaml_end = time.time()
elapsed_yaml = time_checkpoint_yaml_end - time_checkpoint_yaml_start
print(f"⏱️ CHECKPOINT 12.6 - Configuración YAML procesada en {elapsed_yaml:.2f}s: {datetime.now().strftime('%H:%M:%S')}")

# ========== MOSTRAR RESUMEN DE CONFIGURACIONES SPARK ACTIVAS ==========
def mostrar_resumen_configuraciones_spark(spark_session):
    """
    Muestra un resumen de las configuraciones de Spark activas
    """
    print("\n" + "="*80)
    print("📊 RESUMEN DE CONFIGURACIONES SPARK ACTIVAS")
    print("="*80)
    
    configuraciones_clave = {
        "🔄 Adaptive Query Execution": "spark.sql.adaptive.enabled",
        " Particiones Shuffle": "spark.sql.shuffle.partitions",
        "📦 Tamaño Max Partición": "spark.sql.files.maxPartitionBytes",
        "🔗 Broadcast Threshold": "spark.sql.autoBroadcastJoinThreshold",
        "⏱️ Broadcast Timeout": "spark.sql.broadcastTimeout",
        "🧠 CBO Habilitado": "spark.sql.cbo.enabled",
        "🔧 CodeGen WholeStage": "spark.sql.codegen.wholeStage",
        "📈 AQE Coalesce Partitions": "spark.sql.adaptive.coalescePartitions.enabled",
        "⚡ AQE Skew Join": "spark.sql.adaptive.skewJoin.enabled"
    }
    
    for descripcion, config_key in configuraciones_clave.items():
        try:
            valor = spark_session.conf.get(config_key)
            print(f"   {descripcion}: {valor}")
        except:
            print(f"   {descripcion}: No configurado")
    
    print("="*80)
    print("🎯 CONFIGURACIÓN OPTIMIZADA PARA PROCESAMIENTO .PRN PESVNO")
    print("="*80 + "\n")

# Mostrar resumen de configuraciones
mostrar_resumen_configuraciones_spark(spark)

# ========== FUNCIÓN PARA REGISTRAR MÉTRICAS DE RENDIMIENTO ==========
def registrar_metricas_spark():
    """
    Registra métricas clave de rendimiento de Spark para monitoreo
    """
    try:
        # Obtener información del Spark Context
        sc = spark.sparkContext
        
        print(f"📈 MÉTRICAS DE SPARK:")
        print(f"   🖥️ Executors Activos: {len(sc.statusTracker().getExecutorInfos())}")
        print(f"   🔢 Default Parallelism: {sc.defaultParallelism}")
        print(f"   💾 Total Memory: {sc.getConf().get('spark.executor.memory', 'default')}")
        print(f"   🔧 Executor Cores: {sc.getConf().get('spark.executor.cores', 'default')}")
        
        # Mostrar configuraciones de optimización aplicadas
        try:
            print(f"   ⚡ Adaptive Enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
            print(f"   🔄 Shuffle Partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
            print(f"   🔗 Broadcast Threshold: {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")
        except:
            print("   📊 Configuraciones específicas no disponibles")
        
    except Exception as e:
        print(f"⚠️ Error obteniendo métricas: {e}")

def validar_ejecucion_match(df, validar_param, formato):
    # Verificar si el primer registro contiene "VALIDAR=NO"
    primer_registro = df.first()
    validar_no_en_archivo = False
    if primer_registro is not None:
        validar_no_en_archivo = "000000000000VALIDAR=NO" in primer_registro['value']
    
    # Determinar si se debe ejecutar el algoritmo de match y la razón
    ejecutar_match = True
    razon_no_ejecucion = None
    
    # Si "VALIDAR=NO" está en el primer registro, eliminarlo del DataFrame
    if validar_no_en_archivo:
        df = df.filter(col("value") != primer_registro['value'])
        ejecutar_match = False
        razon_no_ejecucion = "Se encontró 'VALIDAR=NO' en el primer registro del archivo PRN"
    elif validar_param == 0:
        ejecutar_match = False
        razon_no_ejecucion = "El parámetro 'validar' es igual a cero"
    elif formato == "1-11":
        ejecutar_match = False
        razon_no_ejecucion = "El formato es 1-11, no hay nombres para comparar"
    
    return ejecutar_match, razon_no_ejecucion, df

# Uso de la función
time_checkpoint_validation_start = time.time()
print(f"⏱️ CHECKPOINT 13 - Iniciando validación de ejecución de match: {datetime.now().strftime('%H:%M:%S')}")

ejecutar_match, razon_no_ejecucion, df_procesado = validar_ejecucion_match(df_base_PESVNO, validar, formato)

time_checkpoint_validation_end = time.time()
elapsed_validation = time_checkpoint_validation_end - time_checkpoint_validation_start
print(f"⏱️ CHECKPOINT 14 - Validación completada en {elapsed_validation:.2f}s: {datetime.now().strftime('%H:%M:%S')}")
print(f"🎯 DESPUÉS validar_ejecucion_match: {cached_count_global('post_validacion_match', df_procesado)} registros")

# Imprimir el resultado
if ejecutar_match:
    print("✅ Se ejecutará el algoritmo de match")
else:
    print(f"❌ No se ejecutará el algoritmo de match. Razón: {razon_no_ejecucion}")
    
# AUDITORÍA: Verificar diferencia después de validación
diferencia_validacion = cached_count_global('base_prn_leidos', df_base_PESVNO) - cached_count_global('post_validacion_match', df_procesado)
if diferencia_validacion != 0:
    print(f"⚠️ ALERTA: Se perdieron {diferencia_validacion} registros en validar_ejecucion_match")





# Función para separar los campos según el formato
def separar_campos(row):
    if formato == "1-11":
        return (row[:1], row[1:12], "")
    elif formato == "1-11-15":
        return (row[:1], row[1:12], row[12:27])
    else:  # 1-11-45
        return (row[:1], row[1:12], row[12:].strip())

# Registrar la UDF
separar_campos_udf = udf(separar_campos, schema)

# Aplicar la separación de campos
time_checkpoint_separation_start = time.time()
print(f"⏱️ CHECKPOINT 15 - Iniciando separación de campos: {datetime.now().strftime('%H:%M:%S')}")
print(f"🔄 ANTES de separar campos: {cached_count_global('base_prn_leidos', df_base_PESVNO)} registros")

df_procesado = df_base_PESVNO.select(separar_campos_udf(col("value")).alias("campos")).select("campos.*")

# Solo repartir si es necesario para archivos grandes
if CATEGORIA_ARCHIVO in ["GRANDE", "MUY_GRANDE"]:
    df_procesado = df_procesado.repartition(SPARK_PARTITIONS_DINAMICAS)

time_checkpoint_separation_end = time.time()
elapsed_separation = time_checkpoint_separation_end - time_checkpoint_separation_start
print(f"⏱️ CHECKPOINT 16 - Separación de campos completada en {elapsed_separation:.2f}s: {datetime.now().strftime('%H:%M:%S')}")
print(f"🔄 DESPUÉS de separar campos: {cached_count_global('post_separacion', df_procesado)} registros")

# VALIDACIÓN CRÍTICA INMEDIATAMENTE DESPUÉS DE SEPARAR CAMPOS
entrada_separacion = cached_count_global('base_prn_leidos', df_base_PESVNO)
salida_separacion = cached_count_global('post_separacion', df_procesado)
diferencia_separacion = entrada_separacion - salida_separacion
print(f"\n🔍 AUDITORÍA SEPARACIÓN DE CAMPOS:")
print(f"   📥 ENTRADA: {entrada_separacion} registros")
print(f"   📤 SALIDA:  {salida_separacion} registros")
print(f"   📊 DIFERENCIA: {diferencia_separacion} registros")

if diferencia_separacion != 0:
    print(f"   🚨 ALERTA CRÍTICA: SE PERDIERON {abs(diferencia_separacion)} REGISTROS EN SEPARAR_CAMPOS")
else:
    print(f"   ✅ SEPARACIÓN correcta: No se perdieron registros")

# Verificar si hay registros nulos después de la separación
registros_con_nulos = df_procesado.filter(
    (col("tipoid").isNull()) | 
    (col("numeroid").isNull()) | 
    ((col("nombre").isNull()) & (lit(formato != "1-11")))
).count()
print(f"⚠️ Registros con campos nulos después de separación: {registros_con_nulos}")


# Contar registros
total_registros = cached_count_global('post_separacion', df_procesado)
print(f"📊 Total de registros en base FINAL: {total_registros}")





# ========== FUNCIÓN ULTRA-OPTIMIZADA PARA MANEJAR DUPLICADOS ==========
def manejar_duplicados_mejorado(df, formato):
    duplicates_start_time = time.time()
    print(f"⏱️ 🔄 Iniciando manejo ULTRA-OPTIMIZADO de duplicados: {datetime.now().strftime('%H:%M:%S')}")
    
    # ===== OPTIMIZACIÓN 1: Cache con persist estratégico =====
    if CATEGORIA_ARCHIVO in ["MEDIANO", "GRANDE", "MUY_GRANDE"]:
        df = df.persist()
        print("💾 DataFrame persistido en memoria para archivos medianos/grandes")
    
    # ===== OPTIMIZACIÓN 2: Conteo inteligente sin acciones innecesarias =====
    def smart_count_when_needed(name, dframe, force=False):
        if ENABLE_LOG_COUNTS or force:
            return dframe.count()
        return -1  # Indicador de "no calculado"
    
    entrada_estimada = ESTIMACION_REGISTROS
    print(f"🎯 ENTRADA manejar_duplicados_mejorado: ~{entrada_estimada:,} registros (estimado)")
    
    if formato not in ["1-11-45", "1-11-15", "1-11"]:
        raise ValueError(f"Formato no soportado: {formato}")
    
    # ===== PASO 0: Validar tipoid =====
    step0_start = time.time()
    print("📝 Paso 0: Validando tipoid...")
    df_con_validacion = df.withColumn("tipoid_valido", regexp_extract(col("tipoid"), "^[1-9]$", 0) != "")
    
    # Separar registros con tipoid no válido
    documento_no_valido = df_con_validacion.filter(col("tipoid_valido") == False)
    df_tipoid_valido = df_con_validacion.filter(col("tipoid_valido") == True)
    
    # Validar formato 1-11
    if formato in ["1-11"]:
        print("📝 Aplicando ajuste para formato 1-11...")
        df_tipoid_valido = df_tipoid_valido.withColumn('nombre', lit(None))
    
    # CACHE ESTRATÉGICO: Solo para archivos grandes donde se reutilizará el DF
    if CATEGORIA_ARCHIVO in ["GRANDE", "MUY_GRANDE"]:
        df_tipoid_valido.persist()
        print("💾 Cache aplicado a df_tipoid_valido para archivo grande")
    
    step0_end = time.time()
    print(f"⏱️ Paso 0 completado en {step0_end - step0_start:.2f}s")
    
    # ===== PASO 1: Identificar duplicados exactos (ULTRA-OPTIMIZADO) =====
    step2_start = time.time()
    print("📝 Paso 1: Identificando duplicados exactos con algoritmo optimizado...")
    
    # OPTIMIZACIÓN CRÍTICA: Usar groupBy + having en lugar de Window functions
    if CATEGORIA_ARCHIVO == "PEQUEÑO":
        # Para archivos pequeños, usar método tradicional más simple
        df_groups_exact = df_tipoid_valido.groupBy("tipoid", "numeroid", "nombre").count()
        df_duplicados_keys = df_groups_exact.filter(col("count") > 1).select("tipoid", "numeroid", "nombre")
        
        df_duplicados_exactos = df_tipoid_valido.join(df_duplicados_keys, ["tipoid", "numeroid", "nombre"])
        df_sin_duplicados_exactos = df_tipoid_valido.join(df_duplicados_keys, ["tipoid", "numeroid", "nombre"], "left_anti")
    else:
        # Para archivos grandes, usar broadcast hints si es posible
        window_exact = Window.partitionBy("tipoid", "numeroid", "nombre").orderBy(lit(1))
        df_con_duplicados_exactos = df_tipoid_valido.withColumn("es_duplicado_exacto", row_number().over(window_exact) > 1)
        
        df_duplicados_exactos = df_con_duplicados_exactos.filter(col("es_duplicado_exacto") == True)
        df_sin_duplicados_exactos = df_con_duplicados_exactos.filter(col("es_duplicado_exacto") == False)
    
    step2_end = time.time()
    print(f"⏱️ Duplicados exactos procesados en {step2_end - step2_start:.2f}s")
    
    # ===== PASO 2: Identificar duplicados en ID (ULTRA-OPTIMIZADO) =====
    step3_start = time.time()
    print("📝 Paso 2: Identificando duplicados en ID con algoritmo optimizado...")
    
    # OPTIMIZACIÓN CRÍTICA: Usar groupBy en lugar de Window count
    if CATEGORIA_ARCHIVO == "PEQUEÑO":
        df_groups_id = df_sin_duplicados_exactos.groupBy("tipoid", "numeroid").count()
        df_duplicados_id_keys = df_groups_id.filter(col("count") > 1).select("tipoid", "numeroid")
        
        df_duplicados_id = df_sin_duplicados_exactos.join(df_duplicados_id_keys, ["tipoid", "numeroid"])
        df_validos = df_sin_duplicados_exactos.join(df_duplicados_id_keys, ["tipoid", "numeroid"], "left_anti")
    else:
        window_id = Window.partitionBy("tipoid", "numeroid")
        df_con_duplicados_id = df_sin_duplicados_exactos.withColumn("count_id", count("*").over(window_id))
        
        df_duplicados_id = df_con_duplicados_id.filter(col("count_id") > 1)
        df_validos = df_con_duplicados_id.filter(col("count_id") == 1)
    
    step3_end = time.time()
    print(f"⏱️ Duplicados ID procesados en {step3_end - step3_start:.2f}s")
    
    # ===== PASO 3: Limpiar columnas temporales de forma segura =====
    print("🧹 Limpiando columnas temporales...")
    
    # Lista de columnas a eliminar que pueden existir
    columnas_a_eliminar = ["es_duplicado_exacto", "count_id", "tipoid_valido", "count"]
    
    # Limpiar cada DataFrame de forma segura
    for col_name in columnas_a_eliminar:
        if col_name in df_duplicados_exactos.columns:
            df_duplicados_exactos = df_duplicados_exactos.drop(col_name)
        if col_name in df_duplicados_id.columns:
            df_duplicados_id = df_duplicados_id.drop(col_name)
        if col_name in df_validos.columns:
            df_validos = df_validos.drop(col_name)
        if col_name in documento_no_valido.columns:
            documento_no_valido = documento_no_valido.drop(col_name)
    
    # Liberar cache si se aplicó
    if CATEGORIA_ARCHIVO in ["GRANDE", "MUY_GRANDE"]:
        df_tipoid_valido.unpersist()
        print("🗑️ Cache liberado de df_tipoid_valido")
    
    duplicates_end_time = time.time()
    total_duplicates_time = duplicates_end_time - duplicates_start_time
    
    print(f"⏱️ 🔄 Manejo OPTIMIZADO de duplicados completado en {total_duplicates_time:.2f}s")
    print("✅ Clasificación completada sin conteos costosos innecesarios")
    
    # ===== DEBUG CRÍTICO: Verificar conteos después del procesamiento =====
    try:
        # Conteos forzados para debug crítico
        count_dup_exactos = df_duplicados_exactos.count()
        count_dup_id = df_duplicados_id.count()
        count_validos = df_validos.count()
        count_no_validos = documento_no_valido.count()
        total_output = count_dup_exactos + count_dup_id + count_validos + count_no_validos
        
        print(f"🔍 DEBUG - Conteos REALES después de manejar duplicados:")
        print(f"   📊 Duplicados exactos: {count_dup_exactos:,}")
        print(f"   📊 Duplicados ID: {count_dup_id:,}")
        print(f"   📊 Válidos: {count_validos:,}")
        print(f"   📊 No válidos (tipoid): {count_no_validos:,}")
        print(f"   📊 TOTAL clasificado: {total_output:,}")
        print(f"   📊 Entrada estimada: {entrada_estimada:,}")
        
        if total_output != entrada_estimada:
            diferencia = entrada_estimada - total_output
            print(f"   ⚠️ DIFERENCIA: {diferencia:,} registros ({(diferencia/entrada_estimada)*100:.1f}%)")
        else:
            print(f"   ✅ PERFECTA clasificación: sin pérdida de registros")
            
    except Exception as e:
        print(f"⚠️ Error en debug de conteos: {e}")
    
    return df_duplicados_exactos, df_duplicados_id, df_validos, documento_no_valido

# Uso de la función
time_checkpoint_duplicates_start = time.time()
print(f"⏱️ CHECKPOINT 17 - Iniciando manejo de duplicados: {datetime.now().strftime('%H:%M:%S')}")
print("\n🔄 INICIANDO PROCESO DE MANEJO DE DUPLICADOS")

df_duplicados_exactos, df_duplicados_id, df_validos, documento_no_valido = manejar_duplicados_mejorado(df_procesado, formato)

time_checkpoint_duplicates_end = time.time()
elapsed_duplicates = time_checkpoint_duplicates_end - time_checkpoint_duplicates_start
print(f"⏱️ CHECKPOINT 18 - Manejo de duplicados completado en {elapsed_duplicates:.2f}s: {datetime.now().strftime('%H:%M:%S')}")

# Imprimir resultados con logs detallados
print("\n" + "="*50)
print("📊 RESUMEN DE CLASIFICACIÓN DE REGISTROS")
print("="*50)
dup_exactos_cnt = cached_count_global('res_dup_exactos', df_duplicados_exactos)
dup_id_cnt = cached_count_global('res_dup_id', df_duplicados_id)
validos_cnt = cached_count_global('res_validos', df_validos)
no_validos_cnt = cached_count_global('res_no_validos', documento_no_valido)
print(f"🔄 Registros duplicados exactos: {dup_exactos_cnt}")
print(f"🆔 Registros duplicados en ID: {dup_id_cnt}")
print(f"✅ Registros válidos: {validos_cnt}")
print(f"❌ Registros con tipoid no válido: {no_validos_cnt}")

# Validación de totales
total_clasificados = dup_exactos_cnt + dup_id_cnt + validos_cnt + no_validos_cnt
total_entrada = cached_count_global('post_validacion_match', df_procesado)
print(f"\n🔢 VALIDACIÓN:")
print(f"   Registros de entrada: {total_entrada}")
print(f"   Registros clasificados: {total_clasificados}")
print(f"   {'✅ Correcta clasificación' if total_clasificados == total_entrada else '⚠️ Error en clasificación'}")
print("="*50)

# AUDITORÍA GENERAL DEL FLUJO COMPLETO HASTA AQUÍ
print(f"\n🔍 AUDITORÍA COMPLETA DEL FLUJO:")
print(f"   📁 Archivo original: {cached_count_global('base_prn_leidos', df_base_PESVNO)}")
print(f"   🔄 Después validación: {cached_count_global('post_validacion_match', df_procesado)}")
print(f"   📊 Total clasificados: {total_clasificados}")
perdida_total = cached_count_global('base_prn_leidos', df_base_PESVNO) - total_clasificados
print(f"   📉 PÉRDIDA TOTAL HASTA AQUÍ: {perdida_total} registros")

if perdida_total > 0:
    print(f"   ⚠️  CRÍTICO: Se han perdido {perdida_total} registros")
    print(f"   🔍 Revisar logs anteriores para identificar dónde ocurrió la pérdida")
else:
    print(f"   ✅ PERFECTO: Sin pérdida de registros hasta este punto")



#--------------------------------------------------------------------------------------------------
#------------------------ Validando en fuente oficial si existe tipoid+numeroid ------------------


# quitando ceros a la izquierda del numeroid archivo prn
def limpiar_numeroid(df):
    print(f"🧹 ANTES limpiar_numeroid: {cached_count_global('limpiar_numeroid_entrada_'+str(hash(df._jdf)), df)} registros")
    df_limpio = df.withColumn("numeroid_limpio", regexp_replace(trim(col("numeroid")), "^0+", ""))
    print(f"🧹 DESPUÉS limpiar_numeroid: {cached_count_global('limpiar_numeroid_salida_'+str(hash(df_limpio._jdf)), df_limpio)} registros")
    
    # Verificar registros con numeroid vacío después de limpiar
    registros_vacios = df_limpio.filter(col("numeroid_limpio") == "").count()  # acción única específica
    if registros_vacios > 0:
        print(f"⚠️ Registros con numeroid_limpio VACÍO: {registros_vacios}")
        
    return df_limpio

# ========== FUNCIÓN ULTRA-OPTIMIZADA PARA CRUCE CON FUENTE OFICIAL ==========
def cruzar_con_fuente_oficial(df_validos, df_duplicados_id, fuente_of, ejecutar_match):
    cruce_start_time = time.time()
    print(f"⏱️ 🚀 [ULTRA-OPTIMIZADO] Iniciando cruce con fuente oficial: {datetime.now().strftime('%H:%M:%S')}")
    print(f"📊 Estrategia para archivo {CATEGORIA_ARCHIVO}")

    # ===== PASO 1: Preparación optimizada de datos PRN =====
    prep_start = time.time()
    
    # Limpiar numeroid de forma eficiente
    df_validos_limpio = df_validos.withColumn("numeroid_limpio", regexp_replace(trim(col("numeroid")), "^0+", ""))
    df_duplicados_id_limpio = df_duplicados_id.withColumn("numeroid_limpio", regexp_replace(trim(col("numeroid")), "^0+", ""))
    
    # Unir DataFrames - solo seleccionar columnas necesarias para reducir memoria
    df_prn_a_cruzar = df_validos_limpio.select("tipoid", "numeroid", "nombre", "numeroid_limpio") \
                      .union(df_duplicados_id_limpio.select("tipoid", "numeroid", "nombre", "numeroid_limpio"))

    # ESTRATEGIA INTELIGENTE DE OPTIMIZACIÓN:
    # - Archivos PEQUEÑOS/MEDIANOS: Broadcast del archivo PRN (pocas filas, cabe en memoria)
    # - Archivos GRANDES: Cache del archivo PRN + SortMergeJoin
    if CATEGORIA_ARCHIVO in ["PEQUEÑO", "MEDIANO"]:
        df_prn_a_cruzar = broadcast(df_prn_a_cruzar)
        print(f"📡 Broadcast aplicado al archivo PRN ({CATEGORIA_ARCHIVO}) para optimizar JOIN")
    elif CATEGORIA_ARCHIVO in ["GRANDE", "MUY_GRANDE"]:
        df_prn_a_cruzar.persist()
        print("💾 Cache aplicado a df_prn_a_cruzar para archivo grande")
    
    prep_end = time.time()
    print(f"⏱️ Preparación de datos PRN completada en {prep_end - prep_start:.2f}s")

    # ===== PASO 2: Preparación optimizada de fuente oficial =====
    dedup_start = time.time()
    print("🔧 Preparando fuente oficial...")
    
    # Limpiar y deduplicar fuente oficial
    fuente_of_optimizada = fuente_of.withColumn("NUMERO_ID_LIMPIO", regexp_replace(trim(col("NUMERO_ID")), "^0+", "")) \
                                   .select("TIPO_ID", "NUMERO_ID", "NUMERO_ID_LIMPIO", "NOMBRE_COMPLETO", 
                                          "FECHA_EXPEDICION_DOCUMENTO", "NOMBRE_MUNICIPIO_EXPEDICION", 
                                          "NOMBRE_DEPARTAMENTO_EXPEDICION", "CODIGO_ESTADO_VIGENCIA_DOCUMENTO",
                                          "GENERO_SEXO", "EDAD_CALCULADA", "RANGO_EDAD")
    
    # Deduplicación optimizada
    window_spec = Window.partitionBy("TIPO_ID", "NUMERO_ID_LIMPIO").orderBy(col("FECHA_EXPEDICION_DOCUMENTO").desc())
    fuente_of_dedup = fuente_of_optimizada.withColumn("rank", row_number().over(window_spec)) \
                                          .filter(col("rank") == 1).drop("rank")
    
    # ⚠️ CRÍTICO: NUNCA hacer broadcast de fuente oficial (81.8M registros, ~9.7GB)
    # El hint no_broadcast ya fue aplicado en la creación de fuente_of (línea 1729)
    # Forzamos SortMergeJoin para evitar error "Cannot broadcast table larger than 8.0 GiB"
    fuente_of_dedup = fuente_of_dedup.hint("no_broadcast")
    
    # Persistir para reutilización eficiente (sin recalcular deduplicación)
    fuente_of_dedup.persist()
    print("💾 Cache aplicado a fuente oficial (SortMergeJoin forzado, sin broadcast)")
    
    dedup_end = time.time()
    print(f"⏱️ Deduplicación de fuente oficial completada en {dedup_end - dedup_start:.2f}s")

    # ===== PASO 3: JOIN ULTRA-OPTIMIZADO =====
    join_start = time.time()
    print("🔗 Realizando JOIN ultra-optimizado...")
    
    # JOIN con hint de estrategia según el tamaño del archivo
    df_cruzados_total = df_prn_a_cruzar.join(
        fuente_of_dedup,
        (df_prn_a_cruzar.tipoid == fuente_of_dedup.TIPO_ID) & 
        (df_prn_a_cruzar.numeroid_limpio == fuente_of_dedup.NUMERO_ID_LIMPIO),
        "left_outer"
    )
    
    # Para archivos grandes, persistir el resultado del join
    if CATEGORIA_ARCHIVO in ["GRANDE", "MUY_GRANDE"]:
        df_cruzados_total.persist()
        print("💾 Cache aplicado al resultado del JOIN para archivo grande")
    
    join_end = time.time()
    print(f"⏱️ JOIN ultra-optimizado completado en {join_end - join_start:.2f}s")

    # ===== PASO 4: Separación eficiente de resultados =====
    print("🔍 Separando registros encontrados vs no encontrados...")
    encontrados_en_fuente_oficial = df_cruzados_total.filter(col("NOMBRE_COMPLETO").isNotNull())
    no_encontrados_en_fuente_oficial = df_cruzados_total.filter(col("NOMBRE_COMPLETO").isNull())

    # ===== PASO 5: Generar DataFrame de resultado optimizado =====
    print("📋 Generando DataFrame resultante...")
    if ejecutar_match:
        df_resultado = encontrados_en_fuente_oficial.select(
            col("tipoid"),
            col("numeroid"),
            col("nombre").alias("nombre_reportado"),
            col("NOMBRE_COMPLETO").alias("nombre_base_oficial")
        )
    else:
        df_resultado = encontrados_en_fuente_oficial.select(
            col("tipoid"),
            col("numeroid"),
            col("NOMBRE_COMPLETO").alias("nombre_base_oficial")
        )

    # ===== LIMPIEZA DE MEMORIA =====
    if CATEGORIA_ARCHIVO in ["GRANDE", "MUY_GRANDE"]:
        df_prn_a_cruzar.unpersist()
        fuente_of_dedup.unpersist()  # Para archivos grandes siempre usamos persist, no broadcast
        df_cruzados_total.unpersist()
        print("🗑️ Cache liberado de DataFrames temporales")

    cruce_end_time = time.time()
    total_cruce_time = cruce_end_time - cruce_start_time
    print(f"⏱️ 🔗 Cruce ULTRA-OPTIMIZADO completado en {total_cruce_time:.2f}s")
    print("✅ JOIN optimizado sin conteos costosos innecesarios")

    return encontrados_en_fuente_oficial, no_encontrados_en_fuente_oficial, df_resultado

# Cargar datos desde Redshift (esquema edf_views)
time_checkpoint_catalog_start = time.time()
print(f"⏱️ CHECKPOINT 19 - Iniciando carga de datos desde Redshift: {datetime.now().strftime('%H:%M:%S')}")
print(f"📚 Redshift: {redshift_cluster_id} / {redshift_database} / schema: {redshift_schema}")

# ========== CARGA DESDE REDSHIFT VÍA SPARK JDBC + AUTH CON USUARIO/PASSWORD ==========
# La autenticación se realiza con usuario y contraseña recuperados desde
# AWS Secrets Manager (secret administrado del usuario de Redshift).
print("🔗 Conectando a Redshift via JDBC con usuario/contraseña (Secrets Manager)...")

# Formato de URL estándar: jdbc:redshift://host:port/database
jdbc_properties = {
    "driver": "com.amazon.redshift.jdbc42.Driver",
    "user": redshift_user,
    "password": redshift_password,
}

print(f"📖 URL JDBC: {redshift_jdbc_url}")
print(f"📖 Leyendo tabla {redshift_schema}.xpm desde Redshift...")
df_xpm = spark.read.jdbc(
    url=redshift_jdbc_url,
    table=f"{redshift_schema}.xpm",
    properties=jdbc_properties
)

print(f"📖 Leyendo tabla {redshift_schema}.xpm_bestidentifications desde Redshift...")
df_xpm_bestidentifications = spark.read.jdbc(
    url=redshift_jdbc_url,
    table=f"{redshift_schema}.xpm_bestidentifications",
    properties=jdbc_properties
)

time_checkpoint_catalog_load = time.time()
elapsed_catalog_load = time_checkpoint_catalog_load - time_checkpoint_catalog_start
print(f"⏱️ CHECKPOINT 20 - Tablas cargadas desde Redshift en {elapsed_catalog_load:.2f}s: {datetime.now().strftime('%H:%M:%S')}")

print("XPM SCHEMA")
df_xpm.printSchema()
print("XPM BEST IDENTIFICATIONS SCHEMA")
df_xpm_bestidentifications.printSchema()

print(f"⏱️ Iniciando JOIN de tablas XPM: {datetime.now().strftime('%H:%M:%S')}")
df_all = df_xpm.join(df_xpm_bestidentifications, df_xpm['bestidentifications'] == df_xpm_bestidentifications['id'], 'left')

time_checkpoint_catalog_end = time.time()
elapsed_catalog_join = time_checkpoint_catalog_end - time_checkpoint_catalog_load
elapsed_catalog_total = time_checkpoint_catalog_end - time_checkpoint_catalog_start
print(f"⏱️ CHECKPOINT 21 - JOIN completado en {elapsed_catalog_join:.2f}s: {datetime.now().strftime('%H:%M:%S')}")
print(f"⏱️ TIEMPO TOTAL carga Redshift: {elapsed_catalog_total:.2f}s")

# Extraer campos requeridos, aplicar la transformación del género y convertir la fecha
time_checkpoint_fuente_start = time.time()
print(f"⏱️ CHECKPOINT 22 - Procesando fuente oficial: {datetime.now().strftime('%H:%M:%S')}")

# NOTA: Los nombres de columna en Redshift (edf_views) son los mismos que en el Glue Catalog
# pero sin los backticks de struct anidados. Si las vistas en edf_views ya aplanan la estructura,
# ajustar los nombres de columna según el esquema real de la vista.
fuente_of = df_all.select(
    col("`bestName.fullName`").alias("NOMBRE_COMPLETO"),
    when(col("`bestPersonalInformation.gender`") == 1, lit("M"))
    .when(col("`bestPersonalInformation.gender`") == 2, lit("F"))
    .otherwise(lit(" ")).alias("GENERO_SEXO"),
    col("`bestPersonalInformation.age`").alias("EDAD_CALCULADA"),
    col("`bestIdentifications.val.personIdNumber`").alias("NUMERO_ID"),
    col("`bestIdentifications.val.personIdType`").alias("TIPO_ID"),
    date_format(from_unixtime(col("`bestIdentifications.val.issueDate`") / 1000), "yyyyMMdd").alias("FECHA_EXPEDICION_DOCUMENTO"),
    col("`bestIdentifications.val.issuingCityName`").alias("NOMBRE_MUNICIPIO_EXPEDICION"),
    col("`bestIdentifications.val.issuingStateName`").alias("NOMBRE_DEPARTAMENTO_EXPEDICION"),
    col("`bestIdentifications.val.idStatus`").alias("CODIGO_ESTADO_VIGENCIA_DOCUMENTO")
).repartition(SPARK_PARTITIONS_DINAMICAS)

# SOLUCIÓN CRÍTICA: Deshabilitar broadcast hint para forzar SortMergeJoin
# Esto evita el error "Cannot broadcast the table that is larger than 8.0 GiB"
fuente_of = fuente_of.hint("no_broadcast")

# Calcular el rango de edad
fuente_of = fuente_of.withColumn("RANGO_EDAD", 
    when(col("EDAD_CALCULADA") < 18, "Menor de 18")
    .when((col("EDAD_CALCULADA") >= 18) & (col("EDAD_CALCULADA") <= 30), "18-30")
    .when((col("EDAD_CALCULADA") > 30) & (col("EDAD_CALCULADA") <= 50), "31-50")
    .when((col("EDAD_CALCULADA") > 50) & (col("EDAD_CALCULADA") <= 70), "51-70")
    .otherwise("Mayor de 70")
)

time_checkpoint_fuente_end = time.time()
elapsed_fuente = time_checkpoint_fuente_end - time_checkpoint_fuente_start
print(f"⏱️ CHECKPOINT 23 - Fuente oficial procesada en {elapsed_fuente:.2f}s: {datetime.now().strftime('%H:%M:%S')}")

# ===== DEBUG LIGERO: Verificar fuente oficial sin conteo costoso =====
try:
    # OPTIMIZACIÓN: Solo verificar con .take(1) en lugar de .count() para ahorrar 60-70s
    tiene_datos = fuente_of.take(1)
    
    if not tiene_datos:
        print("🚨 CRÍTICO: Fuente oficial vacía - revisando df_all...")
        tiene_datos_all = df_all.take(1)
        
        if tiene_datos_all:
            print("🔍 Mostrando primera fila de df_all para debug:")
            df_all.show(1, truncate=False)
            print("🔍 Schema de df_all:")
            df_all.printSchema()
        else:
            print("🚨 ERROR: df_all también está vacío")
    else:
        print("✅ Fuente oficial cargada correctamente desde Data Catalog")
        print("🔍 Muestra de fuente oficial (primeras 2 filas):")
        fuente_of.show(2, truncate=False)
        
except Exception as e:
    print(f"⚠️ Error verificando fuente oficial: {e}")

print(f"📊 Registros en fuente oficial (cached): {cached_count_global('fuente_of_total', fuente_of)}")

# Realizar el cruce
time_checkpoint_cruce_start = time.time()
print(f"⏱️ CHECKPOINT 24 - Iniciando cruce con fuente oficial: {datetime.now().strftime('%H:%M:%S')}")
print("🚀 INICIANDO PROCESO DE CRUCE CON FUENTE OFICIAL")

encontrados_en_fuente_oficial, no_encontrados_en_fuente_oficial, df_resultado = cruzar_con_fuente_oficial(df_validos, df_duplicados_id, fuente_of, ejecutar_match)

time_checkpoint_cruce_end = time.time()
elapsed_cruce = time_checkpoint_cruce_end - time_checkpoint_cruce_start
print(f"⏱️ CHECKPOINT 25 - Cruce con fuente oficial completado en {elapsed_cruce:.2f}s: {datetime.now().strftime('%H:%M:%S')}")

# Imprimir resultados finales con resumen
print("\n" + "="*60)
print("📈 RESUMEN FINAL DEL CRUCE CON FUENTE OFICIAL")
print("="*60)
print(f"Registros encontrados en la fuente oficial: {cached_count_global('cruce_encontrados', encontrados_en_fuente_oficial)}")
print(f"Registros no encontrados en la fuente oficial: {cached_count_global('cruce_no_encontrados', no_encontrados_en_fuente_oficial)}")
print(f"Registros en el DataFrame resultante: {cached_count_global('cruce_df_resultado', df_resultado)}")
print("="*60)

# Mostrar ejemplos de cada categoría
print("\nEjemplos de registros encontrados_en_fuente_oficial:")
# encontrados_en_fuente_oficial.show(10, truncate=False)

print("\nEjemplos de registros no encontrados en la fuente oficial:")
# no_encontrados_en_fuente_oficial.show(5, truncate=False)

print("\nEjemplos del DataFrame resultante:")
# df_resultado.show(10, truncate=False)


#------------------------------------------------------------------------------------------------------------------------------------------
#       PESVNO maneja dos algoritmos principales (match nombres y validación de codigos de vigencia)
#   A continuación se ejecuta el algoritmo  match nombres (normalización)
#------------------------------------------------------------------------------------------------------------------------------------------
if ejecutar_match:
    time_checkpoint_match_start = time.time()
    print(f"⏱️ CHECKPOINT 26 - Iniciando algoritmo de match de nombres: {datetime.now().strftime('%H:%M:%S')}")
    print("\nInicio del procesamiento de dato MATCH DE NOMBRES")
    # incio logica de normalización de los nombres tanto en nombre reportado prn, como el extraido de fuente oficial.
    def normalize_company_name(text):
        if text is None:
            return text
        
        # Convertir a minúsculas y aplicar normalización NFKD
        text = text.lower()
        text = unicodedata.normalize('NFKD', text)
        
        # Convertir a ASCII, eliminando caracteres no ASCII
        text = text.encode('ASCII', 'ignore').decode('utf-8')
        
        # Reemplazos específicos para empresas
        replacements = {
            r'\bs\.?\s?a\.?\s?(s\.?)?\b': '',  # Maneja S.A., S.A, SA, S.A.S, SAS
            r'\bs\.?\s?a\.?\s?s\.?\b': '',     # Maneja S.A.S, SAS de forma específica
            r'\bltda\.?\b': '',                # Maneja LTDA, LTDA.
            r'\blimitada\b': '',
            r'\ben\s+liquidacion\b': '',
            r'\ben\s+reorganizacion\b': '',
            r'\bcia\.?\b': '',
            r'\b(del|de la|de los|de)\b': '',
            r'\b(el|la|los|las)\b': '',
            r'\by\b': '',
            r'\be\.?\s?s\.?\s?p\.?\b': '',     # Maneja E.S.P., E.S.P, ESP., ESP
        }
        
        for pattern, replacement in replacements.items():
            text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    
        # Reemplazar todos los caracteres que no sean letras o números con espacios
        text = re.sub(r'[^a-z0-9]', ' ', text)
    
        # Eliminar espacios múltiples y espacios al inicio y final
        text = re.sub(r'\s+', ' ', text).strip()
        
        #reemplazar z por s
        text = text.replace('z', 's')
        
        
        # Agrupar letras solas si hay dos o más
        words = text.split()
        new_words = []
        temp_letters = ""
        
        for word in words:
            if len(word) == 1 and word.isalpha():
                temp_letters += word
            else:
                if len(temp_letters) >= 2:
                    new_words.append(temp_letters)
                elif len(temp_letters) == 1:
                    new_words.append(temp_letters)
                temp_letters = ""
                new_words.append(word)
        
        # Manejar las letras al final de la cadena
        if len(temp_letters) >= 2:
            new_words.append(temp_letters)
        elif len(temp_letters) == 1:
            new_words.append(temp_letters)
    
        text = " ".join(new_words)
    
        return text
        
    
    
    def normalize_company_name_with_filter(text, tipoid, valid_ids):
        if tipoid in valid_ids:
            return normalize_company_name(text)
        return text
    
    normalize_company_name_udf = spark.udf.register("normalize_company_name_udf", normalize_company_name_with_filter)
    
    #-----------------------------------------------------------------------------------------------------------
    
    def normalize_text_combined(text):
        if text is None:
            return None
            
        # Convertir a minúsculas y aplicar normalización NFKD    
        text = text.lower()
        text = unicodedata.normalize('NFKD', text)
        text = text.encode('ASCII', 'ignore').decode('utf-8')
        text = text.replace('z', 's')
        
        # Reemplazar 'y' por 'i' solo cuando no está al inicio de una palabra
        text = re.sub(r'(\w)y', r'\1i', text)
        
        # Reemplazar 'y' por 'j' cuando está al inicio de una palabra
        text = re.sub(r'\by\b|\by(?=\w)', 'j', text)
        
        # Eliminar caracteres especiales y dígitos, manteniendo solo letras y espacios
        text = re.sub(r'[^a-z\s]', '', text)
        
        # Reemplazar consonantes dobles
        consonants = "bcdfghjklmnpqrstvwxs"
        pattern = r"([{consonants}])(\1+)".format(consonants=consonants)
        text = re.sub(pattern, r"\1", text)
        
        # Eliminar espacios múltiples
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    
    def normalize_text_combined_with_filter(text, tipoid, valid_ids):
        if tipoid in valid_ids:
            return normalize_text_combined(text)
        return text
    
    normalize_udf = spark.udf.register("normalize_udf", normalize_text_combined_with_filter)
    
    #----------------------------------------------------------------------------
    
    def replace_double_consonants(text):
        if text is None:
            return None
        consonants = "bcdfghjklmnñpqrstvwxyz"
        pattern = r"([{consonants}])(\1)".format(consonants=consonants)
        norm_text = re.sub(pattern, r"\1", text, flags=re.IGNORECASE)
        return norm_text
    
    def replace_double_consonants_with_filter(text, tipoid, valid_ids):
        if tipoid in valid_ids:
            return replace_double_consonants(text)
        return text
    
    replace_double_consonants_udf = spark.udf.register("replace_double_consonants_udf", replace_double_consonants_with_filter)
    
    #----------------------------------------------------------------------------
    
    def remove_hyphens(text):
        if text is None:
            return None
        return re.sub(r'-', '', text)
    
    def remove_hyphens_with_filter(text, tipoid, valid_ids):
        if tipoid in valid_ids:
            return remove_hyphens(text)
        return text
    
    remove_hyphens_udf = spark.udf.register("remove_hyphens_udf", remove_hyphens_with_filter)
    
    #----------------------------------------------------------------------------
    
    prefixes = ['del', 'la', 'las', 'el', 'los', 'de', 'di', 'y', 'da', 'das', 'do', 'dos']
    suffixes = ['jr', 'sr', 'iii', 'iv', 'v', 'vi', 'vii', 'viii', 'ix', 'x']
    
    def remove_prefixes_suffixes(text):
        if text is None:
            return None
        norm_text = text.lower()
        for prefix in prefixes:
            if norm_text.startswith(prefix + ' '):
                norm_text = norm_text[len(prefix) + 1:]
        for suffix in suffixes:
            if norm_text.endswith(' ' + suffix):
                norm_text = norm_text[:-len(suffix) - 1]
        return norm_text
    
    def remove_prefixes_suffixes_with_filter(text, tipoid, valid_ids):
        if tipoid in valid_ids:
            return remove_prefixes_suffixes(text)
        return text
    
    remove_prefixes_suffixes_udf = spark.udf.register("remove_prefixes_suffixes_udf", remove_prefixes_suffixes_with_filter)
    
    #----------------------------------------------------------------------------
    
    
    df_resultado = df_resultado.select(
        "tipoid", 
        "numeroid", 
        col("nombre_reportado").alias("nombre"),
        col("nombre_base_oficial").alias("oficial_nombre")
    )
    
    # Definir los tipos de ID válidos para cada función
    function_id_mapping = {
        "normalize_udf": "1,4,5,6,7,8,9",
        "replace_double_consonants_udf": "1,4,5,6,7,8,9",
        "remove_hyphens_udf": "1,4,5,6,7,8,9",
        "remove_prefixes_suffixes_udf": "1,4,5,6,7,8,9",
        "normalize_company_name_udf": "2,3"
    }
    #----------------------------------------------------------------------------
    
    def apply_with_filter(func):
        def filtered_func(text, tipoid, valid_ids):
            if tipoid in valid_ids:
                return func(text)
            return text
        return filtered_func
    
    spark.udf.register("normalize_company_name_udf", apply_with_filter(normalize_company_name),  StringType())
    
    spark.udf.register("normalize_text_combined_udf", apply_with_filter(normalize_text_combined), StringType())
    
    spark.udf.register("replace_double_consonants_udf", apply_with_filter(replace_double_consonants), StringType())
    
    spark.udf.register("remove_hyphens_udf", apply_with_filter(remove_hyphens), StringType())
    
    spark.udf.register("remove_prefixes_suffixes_udf", apply_with_filter(remove_prefixes_suffixes), StringType())
    
    # Definir el número mínimo de partículas para una coincidencia
    def compare_names_with_variable_min_particles(nombre_empresa_normalizado, oficial_nombre_empresa_normalizado, tipoid):
        if tipoid == '1':
            min_particles = 2
        elif tipoid in ['2', '3']:
            min_particles = 1
        elif tipoid >= '4':
            min_particles = 2
        else:
            min_particles = 2
        
        return compare_names(nombre_empresa_normalizado, oficial_nombre_empresa_normalizado, min_particles, tipoid)
    # No es necesario aplicar apply_with_filter a compare_names_udf
    compare_names_udf = F.udf(compare_names_with_variable_min_particles, StringType())
    
    #----------------------------------------------------------------------------
    # Aplicar todas las transformaciones de las funciones UDF anteriores en orden
    
    def procesar_dataframe(df_resultado, ejecutar_match, outputbucket):
        time_normalization_start = time.time()
        print(f"⏱️ Iniciando normalización completa: {datetime.now().strftime('%H:%M:%S')}")
        
        # 1. Normalizar vocales para ambos nombres
        print("\n#1 Normalizar vocales para ambos nombres:")  
        df_resultado = df_resultado.withColumn("nombre_normalizado",
            F.expr("normalize_udf(nombre, tipoid, array(" + 
                   ",".join([f"'{x}'" for x in function_id_mapping["normalize_udf"].split(",")]) + "))"))
        
        df_resultado = df_resultado.withColumn("oficial_nombre_normalizado", 
            F.expr("normalize_udf(oficial_nombre, tipoid, array(" + 
                   ",".join([f"'{x}'" for x in function_id_mapping["normalize_udf"].split(",")]) + "))"))
    
        time_step1 = time.time()
        print(f"⏱️ Paso 1 (vocales) completado en {time_step1 - time_normalization_start:.2f}s")
        
        # 2. Normalizar consonantes para ambos nombres
        print("\n# 2. Normalizar consonantes para ambos nombres:")  
        df_resultado = df_resultado.withColumn("nombre_sin_dobles", 
            F.expr("replace_double_consonants_udf(nombre_normalizado, tipoid, array(" + 
                   ",".join([f"'{x}'" for x in function_id_mapping["replace_double_consonants_udf"].split(",")]) + "))"))
        
        df_resultado = df_resultado.withColumn("oficial_nombre_sin_dobles", 
            F.expr("replace_double_consonants_udf(oficial_nombre_normalizado, tipoid, array(" + 
                   ",".join([f"'{x}'" for x in function_id_mapping["replace_double_consonants_udf"].split(",")]) + "))"))
        
        time_step2 = time.time()
        print(f"⏱️ Paso 2 (consonantes) completado en {time_step2 - time_step1:.2f}s")
        
        # 3. Remover guiones y prefijos/sufijos para ambos nombres
        print("\n# 3. Remover guiones y prefijos/sufijos para ambos nombres:")
        df_resultado = df_resultado.withColumn("nombre_sin_guiones", 
            F.expr("remove_hyphens_udf(nombre_sin_dobles, tipoid, array(" + 
                   ",".join([f"'{x}'" for x in function_id_mapping["remove_hyphens_udf"].split(",")]) + "))"))
        
        df_resultado = df_resultado.withColumn("oficial_nombre_sin_guiones", 
            F.expr("remove_hyphens_udf(oficial_nombre_sin_dobles, tipoid, array(" + 
                   ",".join([f"'{x}'" for x in function_id_mapping["remove_hyphens_udf"].split(",")]) + "))"))
        
        df_resultado = df_resultado.withColumn("nombre_limpio", 
            F.expr("remove_prefixes_suffixes_udf(nombre_sin_guiones, tipoid, array(" + 
                   ",".join([f"'{x}'" for x in function_id_mapping["remove_prefixes_suffixes_udf"].split(",")]) + "))"))
        
        df_resultado = df_resultado.withColumn("oficial_nombre_limpio", 
            F.expr("remove_prefixes_suffixes_udf(oficial_nombre_sin_guiones, tipoid, array(" + 
                   ",".join([f"'{x}'" for x in function_id_mapping["remove_prefixes_suffixes_udf"].split(",")]) + "))"))
        
        time_step3 = time.time()
        print(f"⏱️ Paso 3 (guiones/prefijos) completado en {time_step3 - time_step2:.2f}s")
        
        # 4. Normalizar nombres de empresas para ambos nombres
        print("\n# 4. Normalizar nombres de empresas para ambos nombres:")
        df_resultado = df_resultado.withColumn("nombre_empresa_normalizado", 
            F.expr("normalize_company_name_udf(nombre_limpio, tipoid, array(" + 
                   ",".join([f"'{x}'" for x in function_id_mapping["normalize_company_name_udf"].split(",")]) + "))"))
        
        df_resultado = df_resultado.withColumn("oficial_nombre_empresa_normalizado", 
            F.expr("normalize_company_name_udf(oficial_nombre_limpio, tipoid, array(" + 
                   ",".join([f"'{x}'" for x in function_id_mapping["normalize_company_name_udf"].split(",")]) + "))"))
        
        time_step4 = time.time()
        print(f"⏱️ Paso 4 (empresas) completado en {time_step4 - time_step3:.2f}s")
        
        if ejecutar_match:
            print("\n# 5. Aplicando algoritmo de comparación de nombres:")
            matches = df_resultado.withColumn("name_similarity", 
                compare_names_udf(F.col("nombre_empresa_normalizado"), 
                                  F.col("oficial_nombre_empresa_normalizado"),
                                  F.col("tipoid")))
            time_step5 = time.time()
            print(f"⏱️ Paso 5 (comparación) completado en {time_step5 - time_step4:.2f}s")
        else:
            matches = df_resultado
            time_step5 = time_step4
            
        time_normalization_end = time.time()
        total_normalization_time = time_normalization_end - time_normalization_start
        print(f"⏱️ NORMALIZACIÓN COMPLETA finalizada en {total_normalization_time:.2f}s")
            
        return matches
        
    
    
    print("\nInicio del procesamiento de dato MATCH DE NOMBRES")
    
    # Ejecutar la función procesar_dataframe
    time_processing_start = time.time()
    print(f"⏱️ CHECKPOINT 27 - Iniciando procesamiento de dataframe: {datetime.now().strftime('%H:%M:%S')}")
    
    matches = procesar_dataframe(df_resultado, ejecutar_match, outputbucket)
    
    time_processing_end = time.time()
    elapsed_processing = time_processing_end - time_processing_start
    print(f"⏱️ CHECKPOINT 28 - Procesamiento de dataframe completado en {elapsed_processing:.2f}s: {datetime.now().strftime('%H:%M:%S')}")

    # Usar cached_count_global en lugar de .count() directo para evitar cálculos costosos
    matches_count = cached_count_global('matches_post_processing', matches)
    logger.info(f" Registros que pasan al algoritmo MATCH: {matches_count}")
    

    #--------- PESVNO maneja dos algoritmos principales (match nombres y validación de codigos de vigencia) A continuación se ejecuta el algoritmo  match nombres  ------------
    
    def jaro_winkler_distance(s1, s2):
        """
        Implementación estándar del algoritmo Jaro-Winkler.
        """
        # Si las cadenas son idénticas, retorna 1.0
        if s1 == s2:
            return 1.0
    
        # Obtener longitudes de las cadenas
        len1, len2 = len(s1), len(s2)
    
        # Si alguna cadena está vacía, retorna 0.0
        if len1 == 0 or len2 == 0:
            return 0.0
    
        # Calcular la distancia máxima de coincidencia
        max_dist = max(len1, len2) // 2 - 1
    
        # Encontrar coincidencias
        matches = 0
        match_flags1 = [0] * len1
        match_flags2 = [0] * len2
    
        for i in range(len1):
            start = max(0, i - max_dist)
            end = min(i + max_dist + 1, len2)
            for j in range(start, end):
                if s1[i] == s2[j] and match_flags2[j] == 0:
                    match_flags1[i] = 1
                    match_flags2[j] = 1
                    matches += 1
                    break
    
        if matches == 0:
            return 0.0
    
        # Calcular transposiciones
        t = 0
        point = 0
        for i in range(len1):
            if match_flags1[i]:
                while match_flags2[point] == 0:
                    point += 1
                if s1[i] != s2[point]:
                    t += 1
                point += 1
    
        t = t // 2
    
        # Calcular la distancia Jaro
        jaro = ((matches / len1) + (matches / len2) + ((matches - t) / matches)) / 3
    
        # Calcular el prefijo común
        l = 0
        for i in range(min(4, min(len1, len2))):
            if s1[i] == s2[i]:
                l += 1
            else:
                break
    
        # Calcular la distancia Jaro-Winkler
        p = 0.1  # Factor de escala del prefijo
        jaro_winkler = jaro + (l * p * (1 - jaro))
    
        return jaro_winkler
    
    
    print("\nInicio partial_match")
    def partial_match(word1, word2):
        # Convertir a minúsculas para comparación
        word1 = word1.lower()
        word2 = word2.lower()
        
        # Si alguna de las palabras tiene menos de 2 caracteres, no es una coincidencia parcial
        if len(word1) < 2 or len(word2) < 2:
            return False
        
        # Si las palabras son iguales, es una coincidencia parcial
        if word1 == word2:
            return True
        
        # Si la diferencia de longitud es mayor que 3, no es una coincidencia parcial
        if abs(len(word1) - len(word2)) > 3:
            return False
        
        # Calcular la distancia de Levenshtein
        distance = levenshtein_distance(word1, word2)
        
        # Si la distancia es 1, 2 o 3, consideramos que es una coincidencia parcial
        return distance <= 3
    
    
    def levenshtein_distance(s1, s2):
        if len(s1) < len(s2):
            return levenshtein_distance(s2, s1)
    
        if len(s2) == 0:
            return len(s1)
    
        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
    
        return previous_row[-1]
    
    
    
    
    
    def initial_match(word, full_name):
        return word.lower() == full_name[0].lower()
    
    
    
    
    
    
    def incremental_split(word, reference_words):
        splits = []
        for i in range(2, len(word)):
            left = word[:i]
            right = word[i:]
            if left.lower() in reference_words or right.lower() in reference_words:
                splits.append((left, right))
        return splits
    
    #----------- SIGLAS PARA TIPOS ID  2Y 3 ------------
    
    def flexible_match(short_name, full_name):
        short_name = short_name.lower()
        full_name = full_name.lower()
        
        # Generar sigla del nombre completo
        full_name_words = full_name.split()
        full_name_sigla = ''.join(word[0] for word in full_name_words if word)
        
        # Verificar si short_name es una sigla del full_name
        if short_name == full_name_sigla:
            return True
        
        # Verificar si short_name está contenido en full_name
        if short_name in full_name:
            return True
        
        # Verificar si todas las letras de short_name están presentes en full_name en orden
        full_name_chars = [char for char in full_name if char.isalpha()]
        short_name_chars = [char for char in short_name if char.isalpha()]
        
        if len(short_name_chars) <= len(full_name_chars):
            i, j = 0, 0
            while i < len(short_name_chars) and j < len(full_name_chars):
                if short_name_chars[i] == full_name_chars[j]:
                    i += 1
                j += 1
            if i == len(short_name_chars):
                return True
        
        return False
    
    def bidirectional_match(name1, name2):
        return flexible_match(name1, name2) or flexible_match(name2, name1)
    
    
    
    
    
    #--------------------------------------------------------------------------
    def process_word(word, other_words, all_words):
        if len(word) >= 3 and word in other_words:
            return word, '', True
        else:
            splits = incremental_split(word, all_words)
            for left, right in splits:
                if len(left) >= 3 and left in other_words:
                    return left, right, True
                elif len(right) >= 3 and right in other_words:
                    return right, left, True
        return '', word, False
    
    
    
    def flexible_substring_match(word, full_string):
        """
        Busca una palabra dentro de una cadena más larga, permitiendo flexibilidad.
        """
        word = word.lower()
        full_string = full_string.lower()
        
        # Búsqueda directa
        if word in full_string:
            return True
        
        # Búsqueda con tolerancia a errores
        word_length = len(word)
        for i in range(len(full_string) - word_length + 1):
            substring = full_string[i:i+word_length]
            if levenshtein_distance(word, substring) <= min(2, len(word) // 2):
                return True
        
        return False
    #-------------------------------------------------------------------------------
        
    print("\nInicio compare_names")
    def compare_names(nombre_empresa_normalizado, oficial_nombre_empresa_normalizado, min_particles, tipoid):
        words1 = nombre_empresa_normalizado.lower().split()
        words2 = oficial_nombre_empresa_normalizado.lower().split()
        all_words = set(words1 + words2)
        
        # Capa 0: Coincidencia de siglas
        if tipoid in ['2', '3']:
            if bidirectional_match(nombre_empresa_normalizado, oficial_nombre_empresa_normalizado):
                return "Capa 0 (Siglas)"
        
        # Nueva lógica para manejar nombres pegados
        if len(words1) == 1:
            matches = []
            for word in words2:
                if flexible_substring_match(word, nombre_empresa_normalizado):
                    matches.append(word)
            
            if len(matches) >= min_particles:
                return f"Capa 1 (Compuesta: {len(matches)}, {matches})"
        
        # Capa 1: Coincidencia exacta
        exact_matches = []
        remaining_words1 = []
        remaining_words2 = []
        
        # Procesar words1
        for w1 in words1:
            match, remaining, matched = process_word(w1, words2, all_words)
            if matched:
                exact_matches.append(match)
                words2.remove(match)
                if remaining:
                    remaining_words1.append(remaining)
            else:
                remaining_words1.append(w1)
        
        # Procesar words2 restantes
        for w2 in words2[:]:
            match, remaining, matched = process_word(w2, exact_matches, all_words)
            if not matched:
                remaining_words2.append(w2)
            elif remaining:
                remaining_words2.append(remaining)
    
        if len(exact_matches) >= min_particles:
            result = f"Capa 1 (Exactas: {len(exact_matches)}, {exact_matches})"
        elif len(exact_matches) > 0:
            # Capa 2: Coincidencia parcial
            partial_matches = []
            jaro_winkler_scores = []
            for w1 in remaining_words1[:]:
                if len(w1) >= 3:
                    for w2 in remaining_words2[:]:
                        if len(w2) >= 3:
                            if partial_match(w1, w2):
                                jaro_score = jaro_winkler_distance(w1, w2)
                                if jaro_score >= 0.84:
                                    partial_matches.append((w1, w2))
                                    jaro_winkler_scores.append(jaro_score)
                                    remaining_words1.remove(w1)
                                    remaining_words2.remove(w2)
                                    break
            
            total_matches = len(exact_matches) + len(partial_matches)
            if total_matches >= min_particles:
                avg_jaro_winkler = sum(jaro_winkler_scores) / len(jaro_winkler_scores) if jaro_winkler_scores else 0
                result = f"Capa 2 (Exactas: {len(exact_matches)}, {exact_matches}; Parciales: {len(partial_matches)}, {partial_matches}) (Jaro-Winkler promedio: {avg_jaro_winkler:.4f})"
            else:
                # Capa 3: Coincidencia de iniciales
                initial_matches = []
                for w1 in remaining_words1[:]:
                    if len(w1) == 1:
                        for w2 in remaining_words2[:]:
                            if initial_match(w1, w2) and w2 not in [m[1] for m in partial_matches]:
                                initial_matches.append((w1, w2))
                                remaining_words1.remove(w1)
                                remaining_words2.remove(w2)
                                break
                
                total_matches = len(exact_matches) + len(partial_matches) + len(initial_matches)
                if total_matches >= min_particles and len(remaining_words1) == 0:
                    result = f"Capa 3 (Exactas: {len(exact_matches)}, {exact_matches}; Parciales: {len(partial_matches)}, {partial_matches}; Iniciales: {len(initial_matches)}, {initial_matches})"
                else:
                    result = f"Capa 6 (Exactas: {len(exact_matches)}, {exact_matches}; Parciales: {len(partial_matches)}, {partial_matches}; Iniciales: {len(initial_matches)}, {initial_matches})"
        else:
            # Capa 4: Coincidencia parcial (sin coincidencias exactas)
            partial_matches = []
            jaro_winkler_scores = []
            for w1 in remaining_words1[:]:
                if len(w1) >= 3:
                    for w2 in remaining_words2[:]:
                        if len(w2) >= 3:
                            if partial_match(w1, w2):
                                jaro_score = jaro_winkler_distance(w1, w2)
                                if jaro_score >= 0.84:
                                    partial_matches.append((w1, w2))
                                    jaro_winkler_scores.append(jaro_score)
                                    remaining_words1.remove(w1)
                                    remaining_words2.remove(w2)
                                    break
            
            if len(partial_matches) >= min_particles:
                avg_jaro_winkler = sum(jaro_winkler_scores) / len(jaro_winkler_scores)
                result = f"Capa 4 (Parciales: {len(partial_matches)}, {partial_matches}) (Jaro-Winkler promedio: {avg_jaro_winkler:.4f})"
            elif len(partial_matches) > 0:
                # Capa 5: Coincidencia de iniciales (con al menos una coincidencia parcial)
                initial_matches = []
                for w1 in remaining_words1[:]:
                    if len(w1) == 1:
                        for w2 in remaining_words2[:]:
                            if initial_match(w1, w2):
                                initial_matches.append((w1, w2))
                                remaining_words1.remove(w1)
                                remaining_words2.remove(w2)
                                break
                
                total_matches = len(partial_matches) + len(initial_matches)
                if total_matches >= min_particles:
                    avg_jaro_winkler = sum(jaro_winkler_scores) / len(jaro_winkler_scores)
                    result = f"Capa 5 (Parciales: {len(partial_matches)}, {partial_matches}; Iniciales: {len(initial_matches)}, {initial_matches}) (Jaro-Winkler promedio: {avg_jaro_winkler:.4f})"
                else:
                    result = f"Capa 6 (Parciales: {len(partial_matches)}, {partial_matches}; Iniciales: {len(initial_matches)}, {initial_matches})"
            else:
                # Capa 6: Rechazados (sin coincidencias exactas ni parciales)
                result = "Capa 6 (Rechazados)"
    
        return result
    
    #-------------------------------------------------------------------------------  
        
        
        
    
    # Definir el número mínimo de partículas para una coincidencia
    particulas_info = ""
    def compare_names_with_variable_min_particles(nombre_empresa_normalizado, oficial_nombre_empresa_normalizado, tipoid, particulas_param):
        if particulas_param is not None and particulas_param > 0:
            min_particles = particulas_param
            particulas_info = f"Partículas usadas: {min_particles}"
        else:
            if tipoid == '1':
                min_particles = 2
            elif tipoid in ['2', '3']:
                min_particles = 1
            elif tipoid >= '4':
                min_particles = 2
            else:
                min_particles = 2
            particulas_info = f"Partículas de acuerdo al tipoid"
        
        return compare_names(nombre_empresa_normalizado, oficial_nombre_empresa_normalizado, min_particles, tipoid)

    
    
    # Registrar la función como UDF
    compare_names_udf = udf(lambda nombre, oficial_nombre, tipoid: compare_names_with_variable_min_particles(nombre, oficial_nombre, tipoid, particulas), StringType())


    # Aplicar la UDF a la columna, incluyendo el tipoid
    matches = matches.withColumn("name_similarity", 
                                 compare_names_udf("nombre_empresa_normalizado", 
                                                   "oficial_nombre_empresa_normalizado",
                                                   "tipoid"))
    
    
    # Escribir el DataFrame resultante en un CSV
    #matches.coalesce(1).write.mode("overwrite").csv(path=outputbucket+'5_normalizar_vocales_consonantes_guiones_prefijos_sufijos_jaro_winkler/', sep=";", header=True)



    from pyspark.sql.functions import col, when, lit, coalesce, upper, trim, regexp_replace, struct, row_number
    from pyspark.sql.window import Window
    
    if ejecutar_match:
        time_classification_start = time.time()
        print(f"⏱️ CHECKPOINT 29 - Iniciando clasificación de registros: {datetime.now().strftime('%H:%M:%S')}")
        print("\nInicio de procesamiento y clasificación de registros [OPTIMIZADO]")
    
        # --- PASO 1: Clasificar por capas y persistir el resultado ---
        matches_con_capa = matches.withColumn(
            "capa",
            when(col("name_similarity").startswith("Capa 0"), 0)
            .when(col("name_similarity").startswith("Capa 1"), 1)
            .when(col("name_similarity").startswith("Capa 2"), 2)
            .when(col("name_similarity").startswith("Capa 3"), 3)
            .when(col("name_similarity").startswith("Capa 4"), 4)
            .when(col("name_similarity").startswith("Capa 5"), 5)
            .otherwise(6)
        ).persist() # Persistir para reutilizar sin recalcular

        # --- PASO 2: Separar Aceptados (Capas 0-5) y Rechazados (Capa 6) ---
        aceptados_todos = matches_con_capa.filter(col("capa") < 6)
        rechazados = matches_con_capa.filter(col("capa") == 6)

        # --- PASO 3: Identificar el mejor 'aceptado' y los duplicados ---
        window_mejor_capa = Window.partitionBy("tipoid", "numeroid").orderBy("capa")
        aceptados_rankeados = aceptados_todos.withColumn("rank", row_number().over(window_mejor_capa))

        aceptados = aceptados_rankeados.filter(col("rank") == 1).drop("rank")
        duplicados_aceptados = aceptados_rankeados.filter(col("rank") > 1).drop("rank")

        # --- PASO 4: Consolidar todas las inconsistencias ---
        rechazados_con_codigo = rechazados.withColumn("codigo_error", when(col("tipoid") == "1", 2).otherwise(1))
        no_encontrados_con_codigo = no_encontrados_en_fuente_oficial.withColumn("codigo_error", when(col("tipoid") == "1", 4).otherwise(3))
        documento_no_valido_con_codigo = documento_no_valido.withColumn("codigo_error", lit(5))

        inconsistencias = rechazados_con_codigo.select("tipoid", "numeroid", "nombre", "codigo_error") \
            .union(no_encontrados_con_codigo.select("tipoid", "numeroid", "nombre", "codigo_error")) \
            .union(documento_no_valido_con_codigo.select("tipoid", "numeroid", "nombre", "codigo_error"))

        # --- PASO 5: Manejar la lógica de 'derechos' (vigencia) ---
        # CORRECCIÓN: Seleccionar explícitamente las columnas para evitar ambigüedad
        aceptados_completos = aceptados.alias("a").join(
            encontrados_en_fuente_oficial.alias("b"),
            (col("a.tipoid") == col("b.TIPO_ID")) & 
            (regexp_replace(trim(col("a.numeroid")), "^0+", "") == col("b.NUMERO_ID_LIMPIO")),
            "left"
        ).select(
            "a.*", # Traer todas las columnas de 'aceptados'
            col("b.NOMBRE_COMPLETO"),
            col("b.FECHA_EXPEDICION_DOCUMENTO"),
            col("b.NOMBRE_MUNICIPIO_EXPEDICION"),
            col("b.NOMBRE_DEPARTAMENTO_EXPEDICION"),
            col("b.CODIGO_ESTADO_VIGENCIA_DOCUMENTO"),
            col("b.GENERO_SEXO"),
            col("b.EDAD_CALCULADA"),
            col("b.RANGO_EDAD")
        )

        if tag_derechos == "1":
            # Ahora no hay ambigüedad porque solo hay una columna 'tipoid'
            nuevos_errores = aceptados_completos.filter(
                (col("tipoid") == "1") & 
                ((col("CODIGO_ESTADO_VIGENCIA_DOCUMENTO") == "12") | (col("CODIGO_ESTADO_VIGENCIA_DOCUMENTO") > "20"))
            ).withColumn("nuevo_codigo_error", when(col("CODIGO_ESTADO_VIGENCIA_DOCUMENTO") == "12", 7).otherwise(6))
            
            inconsistencias = inconsistencias.union(
                nuevos_errores.select(col("tipoid"), col("numeroid"), col("nombre"), col("nuevo_codigo_error").alias("codigo_error"))
            )
            
            aceptados_completos = aceptados_completos.join(
                nuevos_errores.select("tipoid", "numeroid"), ["tipoid", "numeroid"], "left_anti"
            )

        # Liberar el DF persistido que ya no se necesita
        matches_con_capa.unpersist()

        # --- PASO 6: Preparar datos para escritura ---
        print(f"⏱️ CHECKPOINT 30 - Iniciando formateo y escritura de archivos: {datetime.now().strftime('%H:%M:%S')}")
        write_start_time = time.time()

        # Preparar datos de ACEPTADOS (se escribirán al final para que .val sea el último archivo transferido)
        aceptados_preparados = aceptados_completos.select(
            col("tipoid").alias("TIPO_ID"),
            col("numeroid").alias("NUMERO_ID"),
            coalesce(col("NOMBRE_COMPLETO"), col("oficial_nombre"), col("nombre")).alias("NOMBRE_COMPLETO"),
            coalesce(col("FECHA_EXPEDICION_DOCUMENTO"), lit("")).alias("FECHA_EXPEDICION_DOCUMENTO"),
            coalesce(col("NOMBRE_MUNICIPIO_EXPEDICION"), lit("")).alias("NOMBRE_MUNICIPIO_EXPEDICION"),
            coalesce(col("NOMBRE_DEPARTAMENTO_EXPEDICION"), lit("")).alias("NOMBRE_DEPARTAMENTO_EXPEDICION"),
            coalesce(col("CODIGO_ESTADO_VIGENCIA_DOCUMENTO"), lit("")).alias("CODIGO_ESTADO_VIGENCIA_DOCUMENTO"),
            coalesce(col("GENERO_SEXO"), lit("")).alias("GENERO_SEXO"),
            coalesce(col("EDAD_CALCULADA"), lit("")).alias("EDAD_CALCULADA"),
            coalesce(col("RANGO_EDAD"), lit("")).alias("RANGO_EDAD")
        )
        aceptados_formateados = aceptados_preparados.select(combine_fields_validos(struct(*aceptados_preparados.columns)).alias("registro"))
        num_registros_estimado = cached_count_global('aceptados_final', aceptados_formateados)

        # --- PASO 7: Formatear y Escribir DUPLICADOS (primero) ---
        duplicados_start_time = time.time()
        duplicados_consolidados = duplicados_aceptados.select("tipoid", "numeroid", "nombre").union(df_duplicados_exactos.select("tipoid", "numeroid", "nombre"))
        
        duplicados_completos = duplicados_consolidados.alias("d").join(
            encontrados_en_fuente_oficial.alias("e"),
            (col("d.tipoid") == col("e.TIPO_ID")) & (regexp_replace(trim(col("d.numeroid")), "^0+", "") == col("e.NUMERO_ID_LIMPIO")),
            "left_outer"
        )
        duplicados_preparados = duplicados_completos.select(
            col("d.tipoid").alias("TIPO_ID"),
            col("d.numeroid").alias("NUMERO_ID"),
            coalesce(col("e.NOMBRE_COMPLETO"), col("d.nombre")).alias("NOMBRE_COMPLETO"),
            coalesce(col("e.FECHA_EXPEDICION_DOCUMENTO"), lit("")).alias("FECHA_EXPEDICION_DOCUMENTO"),
            coalesce(col("e.NOMBRE_MUNICIPIO_EXPEDICION"), lit("")).alias("NOMBRE_MUNICIPIO_EXPEDICION"),
            coalesce(col("e.NOMBRE_DEPARTAMENTO_EXPEDICION"), lit("")).alias("NOMBRE_DEPARTAMENTO_EXPEDICION"),
            coalesce(col("e.CODIGO_ESTADO_VIGENCIA_DOCUMENTO"), lit("")).alias("CODIGO_ESTADO_VIGENCIA_DOCUMENTO"),
            coalesce(col("e.GENERO_SEXO"), lit("")).alias("GENERO_SEXO"),
            coalesce(col("e.EDAD_CALCULADA"), lit("")).alias("EDAD_CALCULADA"),
            coalesce(col("e.RANGO_EDAD"), lit("")).alias("RANGO_EDAD")
        )
        duplicados_formateados = duplicados_preparados.select(combine_fields_validos(struct(*duplicados_preparados.columns)).alias("registro"))
        
        # ===== ESCRITURA ARCHIVO ÚNICO para duplicados =====
        print(f"📝 Escribiendo duplicados con estrategia ARCHIVO ÚNICO {CATEGORIA_ARCHIVO}...")
        
        # Detectar si los datos son demasiado grandes para coalesce(1)
        num_duplicados_estimado = cached_count_global('duplicados_final', duplicados_formateados)
        usar_concatenacion_dup = (CATEGORIA_ARCHIVO in ["GRANDE", "MUY_GRANDE"]) or (num_duplicados_estimado > 500000)
        
        if usar_concatenacion_dup:
            print(f"🔧 Duplicados usando concatenación S3 (registros: {num_duplicados_estimado:,})")
            num_partitions_salida = min(SPARK_PARTITIONS_DINAMICAS // 2, 8)
            # Escribir particiones temporales SIEMPRE en staging bucket (con permisos ListBucket)
            temp_path = f"s3://{staging_bucket}/{folder_input}/{prn_basename}/duplicados/temp_parts/"
            duplicados_formateados.repartition(num_partitions_salida).write.mode("overwrite").options(compression="none").text(path=temp_path)
            
            final_path = obtener_final_path(nombres_salida_yaml['duplicados'])
            concatenar_particiones_s3(temp_path, final_path, "duplicados", nombres_salida_yaml['duplicados'])
            print(f"📁 Duplicados: archivo único generado por concatenación S3: {nombres_salida_yaml['duplicados']}")
        else:
            print(f"⚡ Duplicados usando coalesce directo (registros: {num_duplicados_estimado:,})")
            num_partitions_salida = min(SPARK_PARTITIONS_DINAMICAS // 2, 8)
            # Escribir particiones temporales SIEMPRE en staging bucket (con permisos ListBucket)
            temp_path = f"s3://{staging_bucket}/{folder_input}/{prn_basename}/duplicados/temp_parts/"
            duplicados_formateados.repartition(num_partitions_salida).write.mode("overwrite").options(compression="none").text(path=temp_path)
            final_path = obtener_final_path(nombres_salida_yaml['duplicados'])
            concatenar_particiones_s3(temp_path, final_path, "duplicados", nombres_salida_yaml['duplicados'])
            print("📁 Duplicados: archivo único generado directamente")

        # --- PASO 8: Formatear y Escribir INCONSISTENCIAS ---
        inconsistencias_start_time = time.time()
        inconsistencias_completas = inconsistencias.alias("i").join(
            encontrados_en_fuente_oficial.alias("e"),
            (col("i.tipoid") == col("e.TIPO_ID")) & (regexp_replace(trim(col("i.numeroid")), "^0+", "") == col("e.NUMERO_ID_LIMPIO")),
            "left_outer"
        )
        inconsistencias_preparadas = inconsistencias_completas.select(
            col("i.tipoid").alias("TIP-INC"),
            col("i.numeroid").alias("CED-INC"),
            col("i.nombre").alias("NOM1-INC"),
            coalesce(col("e.NOMBRE_COMPLETO"), lit("")).alias("NOM2-INC"),
            coalesce(col("e.FECHA_EXPEDICION_DOCUMENTO"), lit("")).alias("FEC-EXP-INC"),
            coalesce(col("e.NOMBRE_MUNICIPIO_EXPEDICION"), lit("")).alias("MUN-EXP-INC"),
            coalesce(col("e.NOMBRE_DEPARTAMENTO_EXPEDICION"), lit("")).alias("DEP-EXP-INC"),
            coalesce(col("e.CODIGO_ESTADO_VIGENCIA_DOCUMENTO"), lit("")).alias("VIG-INC"),
            col("i.codigo_error").alias("CODERR-INC")
        )
        inconsistencias_formateadas = inconsistencias_preparadas.select(combine_fields_inconsistencias(struct(*inconsistencias_preparadas.columns)).alias("registro"))
        
        # ===== ESCRITURA ARCHIVO ÚNICO para inconsistencias =====
        print(f"📝 Escribiendo inconsistencias con estrategia ARCHIVO ÚNICO {CATEGORIA_ARCHIVO}...")
        
        # Detectar si los datos son demasiado grandes para coalesce(1)
        num_inconsistencias_estimado = cached_count_global('inconsistencias_final', inconsistencias_formateadas)
        usar_concatenacion_inc = (CATEGORIA_ARCHIVO in ["GRANDE", "MUY_GRANDE"]) or (num_inconsistencias_estimado > 500000)
        
        if usar_concatenacion_inc:
            print(f"🔧 Inconsistencias usando concatenación S3 (registros: {num_inconsistencias_estimado:,})")
            num_partitions_salida = min(SPARK_PARTITIONS_DINAMICAS // 2, 8)
            # Escribir particiones temporales SIEMPRE en staging bucket (con permisos ListBucket)
            temp_path = f"s3://{staging_bucket}/{folder_input}/{prn_basename}/inconsistencias/temp_parts/"
            inconsistencias_formateadas.repartition(num_partitions_salida).write.mode("overwrite").options(compression="none").text(path=temp_path)
            
            final_path = obtener_final_path(nombres_salida_yaml['inconsistencias'])
            concatenar_particiones_s3(temp_path, final_path, "inconsistencias", nombres_salida_yaml['inconsistencias'])
            print(f"📁 Inconsistencias: archivo único generado por concatenación S3: {nombres_salida_yaml['inconsistencias']}")
        else:
            print(f"⚡ Inconsistencias usando coalesce directo (registros: {num_inconsistencias_estimado:,})")
            num_partitions_salida = min(SPARK_PARTITIONS_DINAMICAS // 2, 8)
            # Escribir particiones temporales SIEMPRE en staging bucket (con permisos ListBucket)
            temp_path = f"s3://{staging_bucket}/{folder_input}/{prn_basename}/inconsistencias/temp_parts/"
            inconsistencias_formateadas.repartition(num_partitions_salida).write.mode("overwrite").options(compression="none").text(path=temp_path)
            final_path = obtener_final_path(nombres_salida_yaml['inconsistencias'])
            concatenar_particiones_s3(temp_path, final_path, "inconsistencias", nombres_salida_yaml['inconsistencias'])
            print("📁 Inconsistencias: archivo único generado directamente")

        # --- PASO 9: Escribir ACEPTADOS (.val) - SE ESCRIBE DE ÚLTIMO ---
        print(f"📝 Escribiendo aceptados (.val) DE ÚLTIMO con estrategia ARCHIVO ÚNICO {CATEGORIA_ARCHIVO}...")
        
        # SOLUCIÓN CRÍTICA: Detectar si los datos son demasiado grandes para coalesce(1)
        # Si hay más de 500K registros estimados, usar concatenación S3 directamente
        usar_concatenacion = (CATEGORIA_ARCHIVO in ["GRANDE", "MUY_GRANDE"]) or (num_registros_estimado > 500000)
        
        if usar_concatenacion:
            # Para archivos grandes: escritura paralela + concatenación S3
            print(f"🔧 Usando concatenación S3 (registros estimados: {num_registros_estimado:,})")
            writer_options = {"compression": "none"}  # Sin compresión para concatenación rápida
            num_partitions_salida = min(SPARK_PARTITIONS_DINAMICAS // 2, 8)  # Máximo 8 particiones
            
            # Escribir particiones temporales SIEMPRE en staging bucket (con permisos ListBucket)
            temp_path = f"s3://{staging_bucket}/{folder_input}/{prn_basename}/aceptados/temp_parts/"
            aceptados_formateados.repartition(num_partitions_salida).write.mode("overwrite").options(**writer_options).text(path=temp_path)
            print(f"📁 Escritura paralela completada ({num_partitions_salida} particiones)")
            
            # Concatenar particiones usando S3 con destino FINAL_FLAT_PREFIX (raíz o subcarpeta según FLAT_MODE)
            final_path = obtener_final_path(nombres_salida_yaml['aceptados'])
            concatenar_particiones_s3(temp_path, final_path, "aceptados", nombres_salida_yaml['aceptados'])
            print(f"📁 Archivo único generado por concatenación S3: {nombres_salida_yaml['aceptados']}")
            
        else:
            # Para archivos pequeños/medianos: coalesce directo (más rápido)
            print(f"⚡ Usando coalesce directo (registros estimados: {num_registros_estimado:,})")
            writer_options = {"compression": "none"}
            num_partitions_salida = min(SPARK_PARTITIONS_DINAMICAS // 2, 8)
            # Escribir particiones temporales SIEMPRE en staging bucket (con permisos ListBucket)
            temp_path = f"s3://{staging_bucket}/{folder_input}/{prn_basename}/aceptados/temp_parts/"
            aceptados_formateados.repartition(num_partitions_salida).write.mode("overwrite").options(**writer_options).text(path=temp_path)
            final_path = obtener_final_path(nombres_salida_yaml['aceptados'])
            concatenar_particiones_s3(temp_path, final_path, "aceptados", nombres_salida_yaml['aceptados'])
            print("📁 Archivo único generado directamente")

        time_classification_end = time.time()
        elapsed_classification = time_classification_end - time_classification_start
        print(f"⏱️ CHECKPOINT 33 - Clasificación y escritura completada en {elapsed_classification:.2f}s: {datetime.now().strftime('%H:%M:%S')}")
        print("\nFin de procesamiento y clasificación de registros")
    else:
        print("\nNo se ejecuta el procesamiento y clasificación de registros porque ejecutar_match es False")
        
    print(f"-------------------------------------------------------------")
    print(f"-------------------------------------------------------------")
    print(f"---------------- RESUMEN RESULTADOS--------------------------")
    print(f"Total de registros en base: {total_registros}")
    print(f"Aceptados: {cached_count_global('final_aceptados', aceptados_formateados)}")
    print(f"Duplicados: {cached_count_global('final_duplicados', duplicados_formateados)}")
    print(f"Inconsistencias: {cached_count_global('final_inconsistencias', inconsistencias_formateadas)}")
    print(f"-------------------------------------------------------------")
    print(f"---------------- MANERA EJECUCION----------------------------")
    print(f"Se ejecutó algoritmo de match")
    
    if particulas is not None and particulas > 0:
        print(f"Cantidad de partículas: {particulas}")
    else:
        print("Partículas usadas de acuerdo al tipoid")
    
    print(f"-------------------------------------------------------------")
    
else:
    print("\nNo se ejecuta el algoritmo de match de nombres porque ejecutar_match es False")
    
    # Definir los tipos de ID válidos
    tipos_id_validos = ['1', '2', '3', '4', '5', '6', '7', '8', '9']
    
    # Función para determinar el código de error
    def determinar_codigo_error(row):
        if row['tipoid'] not in tipos_id_validos:
            return 5
        elif row['NOMBRE_COMPLETO'] is None:  # No encontrado en la base oficial
            return 4 if row['tipoid'] == '1' else 3
        elif row['tipoid'] == '1' and tag_derechos == "1":
            if row['CODIGO_ESTADO_VIGENCIA_DOCUMENTO'] == "12":
                return 7
            elif row['CODIGO_ESTADO_VIGENCIA_DOCUMENTO'] > "20":
                return 6
        return 0  # Sin error, registro aceptado




    # Usar df_procesado como base y enriquecerlo con información de la fuente oficial
    df_procesado_limpio = df_procesado.withColumn("numeroid_limpio", F.regexp_replace(F.trim(F.col("numeroid")), "^0+", ""))
    
    # Crear una vista de encontrados_en_fuente_oficial que tenga una fila por cada tipoid+numeroid
    # Usamos row_number() para tomar solo la primera fila cuando hay múltiples coincidencias
    windowSpec = Window.partitionBy("TIPO_ID", "NUMERO_ID_LIMPIO").orderBy(F.lit(1))
    encontrados_deduplicados = encontrados_en_fuente_oficial.withColumn(
        "row_num", F.row_number().over(windowSpec)
    ).filter(F.col("row_num") == 1).drop("row_num")
    
    # Ahora hacemos el join con la vista deduplicada
    df_con_info_oficial = df_procesado_limpio.alias("p").join(
        encontrados_deduplicados.alias("e"),
        (F.col("p.tipoid") == F.col("e.TIPO_ID")) & 
        (F.col("p.numeroid_limpio") == F.col("e.NUMERO_ID_LIMPIO")),
        "left_outer"
    ).select(
        F.col("p.tipoid"),
        F.col("p.numeroid"),
        F.col("p.nombre"),
        F.col("p.numeroid_limpio"),
        F.col("e.NUMERO_ID"),
        F.col("e.TIPO_ID"),
        F.col("e.NOMBRE_COMPLETO"),
        F.col("e.FECHA_EXPEDICION_DOCUMENTO"),
        F.col("e.NOMBRE_MUNICIPIO_EXPEDICION"),
        F.col("e.NOMBRE_DEPARTAMENTO_EXPEDICION"),
        F.col("e.CODIGO_ESTADO_VIGENCIA_DOCUMENTO"),
        F.col("e.GENERO_SEXO"),
        F.col("e.EDAD_CALCULADA"),
        F.col("e.RANGO_EDAD"),
        F.col("e.NUMERO_ID_LIMPIO").alias("FUENTE_NUMERO_ID_LIMPIO")
    )
    
    # Verificar conteos
    conteo_con_info = cached_count_global('enriq_con_info', df_con_info_oficial)
    print(f"CONTEO DE CONTROL - Total df_con_info_oficial: {conteo_con_info}")
    print(f"CONTEO DE CONTROL - Comparación con df_procesado: {cached_count_global('post_validacion_match', df_procesado)}")
    
    # Usar este DataFrame para el procesamiento posterior
    todos_los_registros = df_con_info_oficial




    # Identificar duplicados basados en tipoid + numeroid
    window = Window.partitionBy("tipoid", "numeroid")
    registros_con_conteo = todos_los_registros.withColumn("count", F.count("*").over(window))
    
    # Separar duplicados y no duplicados
    duplicados = registros_con_conteo.filter(F.col("count") > 1)
    no_duplicados = registros_con_conteo.filter(F.col("count") == 1)
    
    # Verificar que los conteos suman correctamente
    conteo_duplicados = cached_count_global('enriq_dup_exactos', duplicados)
    conteo_no_duplicados = cached_count_global('enriq_no_dup', no_duplicados)
    print(f"CONTEO DE CONTROL - Duplicados: {conteo_duplicados}, No Duplicados: {conteo_no_duplicados}")
    print(f"CONTEO DE CONTROL - Total: {conteo_duplicados + conteo_no_duplicados}, Original: {cached_count_global('post_validacion_match', df_procesado)}")




    # Si df_duplicados_exactos no está vacío, ajustarlo para tener la misma estructura que duplicados
    if cached_count_global('res_dup_exactos', df_duplicados_exactos) > 0:
        # Crear una lista de columnas de duplicados para usarla como referencia
        columnas_duplicados = duplicados.columns
        
        # Crear una lista de selección para asegurar que las columnas coincidan
        selecciones = []
        for columna in columnas_duplicados:
            if columna in df_duplicados_exactos.columns:
                selecciones.append(F.col(columna))
            else:
                selecciones.append(F.lit(None).alias(columna))
        
        df_duplicados_exactos_ajustado = df_duplicados_exactos.select(*selecciones)
        
        # Combinar duplicados encontrados con los duplicados exactos ajustados
        todos_los_duplicados = duplicados.union(df_duplicados_exactos_ajustado)
    else:
        todos_los_duplicados = duplicados
    
    # Verificar conteo
    conteo_todos_duplicados = cached_count_global('enriq_todos_dup', todos_los_duplicados)
    print(f"CONTEO DE CONTROL - Todos los duplicados: {conteo_todos_duplicados}")

    # Aplicar la función para determinar el código de error en los registros no duplicados
    no_duplicados_con_error = no_duplicados.withColumn(
        "codigo_error", 
        F.udf(determinar_codigo_error, IntegerType())(
            F.struct([F.col(c) for c in no_duplicados.columns if c != "count"])
        )
    )

    # Separar registros aceptados e inconsistencias
    aceptados = no_duplicados_con_error.filter(F.col("codigo_error") == 0)
    inconsistencias = no_duplicados_con_error.filter(F.col("codigo_error") != 0)

    # Preparar los campos para el formato de longitud fija para aceptados
    aceptados_preparados = aceptados.select(
        F.col("tipoid").alias("TIPO_ID"),
        F.col("numeroid").alias("NUMERO_ID"),
        F.coalesce(F.col("NOMBRE_COMPLETO"), F.col("nombre")).alias("NOMBRE_COMPLETO"),
        F.coalesce(F.col("FECHA_EXPEDICION_DOCUMENTO"), F.lit("")).alias("FECHA_EXPEDICION_DOCUMENTO"),
        F.coalesce(F.col("NOMBRE_MUNICIPIO_EXPEDICION"), F.lit("")).alias("NOMBRE_MUNICIPIO_EXPEDICION"),
        F.coalesce(F.col("NOMBRE_DEPARTAMENTO_EXPEDICION"), F.lit("")).alias("NOMBRE_DEPARTAMENTO_EXPEDICION"),
        F.coalesce(F.col("CODIGO_ESTADO_VIGENCIA_DOCUMENTO"), F.lit("")).alias("CODIGO_ESTADO_VIGENCIA_DOCUMENTO"),
        F.coalesce(F.col("GENERO_SEXO"), F.lit("")).alias("GENERO_SEXO"),
        F.coalesce(F.col("EDAD_CALCULADA"), F.lit("")).alias("EDAD_CALCULADA"),
        F.coalesce(F.col("RANGO_EDAD"), F.lit("")).alias("RANGO_EDAD")
    )

    # Aplicar el formato de longitud fija a los registros aceptados
    aceptados_formateados = aceptados_preparados.select(
        combine_fields_validos(F.struct(*aceptados_preparados.columns)).alias("registro_formateado")
    )

    # Preparar los campos para el formato de longitud fija para inconsistencias
    inconsistencias_preparadas = inconsistencias.select(
        F.col("tipoid").alias("TIP-INC"),
        F.col("numeroid").alias("CED-INC"),
        coalesce(F.col("nombre"), F.lit("")).alias("NOM1-INC"),
        coalesce(F.col("NOMBRE_COMPLETO"), F.lit("")).alias("NOM2-INC"),
        coalesce(F.col("FECHA_EXPEDICION_DOCUMENTO"), F.lit("")).alias("FEC-EXP-INC"),
        coalesce(F.col("NOMBRE_MUNICIPIO_EXPEDICION"), F.lit("")).alias("MUN-EXP-INC"),
        coalesce(F.col("NOMBRE_DEPARTAMENTO_EXPEDICION"), F.lit("")).alias("DEP-EXP-INC"),
        coalesce(F.col("CODIGO_ESTADO_VIGENCIA_DOCUMENTO"), F.lit("")).alias("VIG-INC"),
        col("codigo_error").alias("CODERR-INC")
    )

    # Aplicar el formato de longitud fija a las inconsistencias
    inconsistencias_formateadas = inconsistencias_preparadas.select(
        combine_fields_inconsistencias(F.struct(*inconsistencias_preparadas.columns)).alias("registro_formateado")
    )

    # Preparar los campos para el formato de longitud fija para duplicados
    duplicados_preparados = todos_los_duplicados.select(
        F.col("tipoid").alias("TIPO_ID"),
        F.col("numeroid").alias("NUMERO_ID"),
        F.coalesce(F.col("NOMBRE_COMPLETO"), F.col("nombre")).alias("NOMBRE_COMPLETO"),
        F.coalesce(F.col("FECHA_EXPEDICION_DOCUMENTO"), F.lit("")).alias("FECHA_EXPEDICION_DOCUMENTO"),
        F.coalesce(F.col("NOMBRE_MUNICIPIO_EXPEDICION"), F.lit("")).alias("NOMBRE_MUNICIPIO_EXPEDICION"),
        F.coalesce(F.col("NOMBRE_DEPARTAMENTO_EXPEDICION"), F.lit("")).alias("NOMBRE_DEPARTAMENTO_EXPEDICION"),
        F.coalesce(F.col("CODIGO_ESTADO_VIGENCIA_DOCUMENTO"), F.lit("")).alias("CODIGO_ESTADO_VIGENCIA_DOCUMENTO"),
        F.coalesce(F.col("GENERO_SEXO"), F.lit("")).alias("GENERO_SEXO"),
        F.coalesce(F.col("EDAD_CALCULADA"), F.lit("")).alias("EDAD_CALCULADA"),
        F.coalesce(F.col("RANGO_EDAD"), F.lit("")).alias("RANGO_EDAD")
    )

    # Aplicar el formato de longitud fija a los duplicados
    duplicados_formateados = duplicados_preparados.select(
        combine_fields_validos(F.struct(*duplicados_preparados.columns)).alias("registro_formateado")
    )

    # ===== ESCRITURA ARCHIVO ÚNICO (ejecutar_match = False) =====
    print(f"📝 Escribiendo archivos únicos para ejecución sin match ({CATEGORIA_ARCHIVO})...")
    
    # Función auxiliar para escribir archivo único (usa FLAT_MODE para determinar destino)
    def escribir_archivo_unico_sin_match(df_formateado, tipo, nombre_archivo_yaml):
        num_partitions = min(SPARK_PARTITIONS_DINAMICAS // 2, 8)
        # Escribir particiones temporales SIEMPRE en staging bucket (con permisos ListBucket)
        temp_path = f"s3://{staging_bucket}/{folder_input}/{prn_basename}/{tipo.lower()}/temp_parts/"
        df_formateado.repartition(num_partitions).write.mode("overwrite").text(path=temp_path)
        final_path = obtener_final_path(nombre_archivo_yaml)
        concatenar_particiones_s3(temp_path, final_path, tipo.lower(), nombre_archivo_yaml)
        print(f"📁 {tipo}: archivo único generado por concatenación S3: {nombre_archivo_yaml}")
    
    # Escribir cada tipo de archivo (el .val se escribe DE ÚLTIMO)
    escribir_archivo_unico_sin_match(
        duplicados_formateados.select("registro_formateado"), 
        "Duplicados",
        nombres_salida_yaml['duplicados']
    )
    
    escribir_archivo_unico_sin_match(
        inconsistencias_formateadas.select("registro_formateado"), 
        "Inconsistencias",
        nombres_salida_yaml['inconsistencias']
    )
    
    # ACEPTADOS (.val) SE ESCRIBE DE ÚLTIMO
    escribir_archivo_unico_sin_match(
        aceptados_formateados.select("registro_formateado"), 
        "Aceptados",
        nombres_salida_yaml['aceptados']
    )
    
    # Guardar los DataFrames en archivos parquet
    #aceptados_preparados.write.mode("overwrite").parquet(path=f"{outputbucket}/aceptados/parquet/")
    #duplicados_preparados.write.mode("overwrite").parquet(path=f"{outputbucket}/duplicados/parquet/")
    #inconsistencias_preparadas.write.mode("overwrite").parquet(path=f"{outputbucket}/inconsistencias/parquet/")
    
    
    # (Concatenación y limpieza de parts eliminadas; se mantienen múltiples archivos part-*)

    # Conteos finales para verificación
    conteo_aceptados_final = cached_count_global('final_aceptados', aceptados_formateados)
    conteo_duplicados_final = cached_count_global('final_duplicados', duplicados_formateados)
    conteo_inconsistencias_final = cached_count_global('final_inconsistencias', inconsistencias_formateadas)
    total_procesados = conteo_aceptados_final + conteo_duplicados_final + conteo_inconsistencias_final
    
    print("\nFin de procesamiento y clasificación de registros")
    print(f"-------------------------------------------------------------")
    print(f"-------------------------------------------------------------")
    print(f"---------------- RESUMEN RESULTADOS--------------------------")
    print(f"Total de registros en base original: {total_registros}")
    print(f"Total de registros procesados: {total_procesados}")
    print(f"Aceptados: {conteo_aceptados_final}")
    print(f"Duplicados: {conteo_duplicados_final}")
    print(f"Inconsistencias: {conteo_inconsistencias_final}")
    print(f"Diferencia: {total_registros - total_procesados}")
    print(f"-------------------------------------------------------------")
    print(f"---------------- MANERA EJECUCION----------------------------")
    print(f"No se ejecutó algoritmo de match")
    print(f"-------------------------------------------------------------")
# Finalizar el job
time_cleanup_start = time.time()
print(f"⏱️ CHECKPOINT 34 - Iniciando limpieza final: {datetime.now().strftime('%H:%M:%S')}")

# Nota: Se eliminó la llamada a eliminar_objeto_s3() para conservar el archivo PRN de entrada.

time_cleanup_end = time.time()
elapsed_cleanup = time_cleanup_end - time_cleanup_start
print(f"⏱️ CHECKPOINT 35 - Limpieza completada en {elapsed_cleanup:.2f}s: {datetime.now().strftime('%H:%M:%S')}")

# ========== REPORTE FINAL DE OPTIMIZACIONES APLICADAS ==========
# ========== RESUMEN FINAL (PRODUCCIÓN) ==========
end_time = time.time()
total_execution_time = end_time - start_time
end_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Log final limpio para producción
print("\n" + "="*70)
print("🏁 PROCESO PESVNO COMPLETADO EXITOSAMENTE")
print("="*70)
print(f"Inicio:     {start_timestamp}")
print(f"Fin:        {end_timestamp}")
print(f"Duración:   {total_execution_time:.2f}s ({total_execution_time/60:.2f} min)")
print(f"Categoría:  {CATEGORIA_ARCHIVO} ({ESTIMACION_REGISTROS:,} registros)")
print(f"Particiones: {SPARK_PARTITIONS_DINAMICAS}")
print("="*70)

job.commit()