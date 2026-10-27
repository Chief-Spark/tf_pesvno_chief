"""
Lambda para descargar logs de CloudWatch de un job de Glue fallido y subirlos a S3.

Esta Lambda es invocada por el Step Function cuando un job de Glue falla,
para capturar los logs completos y almacenarlos en S3 para adjuntarlos
al email de notificación de error.
"""

import json
import boto3
import logging
from datetime import datetime
from typing import Dict, Any

# Configurar logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Clientes de AWS
logs_client = boto3.client('logs')
s3_client = boto3.client('s3')


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Descarga logs de CloudWatch y los sube a S3.
    
    Parámetros esperados en event:
    - job_run_id: ID de ejecución del job de Glue
    - job_name: Nombre del job de Glue
    - error_bucket: Bucket S3 donde guardar los logs (opcional)
    - log_group_name: Nombre del log group (opcional, se construye automáticamente)
    
    Retorna:
    - bucket: Nombre del bucket donde se guardó el log
    - key: Key del objeto en S3
    - size_bytes: Tamaño del archivo de log
    - timestamp: Timestamp de cuando se guardó
    """
    try:
        logger.info(f"Evento recibido: {json.dumps(event)}")
        
        # Extraer parámetros del evento
        job_run_id = event.get('job_run_id')
        job_name = event.get('job_name', 'PESVNO')
        error_bucket = event.get('error_bucket', f"{event.get('stage', 'dev')}-{event.get('account_id')}-fwdynbtch-error-files")
        
        if not job_run_id:
            raise ValueError("El parámetro 'job_run_id' es requerido")
        
        # Log stream es el job_run_id
        log_stream_name = job_run_id
        
        # IMPORTANTE: Primero buscar en el log group de ERROR
        # Los logs de jobs fallidos están en /aws-glue/jobs/error
        log_groups_to_try = [
            '/aws-glue/jobs/error',    # Logs de ERROR (prioridad)
            '/aws-glue/jobs/output'    # Logs de OUTPUT (fallback)
        ]
        
        log_content = None
        used_log_group = None
        
        # Intentar descargar logs de cada log group
        for log_group_name in log_groups_to_try:
            logger.info(f"Intentando descargar logs de: {log_group_name}/{log_stream_name}")
            content = download_cloudwatch_logs(log_group_name, log_stream_name)
            
            # Si encontramos logs (contenido real, no mensaje de error), usamos ese
            if content and not content.startswith("Log stream no encontrado"):
                log_content = content
                used_log_group = log_group_name
                logger.info(f"Logs encontrados en: {log_group_name}")
                break
        
        # Si no encontramos logs en ningún log group
        if not log_content:
            logger.warning(f"No se encontraron logs para el job run {job_run_id} en ningún log group")
            log_content = f"No se encontraron logs para el job run {job_run_id}.\nSe buscó en:\n"
            log_content += "\n".join([f"  - {lg}" for lg in log_groups_to_try])
            log_content += f"\n\nLog stream buscado: {log_stream_name}\n"
            used_log_group = log_groups_to_try[0]
        
        # Generar nombre del archivo en S3 con timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        s3_key = f"logs/{job_name.lower()}_error_{timestamp}_{job_run_id}.log"
        
        logger.info(f"Subiendo logs a S3: s3://{error_bucket}/{s3_key}")
        
        # Subir logs a S3
        s3_client.put_object(
            Bucket=error_bucket,
            Key=s3_key,
            Body=log_content.encode('utf-8'),
            ContentType='text/plain'
        )
        
        # Obtener tamaño del archivo
        response = s3_client.head_object(Bucket=error_bucket, Key=s3_key)
        file_size = response['ContentLength']
        
        result = {
            'bucket': error_bucket,
            'key': s3_key,
            'size_bytes': file_size,
            'timestamp': timestamp,
            'job_run_id': job_run_id,
            'log_group': used_log_group,
            'log_stream': log_stream_name
        }
        
        logger.info(f"Logs guardados exitosamente: {json.dumps(result)}")
        return result
        
    except Exception as e:
        logger.error(f"Error al procesar logs: {str(e)}", exc_info=True)
        # Retornar estructura vacía si falla, para que el proceso continúe
        return {
            'bucket': '',
            'key': '',
            'size_bytes': 0,
            'timestamp': datetime.now().strftime('%Y%m%d_%H%M%S'),
            'error': str(e)
        }


def download_cloudwatch_logs(log_group_name: str, log_stream_name: str) -> str:
    """
    Descarga todos los eventos de log de un log stream específico.
    
    Args:
        log_group_name: Nombre del log group
        log_stream_name: Nombre del log stream
        
    Returns:
        String con todos los logs concatenados
    """
    log_events = []
    next_token = None
    
    try:
        while True:
            # Preparar parámetros para get_log_events
            params = {
                'logGroupName': log_group_name,
                'logStreamName': log_stream_name,
                'startFromHead': True  # Leer desde el inicio
            }
            
            if next_token:
                params['nextToken'] = next_token
            
            # Obtener eventos de log
            response = logs_client.get_log_events(**params)
            
            events = response.get('events', [])
            if not events:
                break
            
            log_events.extend(events)
            
            # Verificar si hay más eventos
            next_forward_token = response.get('nextForwardToken')
            if next_forward_token == next_token:
                # No hay más eventos
                break
            
            next_token = next_forward_token
            
        # Formatear los eventos de log
        log_lines = []
        for event in log_events:
            timestamp = datetime.fromtimestamp(event['timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
            message = event['message']
            log_lines.append(f"[{timestamp}] {message}")
        
        return '\n'.join(log_lines)
        
    except logs_client.exceptions.ResourceNotFoundException:
        logger.warning(f"Log stream no encontrado: {log_group_name}/{log_stream_name}")
        return f"Log stream no encontrado: {log_group_name}/{log_stream_name}\n"
    except Exception as e:
        logger.error(f"Error al descargar logs: {str(e)}", exc_info=True)
        return f"Error al descargar logs: {str(e)}\n"
