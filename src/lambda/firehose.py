import json
import base64
import datetime

def lambda_handler(event, context):
    output = []
    
    for record in event['records']:
        # 1. Decodificar el dato que viene de Kinesis Data Stream
        payload = base64.b64decode(record['data']).decode('utf-8')
        data_json = json.loads(payload)
        
        # 2. Generar la fecha de procesamiento para la partición dinámica de Firehose
        # Usamos UTC para evitar problemas de zona horaria en el Data Lake
        processing_time = datetime.datetime.now(datetime.timezone.utc)
        partition_date = processing_time.strftime('%Y-%m-%d')
        
        # 3. Preparar el registro de salida
        # Es CRÍTICO añadir el '\n' al final del JSON para que AWS Glue 
        # pueda procesar los archivos como JSON Lines (ndjson)
        processed_data = json.dumps(data_json) + '\n'
        
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(processed_data.encode('utf-8')).decode('utf-8'),
            'metadata': {
                'partitionKeys': {
                    'processing_date': partition_date
                }
            }
        }
        output.append(output_record)
    
    # 4. Devolver los registros procesados a Firehose
    return {'records': output}