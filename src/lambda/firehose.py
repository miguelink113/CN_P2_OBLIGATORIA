import base64
import json
from datetime import datetime

def lambda_handler(event, context):
    output = []

    processing_date = datetime.now().strftime('%Y-%m-%d')

    for record in event['records']:
        try:
            payload = base64.b64decode(record['data']).decode('utf-8')
            data = json.loads(payload)

            if 'Puntos' in data:
                puntos_normalizados = data['Puntos'].replace(',', '')
                data['Puntos'] = float(puntos_normalizados)

            data['processing_date'] = processing_date

            new_data = json.dumps(data) + '\n'
            output_record = {
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': base64.b64encode(new_data.encode('utf-8')).decode('utf-8')
            }
            output.append(output_record)

        except Exception as e:
            output_record = {
                'recordId': record.get('recordId', 'unknown'),
                'result': 'ProcessingFailed',
                'data': record.get('data', '')
            }
            output.append(output_record)
            print(f"Error procesando registro: {e}")

    return {'records': output}
