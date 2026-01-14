import base64
import json

def lambda_handler(event, context):
    output = []

    for record in event['records']:
        payload = base64.b64decode(record['data']).decode('utf-8')
        data = json.loads(payload)

        if 'Puntos' in data:
            puntos_normalizados = data['Puntos'].replace(',', '')
            data['Puntos'] = float(puntos_normalizados)

        new_data = json.dumps(data) + '\n'
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(new_data.encode('utf-8')).decode('utf-8')
        }
        output.append(output_record)

    return {'records': output}