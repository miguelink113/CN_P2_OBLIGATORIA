import boto3
import json
import sys

kinesis_client = boto3.client('kinesis', region_name='us-east-1') 

STREAM_NAME = 'beach-voley-national-ranking'

def enviar_ranking_completo(ruta_archivo_json):
    try:
        with open(ruta_archivo_json, 'r', encoding='utf-8') as f:
            jugadores = json.load(f)
        
        print(f"Iniciando carga de {len(jugadores)} jugadores...")

        for jugador in jugadores:
            response = kinesis_client.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(jugador),
                PartitionKey=str(jugador['IdPersona'])
            )
            print(f"Enviado: {jugador['ApellidosNombre']} - Sequence: {response['SequenceNumber']}")

    except FileNotFoundError:
        print("Error: No se encontró el archivo JSON con los datos del ranking.")
    except Exception as e:
        print(f"Error inesperado: {e}")

if __name__ == "__main__":
    enviar_ranking_completo('beach_voley_national_ranking.json')