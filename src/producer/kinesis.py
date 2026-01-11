import boto3
import json
import time
from loguru import logger

# =============================================================================
# CONFIGURATION - NATIONAL BEACH VOLLEYBALL RANKING PRODUCER
# =============================================================================
STREAM_NAME = 'player-points-stream'  # Matches the PowerShell script name
REGION = 'us-east-1'
INPUT_FILE = 'ranking_nacional_voleyplaya.json'

# Initialize Kinesis Client
kinesis = boto3.client('kinesis', region_name=REGION)

def load_local_data(file_path):
    """Loads ranking data from local JSON file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Error: File {file_path} not found.")
        return []

def run_producer():
    data = load_local_data(INPUT_FILE)
    if not data:
        return

    records_sent = 0
    logger.info(f"Starting data transmission to stream: {STREAM_NAME}...")
    logger.info(f"Total records to send: {len(data)}")

    for player in data:
        try:
            # Prepare payload - Mapping original JSON to English fields
            # We use replace() just in case the format uses commas for decimals
            points = float(str(player['PuntosSinFormato']).replace(',', '.'))
            
            payload = {
                'id_player': player['IdPersona'],
                'full_name': player['ApellidosNombre'],
                'ranking_points': float(player['PuntosSinFormato']),
                'team': player['EquipoVoleyPlaya'],
                'ingestion_timestamp': time.strftime("%Y-%m-%dT%H:%M:%SZ")
            }
                    
            # Send to Kinesis Data Stream
            response = kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(payload),
                PartitionKey=str(player['IdPersona'])
            )
            
            records_sent += 1
            logger.info(f"[{records_sent}] Sent: {player['ApellidosNombre']} | Points: {points}")
            
            # Small delay to simulate real-time streaming
            time.sleep(1) 
            
        except KeyError as e:
            logger.error(f"Missing key in JSON record: {e}")
        except Exception as e:
            logger.error(f"Failed to send record: {e}")

    logger.success(f"Transmission complete. Total records sent: {records_sent}")

if __name__ == '__main__':
    try:
        run_producer()
    except KeyboardInterrupt:
        logger.warning("Proceso interrumpido por el usuario. Registros enviados hasta ahora.")
