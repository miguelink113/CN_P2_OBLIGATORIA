import boto3
import json
import time
import os
from loguru import logger

# =============================================================================
# CONFIGURATION - NATIONAL BEACH VOLLEYBALL RANKING PRODUCER
# =============================================================================
STREAM_NAME = os.environ.get('STREAM_NAME', 'player-points-stream')  # Matches the PowerShell script name
REGION = os.environ.get('AWS_REGION', 'us-east-1')
# Default input file can be overridden with environment variable `INPUT_FILE`.
INPUT_FILE = os.environ.get(
    'INPUT_FILE',
    'src/data/ranking_nacional_voleyplaya_con_equipos_random.json'
)

def load_local_data(file_path):
    """Loads ranking data from local JSON file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        # Try some common fallback locations inside src/data
        fallback_paths = [
            'src/data/ranking_nacional_voleyplaya_rfevb.json',
            'src/data/ranking_nacional_voleyplaya.json',
            'ranking_nacional_voleyplaya.json'
        ]
        for p in fallback_paths:
            if os.path.exists(p):
                try:
                    with open(p, 'r', encoding='utf-8') as f:
                        return json.load(f)
                except Exception:
                    continue

        logger.error(f"Error: File {file_path} not found and no fallback available.")
        return []

def run_producer():
    data = load_local_data(INPUT_FILE)
    if not data:
        return

    # Initialize Kinesis client here to allow configuration via env vars
    kinesis = boto3.client('kinesis', region_name=REGION)

    records_sent = 0
    logger.info(f"Starting data transmission to stream: {STREAM_NAME}...")
    logger.info(f"Total records to send: {len(data)}")

    for player in data:
        try:
            payload = {
                'id_player': player.get('IdPersona') or player.get('id_player'),
                'full_name': player.get('ApellidosNombre') or player.get('full_name'),
                'ranking_points': float(player.get('Puntos') or player.get('ranking_points') or 0),
                'team': player.get('EquipoVoleyPlaya') or player.get('team'),
                'ingestion_timestamp': time.strftime("%Y-%m-%dT%H:%M:%SZ")
            }

            # Send to Kinesis Data Stream
            kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(payload),
                PartitionKey=str(payload['id_player'])
            )

            records_sent += 1
            logger.info(f"[{records_sent}] Sent: {payload['full_name']} | Points: {payload['ranking_points']}")

            # Small delay to simulate real-time streaming
            time.sleep(0.1)

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
