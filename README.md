# Práctica 2 – Computación en la Nube

Pipeline de ingesta y procesamiento de datos para ranking nacional de vóley playa usando:

- Amazon Kinesis Data Streams
- Kinesis Firehose
- AWS Lambda
- Amazon S3
- AWS Glue

## Estructura
- src/producer → Generador y envío de datos
- src/lambda → Transformación Firehose
- scripts → Infraestructura y ETL Glue
