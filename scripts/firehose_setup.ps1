$env:AWS_REGION = "us-east-1"
$env:ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text)
$env:BUCKET_NAME = "datalake-voley-ranking-$($env:ACCOUNT_ID)"
$env:ROLE_ARN = (aws iam get-role --role-name LabRole --query 'Role.Arn' --output text).Trim()

Write-Host "`nCreando Firehose con Particionamiento Dinámico..." -ForegroundColor Yellow

$DELIVERY_STREAM_NAME = "voley-firehose"
$KINESIS_STREAM_NAME = "beach-voley-national-ranking"

aws firehose delete-delivery-stream --delivery-stream-name $DELIVERY_STREAM_NAME 2>$null
Write-Host "Esperando a que el stream anterior se elimine..."
Start-Sleep -Seconds 30

$FIREHOSE_CONFIG_TEMPLATE = @'
{
  "BucketARN": "arn:aws:s3:::REPLACE_BUCKET",
  "RoleARN": "REPLACE_ROLE",
  "Prefix": "raw/processing_date=!{partitionKeyFromQuery:processing_date}/",
  "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/",
  "BufferingHints": {
    "SizeInMBs": 64,
    "IntervalInSeconds": 60
  },
  "ProcessingConfiguration": {
    "Enabled": true,
    "Processors": [
      {
        "Type": "MetadataExtraction",
        "Parameters": [
          {
            "ParameterName": "MetadataExtractionQuery",
            "ParameterValue": "{processing_date: .processing_date}"
          },
          {
            "ParameterName": "JsonParsingEngine",
            "ParameterValue": "JQ-1.6"
          }
        ]
      },
      {
        "Type": "Lambda",
        "Parameters": [
          {
            "ParameterName": "LambdaArn",
            "ParameterValue": "REPLACE_LAMBDA"
          },
          {
            "ParameterName": "NumberOfRetries",
            "ParameterValue": "3"
          }
        ]
      }
    ]
  },
  "DynamicPartitioningConfiguration": {
    "Enabled": true,
    "RetryOptions": {
      "DurationInSeconds": 300
    }
  }
}
'@

$FIREHOSE_CONFIG_JSON = $FIREHOSE_CONFIG_TEMPLATE `
  -replace "REPLACE_BUCKET", $env:BUCKET_NAME `
  -replace "REPLACE_ROLE", $env:ROLE_ARN `
  -replace "REPLACE_LAMBDA", $LAMBDA_ARN

$CONFIG_FILE = "$PSScriptRoot/firehose_config.json"
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($CONFIG_FILE, $FIREHOSE_CONFIG_JSON, $utf8NoBom)

aws firehose create-delivery-stream `
  --delivery-stream-name $DELIVERY_STREAM_NAME `
  --delivery-stream-type KinesisStreamAsSource `
  --kinesis-stream-source-configuration "KinesisStreamARN=arn:aws:kinesis:$($env:AWS_REGION):$($env:ACCOUNT_ID):stream/$KINESIS_STREAM_NAME,RoleARN=$env:ROLE_ARN" `
  --extended-s3-destination-configuration file://$CONFIG_FILE

if ($?) {
  Write-Host "=== Firehose creado y configurado correctamente ===" -ForegroundColor Green
} else {
  Write-Host "=== Error en la creación de Firehose ===" -ForegroundColor Red
}

Remove-Item $CONFIG_FILE -ErrorAction SilentlyContinue
