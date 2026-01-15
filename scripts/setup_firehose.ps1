$env:AWS_REGION = "us-east-1"
$env:ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text)
$env:BUCKET_NAME = "datalake-voley-ranking-$($env:ACCOUNT_ID)"
$env:ROLE_ARN = (aws iam get-role --role-name LabRole --query 'Role.Arn' --output text).Trim()

$FIREHOSE_NAME = "voley-firehose"
$KINESIS_STREAM_NAME = "beach-voley-national-ranking"

$LAMBDA_NAME = "voley_firehose_processor"
$LAMBDA_ARN = (aws lambda get-function --function-name $LAMBDA_NAME --query 'Configuration.FunctionArn' --output text).Trim()
if (-not $LAMBDA_ARN) {
    Write-Host "ERROR: La Lambda $LAMBDA_NAME no existe. Ejecuta primero deploy_lambda.ps1" -ForegroundColor Red
    exit 1
}
Write-Host "Lambda ARN encontrado: $LAMBDA_ARN" -ForegroundColor Green

Write-Host "`nEliminando Firehose existente (si aplica)..."
aws firehose delete-delivery-stream --delivery-stream-name $FIREHOSE_NAME 2>&1 | Out-Null
Start-Sleep -Seconds 15

$FIREHOSE_CONFIG_JSON = @"
{
    "BucketARN": "arn:aws:s3:::$($env:BUCKET_NAME)",
    "RoleARN": "$env:ROLE_ARN",
    "Prefix": "raw/processing_date=!{partitionKeyFromLambda:processing_date}/",
    "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/",
    "BufferingHints": { "SizeInMBs": 64, "IntervalInSeconds": 60 },
    "ProcessingConfiguration": {
        "Enabled": true,
        "Processors": [
            {
                "Type": "Lambda",
                "Parameters": [
                    { "ParameterName": "LambdaArn", "ParameterValue": "$LAMBDA_ARN" },
                    { "ParameterName": "BufferSizeInMBs", "ParameterValue": "1" },
                    { "ParameterName": "BufferIntervalInSeconds", "ParameterValue": "60" }
                ]
            }
        ]
    },
    "DynamicPartitioningConfiguration": {
        "Enabled": true,
        "RetryOptions": { "DurationInSeconds": 300 }
    }
}
"@

$CONFIG_FILE = "$PSScriptRoot/firehose_config.json"
$FIREHOSE_CONFIG_JSON | Out-File -FilePath $CONFIG_FILE -Encoding ASCII

Write-Host "`nCreando Firehose delivery stream: $FIREHOSE_NAME ..." -ForegroundColor Yellow
aws firehose create-delivery-stream `
    --delivery-stream-name $FIREHOSE_NAME `
    --delivery-stream-type KinesisStreamAsSource `
    --kinesis-stream-source-configuration "KinesisStreamARN=arn:aws:kinesis:$($env:AWS_REGION):$($env:ACCOUNT_ID):stream/$KINESIS_STREAM_NAME,RoleARN=$env:ROLE_ARN" `
    --extended-s3-destination-configuration "file://$CONFIG_FILE"

if ($?) { Write-Host "`nFirehose creado correctamente." -ForegroundColor Green }
else { Write-Host "`nError creando Firehose." -ForegroundColor Red }

Remove-Item $CONFIG_FILE -ErrorAction SilentlyContinue
