$env:AWS_REGION = "us-east-1"
$env:ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text)
$env:ROLE_ARN = (aws iam get-role --role-name LabRole --query 'Role.Arn' --output text).Trim()

$LAMBDA_NAME = "voley_firehose_processor"
$LAMBDA_SRC_PATH = "$PSScriptRoot/../src/lambda/firehose.py"
$ZIP_PATH = "$PSScriptRoot/../src/lambda/firehose.zip"

Write-Host "`n=== DESPLEGANDO LAMBDA: $LAMBDA_NAME ===" -ForegroundColor Cyan

Write-Host "Generando ZIP de Lambda..."
if (Test-Path $ZIP_PATH) { Remove-Item $ZIP_PATH -Force }
Compress-Archive -Path $LAMBDA_SRC_PATH -DestinationPath $ZIP_PATH -Force
Write-Host "ZIP creado en: $ZIP_PATH" -ForegroundColor Green

Write-Host "Eliminando Lambda existente (si aplica)..."
aws lambda delete-function --function-name $LAMBDA_NAME 2>&1 | Out-Null

Write-Host "Creando Lambda..."
aws lambda create-function `
    --function-name $LAMBDA_NAME `
    --runtime python3.9 `
    --role $env:ROLE_ARN `
    --handler firehose.lambda_handler `
    --zip-file "fileb://$ZIP_PATH" `
    --timeout 60 `
    --memory-size 128 | Out-Null

$LAMBDA_ARN = (aws lambda get-function --function-name $LAMBDA_NAME --query 'Configuration.FunctionArn' --output text).Trim()
Write-Host "Lambda ARN: $LAMBDA_ARN" -ForegroundColor Green

Write-Host "`n=== LAMBDA DESPLEGADA CORRECTAMENTE ===" -ForegroundColor Cyan
