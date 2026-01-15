$env:AWS_REGION = "us-east-1"
$env:ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text)

$STREAM_NAME = "beach-voley-national-ranking"

Write-Host "`n=== CREACIÓN DEL STREAM KINESIS ===" -ForegroundColor Cyan
Write-Host "Stream: $STREAM_NAME" -ForegroundColor Yellow

aws kinesis create-stream --stream-name $STREAM_NAME --shard-count 1 2>$null

Start-Sleep -Seconds 5
Write-Host "`n=== STREAM CREADO CORRECTAMENTE ===" -ForegroundColor Green
