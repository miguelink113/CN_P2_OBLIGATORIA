# 1. VARIABLES DE ENTORNO
$env:AWS_REGION = "us-east-1"
$env:ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text)
$env:BUCKET_NAME = "datalake-voley-ranking-$($env:ACCOUNT_ID)"

Write-Host "`n=== CONFIGURACIÓN DE ESTRUCTURA S3: VOLEY PLAYA ===" -ForegroundColor Cyan

# 2. CREACIÓN DEL BUCKET
Write-Host "[1/2] Creando Bucket S3: $($env:BUCKET_NAME)..." -ForegroundColor Yellow
aws s3 mb "s3://$env:BUCKET_NAME"

# 3. CREACIÓN DE LA ESTRUCTURA OBLIGATORIA
# Según el apartado 2.4.1 de la práctica, se requieren: raw, processed y config
Write-Host "[2/2] Creando carpetas internas (raw, processed, config)..." -ForegroundColor Yellow

# En S3, las carpetas se crean mediante objetos vacíos con un "/" al final
aws s3api put-object --bucket $env:BUCKET_NAME --key "raw/"
aws s3api put-object --bucket $env:BUCKET_NAME --key "processed/"
aws s3api put-object --bucket $env:BUCKET_NAME --key "config/"
aws s3api put-object --bucket $env:BUCKET_NAME --key "scripts/"
aws s3api put-object --bucket $env:BUCKET_NAME --key "errors/"

Write-Host "`n=== ESTRUCTURA CREADA CORRECTAMENTE ===" -ForegroundColor Green
Write-Host "Bucket: s3://$env:BUCKET_NAME"